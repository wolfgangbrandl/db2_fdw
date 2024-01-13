/*-------------------------------------------------------------------------
 *
 * db2_fdw.c
 * 		PostgreSQL-related functions for DB2 foreign data wrapper.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#if PG_VERSION_NUM < 100000
#include "libpq/md5.h"
#else
#include "common/md5.h"
#endif /* PG_VERSION_NUM */
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "optimizer/cost.h"

#if PG_VERSION_NUM >= 140000
#include "optimizer/appendinfo.h"
#endif  /* PG_VERSION_NUM */
#include "optimizer/pathnode.h"
#if PG_VERSION_NUM >= 130000
#include "optimizer/paths.h"
#endif  /* PG_VERSION_NUM */

#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "port.h"

#include "storage/ipc.h"
#include "storage/lock.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#include "optimizer/var.h"
#include "utils/tqual.h"
#else
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#include "access/heapam.h"
#endif


#include <string.h>
#include <stdlib.h>

#include "db2_fdw.h"
#if PG_VERSION_NUM >= 150000
#define STRVAL(arg) ((String *)(arg))->sval
#else
#define STRVAL(arg) ((Value*)(arg))->val.str
#endif
#if PG_VERSION_NUM >= 150000
#include <datatype/timestamp.h>
#endif
/* defined in backend/commands/analyze.c */
#ifndef WIDTH_THRESHOLD
#define WIDTH_THRESHOLD 1024
#endif /* WIDTH_THRESHOLD */

#undef OLD_FDW_API
#define WRITE_API

#define IMPORT_API

/* array_create_iterator has a new signature from 9.5 on */
#define array_create_iterator(arr, slice_ndim) array_create_iterator(arr, slice_ndim, NULL)

#define JOIN_API

/* the useful macro IS_SIMPLE_REL is defined in v10, backport */
#ifndef IS_SIMPLE_REL
#define IS_SIMPLE_REL(rel) \
	((rel)->reloptkind == RELOPT_BASEREL || \
	(rel)->reloptkind == RELOPT_OTHER_MEMBER_REL)
#endif

/* GetConfigOptionByName has a new signature from 9.6 on */
#define GetConfigOptionByName(name, varname) GetConfigOptionByName(name, varname, false)

#if PG_VERSION_NUM < 110000
/* backport macro from V11 */
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])
#endif /* PG_VERSION_NUM */

/* list API has changed in v13 */
#if PG_VERSION_NUM < 130000
#define list_next(l, e) lnext((e))
#define do_each_cell(cell, list, element) for_each_cell(cell, (element))
#else
#define list_next(l, e) lnext((l), (e))
#define do_each_cell(cell, list, element) for_each_cell(cell, (list), (element))
#endif  /* PG_VERSION_NUM */


/* older versions don't have JSONOID */
#ifndef JSONOID
#define JSONOID InvalidOid
#endif
/* "table_open" was "heap_open" before v12 */
#if PG_VERSION_NUM < 120000
#define table_open(x, y) heap_open(x, y)
#define table_close(x, y) heap_close(x, y)
#endif  /* PG_VERSION_NUM */



PG_MODULE_MAGIC;

/*
 * "true" if DB2 data have been modified in the current transaction.
 */
static bool dml_in_transaction = false;

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct DB2FdwOption
{
  const char *optname;
  Oid optcontext;		/* Oid of catalog in which option may appear */
  bool optrequired;
};

#define OPT_NLS_LANG "nls_lang"
#define OPT_DBSERVER "dbserver"
#define OPT_USER "user"
#define OPT_PASSWORD "password"
#define OPT_SCHEMA "schema"
#define OPT_TABLE "table"
#define OPT_MAX_LONG "max_long"
#define OPT_READONLY "readonly"
#define OPT_KEY "key"
#define OPT_SAMPLE "sample_percent"
#define OPT_PREFETCH "prefetch"
#define OPT_NO_ENCODING_ERROR "no_encoding_error"

#define DEFAULT_MAX_LONG 32767
#define DEFAULT_PREFETCH 200

/*
 * Options for case folding for names in IMPORT FOREIGN TABLE.
 */
typedef enum
{ CASE_KEEP, CASE_LOWER, CASE_SMART } fold_t;

/*
 * Valid options for db2_fdw.
 */
static struct DB2FdwOption valid_options[] = {
  {OPT_NLS_LANG, ForeignDataWrapperRelationId, false},
  {OPT_DBSERVER, ForeignServerRelationId, true},
  {OPT_USER, UserMappingRelationId, true},
  {OPT_PASSWORD, UserMappingRelationId, true},
  {OPT_SCHEMA, ForeignTableRelationId, false},
  {OPT_TABLE, ForeignTableRelationId, true},
  {OPT_MAX_LONG, ForeignTableRelationId, false},
  {OPT_READONLY, ForeignTableRelationId, false},
  {OPT_SAMPLE, ForeignTableRelationId, false},
  {OPT_PREFETCH, ForeignTableRelationId, false}
  , {OPT_KEY, AttributeRelationId, false}
  , {OPT_NO_ENCODING_ERROR, ForeignDataWrapperRelationId, false}
  , {OPT_NO_ENCODING_ERROR, ForeignTableRelationId, false}
  , {OPT_NO_ENCODING_ERROR, AttributeRelationId, false}
};

#define option_count (sizeof(valid_options)/sizeof(struct DB2FdwOption))

/*
 * Array to hold the type output functions during table modification.
 * It is ok to hold this cache in a static variable because there cannot
 * be more than one foreign table modified at the same time.
 */

static regproc *output_funcs;

/*
 * FDW-specific information for RelOptInfo.fdw_private and ForeignScanState.fdw_state.
 * The same structure is used to hold information for query planning and execution.
 * The structure is initialized during query planning and passed on to the execution
 * step serialized as a List (see serializePlanData and deserializePlanData).
 * For DML statements, the scan stage and the modify stage both hold an
 * DB2FdwState, and the latter is initialized by copying the former (see copyPlanData).
 */
struct DB2FdwState
{
  char *dbserver;		/* DB2 connect string */
  char *user;			/* DB2 username */
  char *password;		/* DB2 password */
  char *nls_lang;		/* DB2 locale information */
  db2Session *session;		/* encapsulates the active DB2 session */
  char *query;			/* query we issue against DB2 */
  List *params;			/* list of parameters needed for the query */
  struct paramDesc *paramList;	/* description of parameters needed for the query */
  struct db2Table *db2Table;	/* description of the remote DB2 table */
  Cost startup_cost;		/* cost estimate, only needed for planning */
  Cost total_cost;		/* cost estimate, only needed for planning */
  unsigned long rowcount;	/* rows already read from DB2 */
  int columnindex;		/* currently processed column for error context */
  MemoryContext temp_cxt;	/* short-lived memory for data modification */
  unsigned int prefetch;	/* number of rows to prefetch */
  char *order_clause;		/* for sort-pushdown */
  char *where_clause;		/* deparsed where clause */

  /*
   * Restriction clauses, divided into safe and unsafe to pushdown subsets.
   *
   * For a base foreign relation this is a list of clauses along-with
   * RestrictInfo wrapper. Keeping RestrictInfo wrapper helps while dividing
   * scan_clauses in db2GetForeignPlan into safe and unsafe subsets.
   * Also it helps in estimating costs since RestrictInfo caches the
   * selectivity and qual cost for the clause in it.
   *
   * For a join relation, however, they are part of otherclause list
   * obtained from extract_actual_join_clauses, which strips RestrictInfo
   * construct. So, for a join relation they are list of bare clauses.
   */
  List *remote_conds;
  List *local_conds;

  /* Join information */
  RelOptInfo *outerrel;
  RelOptInfo *innerrel;
  JoinType jointype;
  List *joinclauses;
};

/*
 * SQL functions
 */
extern PGDLLEXPORT Datum db2_fdw_handler (PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum db2_fdw_validator (PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum db2_close_connections (PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum db2_diag (PG_FUNCTION_ARGS);
void db2Explain (void * fdw, ExplainState * es);

PG_FUNCTION_INFO_V1 (db2_fdw_handler);
PG_FUNCTION_INFO_V1 (db2_fdw_validator);
PG_FUNCTION_INFO_V1 (db2_close_connections);
PG_FUNCTION_INFO_V1 (db2_diag);

/*
 * on-load initializer
 */
extern PGDLLEXPORT void _PG_init (void);

/*
 * FDW callback routines
 */
static void db2GetForeignRelSize (PlannerInfo * root, RelOptInfo * baserel, Oid foreigntableid);
static void db2GetForeignPaths (PlannerInfo * root, RelOptInfo * baserel, Oid foreigntableid);
static void db2GetForeignJoinPaths (PlannerInfo * root, RelOptInfo * joinrel, RelOptInfo * outerrel, RelOptInfo * innerrel, JoinType jointype, JoinPathExtraData * extra);
static ForeignScan *db2GetForeignPlan (PlannerInfo * root, RelOptInfo * foreignrel, Oid foreigntableid, ForeignPath * best_path, List * tlist, List * scan_clauses , Plan * outer_plan);
static bool db2AnalyzeForeignTable (Relation relation, AcquireSampleRowsFunc * func, BlockNumber * totalpages);
static void db2ExplainForeignScan (ForeignScanState * node, ExplainState * es);
static void db2BeginForeignScan (ForeignScanState * node, int eflags);
static TupleTableSlot *db2IterateForeignScan (ForeignScanState * node);
static void db2EndForeignScan (ForeignScanState * node);
static void db2ReScanForeignScan (ForeignScanState * node);

#if PG_VERSION_NUM < 140000
static void db2AddForeignUpdateTargets(Query *parsetree, RangeTblEntry *target_rte, Relation target_relation);
#else
static void db2AddForeignUpdateTargets(PlannerInfo *root, Index rtindex, RangeTblEntry *target_rte, Relation target_relation);
#endif

static List *db2PlanForeignModify (PlannerInfo * root, ModifyTable * plan, Index resultRelation, int subplan_index);
static void db2BeginForeignModify (ModifyTableState * mtstate, ResultRelInfo * rinfo, List * fdw_private, int subplan_index, int eflags);
static TupleTableSlot *db2ExecForeignInsert (EState * estate, ResultRelInfo * rinfo, TupleTableSlot * slot, TupleTableSlot * planSlot);
static TupleTableSlot *db2ExecForeignUpdate (EState * estate, ResultRelInfo * rinfo, TupleTableSlot * slot, TupleTableSlot * planSlot);
static TupleTableSlot *db2ExecForeignDelete (EState * estate, ResultRelInfo * rinfo, TupleTableSlot * slot, TupleTableSlot * planSlot);
static void db2EndForeignModify (EState * estate, ResultRelInfo * rinfo);
static void db2ExplainForeignModify (ModifyTableState * mtstate, ResultRelInfo * rinfo, List * fdw_private, int subplan_index, struct ExplainState *es);
static int db2IsForeignRelUpdatable (Relation rel);
static List *db2ImportForeignSchema (ImportForeignSchemaStmt * stmt, Oid serverOid);

/*
 * Helper functions
 */
static struct DB2FdwState *getFdwState (Oid foreigntableid, double *sample_percent);
static void db2GetOptions (Oid foreigntableid, List ** options);
static char *createQuery (struct DB2FdwState *fdwState, RelOptInfo * foreignrel, bool modify, List * query_pathkeys);
static void deparseFromExprForRel (struct DB2FdwState *fdwState, StringInfo buf, RelOptInfo * joinrel, List ** params_list);
static void appendConditions (List * exprs, StringInfo buf, RelOptInfo * joinrel, List ** params_list);
static bool foreign_join_ok (PlannerInfo * root, RelOptInfo * joinrel, JoinType jointype, RelOptInfo * outerrel, RelOptInfo * innerrel, JoinPathExtraData * extra);
static const char *get_jointype_name (JoinType jointype);
static List *build_tlist_to_deparse (RelOptInfo * foreignrel);
static void getColumnData (struct db2Table *db2Table, Oid foreigntableid);
static int acquireSampleRowsFunc (Relation relation, int elevel, HeapTuple * rows, int targrows, double *totalrows, double *totaldeadrows);
static void appendAsType (StringInfoData * dest, const char *s, Oid type);
static char *deparseExpr (db2Session * session, RelOptInfo * foreignrel, Expr * expr, const struct db2Table *db2Table, List ** params);
static char *datumToString (Datum datum, Oid type);
static void getUsedColumns (Expr * expr, struct db2Table *db2Table, int foreignrelid);
static void checkDataType (db2Type db2type, int scale, Oid pgtype, const char *tablename, const char *colname);
static char *deparseWhereConditions (struct DB2FdwState *fdwState, RelOptInfo * baserel, List ** local_conds, List ** remote_conds);
static char *guessNlsLang (char *nls_lang);
static List *serializePlanData (struct DB2FdwState *fdwState);
static Const *serializeString (const char *s);
static Const *serializeLong (long i);
static struct DB2FdwState *deserializePlanData (List * list);
static char *deserializeString (Const * constant);
static long deserializeLong (Const * constant);
#if PG_VERSION_NUM >= 150000
static Expr *find_em_expr_for_rel (EquivalenceClass * ec, RelOptInfo * rel);
#endif
static char *deparseDate (Datum datum);
static char *deparseTimestamp (Datum datum, bool hasTimezone);
static char *deparseInterval (Datum datum);
static struct DB2FdwState *copyPlanData (struct DB2FdwState *orig);
static void subtransactionCallback (SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void *arg);
static void addParam (struct paramDesc **paramList, char *name, Oid pgtype, db2Type db2type, int colnum);
static void setModifyParameters (struct paramDesc *paramList, TupleTableSlot * newslot, TupleTableSlot * oldslot, struct db2Table *db2Table, db2Session * session);
static void transactionCallback (XactEvent event, void *arg);
static void exitHook (int code, Datum arg);
static void db2Die (SIGNAL_ARGS);
static char *setSelectParameters (struct paramDesc *paramList, ExprContext * econtext);
static void convertTuple (struct DB2FdwState *fdw_state, Datum * values, bool * nulls, bool trunc_lob);
static void errorContextCallback (void *arg);
static char *fold_case (char *name, fold_t foldcase);

#define REL_ALIAS_PREFIX    "r"
/* Handy macro to add relation name qualification */
#define ADD_REL_QUALIFIER(buf, varno)   \
		appendStringInfo((buf), "%s%d.", REL_ALIAS_PREFIX, (varno))

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to callback routines.
 */
PGDLLEXPORT Datum
db2_fdw_handler (PG_FUNCTION_ARGS)
{
  FdwRoutine *fdwroutine = makeNode (FdwRoutine);

  fdwroutine->GetForeignRelSize = db2GetForeignRelSize;
  fdwroutine->GetForeignPaths = db2GetForeignPaths;
  fdwroutine->GetForeignJoinPaths = db2GetForeignJoinPaths;
  fdwroutine->GetForeignPlan = db2GetForeignPlan;
  fdwroutine->AnalyzeForeignTable = db2AnalyzeForeignTable;
  fdwroutine->ExplainForeignScan = db2ExplainForeignScan;
  fdwroutine->BeginForeignScan = db2BeginForeignScan;
  fdwroutine->IterateForeignScan = db2IterateForeignScan;
  fdwroutine->ReScanForeignScan = db2ReScanForeignScan;
  fdwroutine->EndForeignScan = db2EndForeignScan;
  fdwroutine->AddForeignUpdateTargets = db2AddForeignUpdateTargets;
  fdwroutine->PlanForeignModify = db2PlanForeignModify;
  fdwroutine->BeginForeignModify = db2BeginForeignModify;
  fdwroutine->ExecForeignInsert = db2ExecForeignInsert;
  fdwroutine->ExecForeignUpdate = db2ExecForeignUpdate;
  fdwroutine->ExecForeignDelete = db2ExecForeignDelete;
  fdwroutine->EndForeignModify = db2EndForeignModify;
  fdwroutine->ExplainForeignModify = db2ExplainForeignModify;
  fdwroutine->IsForeignRelUpdatable = db2IsForeignRelUpdatable;
  fdwroutine->ImportForeignSchema = db2ImportForeignSchema;

  PG_RETURN_POINTER (fdwroutine);
}

/*
 * db2_fdw_validator
 * 		Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * 		USER MAPPING or FOREIGN TABLE that uses db2_fdw.
 *
 * 		Raise an ERROR if the option or its value are considered invalid
 * 		or a required option is missing.
 */
PGDLLEXPORT Datum
db2_fdw_validator (PG_FUNCTION_ARGS)
{
  List *options_list = untransformRelOptions (PG_GETARG_DATUM (0));
  Oid catalog = PG_GETARG_OID (1);
  ListCell *cell;
  bool option_given[option_count] = { false };
  int i;

  /*
   * Check that only options supported by db2_fdw, and allowed for the
   * current object type, are given.
   */

  foreach (cell, options_list) {
    DefElem *def = (DefElem *) lfirst (cell);
    bool opt_found = false;

    /* search for the option in the list of valid options */
    for (i = 0; i < option_count; ++i) {
      if (catalog == valid_options[i].optcontext && strcmp (valid_options[i].optname, def->defname) == 0) {
	opt_found = true;
	option_given[i] = true;
	break;
      }
    }

    /* option not found, generate error message */
    if (!opt_found) {
      /* generate list of options */
      StringInfoData buf;
      initStringInfo (&buf);
      for (i = 0; i < option_count; ++i) {
	if (catalog == valid_options[i].optcontext)
	  appendStringInfo (&buf, "%s%s", (buf.len > 0) ? ", " : "", valid_options[i].optname);
      }

      ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_OPTION_NAME), errmsg ("invalid option \"%s\"", def->defname), errhint ("Valid options in this context are: %s", buf.data)));
    }

    /* check valid values for "readonly", "key" and "no_encoding_error" */
    if (strcmp (def->defname, OPT_READONLY) == 0 || strcmp (def->defname, OPT_KEY) == 0 || strcmp (def->defname, OPT_NO_ENCODING_ERROR) == 0) {
      char *val = STRVAL(def->arg);
      if (pg_strcasecmp (val, "on") != 0
	  && pg_strcasecmp (val, "off") != 0
	  && pg_strcasecmp (val, "yes") != 0 && pg_strcasecmp (val, "no") != 0 && pg_strcasecmp (val, "true") != 0 && pg_strcasecmp (val, "false") != 0)
	ereport (ERROR,
		 (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
		  errmsg ("invalid value for option \"%s\"", def->defname), errhint ("Valid values in this context are: on/yes/true or off/no/false")));
    }

    /* check valid values for "table" and "schema" */
    if (strcmp (def->defname, OPT_TABLE) == 0 || strcmp (def->defname, OPT_SCHEMA) == 0) {
      char *val = STRVAL(def->arg);
      if (strchr (val, '"') != NULL)
	ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("invalid value for option \"%s\"", def->defname), errhint ("Double quotes are not allowed in names.")));
    }

    /* check valid values for max_long */
    if (strcmp (def->defname, OPT_MAX_LONG) == 0) {
      char *val = STRVAL(def->arg);
      char *endptr;
      unsigned long max_long = strtoul (val, &endptr, 0);
      if (val[0] == '\0' || *endptr != '\0' || max_long < 1 || max_long > 1073741823ul)
	ereport (ERROR,
		 (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
		  errmsg ("invalid value for option \"%s\"", def->defname), errhint ("Valid values in this context are integers between 1 and 1073741823.")));
    }

    /* check valid values for "sample_percent" */
    if (strcmp (def->defname, OPT_SAMPLE) == 0) {
      char *val = STRVAL(def->arg);
      char *endptr;
      double sample_percent;

      errno = 0;
      sample_percent = strtod (val, &endptr);
      if (val[0] == '\0' || *endptr != '\0' || errno != 0 || sample_percent < 0.000001 || sample_percent > 100.0)
	ereport (ERROR,
		 (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
		  errmsg ("invalid value for option \"%s\"", def->defname), errhint ("Valid values in this context are numbers between 0.000001 and 100.")));
    }

    /* check valid values for "prefetch" */
    if (strcmp (def->defname, OPT_PREFETCH) == 0) {
      char *val = STRVAL(def->arg);
      char *endptr;
      unsigned long prefetch = strtol (val, &endptr, 0);
      if (val[0] == '\0' || *endptr != '\0' || prefetch < 0 || prefetch > 10240)
	ereport (ERROR,
		 (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
		  errmsg ("invalid value for option \"%s\"", def->defname), errhint ("Valid values in this context are integers between 0 and 10240.")));
    }
  }

  /* check that all required options have been given */
  for (i = 0; i < option_count; ++i) {
    if (catalog == valid_options[i].optcontext && valid_options[i].optrequired && !option_given[i]) {
      ereport (ERROR, (errcode (ERRCODE_FDW_OPTION_NAME_NOT_FOUND), errmsg ("missing required option \"%s\"", valid_options[i].optname)));
    }
  }

  PG_RETURN_VOID ();
}

/*
 * db2_close_connections
 * 		Close all open DB2 connections.
 */
PGDLLEXPORT Datum
db2_close_connections (PG_FUNCTION_ARGS)
{
  if (dml_in_transaction)
    ereport (ERROR, (errcode (ERRCODE_ACTIVE_SQL_TRANSACTION), errmsg ("connections with an active transaction cannot be closed"),
		     errhint ("The transaction that modified DB2 data must be closed first.")
	     )
      );
  elog (DEBUG1, "db2_fdw: close all DB2 connections");
  db2CloseConnections ();

  PG_RETURN_VOID ();
}

/*
 * db2_diag
 * 		Get the DB2 client version.
 * 		If a non-NULL argument is supplied, it must be a foreign server name.
 * 		In this case, the remote server version is returned as well.
 */
PGDLLEXPORT Datum
db2_diag (PG_FUNCTION_ARGS)
{
  Oid srvId = InvalidOid;
  char *pgversion;
  char  server_version[100];
  int major, minor, update, patch, port_patch;
  StringInfoData version;

  /*
   * Get the PostgreSQL server version.
   * We cannot use PG_VERSION because that would give the version against which
   * db2_fdw was compiled, not the version it is running with.
   */
  pgversion = GetConfigOptionByName ("server_version", NULL);

  /* get the DB2 client version */
  db2ClientVersion (&major, &minor, &update, &patch, &port_patch);

  initStringInfo (&version);
  appendStringInfo (&version, "db2_fdw %s, PostgreSQL %s, DB2 client %d.%d.%d.%d.%d", DB2_FDW_VERSION, pgversion, major, minor, update, patch, port_patch);

  if (PG_ARGISNULL (0)) {
    /* display some important DB2 environment variables */
    static const char *const db2_env[] = {
      "DB2INSTANCE",
      "DB2_HOME",
      "DB2LIB",
      NULL
    };
    int i;

    for (i = 0; db2_env[i] != NULL; ++i) {
      char *val = getenv (db2_env[i]);

      if (val != NULL)
	appendStringInfo (&version, ", %s=%s", db2_env[i], val);
    }
  }
  else {
    /* get the server version only if a non-null argument was given */
    HeapTuple tup;
    Relation rel;
    Name srvname = PG_GETARG_NAME (0);
    ForeignServer *server;
    UserMapping *mapping;
    ForeignDataWrapper *wrapper;
    List *options;
    ListCell *cell;
    char *nls_lang = NULL, *user = NULL, *password = NULL, *dbserver = NULL;

    /* look up foreign server with this name */
    rel = table_open (ForeignServerRelationId, AccessShareLock);

    tup = SearchSysCacheCopy1 (FOREIGNSERVERNAME, NameGetDatum (srvname));
    if (!HeapTupleIsValid (tup))
      ereport (ERROR, (errcode (ERRCODE_UNDEFINED_OBJECT), errmsg ("server \"%s\" does not exist", NameStr (*srvname))));

#if PG_VERSION_NUM < 120000
     srvId = HeapTupleGetOid(tup);
#else
     srvId = ((Form_pg_foreign_server)GETSTRUCT(tup))->oid;
#endif


    table_close (rel, AccessShareLock);

    /* get the foreign server, the user mapping and the FDW */
    server = GetForeignServer (srvId);
    mapping = GetUserMapping (GetUserId (), srvId);
    wrapper = GetForeignDataWrapper (server->fdwid);

    /* get all options for these objects */
    options = wrapper->options;
    options = list_concat (options, server->options);
    options = list_concat (options, mapping->options);

    foreach (cell, options) {
      DefElem *def = (DefElem *) lfirst (cell);
      if (strcmp (def->defname, OPT_NLS_LANG) == 0)
	nls_lang = STRVAL(def->arg);
      if (strcmp (def->defname, OPT_DBSERVER) == 0)
	dbserver = STRVAL(def->arg);
      if (strcmp (def->defname, OPT_USER) == 0)
	user = STRVAL(def->arg);
      if (strcmp (def->defname, OPT_PASSWORD) == 0)
	password = STRVAL(def->arg);
    }

    /* guess a good NLS_LANG environment setting */
    nls_lang = guessNlsLang (nls_lang);

    /* connect to DB2 database */
/*    session = db2GetSession (dbserver, user, password, nls_lang, NULL, 1);*/

    /* get the server version */
    db2ServerVersion (dbserver, user, password, server_version,sizeof(server_version));
    appendStringInfo (&version, ", DB2 server %s", server_version);

    /* free the session (connection will be cached) */
    /*pfree (session);*/
  }

  PG_RETURN_TEXT_P (cstring_to_text (version.data));
}

/*
 * _PG_init
 * 		Library load-time initalization.
 * 		Sets exitHook() callback for backend shutdown.
 */
void _PG_init (void)
{
  /* register an exit hook */
  on_proc_exit (&exitHook, PointerGetDatum (NULL));
}

/*
 * db2GetForeignRelSize
 * 		Get an DB2FdwState for this foreign scan.
 * 		Construct the remote SQL query.
 * 		Provide estimates for the number of tuples, the average width and the cost.
 */
void
db2GetForeignRelSize (PlannerInfo * root, RelOptInfo * baserel, Oid foreigntableid)
{
  struct DB2FdwState *fdwState;
  int i;
  double ntuples = -1;

  elog (DEBUG1, "db2_fdw: plan foreign table scan");

  /* get connection options, connect and get the remote table description */
  fdwState = getFdwState (foreigntableid, NULL);

  /*
   * Store the table OID in each table column.
   * This is redundant for base relations, but join relations will
   * have columns from different tables, and we have to keep track of them.
   */
  for (i = 0; i < fdwState->db2Table->ncols; ++i) {
    fdwState->db2Table->cols[i]->varno = baserel->relid;
  }

  /*
   * Classify conditions into remote_conds or local_conds.
   * These parameters are used in foreign_join_ok and db2GetForeignPlan.
   * Those conditions that can be pushed down will be collected into
   * an DB2 WHERE clause.
   */
  fdwState->where_clause = deparseWhereConditions (fdwState, baserel, &(fdwState->local_conds), &(fdwState->remote_conds)
    );

  /* release DB2 session (will be cached) */
  pfree (fdwState->session);
  fdwState->session = NULL;

  /* use a random "high" value for cost */
  fdwState->startup_cost = 10000.0;

  /* if baserel->pages > 0, there was an ANALYZE; use the row count estimate */
  if (baserel->pages > 0)
    ntuples = baserel->tuples;

  /* estimale selectivity locally for all conditions */

  /* apply statistics only if we have a reasonable row count estimate */
  if (ntuples != -1) {
    /* estimate how conditions will influence the row count */
    ntuples = ntuples * clauselist_selectivity (root, baserel->baserestrictinfo, 0, JOIN_INNER, NULL);
    /* make sure that the estimate is not less that 1 */
    ntuples = clamp_row_est (ntuples);
    baserel->rows = ntuples;
  }

  /* estimate total cost as startup cost + 10 * (returned rows) */
  fdwState->total_cost = fdwState->startup_cost + baserel->rows * 10.0;

  /* store the state so that the other planning functions can use it */
  baserel->fdw_private = (void *) fdwState;
}

/* db2GetForeignPaths
 * 		Create a ForeignPath node and add it as only possible path.
 */
void
db2GetForeignPaths (PlannerInfo * root, RelOptInfo * baserel, Oid foreigntableid)
{
  struct DB2FdwState *fdwState = (struct DB2FdwState *) baserel->fdw_private;

  /*
   * Determine whether we can potentially push query pathkeys to the remote
   * side, avoiding a local sort.
   */
  StringInfoData orderedquery;
  List *usable_pathkeys = NIL;
  ListCell *cell;
  char *delim = " ";

  initStringInfo (&orderedquery);

  foreach (cell, root->query_pathkeys) {
    PathKey *pathkey = (PathKey *) lfirst (cell);
    EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
    Expr *em_expr = NULL;
    char *sort_clause;
    Oid em_type;
    bool can_pushdown;

    /*
     * deparseExpr would detect volatile expressions as well, but
     * ec_has_volatile saves some cycles.
     */
    can_pushdown = !pathkey_ec->ec_has_volatile && ((em_expr = find_em_expr_for_rel (pathkey_ec, baserel)) != NULL);

    if (can_pushdown) {
      em_type = exprType ((Node *) em_expr);

      /* expressions of a type different from this are not safe to push down into ORDER BY clauses */
      if (em_type != INT8OID && em_type != INT2OID && em_type != INT4OID && em_type != OIDOID
	  && em_type != FLOAT4OID && em_type != FLOAT8OID && em_type != NUMERICOID && em_type != DATEOID
	  && em_type != TIMESTAMPOID && em_type != TIMESTAMPTZOID 
	  && em_type != TIMEOID && em_type != TIMETZOID 
          && em_type != INTERVALOID)
	can_pushdown = false;
    }

    if (can_pushdown && ((sort_clause = deparseExpr (fdwState->session, baserel, em_expr, fdwState->db2Table, &(fdwState->params))) != NULL)) {
      /* keep usable_pathkeys for later use. */
      usable_pathkeys = lappend (usable_pathkeys, pathkey);

      /* create orderedquery */
      appendStringInfoString (&orderedquery, delim);
      appendStringInfoString (&orderedquery, sort_clause);
      delim = ", ";

      if (pathkey->pk_strategy == BTLessStrategyNumber)
	appendStringInfoString (&orderedquery, " ASC");
      else
	appendStringInfoString (&orderedquery, " DESC");

      if (pathkey->pk_nulls_first)
	appendStringInfoString (&orderedquery, " NULLS FIRST");
      else
	appendStringInfoString (&orderedquery, " NULLS LAST");
    }
    else {
      /*
       * The planner and executor don't have any clever strategy for
       * taking data sorted by a prefix of the query's pathkeys and
       * getting it to be sorted by all of those pathekeys.  We'll just
       * end up resorting the entire data set.  So, unless we can push
       * down all of the query pathkeys, forget it.
       */
      list_free (usable_pathkeys);
      usable_pathkeys = NIL;
      break;
    }
  }

  /* set order clause */
  if (usable_pathkeys != NIL)
    fdwState->order_clause = orderedquery.data;

  /* add the only path */
  add_path (baserel, (Path *) create_foreignscan_path (root, baserel,
						       NULL,	/* default pathtarget */
						       baserel->rows, fdwState->startup_cost, fdwState->total_cost, usable_pathkeys, NULL,
						       NULL,	/* no extra plan */
						       NIL)
    );
}

/*
 * db2GetForeignJoinPaths
 * 		Add possible ForeignPath to joinrel if the join is safe to push down.
 * 		For now, we can only push down 2-way inner join for SELECT.
 */
static void
db2GetForeignJoinPaths (PlannerInfo * root, RelOptInfo * joinrel, RelOptInfo * outerrel, RelOptInfo * innerrel, JoinType jointype, JoinPathExtraData * extra)
{
  struct DB2FdwState *fdwState;
  ForeignPath *joinpath;
  double joinclauses_selectivity;
  double rows;			/* estimated number of returned rows */
  Cost startup_cost;
  Cost total_cost;

  /*
   * Currently we don't push-down joins in query for UPDATE/DELETE.
   * This would require a path for EvalPlanQual.
   * This restriction might be relaxed in a later release.
   */
  if (root->parse->commandType != CMD_SELECT) {
    elog (DEBUG2, "db2_fdw: don't push down join because it is no SELECT");
    return;
  }

  /*
   * N-way join is not supported, due to the column definition infrastracture.
   * If we can track relid mapping of join relations, we can support N-way join.
   */
  if (!IS_SIMPLE_REL (outerrel) || !IS_SIMPLE_REL (innerrel))
    return;

  /* skip if this join combination has been considered already */
  if (joinrel->fdw_private)
    return;

  /*
   * Create unfinished DB2FdwState which is used to indicate
   * that the join relation has already been considered, so that we won't waste
   * time considering it again and don't add the same path a second time.
   * Once we know that this join can be pushed down, we fill the data structure.
   */
  fdwState = (struct DB2FdwState *) palloc0 (sizeof (struct DB2FdwState));

  joinrel->fdw_private = fdwState;

  /* this performs further checks and completes joinrel->fdw_private */
  if (!foreign_join_ok (root, joinrel, jointype, outerrel, innerrel, extra))
    return;

  /* estimate the number of result rows for the join */
#if PG_VERSION_NUM < 140000
  if (outerrel->pages > 0 && innerrel->pages > 0)
#else
  if (outerrel->tuples >= 0 && innerrel->tuples >= 0)
#endif  /* PG_VERSION_NUM */
  {
    /* both relations have been ANALYZEd, so there should be useful statistics */
    joinclauses_selectivity = clauselist_selectivity (root, fdwState->joinclauses, 0, JOIN_INNER, extra->sjinfo);
    rows = clamp_row_est (innerrel->tuples * outerrel->tuples * joinclauses_selectivity);
  }
  else {
    /* at least one table lacks statistics, so use a fixed estimate */
    rows = 1000.0;
  }

  /* use a random "high" value for startup cost */
  startup_cost = 10000.0;

  /* estimate total cost as startup cost + (returned rows) * 10.0 */
  total_cost = startup_cost + rows * 10.0;

  /* store cost estimation results */
  joinrel->rows = rows;
  fdwState->startup_cost = startup_cost;
  fdwState->total_cost = total_cost;

  /* create a new join path */
  joinpath = create_foreignscan_path (root, joinrel, NULL,	/* default pathtarget */
				      rows, startup_cost, total_cost, NIL,	/* no pathkeys */
				      NULL,	/* no required_outer */
				      NULL,	/* no epq_path */
				      NIL);	/* no fdw_private */

  /* add generated path to joinrel */
  add_path (joinrel, (Path *) joinpath);
}

/*
 * db2GetForeignPlan
 * 		Construct a ForeignScan node containing the serialized DB2FdwState,
 * 		the RestrictInfo clauses not handled entirely by DB2 and the list
 * 		of parameters we need for execution.
 */
ForeignScan * db2GetForeignPlan (PlannerInfo * root, RelOptInfo * foreignrel, Oid foreigntableid, ForeignPath * best_path, List * tlist, List * scan_clauses , Plan * outer_plan)
{
  struct DB2FdwState *fdwState = (struct DB2FdwState *) foreignrel->fdw_private;
  List *fdw_private = NIL;
  int i;
  bool need_keys = false, for_update = false, has_trigger;
  Relation rel;
  Index scan_relid;		/* will be 0 for join relations */
  List *local_exprs = fdwState->local_conds;
  List *fdw_scan_tlist = NIL;

  /* treat base relations and join relations differently */
  if (IS_SIMPLE_REL (foreignrel)) {
    /* for base relations, set scan_relid as the relid of the relation */
    scan_relid = foreignrel->relid;

    /* check if the foreign scan is for an UPDATE or DELETE */
#if PG_VERSION_NUM < 140000
    if (foreignrel->relid == root->parse->resultRelation && (root->parse->commandType == CMD_UPDATE || root->parse->commandType == CMD_DELETE))
#else
    if (bms_is_member(foreignrel->relid, root->all_result_relids) && (root->parse->commandType == CMD_UPDATE || root->parse->commandType == CMD_DELETE))
#endif  /* PG_VERSION_NUM */
    {

      /* we need the table's primary key columns */
      need_keys = true;
    }

    /* check if FOR [KEY] SHARE/UPDATE was specified */
    if (need_keys || get_parse_rowmark (root->parse, foreignrel->relid)) {
      /* we should add FOR UPDATE */
      for_update = true;
    }

    if (need_keys) {
      /* we need to fetch all primary key columns */
      for (i = 0; i < fdwState->db2Table->ncols; ++i)
	if (fdwState->db2Table->cols[i]->pkey)
	  fdwState->db2Table->cols[i]->used = 1;
    }

    /*
     * Core code already has some lock on each rel being planned, so we can
     * use NoLock here.
     */
    rel = table_open (foreigntableid, NoLock);

    /* is there an AFTER trigger FOR EACH ROW? */
    has_trigger = (foreignrel->relid == root->parse->resultRelation) && rel->trigdesc && ((root->parse->commandType == CMD_UPDATE && rel->trigdesc->trig_update_after_row)
											  || (root->parse->commandType == CMD_DELETE && rel->trigdesc->trig_delete_after_row));

    table_close (rel, NoLock);

    if (has_trigger) {
      /* we need to fetch and return all columns */
      for (i = 0; i < fdwState->db2Table->ncols; ++i)
	if (fdwState->db2Table->cols[i]->pgname)
	  fdwState->db2Table->cols[i]->used = 1;
    }
  }
  else {
    /* we have a join relation, so set scan_relid to 0 */
    scan_relid = 0;

    /*
     * create_scan_plan() and create_foreignscan_plan() pass
     * rel->baserestrictinfo + parameterization clauses through
     * scan_clauses. For a join rel->baserestrictinfo is NIL and we are
     * not considering parameterization right now, so there should be no
     * scan_clauses for a joinrel.
     */
    Assert (!scan_clauses);

    /* Build the list of columns to be fetched from the foreign server. */
    fdw_scan_tlist = build_tlist_to_deparse (foreignrel);

    /*
     * Ensure that the outer plan produces a tuple whose descriptor
     * matches our scan tuple slot. This is safe because all scans and
     * joins support projection, so we never need to insert a Result node.
     * Also, remove the local conditions from outer plan's quals, lest
     * they will be evaluated twice, once by the local plan and once by
     * the scan.
     */
    if (outer_plan) {
      ListCell *lc;

      outer_plan->targetlist = fdw_scan_tlist;

      foreach (lc, local_exprs) {
	Join *join_plan = (Join *) outer_plan;
	Node *qual = lfirst (lc);

	outer_plan->qual = list_delete (outer_plan->qual, qual);

	/*
	 * For an inner join the local conditions of foreign scan plan
	 * can be part of the joinquals as well.
	 */
	if (join_plan->jointype == JOIN_INNER)
	  join_plan->joinqual = list_delete (join_plan->joinqual, qual);
      }
    }
  }

  /* create remote query */
  fdwState->query = createQuery (fdwState, foreignrel, for_update, best_path->path.pathkeys);
  elog (DEBUG1, "db2_fdw: remote query is: %s", fdwState->query);

  /* get PostgreSQL column data types, check that they match DB2's */
  for (i = 0; i < fdwState->db2Table->ncols; ++i)
    if (fdwState->db2Table->cols[i]->used)
      checkDataType (fdwState->db2Table->cols[i]->db2type,
		     fdwState->db2Table->cols[i]->scale, fdwState->db2Table->cols[i]->pgtype, fdwState->db2Table->pgname, fdwState->db2Table->cols[i]->pgname);

  fdw_private = serializePlanData (fdwState);

  /*
   * Create the ForeignScan node for the given relation.
   *
   * Note that the remote parameter expressions are stored in the fdw_exprs
   * field of the finished plan node; we can't keep them in private state
   * because then they wouldn't be subject to later planner processing.
   */
  return make_foreignscan (tlist, local_exprs, scan_relid, fdwState->params, fdw_private
			   , fdw_scan_tlist, NIL,	/* no parameterized paths */
			   outer_plan
    );
}

bool
db2AnalyzeForeignTable (Relation relation, AcquireSampleRowsFunc * func, BlockNumber * totalpages)
{
  *func = acquireSampleRowsFunc;
  /* use positive page count as a sign that the table has been ANALYZEd */
  *totalpages = 42;

  return true;
}

void db2Explain (void * fdw, ExplainState * es)
{
  FILE *fp;
  char path[1035];
  char execution_cmd[300];
  struct DB2FdwState * fdw_state = (struct DB2FdwState *) fdw;

  memset(execution_cmd,0x00,sizeof(execution_cmd));
  if (es->verbose) {
    if (strlen(fdw_state->user)){
      sprintf(execution_cmd,"db2expln -t -d %s -u %s %s -q \"%s\" ",fdw_state->dbserver,fdw_state->user,fdw_state->password,fdw_state->query);
    }else{
      sprintf(execution_cmd,"db2expln -t -d %s -q \"%s\" ",fdw_state->dbserver,fdw_state->query);
    }
  } else {
    if (strlen(fdw_state->user)){
      sprintf(execution_cmd,"db2expln -t -d %s -u %s %s -q \"%s\" |grep -E \"Estimated Cost|Estimated Cardinality\" ",fdw_state->dbserver,fdw_state->user,fdw_state->password,fdw_state->query);
    }else{
      sprintf(execution_cmd,"db2expln -t -d %s -q \"%s\" |grep -E \"Estimated Cost|Estimated Cardinality\" ",fdw_state->dbserver,fdw_state->query);
    }
  }

  /* Open the command for reading. */
  fp = popen(execution_cmd, "r");
  if (fp == NULL) {
    elog (ERROR, "db2_fdw: Failed to run command");
    exit(1);
  }

  /* Read the output a line at a time - output it. */
  while (fgets(path, sizeof(path)-1, fp) != NULL) {
    path[strlen (path) - 1] = '\0';
    ExplainPropertyText ("DB2 plan", path, es);
  }
  /* close */
  pclose(fp);
}
/*
 * db2ExplainForeignScan
 * 		Produce extra output for EXPLAIN:
 * 		the DB2 query and, if VERBOSE was given, the execution plan.
 */
void
db2ExplainForeignScan (ForeignScanState * node, ExplainState * es)
{
  struct DB2FdwState *fdw_state = (struct DB2FdwState *) node->fdw_state;

  elog (DEBUG1, "db2_fdw: explain foreign table scan");

  /* show query */
  ExplainPropertyText ("DB2 query", fdw_state->query, es);
  db2Explain (fdw_state, es);
}

/*
 * db2BeginForeignScan
 * 		Recover ("deserialize") connection information, remote query,
 * 		DB2 table description and parameter list from the plan's
 * 		"fdw_private" field.
 * 		Reestablish a connection to DB2.
 */
void
db2BeginForeignScan (ForeignScanState * node, int eflags)
{
  ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
  List *fdw_private = fsplan->fdw_private;
  List *exec_exprs;
  ListCell *cell;
  int index;
  struct paramDesc *paramDesc;
  struct DB2FdwState *fdw_state;

  /* deserialize private plan data */
  fdw_state = deserializePlanData (fdw_private);
  node->fdw_state = (void *) fdw_state;

  /* create an ExprState tree for the parameter expressions */
#if PG_VERSION_NUM < 100000
  exec_exprs = (List *) ExecInitExpr ((Expr *) fsplan->fdw_exprs, (PlanState *) node);
#else
  exec_exprs = (List *) ExecInitExprList (fsplan->fdw_exprs, (PlanState *) node);
#endif /* PG_VERSION_NUM */

  /* create the list of parameters */
  index = 0;
  foreach (cell, exec_exprs) {
    ExprState *expr = (ExprState *) lfirst (cell);
    char parname[10];

    /* count, but skip deleted entries */
    ++index;
    if (expr == NULL)
      continue;

    /* create a new entry in the parameter list */
    paramDesc = (struct paramDesc *) palloc (sizeof (struct paramDesc));
    snprintf (parname, 10, ":p%d", index);
    paramDesc->name = pstrdup (parname);
    paramDesc->type = exprType ((Node *) (expr->expr));

    if (paramDesc->type == TEXTOID || paramDesc->type == VARCHAROID
	|| paramDesc->type == BPCHAROID || paramDesc->type == CHAROID || paramDesc->type == DATEOID || paramDesc->type == TIMESTAMPOID || paramDesc->type == TIMESTAMPTZOID || paramDesc->type == TIMEOID || paramDesc->type == TIMETZOID)
      paramDesc->bindType = BIND_STRING;
    else
      paramDesc->bindType = BIND_NUMBER;

    paramDesc->value = NULL;
    paramDesc->node = expr;
    paramDesc->bindh = NULL;
    paramDesc->colnum = -1;
    paramDesc->next = fdw_state->paramList;
    fdw_state->paramList = paramDesc;
  }

  /* add a fake parameter ":now" if that string appears in the query */
  if (strstr (fdw_state->query, ":now") != NULL) {
    paramDesc = (struct paramDesc *) palloc (sizeof (struct paramDesc));
    paramDesc->name = pstrdup (":now");
    paramDesc->type = TIMESTAMPTZOID;
    paramDesc->bindType = BIND_STRING;
    paramDesc->value = NULL;
    paramDesc->node = NULL;
    paramDesc->bindh = NULL;
    paramDesc->colnum = -1;
    paramDesc->next = fdw_state->paramList;
    fdw_state->paramList = paramDesc;
  }

  if (node->ss.ss_currentRelation)
    elog (DEBUG1, "db2_fdw: begin foreign table scan on %d", RelationGetRelid (node->ss.ss_currentRelation));
  else
    elog (DEBUG1, "db2_fdw: begin foreign join");

  /* connect to DB2 database */
  fdw_state->session = db2GetSession (fdw_state->dbserver, fdw_state->user, fdw_state->password, fdw_state->nls_lang, fdw_state->db2Table->pgname,
				      GetCurrentTransactionNestLevel ()
    );

  /* initialize row count to zero */
  fdw_state->rowcount = 0;
}

/*
 * db2IterateForeignScan
 * 		On first invocation (if there is no DB2 statement yet),
 * 		get the actual parameter values and run the remote query against
 * 		the DB2 database, retrieving the first result row.
 * 		Subsequent invocations will fetch more result rows until there
 * 		are no more.
 * 		The result is stored as a virtual tuple in the ScanState's
 * 		TupleSlot and returned.
 */
TupleTableSlot *
db2IterateForeignScan (ForeignScanState * node)
{
  TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
  ExprContext *econtext = node->ss.ps.ps_ExprContext;
  int have_result;
  struct DB2FdwState *fdw_state = (struct DB2FdwState *) node->fdw_state;

  if (db2IsStatementOpen (fdw_state->session)) {
    elog (DEBUG3, "db2_fdw: get next row in foreign table scan");

    /* fetch the next result row */
    have_result = db2FetchNext (fdw_state->session);
  }
  else {
    /* fill the parameter list with the actual values */
    char *paramInfo = setSelectParameters (fdw_state->paramList, econtext);

    /* execute the DB2 statement and fetch the first row */
    elog (DEBUG1, "db2_fdw: execute query in foreign table scan %s", paramInfo);

    db2PrepareQuery (fdw_state->session, fdw_state->query, fdw_state->db2Table, fdw_state->prefetch);
    have_result = db2ExecuteQuery (fdw_state->session, fdw_state->db2Table, fdw_state->paramList);
  }

  /* initialize virtual tuple */
  ExecClearTuple (slot);

  if (have_result) {
    /* increase row count */
    ++fdw_state->rowcount;

    /* convert result to arrays of values and null indicators */
    convertTuple (fdw_state, slot->tts_values, slot->tts_isnull, false);

    /* store the virtual tuple */
    ExecStoreVirtualTuple (slot);
  }
  else {
    /* close the statement */
    db2CloseStatement (fdw_state->session);
  }

  return slot;
}

/*
 * db2EndForeignScan
 * 		Close the currently active DB2 statement.
 */
void
db2EndForeignScan (ForeignScanState * node)
{
  struct DB2FdwState *fdw_state = (struct DB2FdwState *) node->fdw_state;

  elog (DEBUG1, "db2_fdw: end foreign table scan");

  /* release the DB2 session */
  db2CloseStatement (fdw_state->session);
  pfree (fdw_state->session);
  fdw_state->session = NULL;
}

/*
 * db2ReScanForeignScan
 * 		Close the DB2 statement if there is any.
 * 		That causes the next db2IterateForeignScan call to restart the scan.
 */
void
db2ReScanForeignScan (ForeignScanState * node)
{
  struct DB2FdwState *fdw_state = (struct DB2FdwState *) node->fdw_state;

  elog (DEBUG1, "db2_fdw: restart foreign table scan");

  /* close open DB2 statement if there is one */
  db2CloseStatement (fdw_state->session);

  /* reset row count to zero */
  fdw_state->rowcount = 0;
}

/*
 * db2AddForeignUpdateTargets
 * 		Add the primary key columns as resjunk entries.
 */
void
db2AddForeignUpdateTargets (
#if PG_VERSION_NUM < 140000
        Query *parsetree,
#else
        PlannerInfo *root,
        Index rtindex,
#endif
        RangeTblEntry *target_rte,
        Relation target_relation
)
{
  Oid relid = RelationGetRelid (target_relation);
  TupleDesc tupdesc = target_relation->rd_att;
  int i;
  bool has_key = false;

  elog (DEBUG1, "db2_fdw: add target columns for update on %d", relid);

  /* loop through all columns of the foreign table */
  for (i = 0; i < tupdesc->natts; ++i) {
    Form_pg_attribute att = TupleDescAttr (tupdesc, i);
    AttrNumber attrno = att->attnum;
    List *options;
    ListCell *option;

    /* look for the "key" option on this column */
    options = GetForeignColumnOptions (relid, attrno);
    foreach (option, options) {
      DefElem *def = (DefElem *) lfirst (option);

      /* if "key" is set, add a resjunk for this column */
      if (strcmp (def->defname, OPT_KEY) == 0) {
	if (optionIsTrue (STRVAL(def->arg))) {
	  Var *var;
#if PG_VERSION_NUM < 140000
          TargetEntry *tle;

          /* Make a Var representing the desired value */
          var = makeVar(
            parsetree->resultRelation,
            attrno,
            att->atttypid,
            att->atttypmod,
            att->attcollation,
            0);

          /* Wrap it in a resjunk TLE with the right name ... */
          tle = makeTargetEntry((Expr *)var,
            list_length(parsetree->targetList) + 1,
            pstrdup(NameStr(att->attname)),
            true);

          /* ... and add it to the query's targetlist */
          parsetree->targetList = lappend(parsetree->targetList, tle);
#else
          /* Make a Var representing the desired value */
          var = makeVar(
            rtindex,
            attrno,
            att->atttypid,
            att->atttypmod,
            att->attcollation,
            0);

          add_row_identity_var(root, var, rtindex, NameStr(att->attname));
#endif  /* PG_VERSION_NUM */
	  has_key = true;
	}
      }
      else {
	elog (ERROR, "impossible column option \"%s\"", def->defname);
      }
    }
  }

  if (!has_key)
    ereport (ERROR,
	     (errcode (ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
	      errmsg ("no primary key column specified for foreign DB2 table"),
	      errdetail ("For UPDATE or DELETE, at least one foreign table column must be marked as primary key column."),
	      errhint ("Set the option \"%s\" on the columns that belong to the primary key.", OPT_KEY)));
}

/*
 * db2PlanForeignModify
 * 		Construct an DB2FdwState or copy it from the foreign scan plan.
 * 		Construct the DB2 DML statement and a list of necessary parameters.
 * 		Return the serialized DB2FdwState.
 */
List *
db2PlanForeignModify (PlannerInfo * root, ModifyTable * plan, Index resultRelation, int subplan_index)
{
  CmdType operation = plan->operation;
  RangeTblEntry *rte = planner_rt_fetch (resultRelation, root);
  Relation rel = NULL;
  StringInfoData sql;
  List *targetAttrs = NIL;
  List *returningList = NIL;
  struct DB2FdwState *fdwState;
  int attnum, i;
  ListCell *cell;
  bool has_trigger = false, firstcol;
  struct paramDesc *param;
  char paramName[10];
  TupleDesc tupdesc;
  Bitmapset *updated_cols;
  AttrNumber col;
  int col_idx = -1;

/*
* Get the updated columns and the user for permission checks.
* We put that here at the beginning, since the way to do that changed
* considerably over the different PostgreSQL versions.
*/
#if PG_VERSION_NUM >= 160000
  RTEPermissionInfo *perminfo = getRTEPermissionInfo(root->parse->rteperminfos, rte);
  updated_cols = bms_copy(perminfo->updatedCols);
#else
#if PG_VERSION_NUM >= 90500
  updated_cols = bms_copy(rte->updatedCols);
#else
  updated_cols = bms_copy(rte->modifiedCols);
#endif  /* PG_VERSION_NUM >= 90500 */
#endif  /* PG_VERSION_NUM >= 160000 */

#if PG_VERSION_NUM >= 90500
/* we don't support INSERT ... ON CONFLICT */
  if (plan->onConflictAction != ONCONFLICT_NONE)
    ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION), errmsg("INSERT with ON CONFLICT clause is not supported")));
#endif  /* PG_VERSION_NUM */

/* check if the foreign table is scanned and we already planned that scan */
  if (resultRelation < root->simple_rel_array_size
      && root->simple_rel_array[resultRelation] != NULL
      && root->simple_rel_array[resultRelation]->fdw_private != NULL) {
/* if yes, copy the foreign table information from the associated RelOptInfo */
    fdwState = copyPlanData((struct DB2FdwState *)(root->simple_rel_array[resultRelation]->fdw_private));
  } else {
/*
* If no, we have to construct the foreign table data ourselves.
* To match what ExecCheckRTEPerms does, pass the user whose user mapping
* should be used (if invalid, the current user is used).
*/
    fdwState = getFdwState(rte->relid, NULL);
  }
  initStringInfo(&sql);

/*
* Core code already has some lock on each rel being planned, so we can
* use NoLock here.
*/
    rel = table_open(rte->relid, NoLock);

/* figure out which attributes are affected and if there is a trigger */

  /* figure out which attributes are affected and if there is a trigger */
  switch (operation) {
  case CMD_INSERT:
    /*
     * In an INSERT, we transmit all columns that are defined in the foreign
     * table.  In an UPDATE, we transmit only columns that were explicitly
     * targets of the UPDATE, so as to avoid unnecessary data transmission.
     * (We can't do that for INSERT since we would miss sending default values
     * for columns not listed in the source statement.)
     */

    tupdesc = RelationGetDescr (rel);

    for (attnum = 1; attnum <= tupdesc->natts; attnum++) {
      Form_pg_attribute attr = TupleDescAttr (tupdesc, attnum - 1);

      if (!attr->attisdropped)
	targetAttrs = lappend_int (targetAttrs, attnum);
    }

    /* is there a row level AFTER trigger? */
    has_trigger = rel->trigdesc && rel->trigdesc->trig_insert_after_row;

    break;
  case CMD_UPDATE:
    while ((col_idx = bms_next_member (updated_cols,col_idx)) >= 0) {
      col = col_idx + FirstLowInvalidHeapAttributeNumber;
      if (col <= InvalidAttrNumber)	/* shouldn't happen */
	elog (ERROR, "system-column update is not supported");
      targetAttrs = lappend_int (targetAttrs, col);
    }

    /* is there a row level AFTER trigger? */
    has_trigger = rel->trigdesc && rel->trigdesc->trig_update_after_row;

    break;
  case CMD_DELETE:

    /* is there a row level AFTER trigger? */
    has_trigger = rel->trigdesc && rel->trigdesc->trig_delete_after_row;

    break;
  default:
    elog (ERROR, "unexpected operation: %d", (int) operation);
  }

  table_close (rel, NoLock);

  /* mark all attributes for which we need a RETURNING clause */
  if (has_trigger) {
    /* all attributes are needed for the RETURNING clause */
    for (i = 0; i < fdwState->db2Table->ncols; ++i)
      if (fdwState->db2Table->cols[i]->pgname != NULL) {
	/* throw an error if it is a LONG or LONG RAW column */
	if (fdwState->db2Table->cols[i]->db2type == SQL_TYPE_BIG)
	  ereport (ERROR,
		   (errcode (ERRCODE_FDW_INVALID_DATA_TYPE),
		    errmsg ("columns with DB2 type LONG or LONG RAW cannot be used in RETURNING clause"),
		    errdetail ("Column \"%s\" of foreign table \"%s\" is of DB2 type LONG%s.",
			       fdwState->db2Table->cols[i]->pgname, fdwState->db2Table->pgname, fdwState->db2Table->cols[i]->db2type == SQL_TYPE_BIG ? "" : " RAW")));

	fdwState->db2Table->cols[i]->used = 1;
      }
  }
  else {
    Bitmapset *attrs_used = NULL;

    /* extract the relevant RETURNING list if any */
    if (plan->returningLists)
      returningList = (List *) list_nth (plan->returningLists, subplan_index);

    if (returningList != NIL) {
      /* get all the attributes mentioned there */
      pull_varattnos ((Node *) returningList, resultRelation, &attrs_used);

      /* mark the corresponding columns as used */
      for (i = 0; i < fdwState->db2Table->ncols; ++i) {
	/* ignore columns that are not in the PostgreSQL table */
	if (fdwState->db2Table->cols[i]->pgname == NULL)
	  continue;

	if (bms_is_member (fdwState->db2Table->cols[i]->pgattnum - FirstLowInvalidHeapAttributeNumber, attrs_used)) {
	  /* throw an error if it is a LONG or LONG RAW column */
	  fdwState->db2Table->cols[i]->used = 1;
	}
      }
    }
  }

  /* construct the SQL command string */
  switch (operation) {
  case CMD_INSERT:
    appendStringInfo (&sql, "INSERT INTO %s (", fdwState->db2Table->name);

    firstcol = true;
    for (i = 0; i < fdwState->db2Table->ncols; ++i) {
      /* don't add columns beyond the end of the PostgreSQL table */
      if (fdwState->db2Table->cols[i]->pgname == NULL)
	continue;

      if (firstcol)
	firstcol = false;
      else
	appendStringInfo (&sql, ", ");
      appendStringInfo (&sql, "%s", fdwState->db2Table->cols[i]->name);
    }

    appendStringInfo (&sql, ") VALUES (");

    firstcol = true;
    for (i = 0; i < fdwState->db2Table->ncols; ++i) {
      /* don't add columns beyond the end of the PostgreSQL table */
      if (fdwState->db2Table->cols[i]->pgname == NULL)
	continue;

      /* check that the data types can be converted */
      checkDataType (fdwState->db2Table->cols[i]->db2type,
		     fdwState->db2Table->cols[i]->scale, fdwState->db2Table->cols[i]->pgtype, fdwState->db2Table->pgname, fdwState->db2Table->cols[i]->pgname);

      /* add a parameter description for the column */
      snprintf (paramName, 9, ":p%d", fdwState->db2Table->cols[i]->pgattnum);
      addParam (&fdwState->paramList, paramName, fdwState->db2Table->cols[i]->pgtype, fdwState->db2Table->cols[i]->db2type, i);

      /* add parameter name */
      if (firstcol)
	firstcol = false;
      else
	appendStringInfo (&sql, ", ");

      appendAsType (&sql, paramName, fdwState->db2Table->cols[i]->pgtype);
    }

    appendStringInfo (&sql, ")");

    break;
  case CMD_UPDATE:
    appendStringInfo (&sql, "UPDATE %s SET ", fdwState->db2Table->name);

    firstcol = true;
    i = 0;
    foreach (cell, targetAttrs) {
      /* find the corresponding db2Table entry */
      while (i < fdwState->db2Table->ncols && fdwState->db2Table->cols[i]->pgattnum < lfirst_int (cell))
	++i;
      if (i == fdwState->db2Table->ncols)
	break;

      /* ignore columns that don't occur in the foreign table */
      if (fdwState->db2Table->cols[i]->pgtype == 0)
	continue;

      /* check that the data types can be converted */
      checkDataType (fdwState->db2Table->cols[i]->db2type,
		     fdwState->db2Table->cols[i]->scale, fdwState->db2Table->cols[i]->pgtype, fdwState->db2Table->pgname, fdwState->db2Table->cols[i]->pgname);

      /* add a parameter description for the column */
      snprintf (paramName, 9, ":p%d", lfirst_int (cell));
      addParam (&fdwState->paramList, paramName, fdwState->db2Table->cols[i]->pgtype, fdwState->db2Table->cols[i]->db2type, i);

      /* add the parameter name to the query */
      if (firstcol)
	firstcol = false;
      else
	appendStringInfo (&sql, ", ");

      appendStringInfo (&sql, "%s = ", fdwState->db2Table->cols[i]->name);
      appendAsType (&sql, paramName, fdwState->db2Table->cols[i]->pgtype);
    }

    /* throw a meaningful error if nothing is updated */
    if (firstcol)
      ereport (ERROR,
	       (errcode (ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
		errmsg ("no DB2 column modified by UPDATE"), errdetail ("The UPDATE statement only changes colums that do not exist in the DB2 table.")));

    break;
  case CMD_DELETE:
    appendStringInfo (&sql, "DELETE FROM %s", fdwState->db2Table->name);

    break;
  default:
    elog (ERROR, "unexpected operation: %d", (int) operation);
  }

  if (operation == CMD_UPDATE || operation == CMD_DELETE) {
    /* add WHERE clause with the primary key columns */

    firstcol = true;
    for (i = 0; i < fdwState->db2Table->ncols; ++i) {
      if (fdwState->db2Table->cols[i]->pkey) {
	/* add a parameter description */
	snprintf (paramName, 9, ":k%d", fdwState->db2Table->cols[i]->pgattnum);
	addParam (&fdwState->paramList, paramName, fdwState->db2Table->cols[i]->pgtype, fdwState->db2Table->cols[i]->db2type, i);

	/* add column and parameter name to query */
	if (firstcol) {
	  appendStringInfo (&sql, " WHERE");
	  firstcol = false;
	}
	else
	  appendStringInfo (&sql, " AND");

	appendStringInfo (&sql, " %s = ", fdwState->db2Table->cols[i]->name);
	appendAsType (&sql, paramName, fdwState->db2Table->cols[i]->pgtype);
      }
    }
  }

  /* add RETURNING clause if appropriate */
  firstcol = true;
  for (i = 0; i < fdwState->db2Table->ncols; ++i)
    if (fdwState->db2Table->cols[i]->used) {
      if (firstcol) {
	firstcol = false;
	appendStringInfo (&sql, " RETURNING ");
      }
      else
	appendStringInfo (&sql, ", ");
      appendStringInfo (&sql, "%s", fdwState->db2Table->cols[i]->name);
    }

  /* add the parameters for the RETURNING clause */
  firstcol = true;
  for (i = 0; i < fdwState->db2Table->ncols; ++i)
    if (fdwState->db2Table->cols[i]->used) {
      /* check that the data types can be converted */
      checkDataType (fdwState->db2Table->cols[i]->db2type,
		     fdwState->db2Table->cols[i]->scale, fdwState->db2Table->cols[i]->pgtype, fdwState->db2Table->pgname, fdwState->db2Table->cols[i]->pgname);

      /* create a new entry in the parameter list */
      param = (struct paramDesc *) palloc (sizeof (struct paramDesc));
      snprintf (paramName, 9, ":r%d", fdwState->db2Table->cols[i]->pgattnum);
      param->name = pstrdup (paramName);
      param->type = fdwState->db2Table->cols[i]->pgtype;
      param->bindType = BIND_OUTPUT;
      param->value = NULL;
      param->node = NULL;
      param->bindh = NULL;
      param->colnum = i;
      param->next = fdwState->paramList;
      fdwState->paramList = param;

      if (firstcol) {
	firstcol = false;
	appendStringInfo (&sql, " INTO ");
      }
      else
	appendStringInfo (&sql, ", ");
      appendStringInfo (&sql, "%s", paramName);
    }

  fdwState->query = sql.data;

  elog (DEBUG1, "db2_fdw: remote statement is: %s", fdwState->query);

  /* return a serialized form of the plan state */
  return serializePlanData (fdwState);
}

/*
 * db2BeginForeignModify
 * 		Prepare everything for the DML query:
 * 		The SQL statement is prepared, the type output functions for
 * 		the parameters are fetched, and the column numbers of the
 * 		resjunk attributes are stored in the "pkey" field.
 */
void
db2BeginForeignModify (ModifyTableState * mtstate, ResultRelInfo * rinfo, List * fdw_private, int subplan_index, int eflags)
{
  struct DB2FdwState *fdw_state = deserializePlanData (fdw_private);
  EState *estate = mtstate->ps.state;
  struct paramDesc *param;
  HeapTuple tuple;
  int i;
#if PG_VERSION_NUM < 140000
        Plan *subplan = mtstate->mt_plans[subplan_index]->plan;
#else
        Plan *subplan = outerPlanState(mtstate)->plan;
#endif

  elog (DEBUG1, "db2_fdw: begin foreign table modify on %d", RelationGetRelid (rinfo->ri_RelationDesc));

  rinfo->ri_FdwState = fdw_state;

  /* connect to DB2 database */
  fdw_state->session = db2GetSession (fdw_state->dbserver, fdw_state->user, fdw_state->password, fdw_state->nls_lang, fdw_state->db2Table->pgname, GetCurrentTransactionNestLevel ()
    );

  db2PrepareQuery (fdw_state->session, fdw_state->query, fdw_state->db2Table, 0);

  /* get the type output functions for the parameters */
  output_funcs = (regproc *) palloc0 (fdw_state->db2Table->ncols * sizeof (regproc *));
  for (param = fdw_state->paramList; param != NULL; param = param->next) {
    /* ignore output parameters */
    if (param->bindType == BIND_OUTPUT)
      continue;

    tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (fdw_state->db2Table->cols[param->colnum]->pgtype));
    if (!HeapTupleIsValid (tuple))
      elog (ERROR, "cache lookup failed for type %u", fdw_state->db2Table->cols[param->colnum]->pgtype);
    output_funcs[param->colnum] = ((Form_pg_type) GETSTRUCT (tuple))->typoutput;
    ReleaseSysCache (tuple);
  }

  /* loop through table columns */
  for (i = 0; i < fdw_state->db2Table->ncols; ++i) {
    if (!fdw_state->db2Table->cols[i]->pkey)
      continue;

    /* for primary key columns, get the resjunk attribute number and store it in "pkey" */
    fdw_state->db2Table->cols[i]->pkey = ExecFindJunkAttributeInTlist (subplan->targetlist, fdw_state->db2Table->cols[i]->pgname);
  }

  /* create a memory context for short-lived memory */
  fdw_state->temp_cxt = AllocSetContextCreate (estate->es_query_cxt, "db2_fdw temporary data", ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);
}

/*
 * db2ExecForeignInsert
 * 		Set the parameter values from the slots and execute the INSERT statement.
 * 		Returns a slot with the results from the RETRUNING clause.
 */
TupleTableSlot *
db2ExecForeignInsert (EState * estate, ResultRelInfo * rinfo, TupleTableSlot * slot, TupleTableSlot * planSlot)
{
  struct DB2FdwState *fdw_state = (struct DB2FdwState *) rinfo->ri_FdwState;
  int rows;
  MemoryContext oldcontext;

  elog (DEBUG3, "db2_fdw: execute foreign table insert on %d", RelationGetRelid (rinfo->ri_RelationDesc));

  ++fdw_state->rowcount;
  dml_in_transaction = true;

  MemoryContextReset (fdw_state->temp_cxt);
  oldcontext = MemoryContextSwitchTo (fdw_state->temp_cxt);

  /* extract the values from the slot and store them in the parameters */
  setModifyParameters (fdw_state->paramList, slot, planSlot, fdw_state->db2Table, fdw_state->session);

  /* execute the INSERT statement and store RETURNING values in db2Table's columns */
  rows = db2ExecuteQuery (fdw_state->session, fdw_state->db2Table, fdw_state->paramList);

  if (rows != 1)
    ereport (ERROR, (errcode (ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION), errmsg ("INSERT on DB2 table added %d rows instead of one in iteration %lu", rows, fdw_state->rowcount)));

  MemoryContextSwitchTo (oldcontext);

  /* empty the result slot */
  ExecClearTuple (slot);

  /* convert result for RETURNING to arrays of values and null indicators */
  convertTuple (fdw_state, slot->tts_values, slot->tts_isnull, false);

  /* store the virtual tuple */
  ExecStoreVirtualTuple (slot);

  return slot;
}

/*
 * db2ExecForeignUpdate
 * 		Set the parameter values from the slots and execute the UPDATE statement.
 * 		Returns a slot with the results from the RETRUNING clause.
 */
TupleTableSlot *
db2ExecForeignUpdate (EState * estate, ResultRelInfo * rinfo, TupleTableSlot * slot, TupleTableSlot * planSlot)
{
  struct DB2FdwState *fdw_state = (struct DB2FdwState *) rinfo->ri_FdwState;
  int rows;
  MemoryContext oldcontext;

  elog (DEBUG3, "db2_fdw: execute foreign table update on %d", RelationGetRelid (rinfo->ri_RelationDesc));

  ++fdw_state->rowcount;
  dml_in_transaction = true;

  MemoryContextReset (fdw_state->temp_cxt);
  oldcontext = MemoryContextSwitchTo (fdw_state->temp_cxt);

  /* extract the values from the slot and store them in the parameters */
  setModifyParameters (fdw_state->paramList, slot, planSlot, fdw_state->db2Table, fdw_state->session);

  /* execute the UPDATE statement and store RETURNING values in db2Table's columns */
  rows = db2ExecuteQuery (fdw_state->session, fdw_state->db2Table, fdw_state->paramList);

  if (rows != 1)
    ereport (ERROR,
	     (errcode (ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
	      errmsg ("UPDATE on DB2 table changed %d rows instead of one in iteration %lu", rows, fdw_state->rowcount),
	      errhint ("This probably means that you did not set the \"key\" option on all primary key columns.")));

  MemoryContextSwitchTo (oldcontext);

  /* empty the result slot */
  ExecClearTuple (slot);

  /* convert result for RETURNING to arrays of values and null indicators */
  convertTuple (fdw_state, slot->tts_values, slot->tts_isnull, false);

  /* store the virtual tuple */
  ExecStoreVirtualTuple (slot);

  return slot;
}

/*
 * db2ExecForeignDelete
 * 		Set the parameter values from the slots and execute the DELETE statement.
 * 		Returns a slot with the results from the RETRUNING clause.
 */
TupleTableSlot *
db2ExecForeignDelete (EState * estate, ResultRelInfo * rinfo, TupleTableSlot * slot, TupleTableSlot * planSlot)
{
  struct DB2FdwState *fdw_state = (struct DB2FdwState *) rinfo->ri_FdwState;
  int rows;
  MemoryContext oldcontext;

  elog (DEBUG3, "db2_fdw: execute foreign table delete on %d", RelationGetRelid (rinfo->ri_RelationDesc));

  ++fdw_state->rowcount;
  dml_in_transaction = true;

  MemoryContextReset (fdw_state->temp_cxt);
  oldcontext = MemoryContextSwitchTo (fdw_state->temp_cxt);

  /* extract the values from the slot and store them in the parameters */
  setModifyParameters (fdw_state->paramList, slot, planSlot, fdw_state->db2Table, fdw_state->session);

  /* execute the DELETE statement and store RETURNING values in db2Table's columns */
  rows = db2ExecuteQuery (fdw_state->session, fdw_state->db2Table, fdw_state->paramList);

  if (rows != 1)
    ereport (ERROR,
	     (errcode (ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
	      errmsg ("DELETE on DB2 table removed %d rows instead of one in iteration %lu", rows, fdw_state->rowcount),
	      errhint ("This probably means that you did not set the \"key\" option on all primary key columns.")));

  MemoryContextSwitchTo (oldcontext);

  /* empty the result slot */
  ExecClearTuple (slot);

  /* convert result for RETURNING to arrays of values and null indicators */
  convertTuple (fdw_state, slot->tts_values, slot->tts_isnull, false);

  /* store the virtual tuple */
  ExecStoreVirtualTuple (slot);

  return slot;
}

/*
 * db2EndForeignModify
 * 		Close the currently active DB2 statement.
 */
void
db2EndForeignModify (EState * estate, ResultRelInfo * rinfo)
{
  struct DB2FdwState *fdw_state = (struct DB2FdwState *) rinfo->ri_FdwState;

  elog (DEBUG1, "db2_fdw: end foreign table modify on %d", RelationGetRelid (rinfo->ri_RelationDesc));

  MemoryContextDelete (fdw_state->temp_cxt);

  /* release the DB2 session */
  db2CloseStatement (fdw_state->session);
  pfree (fdw_state->session);
  fdw_state->session = NULL;
}

/*
 * db2ExplainForeignModify
 * 		Show the DB2 DML statement.
 * 		Nothing special is done for VERBOSE because the query plan is likely trivial.
 */
void
db2ExplainForeignModify (ModifyTableState * mtstate, ResultRelInfo * rinfo, List * fdw_private, int subplan_index, struct ExplainState *es)
{
  struct DB2FdwState *fdw_state = (struct DB2FdwState *) rinfo->ri_FdwState;

  elog (DEBUG1, "db2_fdw: explain foreign table modify on %d", RelationGetRelid (rinfo->ri_RelationDesc));

  /* show query */
  ExplainPropertyText ("DB2 statement", fdw_state->query, es);
}

/*
 * db2IsForeignRelUpdatable
 * 		Returns 0 if "readonly" is set, a value indicating that all DML is allowed.
 */
int
db2IsForeignRelUpdatable (Relation rel)
{
  ListCell *cell;

  /* loop foreign table options */
  foreach (cell, GetForeignTable (RelationGetRelid (rel))->options) {
    DefElem *def = (DefElem *) lfirst (cell);
    char *value = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_READONLY) == 0 && optionIsTrue (value))
      return 0;
  }

  return (1 << CMD_UPDATE) | (1 << CMD_INSERT) | (1 << CMD_DELETE);
}

/*
 * db2ImportForeignSchema
 * 		Returns a List of CREATE FOREIGN TABLE statements.
 */
List *
db2ImportForeignSchema (ImportForeignSchemaStmt * stmt, Oid serverOid)
{
  ForeignServer *server;
  UserMapping *mapping;
  ForeignDataWrapper *wrapper;
  char *tabname, *colname, oldtabname[129] = { '\0' }, *foldedname;
  char *nls_lang = NULL, *user = NULL, *password = NULL, *dbserver = NULL;
  db2Type type;
  int charlen, typeprec, typescale, nullable, key, rc;
  List *options, *result = NIL;
  ListCell *cell;
  db2Session *session;
  fold_t foldcase = CASE_SMART;
  StringInfoData buf;
  bool readonly = false, firstcol = true;

  /* get the foreign server, the user mapping and the FDW */
  server = GetForeignServer (serverOid);
  mapping = GetUserMapping (GetUserId (), serverOid);
  wrapper = GetForeignDataWrapper (server->fdwid);

  /* get all options for these objects */
  options = wrapper->options;
  options = list_concat (options, server->options);
  options = list_concat (options, mapping->options);

  foreach (cell, options) {
    DefElem *def = (DefElem *) lfirst (cell);
    if (strcmp (def->defname, OPT_NLS_LANG) == 0)
      nls_lang = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_DBSERVER) == 0)
      dbserver = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_USER) == 0)
      user = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_PASSWORD) == 0)
      password = STRVAL(def->arg);
  }

  /* process the options of the IMPORT FOREIGN SCHEMA command */
  foreach (cell, stmt->options) {
    DefElem *def = (DefElem *) lfirst (cell);

    if (strcmp (def->defname, "case") == 0) {
      char *s = STRVAL(def->arg);
      if (strcmp (s, "keep") == 0)
	foldcase = CASE_KEEP;
      else if (strcmp (s, "lower") == 0)
	foldcase = CASE_LOWER;
      else if (strcmp (s, "smart") == 0)
	foldcase = CASE_SMART;
      else
	ereport (ERROR,
		 (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
		  errmsg ("invalid value for option \"%s\"", def->defname), errhint ("Valid values in this context are: %s", "keep, lower, smart")));
      continue;
    }
    else if (strcmp (def->defname, "readonly") == 0) {
      char *s = STRVAL(def->arg);
      if (pg_strcasecmp (s, "on") != 0 || pg_strcasecmp (s, "yes") != 0 || pg_strcasecmp (s, "true") != 0)
	readonly = true;
      else if (pg_strcasecmp (s, "off") != 0 || pg_strcasecmp (s, "no") != 0 || pg_strcasecmp (s, "false") != 0)
	readonly = false;
      else
	ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("invalid value for option \"%s\"", def->defname)));
      continue;
    }

    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_OPTION_NAME), errmsg ("invalid option \"%s\"", def->defname), errhint ("Valid options in this context are: %s", "case, readonly")));
  }

  elog (DEBUG1, "db2_fdw: import schema \"%s\" from foreign server \"%s\"", stmt->remote_schema, server->servername);

  /* guess a good NLS_LANG environment setting */
  nls_lang = guessNlsLang (nls_lang);

  /* connect to DB2 database */
  session = db2GetSession (dbserver, user, password, nls_lang, NULL, 1);

  initStringInfo (&buf);
  do {
    /* get the next column definition */
    rc = db2GetImportColumn (session, stmt->remote_schema, &tabname, &colname, &type, &charlen, &typeprec, &typescale, &nullable, &key);

    if (rc == -1) {
      /* remote schema does not exist, issue a warning */
      ereport (ERROR,
	       (errcode (ERRCODE_FDW_SCHEMA_NOT_FOUND),
		errmsg ("remote schema \"%s\" does not exist", stmt->remote_schema), errhint ("Enclose the schema name in double quotes to prevent case folding.")));

      return NIL;
    }

    if ((rc == 0 && oldtabname[0] != '\0')
	|| (rc == 1 && oldtabname[0] != '\0' && strcmp (tabname, oldtabname))) {
      /* finish previous CREATE FOREIGN TABLE statement */
      appendStringInfo (&buf, ") SERVER \"%s\" OPTIONS (schema '%s', table '%s'", server->servername, stmt->remote_schema, oldtabname);
      if (readonly)
	appendStringInfo (&buf, ", readonly 'true'");
      appendStringInfo (&buf, ")");

      result = lappend (result, pstrdup (buf.data));
    }

    if (rc == 1 && (oldtabname[0] == '\0' || strcmp (tabname, oldtabname))) {
      /* start a new CREATE FOREIGN TABLE statement */
      resetStringInfo (&buf);
      foldedname = fold_case (tabname, foldcase);
      appendStringInfo (&buf, "CREATE FOREIGN TABLE \"%s\" (", foldedname);
      pfree (foldedname);

      firstcol = true;
      strcpy (oldtabname, tabname);
    }

    if (rc == 1) {
      /*
       * Add a column definition.
       */

      if (firstcol)
	firstcol = false;
      else
	appendStringInfo (&buf, ", ");

      /* column name */
      foldedname = fold_case (colname, foldcase);
      appendStringInfo (&buf, "\"%s\" ", foldedname);
      pfree (foldedname);

      /* data type */
      switch (type) {
      case SQL_TYPE_CHAR:
	appendStringInfo (&buf, "character(%d)", charlen == 0 ? 1 : charlen);
	break;
      case SQL_TYPE_VARCHAR:
	appendStringInfo (&buf, "character varying(%d)", charlen == 0 ? 1 : charlen);
	break;
      case SQL_TYPE_CLOB:
      case SQL_TYPE_VARGRAPHIC:
      case SQL_TYPE_GRAPHIC:
	appendStringInfo (&buf, "text");
	break;
      case SQL_TYPE_SMALL:
	appendStringInfo (&buf, "smallint");
	break;
      case SQL_TYPE_INTEGER:
	appendStringInfo (&buf, "integer");
	break;
      case SQL_TYPE_BIG:
	appendStringInfo (&buf, "bigint");
	break;
      case SQL_TYPE_FLOAT:
      case SQL_TYPE_DECIMAL:
      case SQL_TYPE_DOUBLE:
      case SQL_TYPE_REAL:
	if (typeprec < 54)
          if (typeprec == 0)
	    appendStringInfo (&buf, "float(1)");
          else if(typeprec < 0)
	    appendStringInfo (&buf, "float(1)");
	  else
	    appendStringInfo (&buf, "float(%d)", typeprec);
	else
	  appendStringInfo (&buf, "numeric");
	break;
      case SQL_TYPE_XML:
	appendStringInfo (&buf, "bytea");
	break;
      case SQL_TYPE_BLOB:
	appendStringInfo (&buf, "bytea");
	break;
      case SQL_TYPE_DATE:
	appendStringInfo (&buf, "date");
	break;
      case SQL_TYPE_STAMP:
	appendStringInfo (&buf, "timestamp(%d) without time zone", (typescale > 6) ? 6 : typescale);
	break;
      case SQL_TYPE_TIME:
	appendStringInfo (&buf, "time(%d) without time zone", (typescale > 6) ? 6 : typescale);
	break;
      default:
	elog (DEBUG2, "column \"%s\" of table \"%s\" has an untranslatable data type", colname, tabname);
	appendStringInfo (&buf, "text");
      }

      /* part of the primary key */
      if (key)
	appendStringInfo (&buf, " OPTIONS (key 'true')");

      /* not nullable */
      if (!nullable)
	appendStringInfo (&buf, " NOT NULL");
    }
  }
  while (rc == 1);

  return result;
}

/*
 * getFdwState
 * 		Construct an DB2FdwState from the options of the foreign table.
 * 		Establish an DB2 connection and get a description of the
 * 		remote table.
 * 		"sample_percent" is set from the foreign table options.
 * 		"sample_percent" can be NULL, in that case it is not set.
 */
struct DB2FdwState *
getFdwState (Oid foreigntableid, double *sample_percent)
{
  struct DB2FdwState *fdwState = palloc0 (sizeof (struct DB2FdwState));
  char *pgtablename = get_rel_name (foreigntableid);
  List *options;
  ListCell *cell;
  char *schema = NULL, *table = NULL, *maxlong = NULL, *sample = NULL, *fetch = NULL, *noencerr = NULL;
  long max_long;

  /*
   * Get all relevant options from the foreign table, the user mapping,
   * the foreign server and the foreign data wrapper.
   */
  db2GetOptions (foreigntableid, &options);
  foreach (cell, options) {
    DefElem *def = (DefElem *) lfirst (cell);
    if (strcmp (def->defname, OPT_NLS_LANG) == 0)
      fdwState->nls_lang = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_DBSERVER) == 0)
      fdwState->dbserver = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_USER) == 0)
      fdwState->user = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_PASSWORD) == 0)
      fdwState->password = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_SCHEMA) == 0)
      schema = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_TABLE) == 0)
      table = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_MAX_LONG) == 0)
      maxlong = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_SAMPLE) == 0)
      sample = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_PREFETCH) == 0)
      fetch = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_NO_ENCODING_ERROR) == 0)
      noencerr = STRVAL(def->arg);
  }

  /* convert "max_long" option to number or use default */
  if (maxlong == NULL)
    max_long = DEFAULT_MAX_LONG;
  else
    max_long = strtol (maxlong, NULL, 0);

  /* convert "sample_percent" to double */
  if (sample_percent != NULL) {
    if (sample == NULL)
      *sample_percent = 100.0;
    else
      *sample_percent = strtod (sample, NULL);
  }

  /* convert "prefetch" to number (or use default) */
  if (fetch == NULL)
    fdwState->prefetch = DEFAULT_PREFETCH;
  else
    fdwState->prefetch = (unsigned int) strtoul (fetch, NULL, 0);

  /* check if options are ok */
  if (table == NULL)
    ereport (ERROR, (errcode (ERRCODE_FDW_OPTION_NAME_NOT_FOUND), errmsg ("required option \"%s\" in foreign table \"%s\" missing", OPT_TABLE, pgtablename)));

  /* guess a good NLS_LANG environment setting */
  fdwState->nls_lang = guessNlsLang (fdwState->nls_lang);

  /* connect to DB2 database */
  fdwState->session = db2GetSession (fdwState->dbserver, fdwState->user, fdwState->password, fdwState->nls_lang, pgtablename,
				     GetCurrentTransactionNestLevel ()
    );

  /* get remote table description */
  fdwState->db2Table = db2Describe (fdwState->session, schema, table, pgtablename, max_long, noencerr);

  /* add PostgreSQL data to table description */
  getColumnData (fdwState->db2Table, foreigntableid);

  return fdwState;
}

/*
 * db2GetOptions
 * 		Fetch the options for an db2_fdw foreign table.
 * 		Returns a union of the options of the foreign data wrapper,
 * 		the foreign server, the user mapping and the foreign table,
 * 		in that order.  Column options are ignored.
 */
void
db2GetOptions (Oid foreigntableid, List ** options)
{
  ForeignTable *table;
  ForeignServer *server;
  UserMapping *mapping;
  ForeignDataWrapper *wrapper;

  /*
   * Gather all data for the foreign table.
   */
  table = GetForeignTable (foreigntableid);
  server = GetForeignServer (table->serverid);
  mapping = GetUserMapping (GetUserId (), table->serverid);
  wrapper = GetForeignDataWrapper (server->fdwid);

  /* later options override earlier ones */
  *options = NIL;
  *options = list_concat (*options, wrapper->options);
  *options = list_concat (*options, server->options);
  if (mapping != NULL)
    *options = list_concat (*options, mapping->options);
  *options = list_concat (*options, table->options);
}

/*
 * getColumnData
 * 		Get PostgreSQL column name and number, data type and data type modifier.
 * 		Set db2Table->npgcols.
 * 		For PostgreSQL 9.2 and better, find the primary key columns and mark them in db2Table.
 */
void
getColumnData (struct db2Table *db2Table, Oid foreigntableid)
{
  Relation rel;
  TupleDesc tupdesc;
  int i, index;

  rel = table_open (foreigntableid, NoLock);
  tupdesc = rel->rd_att;

  /* number of PostgreSQL columns */
  db2Table->npgcols = tupdesc->natts;

  /* loop through foreign table columns */
  index = 0;
  for (i = 0; i < tupdesc->natts; ++i) {
    Form_pg_attribute att_tuple = TupleDescAttr (tupdesc, i);
    List *options;
    ListCell *option;

    /* ignore dropped columns */
    if (att_tuple->attisdropped)
      continue;

    ++index;
    /* get PostgreSQL column number and type */
    if (index <= db2Table->ncols) {
      db2Table->cols[index - 1]->pgattnum = att_tuple->attnum;
      db2Table->cols[index - 1]->pgtype = att_tuple->atttypid;
      db2Table->cols[index - 1]->pgtypmod = att_tuple->atttypmod;
      db2Table->cols[index - 1]->pgname = pstrdup (NameStr (att_tuple->attname));
    }

    /* loop through column options */
    options = GetForeignColumnOptions (foreigntableid, att_tuple->attnum);
    foreach (option, options) {
      DefElem *def = (DefElem *) lfirst (option);

      /* is it the "key" option and is it set to "true" ? */
      if (strcmp (def->defname, OPT_KEY) == 0 && optionIsTrue ((STRVAL(def->arg)))) {
	/* mark the column as primary key column */
	db2Table->cols[index - 1]->pkey = 1;
      }

      /* is it the "no_encoding_error" option set ? */
      if (strcmp (def->defname, OPT_NO_ENCODING_ERROR) == 0) {
	db2Table->cols[index - 1]->noencerr = optionIsTrue ((STRVAL(def->arg))) ? NO_ENC_ERR_TRUE : NO_ENC_ERR_FALSE;
      }
    }
  }

  table_close (rel, NoLock);
}

/*
 * createQuery
 * 		Construct a query string for DB2 that
 * 		a) contains only the necessary columns in the SELECT list
 * 		b) has all the WHERE and ORDER BY clauses that can safely be translated to DB2.
 * 		Untranslatable clauses are omitted and left for PostgreSQL to check.
 * 		"query_pathkeys" contains the desired sort order of the scan results
 * 		which will be translated to ORDER BY clauses if possible.
 *		As a side effect for base relations, we also mark the used columns in db2Table.
 */
char *
createQuery (struct DB2FdwState *fdwState, RelOptInfo * foreignrel, bool modify, List * query_pathkeys)
{
  ListCell *cell;
  bool in_quote = false;
  int i, index;
  char *wherecopy, *p, md5[33], parname[10], *separator = "";
  StringInfoData query, result;
  List *columnlist, *conditions = foreignrel->baserestrictinfo;
#if PG_VERSION_NUM >= 150000
  const char *errstr = NULL;
#endif
  columnlist = foreignrel->reltarget->exprs;
#if PG_VERSION_NUM < 90600
  columnlist = foreignrel->reltargetlist;
#else
  columnlist = foreignrel->reltarget->exprs;
#endif

  if (IS_SIMPLE_REL (foreignrel))
  {
    /* find all the columns to include in the select list */

    /* examine each SELECT list entry for Var nodes */
    foreach (cell, columnlist) {
      getUsedColumns ((Expr *) lfirst (cell), fdwState->db2Table,foreignrel->relid);
    }

    /* examine each condition for Var nodes */
    foreach (cell, conditions) {
      getUsedColumns ((Expr *) lfirst (cell), fdwState->db2Table,foreignrel->relid);
    }
  }

  /* construct SELECT list */
  initStringInfo (&query);
  for (i = 0; i < fdwState->db2Table->ncols; ++i) {
    if (fdwState->db2Table->cols[i]->used) {
      StringInfoData alias;
      initStringInfo (&alias);
      /* table alias is created from range table index */
      ADD_REL_QUALIFIER (&alias, fdwState->db2Table->cols[i]->varno);

      /* add qualified column name */
      appendStringInfo (&query, "%s%s%s", separator, alias.data, fdwState->db2Table->cols[i]->name);
      separator = ", ";
    }
  }

  /* dummy column if there is no result column we need from DB2 */
  if (separator[0] == '\0')
    appendStringInfo (&query, "'1'");

  /* append FROM clause */
  appendStringInfo (&query, " FROM ");
  deparseFromExprForRel (fdwState, &query, foreignrel, &(fdwState->params));

  /*
   * For inner joins, all conditions that are pushed down get added
   * to fdwState->joinclauses and have already been added above,
   * so there is no extra WHERE clause.
   */
  if (IS_SIMPLE_REL (foreignrel))
  {
    /* append WHERE clauses */
    if (fdwState->where_clause)
      appendStringInfo (&query, "%s", fdwState->where_clause);
  }

  /* append ORDER BY clause if all its expressions can be pushed down */
  if (fdwState->order_clause)
    appendStringInfo (&query, " ORDER BY%s", fdwState->order_clause);

  /* append FOR UPDATE if if the scan is for a modification */
  if (modify)
    appendStringInfo (&query, " FOR UPDATE");

  /* get a copy of the where clause without single quoted string literals */
  wherecopy = pstrdup (query.data);
  for (p = wherecopy; *p != '\0'; ++p) {
    if (*p == '\'')
      in_quote = !in_quote;
    if (in_quote)
      *p = ' ';
  }

  /* remove all parameters that do not actually occur in the query */
  index = 0;
  foreach (cell, fdwState->params) {
    ++index;
    snprintf (parname, 10, ":p%d", index);
    if (strstr (wherecopy, parname) == NULL) {
      /* set the element to NULL to indicate it's gone */
      lfirst (cell) = NULL;
    }
  }

  pfree (wherecopy);

  /*
   * Calculate MD5 hash of the query string so far.
   * This is needed to find the query in DB2's library cache for EXPLAIN.
   */

#if PG_VERSION_NUM >= 150000
  if (!pg_md5_hash (query.data, strlen (query.data), md5,&errstr)) {
    ereport (ERROR, (errcode (ERRCODE_OUT_OF_MEMORY), errmsg ("out of memory")));
  }
#else
if (!pg_md5_hash (query.data, strlen (query.data), md5)) {
     ereport (ERROR, (errcode (ERRCODE_OUT_OF_MEMORY), errmsg ("out of memory")));
  }
#endif
  /* add comment with MD5 hash to query */
  initStringInfo (&result);
  appendStringInfo (&result, "SELECT /*%s*/ %s", md5, query.data);
  pfree (query.data);

  return result.data;
}

/*
 * deparseFromExprForRel
 * 		Construct FROM clause for given relation.
 * 		The function constructs ... JOIN ... ON ... for join relation. For a base
 * 		relation it just returns the table name.
 * 		All tables get an alias based on the range table index.
 */
static void
deparseFromExprForRel (struct DB2FdwState *fdwState, StringInfo buf, RelOptInfo * foreignrel, List ** params_list)
{
  if (IS_SIMPLE_REL (foreignrel)) {
    appendStringInfo (buf, "%s", fdwState->db2Table->name);

    appendStringInfo (buf, " %s%d", REL_ALIAS_PREFIX, foreignrel->relid);
  }
  else {
    /* join relation */
    RelOptInfo *rel_o = fdwState->outerrel;
    RelOptInfo *rel_i = fdwState->innerrel;
    StringInfoData join_sql_o;
    StringInfoData join_sql_i;
    struct DB2FdwState *fdwState_o = (struct DB2FdwState *) rel_o->fdw_private;
    struct DB2FdwState *fdwState_i = (struct DB2FdwState *) rel_i->fdw_private;

    /* Deparse outer relation */
    initStringInfo (&join_sql_o);
    deparseFromExprForRel (fdwState_o, &join_sql_o, rel_o, params_list);

    /* Deparse inner relation */
    initStringInfo (&join_sql_i);
    deparseFromExprForRel (fdwState_i, &join_sql_i, rel_i, params_list);

    /*
     * For a join relation FROM clause entry is deparsed as
     *
     * (outer relation) <join type> (inner relation) ON joinclauses
     */
    appendStringInfo (buf, "(%s %s JOIN %s ON ", join_sql_o.data, get_jointype_name (fdwState->jointype), join_sql_i.data);

    /* we can only get here if the join is pushed down, so there are join clauses */
    Assert (fdwState->joinclauses);
    appendConditions (fdwState->joinclauses, buf, foreignrel, params_list);

    /* End the FROM clause entry. */
    appendStringInfo (buf, ")");
  }
}

/*
 * appendConditions
 * 		Deparse conditions from the provided list and append them to buf.
 * 		The conditions in the list are assumed to be ANDed.
 * 		This function is used to deparse JOIN ... ON clauses.
 */
static void
appendConditions (List * exprs, StringInfo buf, RelOptInfo * joinrel, List ** params_list)
{
  ListCell *lc;
  bool is_first = true;
  char *where;

  foreach (lc, exprs) {
    Expr *expr = (Expr *) lfirst (lc);

    /* connect expressions with AND */
    if (!is_first)
      appendStringInfo (buf, " AND ");

    /* deparse and append a join condition */
    where = deparseExpr (NULL, joinrel, expr, NULL, params_list);
    appendStringInfo (buf, "%s", where);

    is_first = false;
  }
}

/*
 * foreign_join_ok
 * 		Assess whether the join between inner and outer relations can be pushed down
 * 		to the foreign server. As a side effect, save information we obtain in this
 * 		function to DB2FdwState passed in.
 */
static bool
foreign_join_ok (PlannerInfo * root, RelOptInfo * joinrel, JoinType jointype, RelOptInfo * outerrel, RelOptInfo * innerrel, JoinPathExtraData * extra)
{
  struct DB2FdwState *fdwState;
  struct DB2FdwState *fdwState_o;
  struct DB2FdwState *fdwState_i;

  struct db2Table *db2Table_o;
  struct db2Table *db2Table_i;

  ListCell *lc;
  List *otherclauses;

  char *tabname;		/* for warning messages */

  /* we only support pushing down INNER joins */
  if (jointype != JOIN_INNER)
    return false;

  fdwState = (struct DB2FdwState *) joinrel->fdw_private;
  fdwState_o = (struct DB2FdwState *) outerrel->fdw_private;
  fdwState_i = (struct DB2FdwState *) innerrel->fdw_private;
  Assert (fdwState && fdwState_o && fdwState_i);

  fdwState->outerrel = outerrel;
  fdwState->innerrel = innerrel;
  fdwState->jointype = jointype;

  /*
   * If joining relations have local conditions, those conditions are
   * required to be applied before joining the relations. Hence the join can
   * not be pushed down.
   */
  if (fdwState_o->local_conds || fdwState_i->local_conds)
    return false;

  /* Separate restrict list into join quals and quals on join relation */

  /*
   * Unlike an outer join, for inner join, the join result contains only
   * the rows which satisfy join clauses, similar to the other clause.
   * Hence all clauses can be treated the same.
   */
  otherclauses = extract_actual_clauses (extra->restrictlist, false);

  /*
   * For inner joins, "otherclauses" contains now the join conditions.
   * Check which ones can be pushed down.
   */
  foreach (lc, otherclauses) {
    char *tmp = NULL;
    Expr *expr = (Expr *) lfirst (lc);

    tmp = deparseExpr (fdwState->session, joinrel, expr, fdwState->db2Table, &(fdwState->params));

    if (tmp == NULL)
      fdwState->local_conds = lappend (fdwState->local_conds, expr);
    else
      fdwState->remote_conds = lappend (fdwState->remote_conds, expr);
  }

  /*
   * Only push down joins for which all join conditions can be pushed down.
   *
   * For an inner join it would be ok to only push own some of the join
   * conditions and evaluate the others locally, but we cannot be certain
   * that such a plan is a good or even a feasible one:
   * With one of the join conditions missing in the pushed down query,
   * it could be that the "intermediate" join result fetched from the DB2
   * side has many more rows than the complete join result.
   *
   * We could rely on estimates to see how many rows are returned from such
   * a join where not all join conditions can be pushed down, but we choose
   * the safe road of not pushing down such joins at all.
   */
  if (fdwState->local_conds != NIL)
    return false;

  /* CROSS JOIN (T1 JOIN T2 ON true) is not pushed down */
  if (fdwState->remote_conds == NIL)
    return false;

  /*
   * Pull the other remote conditions from the joining relations into join
   * clauses or other remote clauses (remote_conds) of this relation
   * wherever possible. This avoids building subqueries at every join step,
   * which is not currently supported by the deparser logic.
   *
   * For an inner join, clauses from both the relations are added to the
   * other remote clauses.
   *
   * The joining sides can not have local conditions, thus no need to test
   * shippability of the clauses being pulled up.
   */
  fdwState->remote_conds = list_concat (fdwState->remote_conds, list_copy (fdwState_i->remote_conds));
  fdwState->remote_conds = list_concat (fdwState->remote_conds, list_copy (fdwState_o->remote_conds));

  /*
   * For an inner join, all restrictions can be treated alike. Treating the
   * pushed down conditions as join conditions allows a top level full outer
   * join to be deparsed without requiring subqueries.
   */
  fdwState->joinclauses = fdwState->remote_conds;
  fdwState->remote_conds = NIL;

  /* set fetch size to minimum of the joining sides */
  if (fdwState_o->prefetch < fdwState_i->prefetch)
    fdwState->prefetch = fdwState_o->prefetch;
  else
    fdwState->prefetch = fdwState_i->prefetch;

  /* copy outerrel's infomation to fdwstate */
  fdwState->dbserver = fdwState_o->dbserver;
  fdwState->user = fdwState_o->user;
  fdwState->password = fdwState_o->password;
  fdwState->nls_lang = fdwState_o->nls_lang;

  /* construct db2Table for the result of join */
  db2Table_o = fdwState_o->db2Table;
  db2Table_i = fdwState_i->db2Table;

  fdwState->db2Table = (struct db2Table *) palloc0 (sizeof (struct db2Table));
  fdwState->db2Table->name = pstrdup ("");
  fdwState->db2Table->pgname = pstrdup ("");
  fdwState->db2Table->ncols = 0;
  fdwState->db2Table->npgcols = 0;
  fdwState->db2Table->cols = (struct db2Column **) palloc0 (sizeof (struct db2Column *) * (db2Table_o->ncols + db2Table_i->ncols));

  /*
   * Search db2Column from children's db2Table.
   * Here we assume that children are foreign table, not foreign join.
   * We need capability to track relid chain through join tree to support N-way join.
   */
  tabname = "?";
  foreach (lc, joinrel->reltarget->exprs) {
    int i;
    Var *var = (Var *) lfirst (lc);
    struct db2Column *col = NULL;
    struct db2Column *newcol;
    int used_flag = 0;

    Assert (IsA (var, Var));
    /* Find appropriate entry from children's db2Table. */
    for (i = 0; i < db2Table_o->ncols; ++i) {
      struct db2Column *tmp = db2Table_o->cols[i];

      if (tmp->varno == var->varno) {
	tabname = db2Table_o->pgname;

	if (tmp->pgattnum == var->varattno) {
	  col = tmp;
	  break;
	}
      }
    }
    if (!col) {
      for (i = 0; i < db2Table_i->ncols; ++i) {
	struct db2Column *tmp = db2Table_i->cols[i];

	if (tmp->varno == var->varno) {
	  tabname = db2Table_i->pgname;

	  if (tmp->pgattnum == var->varattno) {
	    col = tmp;
	    break;
	  }
	}
      }
    }

    newcol = (struct db2Column *) palloc0 (sizeof (struct db2Column));
    if (col) {
      memcpy (newcol, col, sizeof (struct db2Column));
      used_flag = 1;
    }
    else
      /* non-existing column, print a warning */
      ereport (WARNING,
	       (errcode (ERRCODE_WARNING), errmsg ("column number %d of foreign table \"%s\" does not exist in foreign DB2 table, will be replaced by NULL", var->varattno, tabname)));

    newcol->used = used_flag;
    /* pgattnum should be the index in SELECT clause of join query. */
    newcol->pgattnum = fdwState->db2Table->ncols + 1;

    fdwState->db2Table->cols[fdwState->db2Table->ncols++] = newcol;
  }

  fdwState->db2Table->npgcols = fdwState->db2Table->ncols;

  return true;
}

/* Output join name for given join type */
const char *
get_jointype_name (JoinType jointype)
{
  switch (jointype) {
  case JOIN_INNER:
    return "INNER";

  case JOIN_LEFT:
    return "LEFT";

  case JOIN_RIGHT:
    return "RIGHT";

  case JOIN_FULL:
    return "FULL";

  default:
    /* Shouldn't come here, but protect from buggy code. */
    elog (ERROR, "unsupported join type %d", jointype);
  }

  /* Keep compiler happy */
  return NULL;
}

/*
 * Build the targetlist for given relation to be deparsed as SELECT clause.
 *
 * The output targetlist contains the columns that need to be fetched from the
 * foreign server for the given relation.
 */
List *
build_tlist_to_deparse (RelOptInfo * foreignrel)
{
  List *tlist = NIL;
  struct DB2FdwState *fdwState = (struct DB2FdwState *) foreignrel->fdw_private;

  /*
   * We require columns specified in foreignrel->reltarget->exprs and those
   * required for evaluating the local conditions.
   */
  tlist = add_to_flat_tlist (tlist, pull_var_clause ((Node *) foreignrel->reltarget->exprs, PVC_RECURSE_PLACEHOLDERS));
  tlist = add_to_flat_tlist (tlist, pull_var_clause ((Node *) fdwState->local_conds, PVC_RECURSE_PLACEHOLDERS));

  return tlist;
}

/*
 * acquireSampleRowsFunc
 * 		Perform a sequential scan on the DB2 table and return a sampe of rows.
 * 		All LOB values are truncated to WIDTH_THRESHOLD+1 because anything
 * 		exceeding this is not used by compute_scalar_stats().
 */
int
acquireSampleRowsFunc (Relation relation, int elevel, HeapTuple * rows, int targrows, double *totalrows, double *totaldeadrows)
{
  int collected_rows = 0, i;
  struct DB2FdwState *fdw_state;
  bool first_column = true;
  StringInfoData query;
  TupleDesc tupDesc = RelationGetDescr (relation);
  Datum *values = (Datum *) palloc (tupDesc->natts * sizeof (Datum));
  bool *nulls = (bool *) palloc (tupDesc->natts * sizeof (bool));
  double rstate, rowstoskip = -1, sample_percent;
  MemoryContext old_cxt, tmp_cxt;

  elog (DEBUG1, "db2_fdw: analyze foreign table %d", RelationGetRelid (relation));

  *totalrows = 0;

  /* create a memory context for short-lived data in convertTuples() */
  tmp_cxt = AllocSetContextCreate (CurrentMemoryContext, "db2_fdw temporary data", ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);

  /* Prepare for sampling rows */
  rstate = anl_init_selection_state (targrows);

  /* get connection options, connect and get the remote table description */
  fdw_state = getFdwState (RelationGetRelid (relation), &sample_percent);
  fdw_state->paramList = NULL;
  fdw_state->rowcount = 0;

  /* construct query */
  initStringInfo (&query);
  appendStringInfo (&query, "SELECT ");

  /* loop columns */
  for (i = 0; i < fdw_state->db2Table->ncols; ++i) {
    /* don't get LONG, LONG RAW and untranslatable values */
    if (fdw_state->db2Table->cols[i]->db2type == SQL_TYPE_BIG || fdw_state->db2Table->cols[i]->db2type == SQL_TYPE_OTHER) {
      fdw_state->db2Table->cols[i]->used = 0;
    }
    else {
      /* all columns are used */
      fdw_state->db2Table->cols[i]->used = 1;

      /* allocate memory for return value */
      fdw_state->db2Table->cols[i]->val = (char *) palloc (fdw_state->db2Table->cols[i]->val_size);
      fdw_state->db2Table->cols[i]->val_len = 0;
      fdw_state->db2Table->cols[i]->val_len4 = 0;
      fdw_state->db2Table->cols[i]->val_null = 1;

      if (first_column)
	first_column = false;
      else
	appendStringInfo (&query, ", ");

      /* append column name */
      appendStringInfo (&query, "%s", fdw_state->db2Table->cols[i]->name);
    }
  }

  /* if there are no columns, use NULL */
  if (first_column)
    appendStringInfo (&query, "NULL");

  /* append DB2 table name */
  appendStringInfo (&query, " FROM %s", fdw_state->db2Table->name);

  /* append SAMPLE clause if appropriate */
  if (sample_percent < 100.0)
    appendStringInfo (&query, " SAMPLE BLOCK (%f)", sample_percent);

  fdw_state->query = query.data;
  elog (DEBUG1, "db2_fdw: remote query is %s", fdw_state->query);

  /* get PostgreSQL column data types, check that they match DB2's */
  for (i = 0; i < fdw_state->db2Table->ncols; ++i)
    if (fdw_state->db2Table->cols[i]->used)
      checkDataType (fdw_state->db2Table->cols[i]->db2type,
		     fdw_state->db2Table->cols[i]->scale, fdw_state->db2Table->cols[i]->pgtype, fdw_state->db2Table->pgname, fdw_state->db2Table->cols[i]->pgname);

  /* loop through query results */
  while (db2IsStatementOpen (fdw_state->session)
	 ? db2FetchNext (fdw_state->session)
	 : (db2PrepareQuery (fdw_state->session, fdw_state->query, fdw_state->db2Table, fdw_state->prefetch),
	    db2ExecuteQuery (fdw_state->session, fdw_state->db2Table, fdw_state->paramList))) {
    /* allow user to interrupt ANALYZE */
    vacuum_delay_point ();

    ++fdw_state->rowcount;

    if (collected_rows < targrows) {
      /* the first "targrows" rows are added as samples */

      /* use a temporary memory context during convertTuple */
      old_cxt = MemoryContextSwitchTo (tmp_cxt);
      convertTuple (fdw_state, values, nulls, true);
      MemoryContextSwitchTo (old_cxt);

      rows[collected_rows++] = heap_form_tuple (tupDesc, values, nulls);
      MemoryContextReset (tmp_cxt);
    }
    else {
      /*
       * Skip a number of rows before replacing a random sample row.
       * A more detailed description of the algorithm can be found in analyze.c
       */
      if (rowstoskip < 0)
	rowstoskip = anl_get_next_S (*totalrows, targrows, &rstate);

      if (rowstoskip <= 0) {
	int k = (int) (targrows * anl_random_fract ());

	heap_freetuple (rows[k]);

	/* use a temporary memory context during convertTuple */
	old_cxt = MemoryContextSwitchTo (tmp_cxt);
	convertTuple (fdw_state, values, nulls, true);
	MemoryContextSwitchTo (old_cxt);

	rows[k] = heap_form_tuple (tupDesc, values, nulls);
	MemoryContextReset (tmp_cxt);
      }
    }
  }

  MemoryContextDelete (tmp_cxt);

  *totalrows = (double) fdw_state->rowcount / sample_percent * 100.0;
  *totaldeadrows = 0;

  /* report report */
  ereport (elevel, (errmsg ("\"%s\": table contains %lu rows; %d rows in sample", RelationGetRelationName (relation), fdw_state->rowcount, collected_rows)));

  return collected_rows;
}

/*
 * appendAsType
 * 		Append "s" to "dest", adding appropriate casts for datetime "type".
 */
void
appendAsType (StringInfoData * dest, const char *s, Oid type)
{
  switch (type) {
  case DATEOID:
    appendStringInfo (dest, "CAST (%s AS DATE)", s);
    break;
  case TIMESTAMPOID:
    appendStringInfo (dest, "CAST (%s AS TIMESTAMP)", s);
    break;
  case TIMESTAMPTZOID:
    appendStringInfo (dest, "CAST (%s AS TIMESTAMP WITH TIME ZONE)", s);
    break;
  case TIMEOID:
    appendStringInfo (dest, "(CAST (%s AS TIME))", s);
    break;
  case TIMETZOID:
    appendStringInfo (dest, "(CAST (%s AS TIME WITH TIME ZONE))", s);
    break;
  default:
    appendStringInfo (dest, "%s", s);
  }
}

/*
 * This macro is used by deparseExpr to identify PostgreSQL
 * types that can be translated to DB2 SQL.
 */
#define canHandleType(x) ((x) == TEXTOID || (x) == CHAROID || (x) == BPCHAROID \
			|| (x) == VARCHAROID || (x) == NAMEOID || (x) == INT8OID || (x) == INT2OID \
			|| (x) == INT4OID || (x) == OIDOID || (x) == FLOAT4OID || (x) == FLOAT8OID \
			|| (x) == NUMERICOID || (x) == DATEOID || (x) == TIMEOID || (x) == TIMESTAMPOID \
			|| (x) == TIMESTAMPTZOID || (x) == INTERVALOID)

/*
 * deparseExpr
 * 		Create and return an DB2 SQL string from "expr".
 * 		Returns NULL if that is not possible, else a palloc'ed string.
 * 		As a side effect, all Params incorporated in the WHERE clause
 * 		will be stored in "params".
 */
char *
deparseExpr (db2Session * session, RelOptInfo * foreignrel, Expr * expr, const struct db2Table *db2Table, List ** params)
{
  char *opername, *left, *right, *arg, oprkind;
#ifndef OLD_FDW_API
  char parname[10];
#endif
  Const *constant;
  OpExpr *oper;
  ScalarArrayOpExpr *arrayoper;
  CaseExpr *caseexpr;
  BoolExpr *boolexpr;
  CoalesceExpr *coalesceexpr;
  CoerceViaIO *coerce;
  Param *param;
  Var *variable;
  FuncExpr *func;
  Expr *rightexpr;
  ArrayExpr *array;
  ArrayCoerceExpr *arraycoerce;
#if PG_VERSION_NUM >= 100000
  SQLValueFunction *sqlvalfunc;
#endif
  regproc typoutput;
  HeapTuple tuple;
  ListCell *cell;
  StringInfoData result;
  Oid leftargtype, rightargtype, schema;
  db2Type db2type;
  ArrayIterator iterator;
  Datum datum;
  bool first_arg, isNull;
  int index;
  StringInfoData alias;
  const struct db2Table *var_table;	/* db2Table that belongs to a Var */

  if (expr == NULL)
    return NULL;

  switch (expr->type) {
  case T_Const:
    constant = (Const *) expr;
    if (constant->constisnull) {
      /* only translate NULLs of a type DB2 can handle */
      if (canHandleType (constant->consttype)) {
	initStringInfo (&result);
	appendStringInfo (&result, "NULL");
      }
      else
	return NULL;
    }
    else {
      /* get a string representation of the value */
      char *c = datumToString (constant->constvalue, constant->consttype);
      if (c == NULL)
	return NULL;
      else {
	initStringInfo (&result);
	appendStringInfo (&result, "%s", c);
      }
    }
    break;
  case T_Param:
    param = (Param *) expr;
#ifdef OLD_FDW_API
    /* don't try to push down parameters with 9.1 */
    return NULL;
#else
    /* don't try to handle interval parameters */
    if (!canHandleType (param->paramtype) || param->paramtype == INTERVALOID)
      return NULL;

    /* find the index in the parameter list */
    index = 0;
    foreach (cell, *params) {
      ++index;
      if (equal (param, (Node *) lfirst (cell)))
	break;
    }
    if (cell == NULL) {
      /* add the parameter to the list */
      ++index;
      *params = lappend (*params, param);
    }

    /* parameters will be called :p1, :p2 etc. */
    snprintf (parname, 10, ":p%d", index);
    initStringInfo (&result);
    appendAsType (&result, parname, param->paramtype);

    break;
#endif /* OLD_FDW_API */
  case T_Var:
    variable = (Var *) expr;
    var_table = NULL;

    /* check if the variable belongs to one of our foreign tables */
#ifdef JOIN_API
    if (IS_SIMPLE_REL (foreignrel)) {
#endif /* JOIN_API */
      if (variable->varno == foreignrel->relid && variable->varlevelsup == 0)
	var_table = db2Table;
#ifdef JOIN_API
    }
    else {
      struct DB2FdwState *joinstate = (struct DB2FdwState *) foreignrel->fdw_private;
      struct DB2FdwState *outerstate = (struct DB2FdwState *) joinstate->outerrel->fdw_private;
      struct DB2FdwState *innerstate = (struct DB2FdwState *) joinstate->innerrel->fdw_private;

      /* we can't get here if the foreign table has no columns, so this is safe */
      if (variable->varno == outerstate->db2Table->cols[0]->varno && variable->varlevelsup == 0)
	var_table = outerstate->db2Table;
      if (variable->varno == innerstate->db2Table->cols[0]->varno && variable->varlevelsup == 0)
	var_table = innerstate->db2Table;
    }
#endif /* JOIN_API */

    if (var_table) {
      /* the variable belongs to a foreign table, replace it with the name */

      /* we cannot handle system columns */
      if (variable->varattno < 1)
	return NULL;

      /*
       * Allow boolean columns here.
       * They will be rendered as ("COL" <> 0).
       */
      if (!(canHandleType (variable->vartype) || variable->vartype == BOOLOID))
	return NULL;

      /* get var_table column index corresponding to this column (-1 if none) */
      index = var_table->ncols - 1;
      while (index >= 0 && var_table->cols[index]->pgattnum != variable->varattno)
	--index;

      /* if no DB2 column corresponds, translate as NULL */
      if (index == -1) {
	initStringInfo (&result);
	appendStringInfo (&result, "NULL");
	break;
      }

      /*
       * Don't try to convert a column reference if the type is
       * converted from a non-string type in DB2 to a string type
       * in PostgreSQL because functions and operators won't work the same.
       */
      db2type = var_table->cols[index]->db2type;
      if ((variable->vartype == TEXTOID || variable->vartype == BPCHAROID || variable->vartype == VARCHAROID)
	  && db2type != SQL_TYPE_VARCHAR && db2type != SQL_TYPE_CHAR)
	return NULL;

      initStringInfo (&result);

      /* work around the lack of booleans in DB2 */
      if (variable->vartype == BOOLOID) {
	appendStringInfo (&result, "(");
      }

      /* qualify with an alias based on the range table index */
      initStringInfo (&alias);
      ADD_REL_QUALIFIER (&alias, var_table->cols[index]->varno);

      appendStringInfo (&result, "%s%s", alias.data, var_table->cols[index]->name);

      /* work around the lack of booleans in DB2 */
      if (variable->vartype == BOOLOID) {
	appendStringInfo (&result, " <> 0)");
      }
    }
    else {
      /* treat it like a parameter */
#ifdef OLD_FDW_API
      /* don't try to push down parameters with 9.1 */
      return NULL;
#else
      /* don't try to handle type interval */
      if (!canHandleType (variable->vartype) || variable->vartype == INTERVALOID)
	return NULL;

      /* find the index in the parameter list */
      index = 0;
      foreach (cell, *params) {
	++index;
	if (equal (variable, (Node *) lfirst (cell)))
	  break;
      }
      if (cell == NULL) {
	/* add the parameter to the list */
	++index;
	*params = lappend (*params, variable);
      }

      /* parameters will be called :p1, :p2 etc. */
      initStringInfo (&result);
      appendStringInfo (&result, ":p%d", index);
#endif /* OLD_FDW_API */
    }

    break;
  case T_OpExpr:
    oper = (OpExpr *) expr;

    /* get operator name, kind, argument type and schema */
    tuple = SearchSysCache1 (OPEROID, ObjectIdGetDatum (oper->opno));
    if (!HeapTupleIsValid (tuple)) {
      elog (ERROR, "cache lookup failed for operator %u", oper->opno);
    }
    opername = pstrdup (((Form_pg_operator) GETSTRUCT (tuple))->oprname.data);
    oprkind = ((Form_pg_operator) GETSTRUCT (tuple))->oprkind;
    leftargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprleft;
    rightargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprright;
    schema = ((Form_pg_operator) GETSTRUCT (tuple))->oprnamespace;
    ReleaseSysCache (tuple);

    /* ignore operators in other than the pg_catalog schema */
    if (schema != PG_CATALOG_NAMESPACE)
      return NULL;

    if (!canHandleType (rightargtype))
      return NULL;

    /*
     * Don't translate operations on two intervals.
     * INTERVAL YEAR TO MONTH and INTERVAL DAY TO SECOND don't mix well.
     */
    if (leftargtype == INTERVALOID && rightargtype == INTERVALOID)
      return NULL;

    /* the operators that we can translate */
    if (strcmp (opername, "=") == 0 || strcmp (opername, "<>") == 0
	/* string comparisons are not safe */
	|| (strcmp (opername, ">") == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID && rightargtype != NAMEOID && rightargtype != CHAROID)
	|| (strcmp (opername, "<") == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID && rightargtype != NAMEOID && rightargtype != CHAROID)
	|| (strcmp (opername, ">=") == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID && rightargtype != NAMEOID && rightargtype != CHAROID)
	|| (strcmp (opername, "<=") == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID && rightargtype != NAMEOID && rightargtype != CHAROID)
	|| strcmp (opername, "+") == 0
	/* subtracting DATEs yields a NUMBER in DB2 */
	|| (strcmp (opername, "-") == 0 && rightargtype != DATEOID && rightargtype != TIMESTAMPOID && rightargtype != TIMESTAMPTZOID)
	|| strcmp (opername, "*") == 0
	|| strcmp (opername, "~~") == 0
	|| strcmp (opername, "!~~") == 0
	|| strcmp (opername, "~~*") == 0
	|| strcmp (opername, "!~~*") == 0
	|| strcmp (opername, "^") == 0 || strcmp (opername, "%") == 0 || strcmp (opername, "&") == 0 || strcmp (opername, "|/") == 0 || strcmp (opername, "@") == 0) {
      left = deparseExpr (session, foreignrel, linitial (oper->args), db2Table, params);
      if (left == NULL) {
	pfree (opername);
	return NULL;
      }

      if (oprkind == 'b') {
	/* binary operator */
	right = deparseExpr (session, foreignrel, lsecond (oper->args), db2Table, params);
	if (right == NULL) {
	  pfree (left);
	  pfree (opername);
	  return NULL;
	}

	initStringInfo (&result);
	if (strcmp (opername, "~~") == 0) {
	  appendStringInfo (&result, "(%s LIKE %s ESCAPE '\\')", left, right);
	}
	else if (strcmp (opername, "!~~") == 0) {
	  appendStringInfo (&result, "(%s NOT LIKE %s ESCAPE '\\')", left, right);
	}
	else if (strcmp (opername, "~~*") == 0) {
	  appendStringInfo (&result, "(UPPER(%s) LIKE UPPER(%s) ESCAPE '\\')", left, right);
	}
	else if (strcmp (opername, "!~~*") == 0) {
	  appendStringInfo (&result, "(UPPER(%s) NOT LIKE UPPER(%s) ESCAPE '\\')", left, right);
	}
	else if (strcmp (opername, "^") == 0) {
	  appendStringInfo (&result, "POWER(%s, %s)", left, right);
	}
	else if (strcmp (opername, "%") == 0) {
	  appendStringInfo (&result, "MOD(%s, %s)", left, right);
	}
	else if (strcmp (opername, "&") == 0) {
	  appendStringInfo (&result, "BITAND(%s, %s)", left, right);
	}
	else {
	  /* the other operators have the same name in DB2 */
	  appendStringInfo (&result, "(%s %s %s)", left, opername, right);
	}
	pfree (right);
	pfree (left);
      }
      else {
	/* unary operator */
	initStringInfo (&result);
	if (strcmp (opername, "|/") == 0) {
	  appendStringInfo (&result, "SQRT(%s)", left);
	}
	else if (strcmp (opername, "@") == 0) {
	  appendStringInfo (&result, "ABS(%s)", left);
	}
	else {
	  /* unary + or - */
	  appendStringInfo (&result, "(%s%s)", opername, left);
	}
	pfree (left);
      }
    }
    else {
      /* cannot translate this operator */
      pfree (opername);
      return NULL;
    }

    pfree (opername);
    break;
  case T_ScalarArrayOpExpr:
    arrayoper = (ScalarArrayOpExpr *) expr;

    /* get operator name, left argument type and schema */
    tuple = SearchSysCache1 (OPEROID, ObjectIdGetDatum (arrayoper->opno));
    if (!HeapTupleIsValid (tuple)) {
      elog (ERROR, "cache lookup failed for operator %u", arrayoper->opno);
    }
    opername = pstrdup (((Form_pg_operator) GETSTRUCT (tuple))->oprname.data);
    leftargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprleft;
    schema = ((Form_pg_operator) GETSTRUCT (tuple))->oprnamespace;
    ReleaseSysCache (tuple);

    /* get the type's output function */
    tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (leftargtype));
    if (!HeapTupleIsValid (tuple)) {
      elog (ERROR, "cache lookup failed for type %u", leftargtype);
    }
    typoutput = ((Form_pg_type) GETSTRUCT (tuple))->typoutput;
    ReleaseSysCache (tuple);

    /* ignore operators in other than the pg_catalog schema */
    if (schema != PG_CATALOG_NAMESPACE)
      return NULL;

    /* don't try to push down anything but IN and NOT IN expressions */
    if ((strcmp (opername, "=") != 0 || !arrayoper->useOr)
	&& (strcmp (opername, "<>") != 0 || arrayoper->useOr))
      return NULL;

    if (!canHandleType (leftargtype))
      return NULL;

    left = deparseExpr (session, foreignrel, linitial (arrayoper->args), db2Table, params);
    if (left == NULL)
      return NULL;

    /* begin to compose result */
    initStringInfo (&result);
    appendStringInfo (&result, "(%s %s (", left, arrayoper->useOr ? "IN" : "NOT IN");

    /* the second (=last) argument can be Const, ArrayExpr or ArrayCoerceExpr */
    rightexpr = (Expr *) llast (arrayoper->args);
    switch (rightexpr->type) {
    case T_Const:
      /* the second (=last) argument is a Const of ArrayType */
      constant = (Const *) rightexpr;

      /* using NULL in place of an array or value list is valid in DB2 and PostgreSQL */
      if (constant->constisnull)
	appendStringInfo (&result, "NULL");
      else {
	/* loop through the array elements */
	iterator = array_create_iterator (DatumGetArrayTypeP (constant->constvalue), 0);
	first_arg = true;
	while (array_iterate (iterator, &datum, &isNull)) {
	  char *c;

	  if (isNull)
	    c = "NULL";
	  else {
	    c = datumToString (datum, leftargtype);
	    if (c == NULL) {
	      array_free_iterator (iterator);
	      return NULL;
	    }
	  }

	  /* append the argument */
	  appendStringInfo (&result, "%s%s", first_arg ? "" : ", ", c);
	  first_arg = false;
	}
	array_free_iterator (iterator);

	/* don't push down empty arrays, since the semantics for NOT x = ANY(<empty array>) differ */
	if (first_arg)
	  return NULL;
      }

      break;

    case T_ArrayCoerceExpr:
      /* the second (=last) argument is an ArrayCoerceExpr */
      arraycoerce = (ArrayCoerceExpr *) rightexpr;

      /* if the conversion requires more than binary coercion, don't push it down */
#if PG_VERSION_NUM < 110000
      if (arraycoerce->elemfuncid != InvalidOid)
	return NULL;
#else
      if (arraycoerce->elemexpr && arraycoerce->elemexpr->type != T_RelabelType)
	return NULL;
#endif

      /* the actual array is here */
      rightexpr = arraycoerce->arg;

      /* fall through ! */

    case T_ArrayExpr:
      /* the second (=last) argument is an ArrayExpr */
      array = (ArrayExpr *) rightexpr;

      /* loop the array arguments */
      first_arg = true;
      foreach (cell, array->elements) {
	/* convert the argument to a string */
	char *element = deparseExpr (session, foreignrel, (Expr *) lfirst (cell), db2Table, params);

	/* if any element cannot be converted, give up */
	if (element == NULL)
	  return NULL;

	/* append the argument */
	appendStringInfo (&result, "%s%s", first_arg ? "" : ", ", element);
	first_arg = false;
      }

      /* don't push down empty arrays, since the semantics for NOT x = ANY(<empty array>) differ */
      if (first_arg)
	return NULL;

      break;

    default:
      return NULL;
    }

    /* two parentheses close the expression */
    appendStringInfo (&result, "))");

    break;
  case T_DistinctExpr:
    /* get argument type */
    tuple = SearchSysCache1 (OPEROID, ObjectIdGetDatum (((DistinctExpr *) expr)->opno));
    if (!HeapTupleIsValid (tuple)) {
      elog (ERROR, "cache lookup failed for operator %u", ((DistinctExpr *) expr)->opno);
    }
    rightargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprright;
    ReleaseSysCache (tuple);

    if (!canHandleType (rightargtype))
      return NULL;

    left = deparseExpr (session, foreignrel, linitial (((DistinctExpr *) expr)->args), db2Table, params);
    if (left == NULL) {
      return NULL;
    }
    right = deparseExpr (session, foreignrel, lsecond (((DistinctExpr *) expr)->args), db2Table, params);
    if (right == NULL) {
      pfree (left);
      return NULL;
    }

    initStringInfo (&result);
    appendStringInfo (&result, "(%s IS DISTINCT FROM %s)", left, right);

    break;
  case T_NullIfExpr:
    /* get argument type */
    tuple = SearchSysCache1 (OPEROID, ObjectIdGetDatum (((NullIfExpr *) expr)->opno));
    if (!HeapTupleIsValid (tuple)) {
      elog (ERROR, "cache lookup failed for operator %u", ((NullIfExpr *) expr)->opno);
    }
    rightargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprright;
    ReleaseSysCache (tuple);

    if (!canHandleType (rightargtype))
      return NULL;

    left = deparseExpr (session, foreignrel, linitial (((NullIfExpr *) expr)->args), db2Table, params);
    if (left == NULL) {
      return NULL;
    }
    right = deparseExpr (session, foreignrel, lsecond (((NullIfExpr *) expr)->args), db2Table, params);
    if (right == NULL) {
      pfree (left);
      return NULL;
    }

    initStringInfo (&result);
    appendStringInfo (&result, "NULLIF(%s, %s)", left, right);

    break;
  case T_BoolExpr:
    boolexpr = (BoolExpr *) expr;

    arg = deparseExpr (session, foreignrel, linitial (boolexpr->args), db2Table, params);
    if (arg == NULL)
      return NULL;

    initStringInfo (&result);
    appendStringInfo (&result, "(%s%s", boolexpr->boolop == NOT_EXPR ? "NOT " : "", arg);

    do_each_cell(cell, boolexpr->args, list_next(boolexpr->args, list_head(boolexpr->args)))
      {     
      arg = deparseExpr (session, foreignrel, (Expr *) lfirst (cell), db2Table, params);
      if (arg == NULL) {
	pfree (result.data);
	return NULL;
      }

      appendStringInfo (&result, " %s %s", boolexpr->boolop == AND_EXPR ? "AND" : "OR", arg);
    }
    appendStringInfo (&result, ")");

    break;
  case T_RelabelType:
    return deparseExpr (session, foreignrel, ((RelabelType *) expr)->arg, db2Table, params);
    break;
  case T_CoerceToDomain:
    return deparseExpr (session, foreignrel, ((CoerceToDomain *) expr)->arg, db2Table, params);
    break;
  case T_CaseExpr:
    caseexpr = (CaseExpr *) expr;

    if (!canHandleType (caseexpr->casetype))
      return NULL;

    initStringInfo (&result);
    appendStringInfo (&result, "CASE");

    /* for the form "CASE arg WHEN ...", add first expression */
    if (caseexpr->arg != NULL) {
      arg = deparseExpr (session, foreignrel, caseexpr->arg, db2Table, params);
      if (arg == NULL) {
	pfree (result.data);
	return NULL;
      }
      else {
	appendStringInfo (&result, " %s", arg);
      }
    }

    /* append WHEN ... THEN clauses */
    foreach (cell, caseexpr->args) {
      CaseWhen *whenclause = (CaseWhen *) lfirst (cell);

      /* WHEN */
      if (caseexpr->arg == NULL) {
	/* for CASE WHEN ..., use the whole expression */
	arg = deparseExpr (session, foreignrel, whenclause->expr, db2Table, params);
      }
      else {
	/* for CASE arg WHEN ..., use only the right branch of the equality */
	arg = deparseExpr (session, foreignrel, lsecond (((OpExpr *) whenclause->expr)->args), db2Table, params);
      }

      if (arg == NULL) {
	pfree (result.data);
	return NULL;
      }
      else {
	appendStringInfo (&result, " WHEN %s", arg);
	pfree (arg);
      }

      /* THEN */
      arg = deparseExpr (session, foreignrel, whenclause->result, db2Table, params);
      if (arg == NULL) {
	pfree (result.data);
	return NULL;
      }
      else {
	appendStringInfo (&result, " THEN %s", arg);
	pfree (arg);
      }
    }

    /* append ELSE clause if appropriate */
    if (caseexpr->defresult != NULL) {
      arg = deparseExpr (session, foreignrel, caseexpr->defresult, db2Table, params);
      if (arg == NULL) {
	pfree (result.data);
	return NULL;
      }
      else {
	appendStringInfo (&result, " ELSE %s", arg);
	pfree (arg);
      }
    }

    /* append END */
    appendStringInfo (&result, " END");
    break;
  case T_CoalesceExpr:
    coalesceexpr = (CoalesceExpr *) expr;

    if (!canHandleType (coalesceexpr->coalescetype))
      return NULL;

    initStringInfo (&result);
    appendStringInfo (&result, "COALESCE(");

    first_arg = true;
    foreach (cell, coalesceexpr->args) {
      arg = deparseExpr (session, foreignrel, (Expr *) lfirst (cell), db2Table, params);
      if (arg == NULL) {
	pfree (result.data);
	return NULL;
      }

      if (first_arg) {
	appendStringInfo (&result, "%s", arg);
	first_arg = false;
      }
      else {
	appendStringInfo (&result, ", %s", arg);
      }
      pfree (arg);
    }

    appendStringInfo (&result, ")");

    break;
  case T_NullTest:
    arg = deparseExpr (session, foreignrel, ((NullTest *) expr)->arg, db2Table, params);
    if (arg == NULL)
      return NULL;

    initStringInfo (&result);
    appendStringInfo (&result, "(%s IS %sNULL)", arg, ((NullTest *) expr)->nulltesttype == IS_NOT_NULL ? "NOT " : "");
    break;
  case T_FuncExpr:
    func = (FuncExpr *) expr;

    if (!canHandleType (func->funcresulttype))
      return NULL;

    /* do nothing for implicit casts */
    if (func->funcformat == COERCE_IMPLICIT_CAST)
      return deparseExpr (session, foreignrel, linitial (func->args), db2Table, params);

    /* get function name and schema */
    tuple = SearchSysCache1 (PROCOID, ObjectIdGetDatum (func->funcid));
    if (!HeapTupleIsValid (tuple)) {
      elog (ERROR, "cache lookup failed for function %u", func->funcid);
    }
    opername = pstrdup (((Form_pg_proc) GETSTRUCT (tuple))->proname.data);
    schema = ((Form_pg_proc) GETSTRUCT (tuple))->pronamespace;
    ReleaseSysCache (tuple);

    /* ignore functions in other than the pg_catalog schema */
    if (schema != PG_CATALOG_NAMESPACE)
      return NULL;

    /* the "normal" functions that we can translate */
    if (strcmp (opername, "abs") == 0
	|| strcmp (opername, "acos") == 0
	|| strcmp (opername, "asin") == 0
	|| strcmp (opername, "atan") == 0
	|| strcmp (opername, "atan2") == 0
	|| strcmp (opername, "ceil") == 0
	|| strcmp (opername, "ceiling") == 0
	|| strcmp (opername, "char_length") == 0
	|| strcmp (opername, "character_length") == 0
	|| strcmp (opername, "concat") == 0
	|| strcmp (opername, "cos") == 0
	|| strcmp (opername, "exp") == 0
	|| strcmp (opername, "initcap") == 0
	|| strcmp (opername, "length") == 0
	|| strcmp (opername, "lower") == 0
	|| strcmp (opername, "lpad") == 0
	|| strcmp (opername, "ltrim") == 0
	|| strcmp (opername, "mod") == 0
	|| strcmp (opername, "octet_length") == 0
	|| strcmp (opername, "position") == 0
	|| strcmp (opername, "pow") == 0
	|| strcmp (opername, "power") == 0
	|| strcmp (opername, "replace") == 0
	|| strcmp (opername, "round") == 0
	|| strcmp (opername, "rpad") == 0
	|| strcmp (opername, "rtrim") == 0
	|| strcmp (opername, "sign") == 0
	|| strcmp (opername, "sin") == 0
	|| strcmp (opername, "sqrt") == 0
	|| strcmp (opername, "strpos") == 0 || strcmp (opername, "substr") == 0 || (strcmp (opername, "substring") == 0 && list_length (func->args) == 3)
	|| strcmp (opername, "tan") == 0
	|| strcmp (opername, "to_char") == 0
	|| strcmp (opername, "to_date") == 0
	|| strcmp (opername, "to_number") == 0
	|| strcmp (opername, "to_timestamp") == 0 || strcmp (opername, "translate") == 0 || strcmp (opername, "trunc") == 0 || strcmp (opername, "upper") == 0) {
      initStringInfo (&result);

      if (strcmp (opername, "ceiling") == 0)
	appendStringInfo (&result, "CEIL(");
      else if (strcmp (opername, "char_length") == 0 || strcmp (opername, "character_length") == 0)
	appendStringInfo (&result, "LENGTH(");
      else if (strcmp (opername, "pow") == 0)
	appendStringInfo (&result, "POWER(");
      else if (strcmp (opername, "octet_length") == 0)
	appendStringInfo (&result, "LENGTHB(");
      else if (strcmp (opername, "position") == 0 || strcmp (opername, "strpos") == 0)
	appendStringInfo (&result, "INSTR(");
      else if (strcmp (opername, "substring") == 0)
	appendStringInfo (&result, "SUBSTR(");
      else
	appendStringInfo (&result, "%s(", opername);

      first_arg = true;
      foreach (cell, func->args) {
	arg = deparseExpr (session, foreignrel, lfirst (cell), db2Table, params);
	if (arg == NULL) {
	  pfree (result.data);
	  pfree (opername);
	  return NULL;
	}

	if (first_arg) {
	  first_arg = false;
	  appendStringInfo (&result, "%s", arg);
	}
	else {
	  appendStringInfo (&result, ", %s", arg);
	}
	pfree (arg);
      }

      appendStringInfo (&result, ")");
    }
    else if (strcmp (opername, "date_part") == 0) {
      /* special case: EXTRACT */
      left = deparseExpr (session, foreignrel, linitial (func->args), db2Table, params);
      if (left == NULL) {
	pfree (opername);
	return NULL;
      }

      /* can only handle these fields in DB2 */
      if (strcmp (left, "'year'") == 0
	  || strcmp (left, "'month'") == 0
	  || strcmp (left, "'day'") == 0
	  || strcmp (left, "'hour'") == 0
	  || strcmp (left, "'minute'") == 0 || strcmp (left, "'second'") == 0 || strcmp (left, "'timezone_hour'") == 0 || strcmp (left, "'timezone_minute'") == 0) {
	/* remove final quote */
	left[strlen (left) - 1] = '\0';

	right = deparseExpr (session, foreignrel, lsecond (func->args), db2Table, params);
	if (right == NULL) {
	  pfree (opername);
	  pfree (left);
	  return NULL;
	}

	initStringInfo (&result);
	appendStringInfo (&result, "EXTRACT(%s FROM %s)", left + 1, right);
      }
      else {
	pfree (opername);
	pfree (left);
	return NULL;
      }

      pfree (left);
      pfree (right);
    }
    else if (strcmp (opername, "now") == 0 || strcmp (opername, "transaction_timestamp") == 0) {
      /* special case: current timestamp */
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST (:now AS TIMESTAMP WITH TIME ZONE))");
    }
    else {
      /* function that we cannot render for DB2 */
      pfree (opername);
      return NULL;
    }

    pfree (opername);
    break;
  case T_CoerceViaIO:
    /*
     * We will only handle casts of 'now'.
     */
    coerce = (CoerceViaIO *) expr;

    /* only casts to these types are handled */
    if (coerce->resulttype != DATEOID && coerce->resulttype != TIMESTAMPOID && coerce->resulttype != TIMESTAMPTZOID)
      return NULL;

    /* the argument must be a Const */
    if (coerce->arg->type != T_Const)
      return NULL;

    /* the argument must be a not-NULL text constant */
    constant = (Const *) coerce->arg;
    if (constant->constisnull || (constant->consttype != CSTRINGOID && constant->consttype != TEXTOID))
      return NULL;

    /* get the type's output function */
    tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (constant->consttype));
    if (!HeapTupleIsValid (tuple)) {
      elog (ERROR, "cache lookup failed for type %u", constant->consttype);
    }
    typoutput = ((Form_pg_type) GETSTRUCT (tuple))->typoutput;
    ReleaseSysCache (tuple);

    /* the value must be "now" */
    if (strcmp (DatumGetCString (OidFunctionCall1 (typoutput, constant->constvalue)), "now") != 0)
      return NULL;

    initStringInfo (&result);
    switch (coerce->resulttype) {
    case DATEOID:
      appendStringInfo (&result, "TRUNC(CAST (CAST(:now AS TIMESTAMP WITH TIME ZONE) AS DATE))");
      break;
    case TIMESTAMPOID:
      appendStringInfo (&result, "(CAST (CAST (:now AS TIMESTAMP WITH TIME ZONE) AS TIMESTAMP))");
      break;
    case TIMESTAMPTZOID:
      appendStringInfo (&result, "(CAST (:now AS TIMESTAMP WITH TIME ZONE))");
      break;
    case TIMEOID:
      appendStringInfo (&result, "(CAST (CAST (:now AS TIME WITH TIME ZONE) AS TIME))");
      break;
    case TIMETZOID:
      appendStringInfo (&result, "(CAST (:now AS TIME WITH TIME ZONE))");
      break;
    }

    break;
#if PG_VERSION_NUM >= 100000
  case T_SQLValueFunction:
    sqlvalfunc = (SQLValueFunction *) expr;

    switch (sqlvalfunc->op) {
    case SVFOP_CURRENT_DATE:
      initStringInfo (&result);
      appendStringInfo (&result, "TRUNC(CAST (CAST(:now AS TIMESTAMP WITH TIME ZONE) AS DATE))");
      break;
    case SVFOP_CURRENT_TIMESTAMP:
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST (:now AS TIMESTAMP WITH TIME ZONE))");
      break;
    case SVFOP_LOCALTIMESTAMP:
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST (CAST (:now AS TIMESTAMP WITH TIME ZONE) AS TIMESTAMP))");
      break;
    case SVFOP_CURRENT_TIME:
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST (:now AS TIME WITH TIME ZONE))");
      break;
    case SVFOP_LOCALTIME:
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST (CAST (:now AS TIME WITH TIME ZONE) AS TIME))");
      break;
    default:
      return NULL;		/* don't push down other functions */
    }

    break;
#endif
  default:
    /* we cannot translate this to DB2 */
    return NULL;
  }

  return result.data;
}

/*
 * datumToString
 * 		Convert a Datum to a string by calling the type output function.
 * 		Returns the result or NULL if it cannot be converted to DB2 SQL.
 */
static char *
datumToString (Datum datum, Oid type)
{
  StringInfoData result;
  regproc typoutput;
  HeapTuple tuple;
  char *str, *p;

  /* get the type's output function */
  tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (type));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for type %u", type);
  }
  typoutput = ((Form_pg_type) GETSTRUCT (tuple))->typoutput;
  ReleaseSysCache (tuple);

  /* render the constant in DB2 SQL */
  switch (type) {
  case TEXTOID:
  case CHAROID:
  case BPCHAROID:
  case VARCHAROID:
  case NAMEOID:
    str = DatumGetCString (OidFunctionCall1 (typoutput, datum));

    /*
     * Don't try to convert empty strings to DB2.
     * DB2 treats empty strings as NULL.
     */
    if (str[0] == '\0')
      return NULL;

    /* quote string */
    initStringInfo (&result);
    appendStringInfo (&result, "'");
    for (p = str; *p; ++p) {
      if (*p == '\'')
	appendStringInfo (&result, "'");
      appendStringInfo (&result, "%c", *p);
    }
    appendStringInfo (&result, "'");
    break;
  case INT8OID:
  case INT2OID:
  case INT4OID:
  case OIDOID:
  case FLOAT4OID:
  case FLOAT8OID:
  case NUMERICOID:
    str = DatumGetCString (OidFunctionCall1 (typoutput, datum));
    initStringInfo (&result);
    appendStringInfo (&result, "%s", str);
    break;
  case DATEOID:
    str = deparseDate (datum);
    initStringInfo (&result);
    appendStringInfo (&result, "(CAST ('%s' AS DATE))", str);
    break;
  case TIMESTAMPOID:
    str = deparseTimestamp (datum, false);
    initStringInfo (&result);
    appendStringInfo (&result, "(CAST ('%s' AS TIMESTAMP))", str);
    break;
  case TIMESTAMPTZOID:
    str = deparseTimestamp (datum, true);
    initStringInfo (&result);
    appendStringInfo (&result, "(CAST ('%s' AS TIMESTAMP WITH TIME ZONE))", str);
    break;
  case TIMEOID:
    str = deparseTimestamp (datum, false);
    initStringInfo (&result);
    appendStringInfo (&result, "(CAST ('%s' AS TIME))", str);
    break;
  case TIMETZOID:
    str = deparseTimestamp (datum, true);
    initStringInfo (&result);
    appendStringInfo (&result, "(CAST ('%s' AS TIME WITH TIME ZONE))", str);
    break;
  case INTERVALOID:
    str = deparseInterval (datum);
    if (str == NULL)
      return NULL;
    initStringInfo (&result);
    appendStringInfo (&result, "%s", str);
    break;
  default:
    return NULL;
  }

  return result.data;
}

/*
 * getUsedColumns
 * 		Set "used=true" in db2Table for all columns used in the expression.
 */
void
getUsedColumns (Expr * expr, struct db2Table *db2Table, int foreignrelid)
{
  ListCell *cell;
  Var *variable;
  int index;

  if (expr == NULL)
    return;

  switch (expr->type) {
  case T_RestrictInfo:
    getUsedColumns (((RestrictInfo *) expr)->clause, db2Table, foreignrelid);
    break;
  case T_TargetEntry:
    getUsedColumns (((TargetEntry *) expr)->expr, db2Table, foreignrelid);
    break;
  case T_Const:
  case T_Param:
  case T_CaseTestExpr:
  case T_CoerceToDomainValue:
  case T_CurrentOfExpr:
#if PG_VERSION_NUM >= 100000
  case T_NextValueExpr:
#endif
    break;
  case T_Var:
    variable = (Var *) expr;

    /* ignore system columns */
    if (variable->varattno < 0)
      break;

    /* if this is a wholerow reference, we need all columns */
    if (variable->varattno == 0) {
      for (index = 0; index < db2Table->ncols; ++index)
	if (db2Table->cols[index]->pgname)
	  db2Table->cols[index]->used = 1;
      break;
    }

    /* get db2Table column index corresponding to this column (-1 if none) */
    index = db2Table->ncols - 1;
    while (index >= 0 && db2Table->cols[index]->pgattnum != variable->varattno)
      --index;

    if (index == -1) {
      ereport (WARNING,
	       (errcode (ERRCODE_WARNING),
		errmsg ("column number %d of foreign table \"%s\" does not exist in foreign DB2 table, will be replaced by NULL", variable->varattno, db2Table->pgname)));
    }
    else {
      db2Table->cols[index]->used = 1;
    }
    break;
  case T_Aggref:
    foreach (cell, ((Aggref *) expr)->args) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    foreach (cell, ((Aggref *) expr)->aggorder) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    foreach (cell, ((Aggref *) expr)->aggdistinct) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    break;
  case T_WindowFunc:
    foreach (cell, ((WindowFunc *) expr)->args) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    break;
#if PG_VERSION_NUM < 120000
  case T_ArrayRef:
    {
    ArrayRef *ref = (ArrayRef *)expr;
#else
  case T_SubscriptingRef:
    {
    SubscriptingRef *ref = (SubscriptingRef *)expr;
#endif
    foreach(cell, ref->refupperindexpr)
    {
      getUsedColumns((Expr *)lfirst(cell), db2Table, foreignrelid);
    }
    foreach(cell, ref->reflowerindexpr)
    {
      getUsedColumns((Expr *)lfirst(cell), db2Table, foreignrelid);
    }
    getUsedColumns(ref->refexpr, db2Table, foreignrelid);
    getUsedColumns(ref->refassgnexpr, db2Table, foreignrelid);
    break;
    }

  case T_FuncExpr:
    foreach (cell, ((FuncExpr *) expr)->args) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    break;
  case T_OpExpr:
    foreach (cell, ((OpExpr *) expr)->args) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    break;
  case T_DistinctExpr:
    foreach (cell, ((DistinctExpr *) expr)->args) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    break;
  case T_NullIfExpr:
    foreach (cell, ((NullIfExpr *) expr)->args) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    break;
  case T_ScalarArrayOpExpr:
    foreach (cell, ((ScalarArrayOpExpr *) expr)->args) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    break;
  case T_BoolExpr:
    foreach (cell, ((BoolExpr *) expr)->args) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    break;
  case T_SubPlan:
    foreach (cell, ((SubPlan *) expr)->args) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    break;
  case T_AlternativeSubPlan:
    /* examine only first alternative */
    getUsedColumns ((Expr *) linitial (((AlternativeSubPlan *) expr)->subplans), db2Table, foreignrelid);
    break;
  case T_NamedArgExpr:
    getUsedColumns (((NamedArgExpr *) expr)->arg, db2Table, foreignrelid);
    break;
  case T_FieldSelect:
    getUsedColumns (((FieldSelect *) expr)->arg, db2Table, foreignrelid);
    break;
  case T_RelabelType:
    getUsedColumns (((RelabelType *) expr)->arg, db2Table, foreignrelid);
    break;
  case T_CoerceViaIO:
    getUsedColumns (((CoerceViaIO *) expr)->arg, db2Table, foreignrelid);
    break;
  case T_ArrayCoerceExpr:
    getUsedColumns (((ArrayCoerceExpr *) expr)->arg, db2Table, foreignrelid);
    break;
  case T_ConvertRowtypeExpr:
    getUsedColumns (((ConvertRowtypeExpr *) expr)->arg, db2Table, foreignrelid);
    break;
  case T_CollateExpr:
    getUsedColumns (((CollateExpr *) expr)->arg, db2Table, foreignrelid);
    break;
  case T_CaseExpr:
    foreach (cell, ((CaseExpr *) expr)->args) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    getUsedColumns (((CaseExpr *) expr)->arg, db2Table, foreignrelid);
    getUsedColumns (((CaseExpr *) expr)->defresult, db2Table, foreignrelid);
    break;
  case T_CaseWhen:
    getUsedColumns (((CaseWhen *) expr)->expr, db2Table, foreignrelid);
    getUsedColumns (((CaseWhen *) expr)->result, db2Table, foreignrelid);
    break;
  case T_ArrayExpr:
    foreach (cell, ((ArrayExpr *) expr)->elements) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    break;
  case T_RowExpr:
    foreach (cell, ((RowExpr *) expr)->args) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    break;
  case T_RowCompareExpr:
    foreach (cell, ((RowCompareExpr *) expr)->largs) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    foreach (cell, ((RowCompareExpr *) expr)->rargs) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    break;
  case T_CoalesceExpr:
    foreach (cell, ((CoalesceExpr *) expr)->args) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    break;
  case T_MinMaxExpr:
    foreach (cell, ((MinMaxExpr *) expr)->args) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    break;
  case T_XmlExpr:
    foreach (cell, ((XmlExpr *) expr)->named_args) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    foreach (cell, ((XmlExpr *) expr)->args) {
      getUsedColumns ((Expr *) lfirst (cell), db2Table, foreignrelid);
    }
    break;
  case T_NullTest:
    getUsedColumns (((NullTest *) expr)->arg, db2Table, foreignrelid);
    break;
  case T_BooleanTest:
    getUsedColumns (((BooleanTest *) expr)->arg, db2Table, foreignrelid);
    break;
  case T_CoerceToDomain:
    getUsedColumns (((CoerceToDomain *) expr)->arg, db2Table, foreignrelid);
    break;
  case T_PlaceHolderVar:
    getUsedColumns (((PlaceHolderVar *) expr)->phexpr, db2Table, foreignrelid);
    break;
#if PG_VERSION_NUM >= 100000
  case T_SQLValueFunction:
    break;			/* contains no column references */
#endif
  default:
    /*
     * We must be able to handle all node types that can
     * appear because we cannot omit a column from the remote
     * query that will be needed.
     * Throw an error if we encounter an unexpected node type.
     */
    ereport (ERROR, (errcode (ERRCODE_FDW_UNABLE_TO_CREATE_REPLY), errmsg ("Internal db2_fdw error: encountered unknown node type %d.", expr->type)));
  }
}

/*
 * checkDataType
 * 		Check that the DB2 data type of a column can be
 * 		converted to the PostgreSQL data type, raise an error if not.
 */
void
checkDataType (db2Type db2type, int scale, Oid pgtype, const char *tablename, const char *colname)
{
  db2Debug2("checkDataType: db2type: %d   pgtype: %d",db2type,pgtype);
  /* the binary DB2 types can be converted to bytea */
  if (db2type == SQL_TYPE_BLOB && pgtype == BYTEAOID)
    return;
  if (db2type == SQL_TYPE_XML && pgtype == BYTEAOID)
    return;


  /* all other DB2 types can be transformed to strings */
  if (db2type != SQL_TYPE_OTHER && db2type != SQL_TYPE_BLOB && (pgtype == TEXTOID || pgtype == VARCHAROID || pgtype == BPCHAROID))
    return;

  /* all numeric DB2 types can be transformed to floating point types */
  if ((db2type == SQL_TYPE_INTEGER || db2type == SQL_TYPE_SMALL || db2type == SQL_TYPE_BIG || db2type == SQL_TYPE_FLOAT || db2type == SQL_TYPE_DOUBLE || db2type == SQL_TYPE_REAL || db2type == SQL_TYPE_DECIMAL)
      && (pgtype == NUMERICOID || pgtype == FLOAT4OID || pgtype == FLOAT8OID))
    return;

  /*
   * NUMBER columns without decimal fractions can be transformed to
   * integers or booleans
   */
  if ((db2type == SQL_TYPE_INTEGER || db2type == SQL_TYPE_SMALL || db2type == SQL_TYPE_BIG)
      && scale <= 0 && (pgtype == INT2OID || pgtype == INT4OID || pgtype == INT8OID || pgtype == BOOLOID)
    )
    return;

  /* DATE and timestamps can be transformed to each other */
  if ((db2type == SQL_TYPE_DATE || db2type == SQL_TYPE_STAMP)
      && (pgtype == DATEOID || pgtype == TIMESTAMPOID || pgtype == TIMESTAMPTZOID || pgtype == TIMEOID || pgtype == TIMETZOID))
    return;


  /* VARCHAR2 and CLOB can be converted to json */
  if ((db2type == SQL_TYPE_VARCHAR || db2type == SQL_TYPE_CLOB)
      && pgtype == JSONOID)
    return;

  /* otherwise, report an error */
  ereport (ERROR,
	   (errcode (ERRCODE_FDW_INVALID_DATA_TYPE),
	    errmsg ("column \"%s\" of type \"%d\" of foreign DB2 table \"%s\" cannot be converted to \"%d\" ", colname, db2type, tablename, pgtype)));
}

/*
 * deparseWhereConditions
 * 		Classify conditions into remote_conds or local_conds.
 * 		Those conditions that can be pushed down will be collected into
 * 		an DB2 WHERE clause that is returned.
 */
char *
deparseWhereConditions (struct DB2FdwState *fdwState, RelOptInfo * baserel, List ** local_conds, List ** remote_conds)
{
  List *conditions = baserel->baserestrictinfo;
  ListCell *cell;
  char *where;
  char *keyword = "WHERE";
  StringInfoData where_clause;

  initStringInfo (&where_clause);
  foreach (cell, conditions) {
    /* check if the condition can be pushed down */
    where = deparseExpr (fdwState->session, baserel, ((RestrictInfo *) lfirst (cell))->clause, fdwState->db2Table, &(fdwState->params)
      );
    if (where != NULL) {
      *remote_conds = lappend (*remote_conds, ((RestrictInfo *) lfirst (cell))->clause);

      /* append new WHERE clause to query string */
      appendStringInfo (&where_clause, " %s %s", keyword, where);
      keyword = "AND";
      pfree (where);
    }
    else
      *local_conds = lappend (*local_conds, ((RestrictInfo *) lfirst (cell))->clause);
  }
  return where_clause.data;
}

/*
 * guessNlsLang
 * 		If nls_lang is not NULL, return "NLS_LANG=<nls_lang>".
 * 		Otherwise, return a good guess for DB2's NLS_LANG.
 */
char *
guessNlsLang (char *nls_lang)
{
  char *server_encoding, *lc_messages, *language = "AMERICAN_AMERICA", *charset = NULL;
  StringInfoData buf;

  initStringInfo (&buf);
  if (nls_lang == NULL) {
    server_encoding = pstrdup (GetConfigOption ("server_encoding", false, true));

    /* find an DB2 client character set that matches the database encoding */
    if (strcmp (server_encoding, "UTF8") == 0)
      charset = "AL32UTF8";
    else if (strcmp (server_encoding, "EUC_JP") == 0)
      charset = "JA16EUC";
    else if (strcmp (server_encoding, "EUC_JIS_2004") == 0)
      charset = "JA16SJIS";
    else if (strcmp (server_encoding, "EUC_TW") == 0)
      charset = "ZHT32EUC";
    else if (strcmp (server_encoding, "ISO_8859_5") == 0)
      charset = "CL8ISO8859P5";
    else if (strcmp (server_encoding, "ISO_8859_6") == 0)
      charset = "AR8ISO8859P6";
    else if (strcmp (server_encoding, "ISO_8859_7") == 0)
      charset = "EL8ISO8859P7";
    else if (strcmp (server_encoding, "ISO_8859_8") == 0)
      charset = "IW8ISO8859P8";
    else if (strcmp (server_encoding, "KOI8R") == 0)
      charset = "CL8KOI8R";
    else if (strcmp (server_encoding, "KOI8U") == 0)
      charset = "CL8KOI8U";
    else if (strcmp (server_encoding, "LATIN1") == 0)
      charset = "WE8ISO8859P1";
    else if (strcmp (server_encoding, "LATIN2") == 0)
      charset = "EE8ISO8859P2";
    else if (strcmp (server_encoding, "LATIN3") == 0)
      charset = "SE8ISO8859P3";
    else if (strcmp (server_encoding, "LATIN4") == 0)
      charset = "NEE8ISO8859P4";
    else if (strcmp (server_encoding, "LATIN5") == 0)
      charset = "WE8ISO8859P9";
    else if (strcmp (server_encoding, "LATIN6") == 0)
      charset = "NE8ISO8859P10";
    else if (strcmp (server_encoding, "LATIN7") == 0)
      charset = "BLT8ISO8859P13";
    else if (strcmp (server_encoding, "LATIN8") == 0)
      charset = "CEL8ISO8859P14";
    else if (strcmp (server_encoding, "LATIN9") == 0)
      charset = "WE8ISO8859P15";
    else if (strcmp (server_encoding, "WIN866") == 0)
      charset = "RU8PC866";
    else if (strcmp (server_encoding, "WIN1250") == 0)
      charset = "EE8MSWIN1250";
    else if (strcmp (server_encoding, "WIN1251") == 0)
      charset = "CL8MSWIN1251";
    else if (strcmp (server_encoding, "WIN1252") == 0)
      charset = "WE8MSWIN1252";
    else if (strcmp (server_encoding, "WIN1253") == 0)
      charset = "EL8MSWIN1253";
    else if (strcmp (server_encoding, "WIN1254") == 0)
      charset = "TR8MSWIN1254";
    else if (strcmp (server_encoding, "WIN1255") == 0)
      charset = "IW8MSWIN1255";
    else if (strcmp (server_encoding, "WIN1256") == 0)
      charset = "AR8MSWIN1256";
    else if (strcmp (server_encoding, "WIN1257") == 0)
      charset = "BLT8MSWIN1257";
    else if (strcmp (server_encoding, "WIN1258") == 0)
      charset = "VN8MSWIN1258";
    else {
      /* warn if we have to resort to 7-bit ASCII */
      charset = "US7ASCII";

      ereport (WARNING,
	       (errcode (ERRCODE_WARNING),
		errmsg ("no DB2 character set for database encoding \"%s\"", server_encoding),
		errdetail ("All but ASCII characters will be lost."),
		errhint ("You can set the option \"%s\" on the foreign data wrapper to force an DB2 character set.", OPT_NLS_LANG)));
    }

    lc_messages = pstrdup (GetConfigOption ("lc_messages", false, true));
    /* try to guess those for which there is a backend translation */
    if (strncmp (lc_messages, "de_", 3) == 0 || pg_strncasecmp (lc_messages, "german", 6) == 0)
      language = "GERMAN_GERMANY";
    if (strncmp (lc_messages, "es_", 3) == 0 || pg_strncasecmp (lc_messages, "spanish", 7) == 0)
      language = "SPANISH_SPAIN";
    if (strncmp (lc_messages, "fr_", 3) == 0 || pg_strncasecmp (lc_messages, "french", 6) == 0)
      language = "FRENCH_FRANCE";
    if (strncmp (lc_messages, "in_", 3) == 0 || pg_strncasecmp (lc_messages, "indonesian", 10) == 0)
      language = "INDONESIAN_INDONESIA";
    if (strncmp (lc_messages, "it_", 3) == 0 || pg_strncasecmp (lc_messages, "italian", 7) == 0)
      language = "ITALIAN_ITALY";
    if (strncmp (lc_messages, "ja_", 3) == 0 || pg_strncasecmp (lc_messages, "japanese", 8) == 0)
      language = "JAPANESE_JAPAN";
    if (strncmp (lc_messages, "pt_", 3) == 0 || pg_strncasecmp (lc_messages, "portuguese", 10) == 0)
      language = "BRAZILIAN PORTUGUESE_BRAZIL";
    if (strncmp (lc_messages, "ru_", 3) == 0 || pg_strncasecmp (lc_messages, "russian", 7) == 0)
      language = "RUSSIAN_RUSSIA";
    if (strncmp (lc_messages, "tr_", 3) == 0 || pg_strncasecmp (lc_messages, "turkish", 7) == 0)
      language = "TURKISH_TURKEY";
    if (strncmp (lc_messages, "zh_CN", 5) == 0 || pg_strncasecmp (lc_messages, "chinese-simplified", 18) == 0)
      language = "SIMPLIFIED CHINESE_CHINA";
    if (strncmp (lc_messages, "zh_TW", 5) == 0 || pg_strncasecmp (lc_messages, "chinese-traditional", 19) == 0)
      language = "TRADITIONAL CHINESE_TAIWAN";

    appendStringInfo (&buf, "NLS_LANG=%s.%s", language, charset);
  }
  else {
    appendStringInfo (&buf, "NLS_LANG=%s", nls_lang);
  }

  elog (DEBUG1, "db2_fdw: set %s", buf.data);

  return buf.data;
}

#define serializeInt(x) makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum((int32)(x)), false, true)
#define serializeOid(x) makeConst(OIDOID, -1, InvalidOid, 4, ObjectIdGetDatum(x), false, true)

/*
 * serializePlanData
 * 		Create a List representation of plan data that copyObject can copy.
 * 		This List can be parsed by deserializePlanData.
 */

List * serializePlanData (struct DB2FdwState * fdwState)
{
  List *result = NIL;
  int i, len = 0;
  const struct paramDesc *param;

  /* dbserver */
  result = lappend (result, serializeString (fdwState->dbserver));
  /* user name */
  result = lappend (result, serializeString (fdwState->user));
  /* password */
  result = lappend (result, serializeString (fdwState->password));
  /* nls_lang */
  result = lappend (result, serializeString (fdwState->nls_lang));
  /* query */
  result = lappend (result, serializeString (fdwState->query));
  /* DB2 prefetch count */
  result = lappend (result, serializeInt ((int) fdwState->prefetch));
  /* DB2 table name */
  result = lappend (result, serializeString (fdwState->db2Table->name));
  /* PostgreSQL table name */
  result = lappend (result, serializeString (fdwState->db2Table->pgname));
  /* number of columns in DB2 table */
  result = lappend (result, serializeInt (fdwState->db2Table->ncols));
  /* number of columns in PostgreSQL table */
  result = lappend (result, serializeInt (fdwState->db2Table->npgcols));
  /* column data */
  for (i = 0; i < fdwState->db2Table->ncols; ++i) {
    result = lappend (result, serializeString (fdwState->db2Table->cols[i]->name));
    result = lappend (result, serializeInt (fdwState->db2Table->cols[i]->db2type));
    result = lappend (result, serializeInt (fdwState->db2Table->cols[i]->scale));
    result = lappend (result, serializeString (fdwState->db2Table->cols[i]->pgname));
    result = lappend (result, serializeInt (fdwState->db2Table->cols[i]->pgattnum));
    result = lappend (result, serializeOid (fdwState->db2Table->cols[i]->pgtype));
    result = lappend (result, serializeInt (fdwState->db2Table->cols[i]->pgtypmod));
    result = lappend (result, serializeInt (fdwState->db2Table->cols[i]->used));
    result = lappend (result, serializeInt (fdwState->db2Table->cols[i]->pkey));
    result = lappend (result, serializeLong (fdwState->db2Table->cols[i]->val_size));
    result = lappend (result, serializeInt (fdwState->db2Table->cols[i]->noencerr));
    /* don't serialize val, val_len, val_len4, val_null and varno */
  }

  /* find length of parameter list */
  for (param = fdwState->paramList; param; param = param->next)
    ++len;
  /* serialize length */
  result = lappend (result, serializeInt (len));
  /* parameter list entries */
  for (param = fdwState->paramList; param; param = param->next) {
    result = lappend (result, serializeString (param->name));
    result = lappend (result, serializeOid (param->type));
    result = lappend (result, serializeInt ((int) param->bindType));
    result = lappend (result, serializeInt ((int) param->colnum));
    /* don't serialize value, node and bindh */
  }
  /* don't serialize params, startup_cost, total_cost, rowcount, columnindex, temp_cxt, order_clause and where_clause */

  return result;
}

/*
 * serializeString
 * 		Create a Const that contains the string.
 */

Const * serializeString (const char *s)
{
  if (s == NULL)
    return makeNullConst (TEXTOID, -1, InvalidOid);
  else
    return makeConst (TEXTOID, -1, InvalidOid, -1, PointerGetDatum (cstring_to_text (s)), false, false);
}

/*
 * serializeLong
 * 		Create a Const that contains the long integer.
 */

Const * serializeLong (long i)
{
  if (sizeof (long) <= 4)
    return makeConst (INT4OID, -1, InvalidOid, 4, Int32GetDatum ((int32) i), false, true);
  else
    return makeConst (INT4OID, -1, InvalidOid, 8, Int64GetDatum ((int64) i), false,
#ifdef USE_FLOAT8_BYVAL
		      true
#else
		      false
#endif /* USE_FLOAT8_BYVAL */
      );
}

/*
 * deserializePlanData
 * 		Extract the data structures from a List created by serializePlanData.
 */

struct DB2FdwState *
deserializePlanData (List * list)
{
  struct DB2FdwState *state = palloc (sizeof (struct DB2FdwState));
  ListCell *cell = list_head (list);
  int i, len;
  struct paramDesc *param;

  /* session will be set upon connect */
  state->session = NULL;
  /* these fields are not needed during execution */
  state->startup_cost = 0;
  state->total_cost = 0;
  /* these are not serialized */
  state->rowcount = 0;
  state->columnindex = 0;
  state->params = NULL;
  state->temp_cxt = NULL;
  state->order_clause = NULL;

  /* dbserver */
  state->dbserver = deserializeString (lfirst (cell));
  cell = list_next (list,cell);

  /* user */
  state->user = deserializeString (lfirst (cell));
  cell = list_next (list,cell);

  /* password */
  state->password = deserializeString (lfirst (cell));
  cell = list_next (list,cell);

  /* nls_lang */
  state->nls_lang = deserializeString (lfirst (cell));
  cell = list_next (list,cell);

  /* query */
  state->query = deserializeString (lfirst (cell));
  cell = list_next (list,cell);

  /* DB2 prefetch count */
  state->prefetch = (unsigned int) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
  cell = list_next (list,cell);

  /* table data */
  state->db2Table = (struct db2Table *) palloc (sizeof (struct db2Table));
  state->db2Table->name = deserializeString (lfirst (cell));
  cell = list_next (list,cell);
  state->db2Table->pgname = deserializeString (lfirst (cell));
  cell = list_next (list,cell);
  state->db2Table->ncols = (int) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
  cell = list_next (list,cell);
  state->db2Table->npgcols = (int) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
  cell = list_next (list,cell);
  state->db2Table->cols = (struct db2Column **) palloc (sizeof (struct db2Column *) * state->db2Table->ncols);

  /* loop columns */
  for (i = 0; i < state->db2Table->ncols; ++i) {
    state->db2Table->cols[i] = (struct db2Column *) palloc (sizeof (struct db2Column));
    state->db2Table->cols[i]->name = deserializeString (lfirst (cell));
    cell = list_next (list,cell);
    state->db2Table->cols[i]->db2type = (db2Type) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->scale = (int) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->pgname = deserializeString (lfirst (cell));
    cell = list_next (list,cell);
    state->db2Table->cols[i]->pgattnum = (int) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->pgtype = DatumGetObjectId (((Const *) lfirst (cell))->constvalue);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->pgtypmod = (int) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->used = (int) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->pkey = (int) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->val_size = deserializeLong (lfirst (cell));
    cell = list_next (list,cell);
    state->db2Table->cols[i]->noencerr = (int) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
    cell = list_next (list,cell);
    /* allocate memory for the result value */
    state->db2Table->cols[i]->val = (char *) palloc (state->db2Table->cols[i]->val_size + 1);
    state->db2Table->cols[i]->val_len = 0;
    state->db2Table->cols[i]->val_len4 = 0;
    state->db2Table->cols[i]->val_null = 1;
  }

  /* length of parameter list */
  len = (int) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
  cell = list_next (list,cell);

  /* parameter table entries */
  state->paramList = NULL;
  for (i = 0; i < len; ++i) {
    param = (struct paramDesc *) palloc (sizeof (struct paramDesc));
    param->name = deserializeString (lfirst (cell));
    cell = list_next (list,cell);
    param->type = DatumGetObjectId (((Const *) lfirst (cell))->constvalue);
    cell = list_next (list,cell);
    param->bindType = (db2BindType) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
    cell = list_next (list,cell);
    if (param->bindType == BIND_OUTPUT)
      param->value = (void *) 42;	/* something != NULL */
    else
      param->value = NULL;
    param->node = NULL;
    param->bindh = NULL;
    param->colnum = (int) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
    cell = list_next (list,cell);
    param->next = state->paramList;
    state->paramList = param;
  }

  return state;
}

/*
 * deserializeString
 * 		Extracts a string from a Const, returns a palloc'ed copy.
 */

char *
deserializeString (Const * constant)
{
  if (constant->constisnull)
    return NULL;
  else
    return text_to_cstring (DatumGetTextP (constant->constvalue));
}

/*
 * deserializeLong
 * 		Extracts a long integer from a Const.
 */

long
deserializeLong (Const * constant)
{
  if (sizeof (long) <= 4)
    return (long) DatumGetInt32 (constant->constvalue);
  else
    return (long) DatumGetInt64 (constant->constvalue);
}

#ifndef OLD_FDW_API
/*
 * optionIsTrue
 * 		Returns true if the string is "true", "on" or "yes".
 */
bool
optionIsTrue (const char *value)
{
  if (pg_strcasecmp (value, "on") == 0 || pg_strcasecmp (value, "yes") == 0 || pg_strcasecmp (value, "true") == 0)
    return true;
  else
    return false;
}

/*
 * find_em_expr_for_rel
 * 		Find an equivalence class member expression, all of whose Vars come from
 * 		the indicated relation.
 */
Expr *
find_em_expr_for_rel (EquivalenceClass * ec, RelOptInfo * rel)
{
  ListCell *lc_em;

  foreach (lc_em, ec->ec_members) {
    EquivalenceMember *em = lfirst (lc_em);

    if (bms_equal (em->em_relids, rel->relids)) {
      /*
       * If there is more than one equivalence member whose Vars are
       * taken entirely from this relation, we'll be content to choose
       * any one of those.
       */
      return em->em_expr;
    }
  }

  /* We didn't find any suitable equivalence class expression */
  return NULL;
}
#endif /* OLD_FDW_API */

/*
 * deparseDate
 * 		Render a PostgreSQL date so that DB2 can parse it.
 */
char *
deparseDate (Datum datum)
{
  struct pg_tm datetime_tm;
  StringInfoData s;

  if (DATE_NOT_FINITE (DatumGetDateADT (datum)))
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("infinite date value cannot be stored in DB2")));

  /* get the parts */
  (void) j2date (DatumGetDateADT (datum) + POSTGRES_EPOCH_JDATE, &(datetime_tm.tm_year), &(datetime_tm.tm_mon), &(datetime_tm.tm_mday));

  if (datetime_tm.tm_year < 0)
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("BC date value cannot be stored in DB2")));

  initStringInfo (&s);
  appendStringInfo (&s, "%04d-%02d-%02d 00:00:00",
		    datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1, datetime_tm.tm_mon, datetime_tm.tm_mday);

  return s.data;
}

/*
 * deparseTimestamp
 * 		Render a PostgreSQL timestamp so that DB2 can parse it.
 */
char *
deparseTimestamp (Datum datum, bool hasTimezone)
{
  struct pg_tm datetime_tm;
  int32 tzoffset;
  fsec_t datetime_fsec;
  StringInfoData s;

  /* this is sloppy, but DatumGetTimestampTz and DatumGetTimestamp are the same */
  if (TIMESTAMP_NOT_FINITE (DatumGetTimestampTz (datum)))
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("infinite timestamp value cannot be stored in DB2")));

  /* get the parts */
  tzoffset = 0;
  (void) timestamp2tm (DatumGetTimestampTz (datum), hasTimezone ? &tzoffset : NULL, &datetime_tm, &datetime_fsec, NULL, NULL);

  if (datetime_tm.tm_year < 0)
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("BC date value cannot be stored in DB2")));

  initStringInfo (&s);
  if (hasTimezone)
    appendStringInfo (&s, "%04d-%02d-%02d %02d:%02d:%02d.%06d%+03d:%02d",
		      datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1,
		      datetime_tm.tm_mon, datetime_tm.tm_mday, datetime_tm.tm_hour,
		      datetime_tm.tm_min, datetime_tm.tm_sec, (int32) datetime_fsec,
		      -tzoffset / 3600, ((tzoffset > 0) ? tzoffset % 3600 : -tzoffset % 3600) / 60);
  else
    appendStringInfo (&s, "%04d-%02d-%02d %02d:%02d:%02d.%06d",
		      datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1,
		      datetime_tm.tm_mon, datetime_tm.tm_mday, datetime_tm.tm_hour,
		      datetime_tm.tm_min, datetime_tm.tm_sec, (int32) datetime_fsec);

  return s.data;
}

/*
 * deparsedeparseInterval
 * 		Render a PostgreSQL timestamp so that DB2 can parse it.
 */
char *
deparseInterval (Datum datum)
{
#if PG_VERSION_NUM >= 150000
  struct pg_itm tm;
#else
  struct pg_tm tm;
#endif
  fsec_t fsec=0;
  StringInfoData s;
  char *sign;

#if PG_VERSION_NUM >= 150000
  interval2itm (*DatumGetIntervalP (datum), &tm);
#else
 if (interval2tm (*DatumGetIntervalP (datum), &tm, &fsec) != 0) {
	     elog (ERROR, "could not convert interval to tm");
	   }
#endif
  /* only translate intervals that can be translated to INTERVAL DAY TO SECOND */
  if (tm.tm_year != 0 || tm.tm_mon != 0)
    return NULL;

  /* DB2 intervals have only one sign */
  if (tm.tm_mday < 0 || tm.tm_hour < 0 || tm.tm_min < 0 || tm.tm_sec < 0 || fsec < 0) {
    sign = "-";
    /* all signs must match */
    if (tm.tm_mday > 0 || tm.tm_hour > 0 || tm.tm_min > 0 || tm.tm_sec > 0 || fsec > 0)
      return NULL;
    tm.tm_mday = -tm.tm_mday;
    tm.tm_hour = -tm.tm_hour;
    tm.tm_min = -tm.tm_min;
    tm.tm_sec = -tm.tm_sec;
    fsec = -fsec;
  }
  else
    sign = "";

  initStringInfo (&s);
#if PG_VERSION_NUM >= 150000
  appendStringInfo (&s, "INTERVAL '%s%d %02ld:%02d:%02d.%06d' DAY(9) TO SECOND(6)", sign, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, fsec);
#else
  appendStringInfo (&s, "INTERVAL '%s%d %02d:%02d:%02d.%06d' DAY(9) TO SECOND(6)", sign, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, fsec);
#endif
  return s.data;
}

#ifdef WRITE_API
/*
 * copyPlanData
 * 		Create a deep copy of the argument, copy only those fields needed for planning.
 */

struct DB2FdwState *
copyPlanData (struct DB2FdwState *orig)
{
  int i;
  struct DB2FdwState *copy = palloc (sizeof (struct DB2FdwState));

  copy->dbserver = pstrdup (orig->dbserver);
  copy->user = pstrdup (orig->user);
  copy->password = pstrdup (orig->password);
  copy->nls_lang = pstrdup (orig->nls_lang);
  copy->session = NULL;
  copy->query = NULL;
  copy->paramList = NULL;
  copy->db2Table = (struct db2Table *) palloc (sizeof (struct db2Table));
  copy->db2Table->name = pstrdup (orig->db2Table->name);
  copy->db2Table->pgname = pstrdup (orig->db2Table->pgname);
  copy->db2Table->ncols = orig->db2Table->ncols;
  copy->db2Table->npgcols = orig->db2Table->npgcols;
  copy->db2Table->cols = (struct db2Column **) palloc (sizeof (struct db2Column *) * orig->db2Table->ncols);
  for (i = 0; i < orig->db2Table->ncols; ++i) {
    copy->db2Table->cols[i] = (struct db2Column *) palloc (sizeof (struct db2Column));
    copy->db2Table->cols[i]->name = pstrdup (orig->db2Table->cols[i]->name);
    copy->db2Table->cols[i]->db2type = orig->db2Table->cols[i]->db2type;
    copy->db2Table->cols[i]->scale = orig->db2Table->cols[i]->scale;
    if (orig->db2Table->cols[i]->pgname == NULL)
      copy->db2Table->cols[i]->pgname = NULL;
    else
      copy->db2Table->cols[i]->pgname = pstrdup (orig->db2Table->cols[i]->pgname);
    copy->db2Table->cols[i]->pgattnum = orig->db2Table->cols[i]->pgattnum;
    copy->db2Table->cols[i]->pgtype = orig->db2Table->cols[i]->pgtype;
    copy->db2Table->cols[i]->pgtypmod = orig->db2Table->cols[i]->pgtypmod;
    copy->db2Table->cols[i]->used = 0;
    copy->db2Table->cols[i]->pkey = orig->db2Table->cols[i]->pkey;
    copy->db2Table->cols[i]->val = NULL;
    copy->db2Table->cols[i]->val_size = orig->db2Table->cols[i]->val_size;
    copy->db2Table->cols[i]->val_len = 0;
    copy->db2Table->cols[i]->val_len4 = 0;
    copy->db2Table->cols[i]->val_null = 0;
    copy->db2Table->cols[i]->noencerr = orig->db2Table->cols[i]->noencerr;
  }
  copy->startup_cost = 0.0;
  copy->total_cost = 0.0;
  copy->rowcount = 0;
  copy->columnindex = 0;
  copy->temp_cxt = NULL;
  copy->order_clause = NULL;

  return copy;
}

/*
 * subtransactionCallback
 * 		Set or rollback to DB2 savepoints when appropriate.
 */
void
subtransactionCallback (SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void *arg)
{
  /* rollback to the appropriate savepoint on subtransaction abort */
  if (event == SUBXACT_EVENT_ABORT_SUB || event == SUBXACT_EVENT_PRE_COMMIT_SUB)
    db2EndSubtransaction (arg, GetCurrentTransactionNestLevel (), event == SUBXACT_EVENT_PRE_COMMIT_SUB);
}

/*
 * addParam
 * 		Creates a new struct paramDesc with the given values and adds it to the list.
 * 		A palloc'ed copy of "name" is used.
 */
void
addParam (struct paramDesc **paramList, char *name, Oid pgtype, db2Type db2type, int colnum)
{
  struct paramDesc *param;

  param = palloc (sizeof (struct paramDesc));
  param->name = pstrdup (name);
  param->type = pgtype;
  switch (db2type) {
  case SQL_TYPE_INTEGER:
  case SQL_TYPE_BIG:
  case SQL_TYPE_SMALL:
  case SQL_TYPE_FLOAT:
    param->bindType = BIND_NUMBER;
    break;
  case SQL_TYPE_CLOB:
    param->bindType = BIND_LONG;
    break;
  case SQL_TYPE_BLOB:
    param->bindType = BIND_LONGRAW;
    break;
  default:
    param->bindType = BIND_STRING;
  }
  param->value = NULL;
  param->node = NULL;
  param->bindh = NULL;
  param->colnum = colnum;
  param->next = *paramList;
  *paramList = param;
}

/*
 * setModifyParameters
 * 		Set the parameter values from the values in the slots.
 * 		"newslot" contains the new values, "oldslot" the old ones.
 */
void
setModifyParameters (struct paramDesc *paramList, TupleTableSlot * newslot, TupleTableSlot * oldslot, struct db2Table *db2Table, db2Session * session)
{
  struct paramDesc *param;
  Datum datum;
  bool isnull;
  int32 value_len;
  char *p, *q;
  Oid pgtype;

  for (param = paramList; param != NULL; param = param->next) {
    /* don't do anything for output parameters */
    if (param->bindType == BIND_OUTPUT)
      continue;

    if (param->name[1] == 'k') {
      /* for primary key parameters extract the resjunk entry */
      datum = ExecGetJunkAttribute (oldslot, db2Table->cols[param->colnum]->pkey, &isnull);
    }
    else {
      /* for other parameters extract the datum from newslot */
      datum = slot_getattr (newslot, db2Table->cols[param->colnum]->pgattnum, &isnull);
    }

    switch (param->bindType) {
    case BIND_STRING:
    case BIND_NUMBER:
      if (isnull) {
	param->value = NULL;
	break;
      }

      pgtype = db2Table->cols[param->colnum]->pgtype;

      /* special treatment for date, timestamps and intervals */
      if (pgtype == DATEOID) {
	param->value = deparseDate (datum);
	break;			/* from switch (param->bindType) */
      } else if (pgtype == TIMESTAMPOID || pgtype == TIMESTAMPTZOID) {
	param->value = deparseTimestamp (datum, (pgtype == TIMESTAMPTZOID));
	break;			/* from switch (param->bindType) */
      } else if (pgtype == TIMEOID || pgtype == TIMETZOID) {
	param->value = deparseTimestamp (datum, (pgtype == TIMETZOID));
	break;			/* from switch (param->bindType) */
      }

      /* convert the parameter value into a string */
      param->value = DatumGetCString (OidFunctionCall1 (output_funcs[param->colnum], datum));

      /* some data types need additional processing */
      switch (db2Table->cols[param->colnum]->pgtype) {
      case UUIDOID:
	/* remove the minus signs for UUIDs */
	for (p = q = param->value; *p != '\0'; ++p, ++q) {
	  if (*p == '-')
	    ++p;
	  *q = *p;
	}
	*q = '\0';
	break;
      case BOOLOID:
	/* convert booleans to numbers */
	if (param->value[0] == 't')
	  param->value[0] = '1';
	else
	  param->value[0] = '0';
	param->value[1] = '\0';
	break;
      default:
	/* nothing to be done */
	break;
      }
      break;
    case BIND_LONG:
    case BIND_LONGRAW:
      if (isnull) {
	param->value = NULL;
	break;
      }

      /* detoast it if necessary */
      datum = (Datum) PG_DETOAST_DATUM (datum);

      value_len = VARSIZE (datum) - VARHDRSZ;

      /* the first 4 bytes contain the length */
      param->value = palloc (value_len + 4);
      memcpy (param->value, (const char *) &value_len, 4);
      memcpy (param->value + 4, VARDATA (datum), value_len);
      break;
    case BIND_OUTPUT:
      /* unreachable */
      break;
    }
  }
}
#endif /* WRITE_API */

/*
 * transactionCallback
 * 		Commit or rollback DB2 transactions when appropriate.
 */
void
transactionCallback (XactEvent event, void *arg)
{
  switch (event) {
  case XACT_EVENT_PRE_COMMIT:
  case XACT_EVENT_PARALLEL_PRE_COMMIT:
    /* remote commit */
    db2EndTransaction (arg, 1, 0);
    break;
  case XACT_EVENT_PRE_PREPARE:
    ereport (ERROR, (errcode (ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION), errmsg ("cannot prepare a transaction that used remote tables")));
    break;
  case XACT_EVENT_COMMIT:
  case XACT_EVENT_PREPARE:
  case XACT_EVENT_PARALLEL_COMMIT:
    /*
     * Commit the remote transaction ignoring errors.
     * In 9.3 or higher, the transaction must already be closed, so this does nothing.
     * In 9.2 or lower, this is ok since nothing can have been modified remotely.
     */
    db2EndTransaction (arg, 1, 1);
    break;
  case XACT_EVENT_ABORT:
  case XACT_EVENT_PARALLEL_ABORT:
    /* remote rollback */
    db2EndTransaction (arg, 0, 1);
    break;
  }

  dml_in_transaction = false;
}

/*
 * exitHook
 * 		Close all DB2 connections on process exit.
 */

void
exitHook (int code, Datum arg)
{
  db2Shutdown ();
}

/*
 * db2Die
 * 		Terminate the current query and prepare backend shutdown.
 * 		This is a signal handler function.
 */
void
db2Die (SIGNAL_ARGS)
{
  /*
   * Terminate any running queries.
   * The DB2 sessions will be terminated by exitHook().
   */
  db2Cancel ();

  /*
   * Call the original backend shutdown function.
   * If a query was canceled above, an error from DB2 would result.
   * To have the backend report the correct FATAL error instead,
   * we have to call CHECK_FOR_INTERRUPTS() before we report that error;
   * this is done in db2Error_d.
   */
  die (postgres_signal_arg);
}

/*
 * setSelectParameters
 * 		Set the current values of the parameters into paramList.
 * 		Return a string containing the parameters set for a DEBUG message.
 */
char *
setSelectParameters (struct paramDesc *paramList, ExprContext * econtext)
{
  struct paramDesc *param;
  Datum datum;
  HeapTuple tuple;
  TimestampTz tstamp;
  bool is_null;
  bool first_param = true;
#ifndef OLD_FDW_API
  MemoryContext oldcontext;
#endif /* OLD_FDW_API */
  StringInfoData info;		/* list of parameters for DEBUG message */
  initStringInfo (&info);

#ifndef OLD_FDW_API
  /* switch to short lived memory context */
  oldcontext = MemoryContextSwitchTo (econtext->ecxt_per_tuple_memory);
#endif /* OLD_FDW_API */

  /* iterate parameter list and fill values */
  for (param = paramList; param; param = param->next) {
    if (strcmp (param->name, ":now") == 0) {
      /* get transaction start timestamp */
      tstamp = GetCurrentTransactionStartTimestamp ();

      datum = TimestampGetDatum (tstamp);
      is_null = false;
    }
    else {
      /*
       * Evaluate the expression.
       * This code path cannot be reached in 9.1
       */
#if PG_VERSION_NUM < 100000
      datum = ExecEvalExpr ((ExprState *) (param->node), econtext, &is_null, NULL);
#else
      datum = ExecEvalExpr ((ExprState *) (param->node), econtext, &is_null);
#endif /* PG_VERSION_NUM */
    }

    if (is_null) {
      param->value = NULL;
    }
    else {
      if (param->type == DATEOID)
	param->value = deparseDate (datum);
      else if (param->type == TIMESTAMPOID || param->type == TIMESTAMPTZOID)
	param->value = deparseTimestamp (datum, (param->type == TIMESTAMPTZOID));
      else if (param->type == TIMEOID || param->type == TIMETZOID)
	param->value = deparseTimestamp (datum, (param->type == TIMETZOID));
      else {
	regproc typoutput;

	/* get the type's output function */
	tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (param->type));
	if (!HeapTupleIsValid (tuple)) {
	  elog (ERROR, "cache lookup failed for type %u", param->type);
	}
	typoutput = ((Form_pg_type) GETSTRUCT (tuple))->typoutput;
	ReleaseSysCache (tuple);

	/* convert the parameter value into a string */
	param->value = DatumGetCString (OidFunctionCall1 (typoutput, datum));
      }
    }

    /* build a parameter list for the DEBUG message */
    if (first_param) {
      first_param = false;
      appendStringInfo (&info, ", parameters %s=\"%s\"", param->name, (param->value ? param->value : "(null)"));
    }
    else {
      appendStringInfo (&info, ", %s=\"%s\"", param->name, (param->value ? param->value : "(null)"));
    }
  }

#ifndef OLD_FDW_API
  /* reset memory context */
  MemoryContextSwitchTo (oldcontext);
#endif /* OLD_FDW_API */

  return info.data;
}

/*
 * convertTuple
 * 		Convert a result row from DB2 stored in db2Table
 * 		into arrays of values and null indicators.
 * 		If trunc_lob it true, truncate LOBs to WIDTH_THRESHOLD+1 bytes.
 */
void
convertTuple (struct DB2FdwState *fdw_state, Datum * values, bool * nulls, bool trunc_lob)
{
  char *tmp_value = NULL;
  char *value = NULL;
  long value_len = 0;
  int j, index = -1;
  ErrorContextCallback errcb;
  Oid pgtype;

  /* initialize error context callback, install it only during conversions */
  errcb.callback = errorContextCallback;
  errcb.arg = (void *) fdw_state;

  /* assign result values */
  for (j = 0; j < fdw_state->db2Table->npgcols; ++j) {
    /* for dropped columns, insert a NULL */
    if ((index + 1 < fdw_state->db2Table->ncols)
	&& (fdw_state->db2Table->cols[index + 1]->pgattnum > j + 1)) {
      nulls[j] = true;
      values[j] = PointerGetDatum (NULL);
      continue;
    }
    else
      ++index;

    /*
     * Columns exceeding the length of the DB2 table will be NULL,
     * as well as columns that are not used in the query.
     * Geometry columns are NULL if the value is NULL,
     * for all other types use the NULL indicator.
     */
    if (index >= fdw_state->db2Table->ncols || fdw_state->db2Table->cols[index]->used == 0 || fdw_state->db2Table->cols[index]->val_null == -1) {
      nulls[j] = true;
      values[j] = PointerGetDatum (NULL);
      continue;
    }

    /* from here on, we can assume columns to be NOT NULL */
    nulls[j] = false;
    pgtype = fdw_state->db2Table->cols[index]->pgtype;

    /* get the data and its length */
    if (fdw_state->db2Table->cols[index]->db2type == SQL_TYPE_BLOB || fdw_state->db2Table->cols[index]->db2type == SQL_TYPE_CLOB) {
      /* for LOBs, get the actual LOB contents (palloc'ed), truncated if desired */
      db2GetLob (fdw_state->session,
		 (void *) fdw_state->db2Table->cols[index]->val, fdw_state->db2Table->cols[index]->db2type, &value, &value_len, trunc_lob ? (WIDTH_THRESHOLD + 1) : 0);
    }
    else if (fdw_state->db2Table->cols[index]->db2type == SQL_TYPE_BIG) {
      /* for LONG and LONG RAW, the first 4 bytes contain the length */
      value_len = *((int32 *) fdw_state->db2Table->cols[index]->val);
      /* the rest is the actual data */
      value = fdw_state->db2Table->cols[index]->val + 4;
      /* terminating zero byte (needed for LONGs) */
      value[value_len] = '\0';
    }
    else if(fdw_state->db2Table->cols[index]->db2type ==  SQL_TYPE_FLOAT
	    || fdw_state->db2Table->cols[index]->db2type == SQL_TYPE_DECIMAL
	    || fdw_state->db2Table->cols[index]->db2type == SQL_TYPE_INTEGER
	    || fdw_state->db2Table->cols[index]->db2type == SQL_TYPE_REAL
	    || fdw_state->db2Table->cols[index]->db2type == SQL_TYPE_DOUBLE) {
      value = fdw_state->db2Table->cols[index]->val;
      value_len = fdw_state->db2Table->cols[index]->val_len;
      tmp_value = value;

      if((tmp_value = strchr(value,','))!=NULL) {
               *tmp_value = '.';
      }


    } else {
      /* for other data types, db2Table contains the results */
      value = fdw_state->db2Table->cols[index]->val;
      value_len = fdw_state->db2Table->cols[index]->val_len;
    }

    /* fill the TupleSlot with the data (after conversion if necessary) */
    if (pgtype == BYTEAOID) {
      /* binary columns are not converted */
      bytea *result = (bytea *) palloc (value_len + VARHDRSZ);
      memcpy (VARDATA (result), value, value_len);
      SET_VARSIZE (result, value_len + VARHDRSZ);

      values[j] = PointerGetDatum (result);
    }
    else {
      regproc typinput;
      HeapTuple tuple;
      Datum dat;

      /* find the appropriate conversion function */
      tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (pgtype));
      if (!HeapTupleIsValid (tuple)) {
	elog (ERROR, "cache lookup failed for type %u", pgtype);
      }
      typinput = ((Form_pg_type) GETSTRUCT (tuple))->typinput;
      ReleaseSysCache (tuple);

      dat = CStringGetDatum (value);

      /* install error context callback */
      errcb.previous = error_context_stack;
      error_context_stack = &errcb;
      fdw_state->columnindex = index;

      /* for string types, check that the data are in the database encoding */
      if (pgtype == BPCHAROID || pgtype == VARCHAROID || pgtype == TEXTOID)
	(void) pg_verify_mbstr (GetDatabaseEncoding (), value, value_len, fdw_state->db2Table->cols[index]->noencerr == NO_ENC_ERR_TRUE);

      /* call the type input function */
      switch (pgtype) {
      case BPCHAROID:
      case VARCHAROID:
      case TIMESTAMPOID:
      case TIMESTAMPTZOID:
      case TIMEOID:
      case TIMETZOID:
      case INTERVALOID:
      case NUMERICOID:
	/* these functions require the type modifier */
	values[j] = OidFunctionCall3 (typinput, dat, ObjectIdGetDatum (InvalidOid), Int32GetDatum (fdw_state->db2Table->cols[index]->pgtypmod));
	break;
      default:
	/* the others don't */
	values[j] = OidFunctionCall1 (typinput, dat);
      }

      /* uninstall error context callback */
      error_context_stack = errcb.previous;
    }

    /* free the data buffer for LOBs */
    if (fdw_state->db2Table->cols[index]->db2type == SQL_TYPE_BLOB || fdw_state->db2Table->cols[index]->db2type == SQL_TYPE_CLOB)
      pfree (value);
  }
}

/*
 * errorContextCallback
 * 		Provides the context for an error message during a type input conversion.
 * 		The argument must be a pointer to a "struct DB2FdwState".
 */
void
errorContextCallback (void *arg)
{
  struct DB2FdwState *fdw_state = (struct DB2FdwState *) arg;

  errcontext ("converting column \"%s\" for foreign table scan of \"%s\", row %lu",
	      quote_identifier (fdw_state->db2Table->cols[fdw_state->columnindex]->pgname), quote_identifier (fdw_state->db2Table->pgname), fdw_state->rowcount);
}

#ifdef IMPORT_API
/*
 * fold_case
 * 		Returns a palloc'ed string that is the case-folded first argument.
 */
char *
fold_case (char *name, fold_t foldcase)
{
  if (foldcase == CASE_KEEP)
    return pstrdup (name);

  if (foldcase == CASE_LOWER)
    return str_tolower (name, strlen (name), DEFAULT_COLLATION_OID);

  if (foldcase == CASE_SMART) {
    char *upstr = str_toupper (name, strlen (name), DEFAULT_COLLATION_OID);

    /* fold case only if it does not contain lower case characters */
    if (strcmp (upstr, name) == 0)
      return str_tolower (name, strlen (name), DEFAULT_COLLATION_OID);
    else
      return pstrdup (name);
  }

  elog (ERROR, "impossible case folding type %d", foldcase);

  return NULL;			/* unreachable, but keeps compiler happy */
}
#endif /* IMPORT_API */

/*
 * db2GetShareFileName
 * 		Returns the (palloc'ed) absolute path of a file in the "share" directory.
 */
char *
db2GetShareFileName (const char *relativename)
{
  char share_path[MAXPGPATH], *result;

  get_share_path (my_exec_path, share_path);

  result = palloc (MAXPGPATH);
  snprintf (result, MAXPGPATH, "%s/%s", share_path, relativename);

  return result;
}

/*
 * db2RegisterCallback
 * 		Register a callback for PostgreSQL transaction events.
 */
void
db2RegisterCallback (void *arg)
{
  RegisterXactCallback (transactionCallback, arg);
#ifdef WRITE_API
  RegisterSubXactCallback (subtransactionCallback, arg);
#endif /* WRITE_API */
}

/*
 * db2UnregisterCallback
 * 		Unregister a callback for PostgreSQL transaction events.
 */
void
db2UnregisterCallback (void *arg)
{
  UnregisterXactCallback (transactionCallback, arg);
#ifdef WRITE_API
  UnregisterSubXactCallback (subtransactionCallback, arg);
#endif /* WRITE_API */
}

/*
 * db2Alloc
 * 		Expose palloc() to DB2 functions.
 */
void *
db2Alloc (size_t size)
{
  return palloc (size);
}

/*
 * db2Realloc
 * 		Expose repalloc() to DB2 functions.
 */
void *
db2Realloc (void *p, size_t size)
{
  return repalloc (p, size);
}

/*
 * db2Free
 * 		Expose pfree() to DB2 functions.
 */
void
db2Free (void *p)
{
  pfree (p);
}

/*
 * db2SetHandlers
 * 		Set signal handler for SIGTERM.
 */
void
db2SetHandlers ()
{
  pqsignal (SIGTERM, db2Die);
}

/* get a PostgreSQL error code from an db2error */
#define to_sqlstate(x) \
	(x==FDW_UNABLE_TO_ESTABLISH_CONNECTION ? ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION : \
	(x==FDW_UNABLE_TO_CREATE_REPLY ? ERRCODE_FDW_UNABLE_TO_CREATE_REPLY : \
	(x==FDW_TABLE_NOT_FOUND ? ERRCODE_FDW_TABLE_NOT_FOUND : \
	(x==FDW_UNABLE_TO_CREATE_EXECUTION ? ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION : \
	(x==FDW_OUT_OF_MEMORY ? ERRCODE_FDW_OUT_OF_MEMORY : \
	(x==FDW_SERIALIZATION_FAILURE ? ERRCODE_T_R_SERIALIZATION_FAILURE : ERRCODE_FDW_ERROR))))))
/*
 *  * db2Error_d
 *   * 		Report a PostgreSQL error with a detail message.
 *    */
void db2Error_d (db2error sqlstate, const char *message, const char *detail, ...)
{
  char cBuffer [4000];
  va_list arg_marker;

  /* if the backend was terminated, report that rather than the DB2 error */
  CHECK_FOR_INTERRUPTS ();
  va_start (arg_marker, detail);
  vsprintf (cBuffer, detail, arg_marker);
  ereport (ERROR, (errcode (to_sqlstate (sqlstate)), errmsg ("%s", message), errdetail ("%s", cBuffer)));
  va_end   (arg_marker);
}

/*
 *  * db2error
 *   * 		Report a PostgreSQL error without detail message.
 *    */
void
db2Error (db2error sqlstate, const char *message)
{
  /* use errcode_for_file_access() if the message contains %m */
  if (strstr (message, "%m")) {
    ereport (ERROR, (errcode_for_file_access (), errmsg (message, "")));
  }
  else {
    ereport (ERROR, (errcode (to_sqlstate (sqlstate)), errmsg ("%s", message)));
  }
}

/********************************************************************************/
/********************************************************************************/
/*   db2Debug2,debug3 4 5                                                       */
/*   Report a PostgreSQL message at level DEBUG2.                               */
/********************************************************************************/
void db2Debug2 (const char *message, ...)
{
char cBuffer [4000];
va_list arg_marker;
va_start (arg_marker, message);
vsprintf (cBuffer, message, arg_marker);
elog (DEBUG2, "%s", cBuffer);
va_end   (arg_marker);
}

void db2Debug3 (const char *message, ...)
{
char cBuffer [4000];
va_list arg_marker;
va_start (arg_marker, message);
vsprintf (cBuffer, message, arg_marker);
elog (DEBUG3, "%s", cBuffer);
va_end   (arg_marker);
}
void db2Debug4 (const char *message, ...)
{
char cBuffer [4000];
va_list arg_marker;
va_start (arg_marker, message);
vsprintf (cBuffer, message, arg_marker);
elog (DEBUG4, "%s", cBuffer);
va_end   (arg_marker);
}
void db2Debug5 (const char *message, ...)
{
char cBuffer [4000];
va_list arg_marker;
va_start (arg_marker, message);
vsprintf (cBuffer, message, arg_marker);
elog (DEBUG5, "%s", cBuffer);
va_end   (arg_marker);
}
