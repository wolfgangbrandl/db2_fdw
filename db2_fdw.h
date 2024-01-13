/*-------------------------------------------------------------------------
 *
 * db2_fdw.h
 * 		This header file contains all definitions that are shared by
 * 		db2_fdw.c and db2_utils.c.
 * 		It is necessary to split db2_fdw into two source files because
 * 		PostgreSQL and DB2 headers cannot be #included at the same time.
 *
 *-------------------------------------------------------------------------
 */

/* this one is safe to include and gives us Oid */
#include "postgres_ext.h"

#include <stdbool.h>
#include <sys/types.h>

/* db2_fdw version */
#define DB2_FDW_VERSION "6.0.1"

#ifdef OCI_ORACLE
/*
 * Types for a linked list for various handles.
 * DB2 sessions can be multiplexed over one server connection.
 */
struct connEntry
{
  char *user;
  OCISvcCtx *svchp;
  OCISession *userhp;
  struct handleEntry *handlelist;
  int xact_level;		/* 0 = none, 1 = main, else subtransaction */
  struct connEntry *left;
  struct connEntry *right;
};

struct srvEntry
{
  char *connectstring;
  OCIServer *srvhp;
  struct connEntry *connlist;
  struct srvEntry *left;
  struct srvEntry *right;
};

struct envEntry
{
  char *nls_lang;
  OCIEnv *envhp;
  OCIError *errhp;
  struct srvEntry *srvlist;
  struct envEntry *left;
  struct envEntry *right;
};
typedef unsigned char DB2Text;
/*
 * Represents one DB2 connection, points to cached entries.
 * This is necessary to be able to pass them back to
 * db2_fdw.c without having to #include oci.h there.
 */
struct db2Session
{
  struct envEntry *envp;
  struct srvEntry *srvp;
  struct connEntry *connp;
  OCIStmt *stmthp;
};
#endif
typedef struct db2Session db2Session;

/* types for the DB2 table description */
typedef enum
{
  SQL_TYPE_VARCHAR,
  SQL_TYPE_CHAR,
  SQL_TYPE_SMALL,
  SQL_TYPE_INTEGER,
  SQL_TYPE_BIG,
  SQL_TYPE_DATE,
  SQL_TYPE_STAMP,
  SQL_TYPE_TIME,
  SQL_TYPE_XML,
  SQL_TYPE_BLOB,
  SQL_TYPE_CLOB,
  SQL_TYPE_DECIMAL,
  SQL_TYPE_GRAPHIC,
  SQL_TYPE_VARGRAPHIC,
  SQL_TYPE_DOUBLE,
  SQL_TYPE_REAL,
  SQL_TYPE_FLOAT,
  SQL_TYPE_BOOLEAN,
  SQL_TYPE_OTHER
} db2Type;

/* Some PostgreSQL versions have no constant definition for the OID of type uuid */
#ifndef UUIDOID
#define UUIDOID 2950
#endif

typedef enum {
  NO_ENC_ERR_NULL,
  NO_ENC_ERR_TRUE,
  NO_ENC_ERR_FALSE
} db2NoEncErrType;

struct db2Column
{
  char *name;			/* name in DB2 */
  db2Type db2type;		/* data type in DB2 */
  int scale;			/* "scale" type modifier, used for NUMBERs */
  char *pgname;			/* PostgreSQL column name */
  int pgattnum;			/* PostgreSQL attribute number */
  Oid pgtype;			/* PostgreSQL data type */
  int pgtypmod;			/* PostgreSQL type modifier */
  int used;			/* is the column used in the query? */
  int pkey;			/* nonzero for primary keys, later set to the resjunk attribute number */
  char *val;			/* buffer for DB2 to return results in (LOB locator for LOBs) */
  long val_size;		/* allocated size in val */
  unsigned short val_len;	/* actual length of val */
  unsigned int val_len4;	/* actual length of val - for bind callbacks */
  short val_null;		/* indicator for NULL value */
  int varno;			/* range table index of this column's relation */
  db2NoEncErrType noencerr;	/* no encoding error produced */
};

struct db2Table
{
  char *name;			/* name in DB2 */
  char *pgname;			/* for error messages */
  int ncols;			/* number of columns */
  int npgcols;			/* number of columns (including dropped) in the PostgreSQL foreign table */
  struct db2Column **cols;
};

/* types to store parameter descriprions */

typedef enum
{
  BIND_STRING,
  BIND_NUMBER,
  BIND_LONG,
  BIND_LONGRAW,
  BIND_OUTPUT
} db2BindType;

struct paramDesc
{
  char *name;			/* name we give the parameter */
  Oid type;			/* PostgreSQL data type */
  db2BindType bindType;		/* which type to use for binding to DB2 statement */
  char *value;			/* value rendered for DB2 */
  void *node;			/* the executable expression */
  int colnum;			/* corresponding column in db2Table (-1 in SELECT queries unless output column) */
  void *bindh;			/* bind handle */
  struct paramDesc *next;
};

/* PostgreSQL error messages we need */
typedef enum
{
  FDW_ERROR,
  FDW_UNABLE_TO_ESTABLISH_CONNECTION,
  FDW_UNABLE_TO_CREATE_REPLY,
  FDW_UNABLE_TO_CREATE_EXECUTION,
  FDW_TABLE_NOT_FOUND,
  FDW_OUT_OF_MEMORY,
  FDW_SERIALIZATION_FAILURE
} db2error;

/*
 * functions defined in db2_utils.c
 */
extern db2Session *db2GetSession (const char *connectstring, char *user, char *password, const char *nls_lang, const char *tablename, int curlevel);
extern void db2CloseStatement (db2Session * session);
extern void db2CloseConnections (void);
extern void db2Shutdown (void);
extern void db2Cancel (void);
extern void db2EndTransaction (void *arg, int is_commit, int silent);
extern void db2EndSubtransaction (void *arg, int nest_level, int is_commit);
extern int db2IsStatementOpen (db2Session * session);
extern struct db2Table *db2Describe (db2Session * session, char *schema, char *table, char *pgname, long max_long, char *noencerr);
extern void db2ExplainOLD (db2Session * session, const char *query, int *nrows, char ***plan);
extern void db2PrepareQuery (db2Session * session, const char *query, const struct db2Table *db2Table, unsigned int prefetch);
extern int db2ExecuteQuery (db2Session * session, const struct db2Table *db2Table, struct paramDesc *paramList);
extern int db2FetchNext (db2Session * session);
extern void db2GetLob (db2Session * session, void *locptr, db2Type type, char **value, long *value_len, unsigned long trunc);
extern void db2ClientVersion (int *major, int *minor, int *update, int *patch, int *port_patch);
extern void db2ServerVersion (const char *connectstring, char *user, char *password, char * version, int len);
extern void *db2GetGeometryType (db2Session * session);
extern int db2GetImportColumn (db2Session * session, char *schema, char **tabname, char **colname, db2Type * type, int *charlen, int *typeprec, int *typescale, int *nullable, int *key);

/*
 * functions defined in db2_fdw.c
 */
extern char *db2GetShareFileName (const char *relativename);
extern void db2RegisterCallback (void *arg);
extern void db2UnregisterCallback (void *arg);
extern void *db2Alloc (size_t size);
extern void *db2Realloc (void *p, size_t size);
extern void db2Free (void *p);
extern void db2SetHandlers (void);
extern void db2Error_d (db2error sqlstate, const char *message, const char *detail, ...);
extern void db2Error (db2error sqlstate, const char *message);
extern void db2Debug2 (const char *message,...);
extern void db2Debug3 (const char *message,...);
extern void db2Debug4 (const char *message,...);
extern void db2Debug5 (const char *message,...);
extern bool optionIsTrue (const char *value);
