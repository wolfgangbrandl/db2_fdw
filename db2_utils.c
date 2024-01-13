/*-------------------------------------------------------------------------
 *
 * db2_utils.c
 * 		routines that use OCI (DB2's C API)
 *
 *-------------------------------------------------------------------------
 */

#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <pthread.h>
#if defined _WIN32 || defined _WIN64
/* for getpid */
#include <process.h>
/* Windows doesn't have snprintf */
#define snprintf _snprintf
#endif

/* DB2 header */
#include <db2ci.h>

#include "db2_fdw.h"

/* number of bytes to read per LOB chunk */
#define LOB_CHUNK_SIZE 8132

/* emit no error messages when set, used for shutdown */
static int silent = 0;

/* contains DB2 error messages, set by checkerr() */
#define ERRBUFSIZE 2000
static char db2Message[ERRBUFSIZE];
static sb4 err_code;

/* set to "1" as soon as OCIEnvCreate is called */
static int oci_initialized = 0;

/*
 * Linked list for temporary DB2 handles and descriptors.
 * Stores statement and describe handles as well as timetamp and LOB descriptors.
 * Other handles are stored in the handle cache below.
 */

struct handleEntry
{
  dvoid *handlep;
  ub4 type;
  int isDescriptor;
  struct handleEntry *next;
};

/*
 * Linked list of handles for cached DB2 connections.
 */
static struct envEntry *rootenvEntry = NULL;

/*
 * NULL value used for "in" callback in RETURNING clauses.
 */

/*
 * Helper functions
 */
struct srvEntry * findsrvEntryHandle (struct srvEntry * start, OCIServer * srvhp);
struct srvEntry * findsrvEntry (struct srvEntry * start, const char * connectstring);
int deletesrvEntry(struct srvEntry * start, struct srvEntry * node);
struct srvEntry * insertsrvEntry(struct srvEntry * start, const char * connectstring, OCIServer * srvhp);
struct connEntry * findconnEntry(struct connEntry * start, const char * user);
struct connEntry * findconnEntryHandle (struct connEntry * start, OCISession  * userhp);
int deleteconnEntry(struct connEntry * start, struct connEntry * node);
struct connEntry * insertconnEntry(struct connEntry * start, const char * user, OCISvcCtx *svchp,OCISession * userhp);
struct envEntry * findenvEntry(struct envEntry * start, const char * nlslang);
struct envEntry * findenvEntryHandle (struct envEntry * start, OCIEnv * envhp);
int deleteenvEntry(struct envEntry * start, struct envEntry * node);
int deleteenvEntryLang(struct envEntry * start, const char * nlslang);
struct envEntry * insertenvEntry(struct envEntry * start, const char * nlslang, OCIEnv *envhp, OCIError *errhp);
int printstruct(void);

static void db2SetSavepoint (db2Session * session, int nest_level);
static void setDB2Environment (char *nls_lang);
static sword checkerr (sword status, dvoid * handle, ub4 handleType, int line, char * file);
static char *copyDB2Text (const char *string, int size, int quote);
static void closeSession (OCIEnv * envhp, OCIServer * srvhp, OCISession * userhp, int disconnect);
static void disconnectServer (OCIEnv * envhp, OCIServer * srvhp);
static void removeEnvironment (OCIEnv * envhp);
static void allocHandle (dvoid ** handlep, ub4 type, int isDescriptor, OCIEnv * envhp, struct connEntry *connp, db2error error, const char *errmsg);
static void freeHandle (dvoid * handlep, struct connEntry *connp);
static ub2 getDB2Type (db2Type arg);
static sb4 bind_out_callback (void *octxp, OCIBind * bindp, ub4 iter, ub4 index, void **bufpp, ub4 ** alenp, ub1 * piecep, void **indp, ub2 ** rcodep);
static sb4 bind_in_callback (void *ictxp, OCIBind * bindp, ub4 iter, ub4 index, void **bufpp, ub4 * alenp, ub1 * piecep, void **indpp);

/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/
struct srvEntry * findsrvEntryHandle (struct srvEntry * start, OCIServer * srvhp) 
{
  struct srvEntry *step;
  for (step = start; step != NULL; step = step->right){
    if (start->srvhp == srvhp) {
      return step;
    }
  }
  return NULL;
}

/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/
struct srvEntry * findsrvEntry (struct srvEntry * start,const  char * connectstring) 
{
  struct srvEntry *step;
  for (step = start; step != NULL; step = step->right){
    if (strcmp (step->connectstring, connectstring) == 0) {
      return step;
    }
  }
  return NULL;
}
/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/
int deletesrvEntry(struct srvEntry * start, struct srvEntry * node) 
{ 
  struct srvEntry *step;
  for (step = start; step != NULL; step = step->right){
    if (step == node) {
      free (step->connectstring);
      step->connectstring = NULL;
      if (step->left == NULL && step->right == NULL){
      } else if (step->left == NULL) {
        step->right->left = NULL;
      } else if (step->right == NULL) {
        step->left->right = NULL;
      } else {
        step->left->right = step->right;
        step->right->left = step->left;
      }
      free (step);
      return FALSE;
    }
  }
  return TRUE;
}
/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/
struct srvEntry * insertsrvEntry(struct srvEntry * start,const char * connectstring, OCIServer * srvhp) 
{ 
  struct srvEntry *step;
  struct srvEntry *new;
  if (start == NULL){ /* first entry in list */
    new = malloc(sizeof(struct srvEntry));
    new->right = new->left = NULL;
  } else {
    for (step = start; step->right != NULL; step = step->right){ }
    new = malloc(sizeof(struct srvEntry));
    step->right = new; 
    new->left = step;
    new->right = NULL;
  }
  new->connectstring = strdup(connectstring);
  new->srvhp = srvhp;
  new->connlist = NULL;
  return new; 
} 
/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/
struct connEntry * findconnEntry(struct connEntry * start, const char * user)
{
  struct connEntry *step;
  for (step = start; step != NULL; step = step->right){
    if (strcmp (step->user, user) == 0) {
      return step;
    }
  }
  return NULL;
}

/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/
struct connEntry * findconnEntryHandle (struct connEntry * start, OCISession  * userhp)
{
  struct connEntry *step;
  for (step = start; step != NULL; step = step->right){
    if (step->userhp == userhp) {
      return step;
    }
  }
  return NULL;
}

/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/
int deleteconnEntry(struct connEntry * start, struct connEntry * node)
{
  struct connEntry *step;
  for (step = start; step != NULL; step = step->right){
    if (step == node) {
      free (step->user);
      step->user = 0x00;
      if (step->left == NULL && step->right == NULL){
      } else if (step->left == NULL) {
        step->right->left = NULL;
      } else if (step->right == NULL) {
        step->left->right = NULL;
      } else {
        step->left->right = step->right;
        step->right->left = step->left;
      }
      free (step);
      return FALSE;
    }
  }
  return TRUE;
}

/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/
struct connEntry * insertconnEntry(struct connEntry * start, const char * user, OCISvcCtx *svchp,OCISession * userhp)
{
  struct connEntry *step;
  struct connEntry *new;
  if (start == NULL){ /* first entry in list */
    new = malloc(sizeof(struct connEntry));
    new->right = new->left = NULL;
  } else {
    for (step = start; step->right != NULL; step = step->right){ }
    new = malloc(sizeof(struct connEntry));
    step->right = new;
    new->left = step;
    new->right = NULL;
  }
  new->user = strdup(user);
  new->handlelist = NULL;
  new->xact_level = 0;
  new->svchp = svchp;
  new->userhp = userhp;
  return new;
}

/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/
struct envEntry * findenvEntry(struct envEntry * start, const char * nlslang) 
{
  struct envEntry *step;
  for (step = start; step != NULL; step = step->right){
    if (strcmp (step->nls_lang, nlslang) == 0) {
      return step;
    }
  }
  return NULL;
}

/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/
struct envEntry * findenvEntryHandle (struct envEntry * start, OCIEnv * envhp)
{
  struct envEntry *step;
  for (step = start; step != NULL; step = step->right){
    if (step->envhp == envhp) {
      return step;
    }
  }
  return NULL;
}


/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/
int deleteenvEntryLang(struct envEntry * start, const char * nlslang) 
{ 
  struct envEntry *step;
  for (step = start; step != NULL; step = step->right){
    if (strcmp (step->nls_lang, nlslang) == 0) {
      free (step->nls_lang);
      if (step->left == NULL && step->right == NULL){
        rootenvEntry = NULL;
        free (step);
        step = NULL;
      } else if (step->left == NULL) {
        step->right->left = NULL;
        free (step);
        step = NULL;
      } else if (step->right == NULL) {
        step->left->right = NULL;
        free (step);
        step = NULL;
      } else {
        step->left->right = step->right;
        step->right->left = step->left;
        free (step);
        step = NULL;
      }
      return FALSE;
    }
  }
  return TRUE;

}
/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/
int deleteenvEntry(struct envEntry * start, struct envEntry * node) 
{ 
  struct envEntry *step;
  for (step = start; step != NULL; step = step->right){
    if (step == node) {
      free (step->nls_lang);
      step->nls_lang = NULL;
      if (step->left == NULL && step->right == NULL){
        rootenvEntry = NULL;
        free (step);
        step = NULL;
      } else if (step->left == NULL) {
        step->right->left = NULL;
        free (step);
        step = NULL;
      } else if (step->right == NULL) {
        step->left->right = NULL;
        free (step);
        step = NULL;
      } else {
        step->left->right = step->right;
        step->right->left = step->left;
        free (step);
        step = NULL;
      }
      return FALSE;
    }
  }
  return TRUE;
}
/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/
struct envEntry * insertenvEntry(struct envEntry * start, const char * nlslang, OCIEnv *envhp, OCIError *errhp) 
{ 
  struct envEntry *step;
  struct envEntry *new;
  if (start == NULL){ /* first entry in list */
    new = malloc(sizeof(struct envEntry));
    new->right = new->left = NULL; 
  } else {
    for (step = start; step->right != NULL; step = step->right){ }
    new = malloc(sizeof(struct envEntry));
    step->right = new; 
    new->left = step;
    new->right = NULL;
  }
  new->nls_lang = strdup(nlslang);
  new->envhp = envhp;
  new->errhp = errhp;
  new->srvlist = NULL;
  return new; 
} 
/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/
int printstruct()
{
  struct envEntry *envstep;
  struct srvEntry *srvstep;
  struct connEntry *constep;
  char szBuffer[1000];
  sprintf(szBuffer,"before calling pthread_create getpid: %d getpthread_self: %d",getpid(), (int)pthread_self());
  db2Debug5("printstruct for: %s",szBuffer);
  for (envstep = rootenvEntry; envstep != NULL; envstep = envstep->right){
    db2Debug5("EnvEntry: %x",envstep);
    db2Debug5("  nls_lang: %s",envstep->nls_lang);
    db2Debug5("  step->*envhp:%x",envstep->envhp);
    db2Debug5("  step->*errhp:%x",envstep->errhp);
    db2Debug5("  srvEntry step->*srvlist:%x",envstep->srvlist);
    db2Debug5("  step->*left:%x",envstep->left);
    db2Debug5("  step->*right:%x",envstep->right);
    for (srvstep = envstep->srvlist; srvstep != NULL; srvstep = srvstep->right){
      db2Debug5("    connectstring:%s",srvstep->connectstring);
      db2Debug5("    *srvhp:%x",srvstep->srvhp);
      db2Debug5("    *connlist:%x",srvstep->connlist);
      db2Debug5("    *left:%x",srvstep->left);
      db2Debug5("    *right:%x",srvstep->right);
      for (constep = srvstep->connlist; constep != NULL; constep = constep->right){
        db2Debug5("      user:%s",constep->user);
        db2Debug5("      *svchp:%x",constep->svchp);
        db2Debug5("      *userhp:%x",constep->userhp);
        db2Debug5("      *handlelist:%x",constep->handlelist);
        db2Debug5("      xact_level:%d",constep->xact_level);
        db2Debug5("      struct connEntry *left:%x",constep->left);
        db2Debug5("      struct connEntry *right:%x",constep->right);

      }
    }
  }
  return FALSE;
}
/**************************************************************************************************/
/* db2GetSession */
/* 		Look up an DB2 connection in the cache, create a new one if there is none. */
/* 		The result is a palloc'ed data structure containing the connection. */
/* 		"curlevel" is the current PostgreSQL transaction level. */
/**************************************************************************************************/
db2Session * db2GetSession (const char *connectstring, char *user, char *password, const char *nls_lang, const char *tablename, int curlevel)
{
  OCIEnv *envhp = NULL;
  OCIError *errhp = NULL;
  OCISvcCtx *svchp = NULL;
  OCIServer *srvhp = NULL;
  OCISession *userhp = NULL;
  OCITrans *txnhp = NULL;
  db2Session *session;
  struct envEntry *envp;
  struct srvEntry *srvp;
  struct connEntry *connp;
  char *nlscopy = NULL;
  ub4 is_connected;
  int retry;

  /* it's easier to deal with empty strings */
  if (!connectstring) connectstring = "";
  if (!user) user = "";
  if (!password) password = "";
  if (!nls_lang) nls_lang = "";
 /* search environment and server handle in cache */
  envp = findenvEntry (rootenvEntry,nls_lang);
  if (envp) {
    db2Debug2("db2GetSession found envp: %x   envhp: %x    errhp: %x",envp,envp->envhp,envp->errhp);
    envhp = envp->envhp;
    errhp = envp->errhp;
    if (checkerr (OCIHandleAlloc ((dvoid *) envhp, (dvoid **) & srvhp, (ub4) OCI_HTYPE_SERVER, (size_t) 0, (dvoid **) 0), (dvoid *) envhp, OCI_HTYPE_ENV,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIHandleAlloc failed to allocate server handle", db2Message);
       /* free error handle */
      db2Debug2("db2GetSession environment not active anymore");
      if (checkerr (OCIHandleFree( errhp, OCI_HTYPE_ERROR),(dvoid *) envhp, OCI_HTYPE_ENV,__LINE__, __FILE__) != OCI_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot free error handle","%s", db2Message);
      }

      /* free environment handle */
      if (checkerr (OCIHandleFree( envhp, OCI_HTYPE_ENV),(dvoid *) envhp, OCI_HTYPE_ENV,__LINE__, __FILE__) != OCI_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot free environment handle","%s", db2Message);
      }
      deleteenvEntryLang(rootenvEntry, nls_lang);
      envp = NULL;
    } /* check of handle not ok */
  }
  if (envp == NULL) {
    db2Debug2("db2GetSession create new environment");
    /* Create environment and error handle.  */

    /* create persistent copy of "nls_lang" */
    if ((nlscopy = strdup (nls_lang)) == NULL)
      db2Error_d (FDW_OUT_OF_MEMORY, "error connecting to DB2:"," failed to allocate %d bytes of memory", strlen (nls_lang) + 1);

    /* set DB2 environment */
    setDB2Environment (nlscopy);

    /* create environment handle */
    if (checkerr (OCIEnvCreate ((OCIEnv **) & envhp, (ub4) OCI_OBJECT, (dvoid *) 0, (dvoid * (*)(dvoid *, size_t)) 0,
				(dvoid * (*)(dvoid *, dvoid *, size_t)) 0, (void (*)(dvoid *, dvoid *)) 0, (size_t) 0, (dvoid **) 0), (dvoid *) envhp, OCI_HTYPE_ENV,__LINE__, __FILE__) != 0) {
      free (nlscopy);
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIEnvCreate failed to create environment handle", db2Message);
    }

    /* we can call OCITerminate now */
    oci_initialized = 1;

    /*
     * DB2 overwrites PostgreSQL's signal handlers, so we have to restore them.
     * DB2's SIGINT handler is ok (it cancels the query), but we must do something
     * reasonable for SIGTERM.
     */
    db2SetHandlers ();

    /* allocate error handle */
    if (checkerr (OCIHandleAlloc ((dvoid *) envhp, (dvoid **) & errhp, (ub4) OCI_HTYPE_ERROR, (size_t) 0, (dvoid **) 0), (dvoid *) envhp, OCI_HTYPE_ENV,__LINE__, __FILE__) != OCI_SUCCESS) {
      free (nlscopy);
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIHandleAlloc failed to allocate error handle", db2Message);
    }

    /* add handles to cache */
    envp = insertenvEntry(rootenvEntry,nlscopy,envhp,errhp);
    if (envp  == NULL) {
      db2Error_d (FDW_OUT_OF_MEMORY, "error connecting to DB2:"," failed to allocate %d bytes of memory", sizeof (struct envEntry));
    }
    if ( rootenvEntry == NULL) {
      rootenvEntry = envp;
    }
  }

  /* search connect string in cache */
  srvp = findsrvEntry (envp->srvlist,connectstring);
  if (srvp != NULL) {
    db2Debug2("db2GetSession found connect string");
    srvhp = srvp->srvhp;
    /* Test if we are still connected.  If not, clean up the mess.  */
    if (checkerr (OCIAttrGet ((dvoid *) srvhp, (ub4) OCI_HTYPE_SERVER,
			      (dvoid *) & is_connected, (ub4 *) 0, (ub4) OCI_ATTR_SERVER_STATUS, errhp), (dvoid *) errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error connecting to DB2: OCIAttrGet failed to get connection status", db2Message);
    }

    if (is_connected == OCI_SERVER_NOT_CONNECTED) {
      db2Debug2("db2GetSession OCI Server not connected");
      /* clean up */
      silent = 1;
      while (srvp->connlist != NULL) {
        db2Debug2("db2GetSession OCI Server not connected close Session");
	closeSession (envhp, srvhp, srvp->connlist->userhp, 0);
      }
      db2Debug2("db2GetSession OCI Server not connected disconnet");
      disconnectServer (envhp, srvhp);
      silent = 0;

      srvp = NULL;
    }
  } 
retry_connect:
  if (srvp == NULL) { /* srvp == NULL */
    db2Debug2("db2GetSession create new connect string");

    /* * No cache entry was found, we have to create a new server connection.  */

    /* create new server handle */
    if (checkerr (OCIHandleAlloc ((dvoid *) envhp, (dvoid **) & srvhp, (ub4) OCI_HTYPE_SERVER, (size_t) 0, (dvoid **) 0), (dvoid *) envhp, OCI_HTYPE_ENV,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIHandleAlloc failed to allocate server handle", db2Message);
    }

    /* connect to the DB2 server */
    if (checkerr (OCIServerAttach (srvhp, errhp, (text *) connectstring, strlen (connectstring), 0), (dvoid *) errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      if (tablename)
	db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "connection for foreign table"," <%s> cannot be established. %s", tablename, db2Message);
      else
	db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot connect to foreign DB2 server", db2Message);
    }

    /* add server handle to cache */
    envp->srvlist  = srvp = insertsrvEntry (envp->srvlist,connectstring,srvhp);
    if (srvp == NULL) {
      db2Error_d (FDW_OUT_OF_MEMORY, "error connecting to DB2:"," failed to allocate %d bytes of memory", sizeof (struct srvEntry));
    }
  }

  /* search user session for this server in cache */
  connp = findconnEntry(srvp->connlist,user);
  if (connp) {
    svchp = connp->svchp;
    userhp = connp->userhp;
  } else {
    svchp = NULL;
    userhp =  NULL;
  }

  if (userhp == NULL) {
    /*
     * If no cached user session was found, authenticate.
     */

    /* allocate service handle */
    db2Debug2("db2GetSession allocate service handle");
    if (checkerr (OCIHandleAlloc ((dvoid *) envhp, (dvoid **) & svchp, (ub4) OCI_HTYPE_SVCCTX, (size_t) 0, (dvoid **) 0), (dvoid *) envhp, OCI_HTYPE_ENV,__LINE__, __FILE__) != OCI_SUCCESS) {
      free (nlscopy);
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIHandleAlloc failed to allocate service handle", db2Message);
    }

    /* create transaction handle */
    if (checkerr (OCIHandleAlloc ((dvoid *) envhp, (dvoid **) & txnhp, (ub4) OCI_HTYPE_TRANS, (size_t) 0, (dvoid **) 0), (dvoid *) envhp, OCI_HTYPE_ENV,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIHandleAlloc failed to allocate transaction handle", db2Message);
    }


    /* create session handle */
    if (checkerr (OCIHandleAlloc ((dvoid *) envhp, (dvoid **) & userhp, (ub4) OCI_HTYPE_SESSION, (size_t) 0, (dvoid **) 0), (dvoid *) envhp, OCI_HTYPE_ENV,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIHandleAlloc failed to allocate session handle", db2Message);
    }
    /* connect to the database */
    if (checkerr (OCILogon (envhp,
			    errhp,
			    &svchp,
			    (DB2Text *) user,
			    strlen ((char *) user),
			    (DB2Text *) password,
			    strlen ((char *) password), (DB2Text *) connectstring, strlen ((char *) connectstring)), (dvoid *) errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection User: %s ,%s", user, db2Message);
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection password: %s ,%s", password, db2Message);
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection connectstring: %s ,%s", connectstring, db2Message);
      db2Error_d  (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection to foreign DB2 server,%s", db2Message);
    }
    envp->srvlist->connlist = connp = insertconnEntry (srvp->connlist,user,svchp,userhp);
    /* add session handle to cache */

    /* register callback for PostgreSQL transaction events */
    db2RegisterCallback (connp);
  }

  if (connp->xact_level <= 0) {
    db2Debug2 ("db2_fdw: begin serializable remote transaction");

    /* start a read-only or "serializable" (= repeatable read) transaction */
    if (checkerr (OCITransStart (svchp, errhp, (uword) 0, OCI_TRANS_SERIALIZABLE), (dvoid *) errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCITransStart failed to start a transaction", db2Message);
      if (retry && (err_code == 1012 || err_code == 28 || err_code == 3113 || err_code == 3135)) {
        db2Debug2 ("db2_fdw: session has been terminated, try to reconnect");
        silent = 1;
        while (srvp->connlist != NULL) {
          closeSession (envhp, srvhp, srvp->connlist->userhp, 0);
        }
        disconnectServer (envhp, srvhp);
        silent = 0;
        srvp = NULL;
        userhp = NULL;
        retry = 0;
        goto retry_connect;
      } else {
        db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCITransStart failed to start a transaction", db2Message);
      }
    }
    connp->xact_level = 1;
  }

  /* palloc a data structure pointing to the cached entries */
  session = db2Alloc (sizeof (struct db2Session));
  session->envp = envp;
  session->srvp = srvp;
  session->connp = connp;
  session->stmthp = NULL;

  /* set savepoints up to the current level */
  db2SetSavepoint (session, curlevel);

  return session;
}

/*
 * db2CloseStatement
 * 		Close any open statement associated with the session.
 */
void
db2CloseStatement (db2Session * session)
{
  /* free statement handle, if it exists */
  if (session->stmthp != NULL) {
    /* free the statement handle */
    freeHandle (session->stmthp, session->connp);
    session->stmthp = NULL;
  }
}

/*
 * db2CloseConnections
 * 		Close everything in the cache.
 */
void
db2CloseConnections (void)
{
  while (rootenvEntry != NULL) {
    while (rootenvEntry->srvlist != NULL) {
      while (rootenvEntry->srvlist->connlist != NULL) {
	closeSession (rootenvEntry->envhp, rootenvEntry->srvlist->srvhp, rootenvEntry->srvlist->connlist->userhp, 0);
      }
      disconnectServer (rootenvEntry->envhp, rootenvEntry->srvlist->srvhp);
    }
    removeEnvironment (rootenvEntry->envhp);
  }
}

/*
 * db2Shutdown
 * 		Close all open connections, free handles, terminate DB2.
 * 		This will be called at the end of the PostgreSQL session.
 */
void
db2Shutdown (void)
{
  /* don't report error messages */
  silent = 1;

  db2CloseConnections ();

  /* done with DB2 */
  if (oci_initialized)
    (void) OCITerminate (OCI_DEFAULT);
}

/*
 * db2Cancel
 * 		Cancel all running DB2 queries.
 */
void
db2Cancel (void)
{
  struct envEntry *envp;
  struct srvEntry *srvp;

  /* send a cancel request for all servers ignoring errors */
  for (envp = rootenvEntry; envp != NULL; envp = envp->right)
    for (srvp = envp->srvlist; srvp != NULL; srvp = srvp->right)
      (void) OCIBreak (srvp->srvhp, envp->errhp);
}

/*
 * db2EndTransaction
 * 		Commit or rollback the transaction.
 * 		The first argument must be a connEntry.
 * 		If "noerror" is true, don't throw errors.
 */
void
db2EndTransaction (void *arg, int is_commit, int noerror)
{
  struct connEntry *connp = NULL;
  struct srvEntry *srvp = NULL;
  struct envEntry *envp = NULL;
  int found = 0;

  /* do nothing if there is no transaction */
  if (((struct connEntry *) arg)->xact_level == 0)
    return;

  /* find the cached handles for the argument */
  envp = rootenvEntry;
  while (envp) {
    srvp = envp->srvlist;
    while (srvp) {
      connp = srvp->connlist;
      while (connp) {
	if (connp == (struct connEntry *) arg) {
	  found = 1;
	  break;
	}
	connp = connp->right;
      }
      if (found)
	break;
      srvp = srvp->right;
    }
    if (found)
      break;
    envp = envp->right;
  }

  if (!found)
    db2Error (FDW_ERROR, "db2EndTransaction internal error: handle not found in cache");

  /* free handles */
  while (connp->handlelist != NULL)
    freeHandle (connp->handlelist->handlep, connp);

  /* free objects in cache (might be left behind in case of errors) */
  /* OCICacheFree does not exist in db2 */
  /*(void)OCICacheFree(envp->envhp, envp->errhp, NULL);  */

  /* commit or rollback */
  if (is_commit) {
    db2Debug2 ("db2_fdw: commit remote transaction");

    if (checkerr (OCITransCommit (connp->svchp, envp->errhp, OCI_DEFAULT), (dvoid *) envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS && !noerror) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error committing transaction: OCITransCommit failed", db2Message);
    }
  }
  else {
    db2Debug2 ("db2_fdw: roll back remote transaction");

    if (checkerr (OCITransRollback (connp->svchp, envp->errhp, OCI_DEFAULT), (dvoid *) envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS && !noerror) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error rolling back transaction: OCITransRollback failed", db2Message);
    }
  }

  connp->xact_level = 0;
}

/*
 * db2EndSubtransaction
 * 		Commit or rollback all subtransaction up to savepoint "nest_nevel".
 * 		The first argument must be a connEntry.
 * 		If "is_commit" is not true, rollback.
 */
void
db2EndSubtransaction (void *arg, int nest_level, int is_commit)
{
  char query[50], message[60];
  struct connEntry *connp = NULL;
  struct srvEntry *srvp = NULL;
  struct envEntry *envp = NULL;
  OCIStmt *stmthp = NULL;
  int found = 0;

  /* do nothing if the transaction level is lower than nest_level */
  if (((struct connEntry *) arg)->xact_level < nest_level)
    return;

  ((struct connEntry *) arg)->xact_level = nest_level - 1;

  if (is_commit) {
    /*
     * There is nothing to do as savepoints don't get released in DB2:
     * Setting the same savepoint again just overwrites the previous one.
     */
    return;
  }

  /* find the cached handles for the argument */
  envp = rootenvEntry;
  while (envp) {
    srvp = envp->srvlist;
    while (srvp) {
      connp = srvp->connlist;
      while (connp) {
	if (connp == (struct connEntry *) arg) {
	  found = 1;
	  break;
	}
	connp = connp->right;
      }
      if (found)
	break;
      srvp = srvp->right;
    }
    if (found)
      break;
    envp = envp->right;
  }

  if (!found)
    db2Error (FDW_ERROR, "db2RollbackSavepoint internal error: handle not found in cache");

  snprintf (message, 59, "db2_fdw: rollback to savepoint s%d", nest_level);
  db2Debug2 (message);

  snprintf (query, 49, "ROLLBACK TO SAVEPOINT s%d", nest_level);

  /* create statement handle */
  allocHandle ((void **) &stmthp, OCI_HTYPE_STMT, 0, envp->envhp, connp, FDW_OUT_OF_MEMORY, "error rolling back to savepoint: OCIHandleAlloc failed to allocate statement handle");

  /* prepare the query */
  if (checkerr (OCIStmtPrepare (stmthp, envp->errhp, (text *) query, (ub4) strlen (query),
				(ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT), (dvoid *) envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error rolling back to savepoint: OCIStmtPrepare failed to prepare rollback statement", db2Message);
  }

  /* rollback to savepoint */
  if (checkerr (OCIStmtExecute (connp->svchp, stmthp, envp->errhp, (ub4) 1, (ub4) 0,
				(CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DEFAULT), (dvoid *) envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error rolling back to savepoint: OCIStmtExecute failed to set savepoint", db2Message);
  }

  /* free statement handle */
  freeHandle (stmthp, connp);
}

/*
 * db2IsStatementOpen
 * 		Return 1 if there is a statement handle, else 0.
 */
int
db2IsStatementOpen (db2Session * session)
{
  return (session->stmthp != NULL);
}

/*
 * db2Describe
 * 		Find the remote DB2 table and describe it.
 * 		Returns a palloc'ed data structure with the results.
 */
struct db2Table *
db2Describe (db2Session * session, char *schema, char *table, char *pgname, long max_long, char *noencerr)
{
  struct db2Table *reply;
  ub2 Type;
  OCIStmt *stmthp;
  OCIParam *colp;
  ub2 db2type;
  ub2 charsize;
  ub2 bin_size;
  ub1 csfrm;
  sb2 precision;
  sb1 scale;
  char *qtable, *qschema = NULL, *tablename, *query;
  DB2Text *ident, *typname, *typschema;
  char *type_name, *type_schema;
  ub4 ncols, ident_size, typname_size, typschema_size;
  int i, length;

  /* get a complete quoted table name */
  qtable = copyDB2Text (table, strlen (table), 1);
  length = strlen (qtable);
  if (schema != NULL) {
    qschema = copyDB2Text (schema, strlen (schema), 1);
    length += strlen (qschema) + 1;
  }
  tablename = db2Alloc (length + 1);
  tablename[0] = '\0';		/* empty */
  if (schema != NULL) {
    strcat (tablename, qschema);
    strcat (tablename, ".");
  }
  strcat (tablename, qtable);
  db2Free (qtable);
  if (schema != NULL)
    db2Free (qschema);

  /* construct a "SELECT * FROM ..." query to describe columns */
  length += 14;
  query = db2Alloc (length + 1);
  strcpy (query, "SELECT * FROM ");
  strcat (query, tablename);

  /* create statement handle */
  allocHandle ((void **) &stmthp, OCI_HTYPE_STMT, 0, session->envp->envhp, session->connp,
	       FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIHandleAlloc failed to allocate statement handle");

  /* prepare the query */
  if (checkerr (OCIStmtPrepare (stmthp, session->envp->errhp, (text *) query, (ub4) strlen (query),
				(ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIStmtPrepare failed to prepare query", db2Message);
  }

  if (checkerr (OCIStmtExecute (session->connp->svchp, stmthp, session->envp->errhp, (ub4) 0, (ub4) 0,
				(CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DESCRIBE_ONLY), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
    if (err_code == 942)
      db2Error_d (FDW_TABLE_NOT_FOUND, "table not found",
		     "DB2 table %s for foreign table \"%s\" does not exist or does not allow read access;%s,%s", tablename, pgname,
		     db2Message, "DB2 table names are case sensitive (normally all uppercase).");
    else
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIStmtExecute failed to describe table", db2Message);
  }

  /* allocate an db2Table struct for the results */
  reply = db2Alloc (sizeof (struct db2Table));
  reply->name = tablename;
  reply->pgname = pgname;
  reply->npgcols = 0;

  /* get the number of columns */
  if (checkerr (OCIAttrGet ((dvoid *) stmthp, (ub4) OCI_HTYPE_STMT,
			    (dvoid *) & ncols, (ub4 *) 0, (ub4) OCI_ATTR_PARAM_COUNT, session->envp->errhp), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get number of columns", db2Message);
  }

  reply->ncols = ncols;
  reply->cols = (struct db2Column **) db2Alloc (sizeof (struct db2Column *) * reply->ncols);

  /* loop through the column list */
  for (i = 1; i <= reply->ncols; ++i) {
    /* allocate an db2Column struct for the column */
    reply->cols[i - 1] = (struct db2Column *) db2Alloc (sizeof (struct db2Column));
    reply->cols[i - 1]->pgname = NULL;
    reply->cols[i - 1]->pgattnum = 0;
    reply->cols[i - 1]->pgtype = 0;
    reply->cols[i - 1]->pgtypmod = 0;
    reply->cols[i - 1]->used = 0;
    reply->cols[i - 1]->pkey = 0;
    reply->cols[i - 1]->val = NULL;
    reply->cols[i - 1]->val_len = 0;
    reply->cols[i - 1]->val_null = 1;
    reply->cols[i - 1]->noencerr = NO_ENC_ERR_NULL;

    if (noencerr != NULL) {
      if (optionIsTrue(noencerr)) {
	reply->cols[i - 1]->noencerr = NO_ENC_ERR_TRUE;
     } else {
	reply->cols[i - 1]->noencerr = NO_ENC_ERR_FALSE;
     }
    }

    /* get the parameter descriptor for the column */
    if (checkerr (OCIParamGet ((void *) stmthp, OCI_HTYPE_STMT, session->envp->errhp, (dvoid **) & colp, i), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIParamGet failed to get column data", db2Message);
    }

    /* get the column name */
    if (checkerr (OCIAttrGet ((dvoid *) colp, OCI_DTYPE_PARAM, (dvoid *) & ident,
			      &ident_size, (ub4) OCI_ATTR_NAME, session->envp->errhp), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get column name", db2Message);
    }

    reply->cols[i - 1]->name = copyDB2Text ((char *) ident, (int) ident_size, 1);

    /* get the data type */
    if (checkerr (OCIAttrGet ((dvoid *) colp, OCI_DTYPE_PARAM, (dvoid *) & db2type,
			      (ub4 *) 0, (ub4) OCI_ATTR_TYPECODE, session->envp->errhp), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get column type", db2Message);
    }
    Type = db2type;

    /* get the column type name */
    if (checkerr (OCIAttrGet ((dvoid *) colp, OCI_DTYPE_PARAM, (dvoid *) & typname,
			      &typname_size, (ub4) OCI_ATTR_TYPE_NAME, session->envp->errhp), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get column type name", db2Message);
    }

    /* create a zero-terminated copy */
    type_name = db2Alloc (typname_size + 1);
    strncpy (type_name, (char *) typname, typname_size);
    type_name[typname_size] = '\0';

    /* get the column type schema */
    if (checkerr (OCIAttrGet ((dvoid *) colp, OCI_DTYPE_PARAM, (dvoid *) & typschema,
			      &typschema_size, (ub4) OCI_ATTR_SCHEMA_NAME, session->envp->errhp), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get column type schema name", db2Message);
    }

    /* create a zero-terminated copy */
    type_schema = db2Alloc (typschema_size + 1);
    strncpy (type_schema, (char *) typschema, typschema_size);
    type_schema[typschema_size] = '\0';

    /* get the character set form */
    if (checkerr (OCIAttrGet ((dvoid *) colp, OCI_DTYPE_PARAM, (dvoid *) & csfrm,
			      (ub4 *) 0, (ub4) OCI_ATTR_CHARSET_FORM, session->envp->errhp), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get character set form", db2Message);
    }

    /* get the number of characters for string fields */
    if (checkerr (OCIAttrGet ((dvoid *) colp, OCI_DTYPE_PARAM, (dvoid *) & charsize,
			      (ub4 *) 0, (ub4) OCI_ATTR_CHAR_USED, session->envp->errhp), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get column length", db2Message);
    }

    /* get the binary length for RAW fields */
    if (checkerr (OCIAttrGet ((dvoid *) colp, OCI_DTYPE_PARAM, (dvoid *) & bin_size,
			      (ub4 *) 0, (ub4) OCI_ATTR_DATA_SIZE, session->envp->errhp), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get column size", db2Message);
    }

    /* get the precision */
    if (checkerr (OCIAttrGet ((dvoid *) colp, OCI_DTYPE_PARAM, (dvoid *) & precision,
			      (ub4 *) 0, (ub4) OCI_ATTR_PRECISION, session->envp->errhp), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get column precision", db2Message);
    }

    /* get the scale */
    if (checkerr (OCIAttrGet ((dvoid *) colp, OCI_DTYPE_PARAM, (dvoid *) & scale,
			      (ub4 *) 0, (ub4) OCI_ATTR_SCALE, session->envp->errhp), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get column scale", db2Message);
    }

    reply->cols[i - 1]->scale = scale;

    /* determine db2Type and length to allocate */
    db2Debug2("db2Describe Type: %d",Type);
    switch (Type) {
    case SQLT_AFC:
      /* CHAR(n) */
      reply->cols[i - 1]->db2type = SQL_TYPE_CHAR;
      reply->cols[i - 1]->val_size = bin_size * 4 + 1;
      break;
    case SQLT_CHR:
    case SQLT_VCS:
      /* VARCHAR(n) and VARCHAR2(n) */
      reply->cols[i - 1]->db2type = SQL_TYPE_VARCHAR;
      reply->cols[i - 1]->val_size = bin_size * 4 + 1;
      break;
    case SQLT_BLOB:
      /* BLOB */
      reply->cols[i - 1]->db2type = SQL_TYPE_BLOB;
      /* for LOB columns, "val" will contain a pointer to the locator */
      reply->cols[i - 1]->val_size = sizeof (OCILobLocator *);
      break;
    case SQLT_CLOB:
      /* CLOB and CFILE */
      if (csfrm == SQLCS_NCHAR) {
	/*
	 * We don't support NCLOB because DB2 cannot
	 * transform it to the client character set automatically.
	 */
	reply->cols[i - 1]->db2type = SQL_TYPE_OTHER;
	reply->cols[i - 1]->val_size = 0;
      }
      else {
	reply->cols[i - 1]->db2type = SQL_TYPE_CLOB;
	/* for LOB columns, "val" will contain a pointer to the locator */
	reply->cols[i - 1]->val_size = sizeof (OCILobLocator *);
      }
      break;
    case SQLT_INT:
    case OCI_TYPECODE_SMALLINT:
      /* NUMBER */
      reply->cols[i - 1]->db2type = SQL_TYPE_INTEGER;
      if (precision == 0)
	/* this should be big enough for unrestricted NUMBERs */
	reply->cols[i - 1]->val_size = bin_size;
      else
	reply->cols[i - 1]->val_size = ((-scale) > precision ? (-scale) : precision) + 5;
      break;
    case SQLT_PDN:
      /* NUMBER */
      reply->cols[i - 1]->db2type = SQL_TYPE_INTEGER;
      if (precision == 0)
	/* this should be big enough for unrestricted NUMBERs */
	reply->cols[i - 1]->val_size = bin_size;
      else
	reply->cols[i - 1]->val_size = ((-scale) > precision ? (-scale) : precision) + 5;
      break; 
    case SQLT_FLT:
    case SQLT_BDOUBLE:
      /* FLOAT */
      reply->cols[i - 1]->db2type = SQL_TYPE_FLOAT;
      reply->cols[i - 1]->val_size = bin_size;
      break;
    case SQLT_DAT:
      /* DATE */
      reply->cols[i - 1]->db2type = SQL_TYPE_DATE;
      reply->cols[i - 1]->val_size = bin_size;
      break;
    case SQLT_TIMESTAMP:
      /* TIMESTAMP */
      reply->cols[i - 1]->db2type = SQL_TYPE_STAMP;
      reply->cols[i - 1]->val_size = bin_size;
      break;
    case SQLT_TIME:
      /* TIME */
      reply->cols[i - 1]->db2type = SQL_TYPE_TIME;
      reply->cols[i - 1]->val_size = bin_size;
      break;
    case SQLT_LNG:
      /* LONG */
      reply->cols[i - 1]->db2type = SQL_TYPE_BIG;
      reply->cols[i - 1]->val_size = max_long + 4;
      break;
    default:
      db2Debug2("xml col: char_size: %d bin_size %d ",charsize,bin_size);
      reply->cols[i - 1]->db2type = SQL_TYPE_XML;
      reply->cols[i - 1]->val_size = charsize + 1;
/*      reply->cols[i - 1]->val_size = bin_size * 4 + 1; */
/*      reply->cols[i - 1]->db2type = SQL_TYPE_OTHER; */
/*      reply->cols[i - 1]->val_size = 0;*/
    }
  }

  /* free statement handle, this takes care of the parameter handles */
  freeHandle (stmthp, session->connp);

  return reply;
}

#define EXPLAIN_LINE_SIZE 1000

/*
 * db2SetSavepoint
 * 		Set savepoints up to level "nest_level".
 */
void
db2SetSavepoint (db2Session * session, int nest_level)
{
  while (session->connp->xact_level < nest_level) {
    char query[40], message[50];

    snprintf (message, 49, "db2_fdw: set savepoint s%d", session->connp->xact_level + 1);
    db2Debug2 (message);

    snprintf (query, 39, "SAVEPOINT s%d", session->connp->xact_level + 1);

    /* create statement handle */
    allocHandle ((void **) &(session->stmthp), OCI_HTYPE_STMT, 0, session->envp->envhp, session->connp,
		 FDW_OUT_OF_MEMORY, "error setting savepoint: OCIHandleAlloc failed to allocate statement handle");

    /* prepare the query */
    if (checkerr (OCIStmtPrepare (session->stmthp, session->envp->errhp, (text *) query, (ub4) strlen (query),
				  (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error setting savepoint: OCIStmtPrepare failed to prepare savepoint statement", db2Message);
    }

    /* set savepoint */
    if (checkerr (OCIStmtExecute (session->connp->svchp, session->stmthp, session->envp->errhp, (ub4) 1, (ub4) 0,
				  (CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error setting savepoint: OCIStmtExecute failed to set savepoint", db2Message);
    }

    /* free statement handle */
    freeHandle (session->stmthp, session->connp);
    session->stmthp = NULL;

    ++session->connp->xact_level;
  }
}

/*
 * setDB2
 * 		Set environment variables do that DB2 works as we want.
 *
 * 		NLS_LANG sets the language and client encoding
 * 		NLS_NCHAR is unset so that N* data types are converted to the
 * 		character set specified in NLS_LANG.
 *
 * 		The following variables are set to values that make DB2 convert
 * 		numeric and date/time values to strings PostgreSQL can parse:
 * 		NLS_DATE_FORMAT
 * 		NLS_TIMESTAMP_FORMAT
 * 		NLS_TIMESTAMP_TZ_FORMAT
 * 		NLS_NUMERIC_CHARACTERS
 * 		NLS_CALENDAR
 */
void
setDB2Environment (char *nls_lang)
{
  if (putenv (nls_lang) != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_LANG cannot be set.");
  }

  /* other environment variables that control DB2 formats */
  if (putenv ("NLS_DATE_LANGUAGE=AMERICAN") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_DATE_LANGUAGE cannot be set.");
  }

  if (putenv ("NLS_DATE_FORMAT=YYYY-MM-DD HH24:MI:SS BC") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_DATE_FORMAT cannot be set.");
  }

  if (putenv ("NLS_TIMESTAMP_FORMAT=YYYY-MM-DD HH24:MI:SS.FF9 BC") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_TIMESTAMP_FORMAT cannot be set.");
  }

  if (putenv ("NLS_TIMESTAMP_TZ_FORMAT=YYYY-MM-DD HH24:MI:SS.FF9TZH:TZM BC") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_TIMESTAMP_TZ_FORMAT cannot be set.");
  }
  if (putenv ("NLS_TIME_FORMAT=HH24:MI:SS.FF9 BC") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_TIME_FORMAT cannot be set.");
  }

  if (putenv ("NLS_TIME_TZ_FORMAT= HH24:MI:SS.FF9TZH:TZM BC") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_TIME_TZ_FORMAT cannot be set.");
  }

  if (putenv ("NLS_NUMERIC_CHARACTERS=.,") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_NUMERIC_CHARACTERS cannot be set.");
  }

  if (putenv ("NLS_CALENDAR=") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_CALENDAR cannot be set.");
  }

  if (putenv ("NLS_NCHAR=") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_NCHAR cannot be set.");
  }
}

/*
 * db2PrepareQuery
 * 		Prepares an SQL statement for execution.
 * 		This function should handle everything that has to be done only once
 * 		even if the statement is executed multiple times, that is:
 * 		- For SELECT statements, defines the result values to be stored in db2Table.
 * 		- For DML statements, allocates LOB locators for the RETURNING clause in db2Table.
 * 		- Set the prefetch options.
 */
void
db2PrepareQuery (db2Session * session, const char *query, const struct db2Table *db2Table, unsigned int prefetch)
{
  int i, col_pos, is_select;
  OCIDefine *defnhp;
  static char dummy[4];
  static sb4 dummy_size = 4;
  static sb2 dummy_null;
  ub4 prefetch_rows = prefetch;

  /* figure out if the query is FOR UPDATE */
  is_select = (strncmp (query, "SELECT", 6) == 0);

  /* make sure there is no statement handle stored in "session" */
  if (session->stmthp != NULL) {
    db2Error (FDW_ERROR, "db2PrepareQuery internal error: statement handle is not NULL");
  }

  /* create statement handle */
  allocHandle ((void **) &(session->stmthp), OCI_HTYPE_STMT, 0, session->envp->envhp, session->connp,
	       FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIHandleAlloc failed to allocate statement handle");

  /* prepare the statement */
  if (checkerr (OCIStmtPrepare (session->stmthp, session->envp->errhp, (text *) query, (ub4) strlen (query),
				(ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIStmtPrepare failed to prepare remote query", db2Message);
  }

  /* loop through table columns */
  col_pos = 0;
  for (i = 0; i < db2Table->ncols; ++i) {
    if (db2Table->cols[i]->used) {
      /*
       * Unfortunately DB2 handles DML statements with a RETURNING clause
       * quite different from SELECT statements.  In the latter, the result
       * columns are "defined", i.e. bound to some storage space.
       * This definition is only necessary once, even if the query is executed
       * multiple times, so we do this here.
       * RETURNING clause are handled in db2ExecuteQuery, here we only
       * allocate locators for LOB columns in RETURNING clauses.
       */
      if (is_select) {
	ub2 type;

	/* figure out in which format we want the results */
	type = getDB2Type (db2Table->cols[i]->db2type);
	if (db2Table->cols[i]->pgtype == UUIDOID)
	  type = SQLT_STR;

	/* check if it is a LOB column */
	if (type == SQLT_BLOB || type == SQLT_CLOB) {
	  /* allocate a LOB locator, store a pointer to it in "val" */
	  allocHandle ((void **) db2Table->cols[i]->val, OCI_DTYPE_LOB, 1, session->envp->envhp, session->connp,
		       FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIDescriptorAlloc failed to allocate LOB descriptor");
	}

	/* define result value */
	defnhp = NULL;
	if (checkerr (OCIDefineByPos (session->stmthp, &defnhp, session->envp->errhp, (ub4)++ col_pos,
				      (dvoid *) db2Table->cols[i]->val, (sb4) db2Table->cols[i]->val_size,
				      type, (dvoid *) & db2Table->cols[i]->val_null,
				      (ub2 *) & db2Table->cols[i]->val_len, NULL, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
	  db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIDefineByPos failed to define result value", db2Message);
	}

      }
      else {
	ub2 type;

	/* for other statements, allocate LOB locators for RETURNING parameters */
	type = getDB2Type (db2Table->cols[i]->db2type);
	if (type == SQLT_BLOB || type == SQLT_CLOB) {
	  /* allocate a LOB locator, store a pointer to it in "val" */
	  allocHandle ((void **) db2Table->cols[i]->val, OCI_DTYPE_LOB, 1, session->envp->envhp, session->connp,
		       FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIDescriptorAlloc failed to allocate LOB descriptor");
	}
      }
    }
  }

  if (is_select && col_pos == 0) {
    /*
     * No columns selected (i.e., SELECT '1' FROM).
     * Define dummy result columnn.
     */
    defnhp = NULL;
    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp, session->envp->errhp, (ub4) 1,
				  (dvoid *) dummy, dummy_size, SQLT_STR, (dvoid *) & dummy_null,
				  NULL, NULL, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIDefineByPos failed to define result value", db2Message);
    }
  }

  /* set prefetch options */
  if (checkerr (OCIAttrSet ((dvoid *) session->stmthp, OCI_HTYPE_STMT, (dvoid *) & prefetch_rows, 0,
			    OCI_ATTR_PREFETCH_ROWS, session->envp->errhp), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIAttrSet failed to set number of prefetched rows in statement handle", db2Message);
  }
}

/*
 * db2ExecuteQuery
 * 		Execute a prepared statement and fetches the first result row.
 * 		The parameters ("bind variables") are filled from paramList.
 * 		Returns the count of processed rows.
 * 		This can be called several times for a prepared SQL statement.
 */
int
db2ExecuteQuery (db2Session * session, const struct db2Table *db2Table, struct paramDesc *paramList)
{
  sb2 *indicators;
  struct paramDesc *param;
  sword result;
  ub4 rowcount;
  int param_count = 0;
  for (param = paramList; param; param = param->next)
    ++param_count;

  /* allocate a temporary array of indicators */
  indicators = db2Alloc (param_count * sizeof (sb2 *));

  /* bind the parameters */
  param_count = -1;
  for (param = paramList; param; param = param->next) {
    dvoid *value = NULL;	/* will contain the value as bound */
    sb4 value_len = 0;		/* length of "value" */
    ub2 value_type = SQLT_STR;	/* SQL_STR works for NULLs of all types */
    ub4 oci_mode = OCI_DEFAULT;	/* changed only for output parameters */
    OCINumber *number;
    char *num_format, *pos;

    ++param_count;
    indicators[param_count] = (sb2) ((param->value == NULL) ? -1 : 0);

    if (param->value != NULL)
      switch (param->bindType) {
      case BIND_NUMBER:
	/* allocate a new NUMBER */
	number = db2Alloc (sizeof (OCINumber));

	/*
	 * Construct number format.
	 */
	value_len = strlen (param->value);
	num_format = db2Alloc (value_len + 1);
	/* fill everything with '9' */
	memset (num_format, '9', value_len);
	num_format[value_len] = '\0';
	/* write 'D' in the decimal point position */
	if ((pos = strchr (param->value, '.')) != NULL)
	  num_format[pos - param->value] = 'D';
	/* replace the scientific notation part with 'E' */
	if ((pos = strchr (param->value, 'e')) != NULL)
	  memset (num_format + (pos - param->value), 'E', value_len - (pos - param->value));

	/* convert parameter string to NUMBER */
	if (checkerr (OCINumberFromText (session->envp->errhp, (const DB2Text *) param->value,
					 (ub4) value_len, (const DB2Text *) num_format, (ub4) value_len,
					 (const DB2Text *) NULL, (ub4) 0, number), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
	  db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCINumberFromText failed to convert parameter", db2Message);
	}
	db2Free (num_format);

	value = (dvoid *) number;
	value_len = sizeof (OCINumber);
	value_type = SQLT_VNU;
	break;
      case BIND_STRING:
	value = param->value;
	value_len = strlen (param->value) + 1;
	value_type = SQLT_STR;
	break;
      case BIND_LONGRAW:
	value = param->value;
	value_len = *((sb4 *) param->value) + 4;
	value_type = SQLT_LVB;
	break;
      case BIND_LONG:
	value = param->value;
	value_len = *((sb4 *) param->value) + 4;
	value_type = SQLT_LVC;
	break;
      case BIND_OUTPUT:
	value = NULL;
	value_len = db2Table->cols[param->colnum]->val_size;
	value_type = getDB2Type (db2Table->cols[param->colnum]->db2type);
	if (db2Table->cols[param->colnum]->pgtype == UUIDOID) {
	  /* the type input function will interpret the string value correctly */
	  value_type = SQLT_STR;
	}
	oci_mode = OCI_DATA_AT_EXEC;
	break;
      }

    /*
     * Since this is a bit convoluted, here is a description of how different
     * parameters are bound:
     * - Input parameters of normal data types require just a simple OCIBindByName.
     * - For named data type input parameters, a new object and its indicator structure
     *   are allocated and filled, and in addition to OCIBindByName there is a call
     *   to OCIBindObject that specifies the data type.
     * - For all output parameters, OCIBindDynamic allocates two callback functions.
     *   The "in" callback should just return a NULL values, and the "out" callback
     *   returns a place where to store the retrieved data:
     *   - For normal data types, a pointer to the column's "val" in the db2Table
     *     is returned by the callback.
     *   - For LOBs, the callback also returns the column's "val", which points to
     *     a LOB locator allocated in db2PrepareQuery.
     *   - For named data types, the callback returns the adress of a pointer initialized
     *     to NULL, and DB2 will allocate space for the object.  The indicator value
     *     has to be retrieved with OCIObjectGetInd.
     */

    /* bind the value to the parameter */
    if (checkerr (OCIBindByName (session->stmthp, (OCIBind **) & param->bindh, session->envp->errhp, (text *) param->name,
				 (sb4) strlen (param->name), value, value_len, value_type,
				 (dvoid *) & indicators[param_count], NULL, NULL, (ub4) 0, NULL, oci_mode), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIBindByName failed to bind parameter", db2Message);
    }


    /* for output parameters, define callbacks that provide storage space */
    if (param->bindType == BIND_OUTPUT) {
      if (checkerr (OCIBindDynamic ((OCIBind *) param->bindh, session->envp->errhp,
				    db2Table->cols[param->colnum], &bind_in_callback,
				    db2Table->cols[param->colnum], &bind_out_callback), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
	db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIBindDynamic failed to bind callback for parameter", db2Message);
      }
    }
  }
  /* execute the query and get the first result row */
  result = checkerr (OCIStmtExecute (session->connp->svchp, session->stmthp, session->envp->errhp, (ub4) 1, (ub4) 0,
				     (CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__);

  if (result != OCI_SUCCESS && result != OCI_NO_DATA) {
    /* use the correct SQLSTATE for serialization failures */
    db2Error_d (err_code == 8177 ? FDW_SERIALIZATION_FAILURE : FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIStmtExecute failed to execute remote query", db2Message);
  }

  /* free indicators */
  db2Free (indicators);

  if (result == OCI_NO_DATA)
    return 0;

  /* get the number of processed rows (important for DML) */
  if (checkerr (OCIAttrGet ((dvoid *) session->stmthp, (ub4) OCI_HTYPE_STMT,
			    (dvoid *) & rowcount, (ub4 *) 0, (ub4) OCI_ATTR_ROW_COUNT, session->envp->errhp), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIAttrGet failed to get number of affected rows", db2Message);
  }

  /* post processing of output parameters */
  for (param = paramList; param; param = param->next)
    if (param->bindType == BIND_OUTPUT) {
      /*
       * Store the received length in the table column.
       * Should not lose any data in all possible cases
       * since LONG and LONG RAW don't work with RETURNING anyway.
       */
      db2Table->cols[param->colnum]->val_len = (unsigned short) db2Table->cols[param->colnum]->val_len4;

    }

  return rowcount;
}

/*
 * db2FetchNext
 * 		Fetch the next result row, return 1 if there is one, else 0.
 */
int
db2FetchNext (db2Session * session)
{
  sword result;

  /* make sure there is a statement handle stored in "session" */
  if (session->stmthp == NULL) {
    db2Error (FDW_ERROR, "db2FetchNext internal error: statement handle is NULL");
  }

  /* fetch the next result row */
  result = checkerr (OCIStmtFetch2 (session->stmthp, session->envp->errhp, 1, OCI_FETCH_NEXT, 0, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__);

  if (result != OCI_SUCCESS && result != OCI_NO_DATA) {
    db2Error_d (err_code == 8177 ? FDW_SERIALIZATION_FAILURE : FDW_UNABLE_TO_CREATE_EXECUTION, "error fetching result: OCIStmtFetch2 failed to fetch next result row", db2Message);
  }

  return (result == OCI_SUCCESS);
}

/*
 * db2GetLob
 * 		Get the LOB contents and store them in *value and *value_len.
 * 		If "trunc" is nonzero, it contains the number of bytes or characters to get.
 */
void
db2GetLob (db2Session * session, void *locptr, db2Type type, char **value, long *value_len, unsigned long trunc)
{
  OCILobLocator *locp = *(OCILobLocator **) locptr;
  ub4 amount_char= 4096000000;
  sword result = OCI_SUCCESS;
  ub4 resultlob;

  /* initialize result buffer length */
  *value_len = 0;

  /* read the LOB in chunks */
  do {
    /* extend result buffer */
    if (*value_len == 0)
      *value = db2Alloc (LOB_CHUNK_SIZE + 1);
    else
      *value = db2Realloc (*value, *value_len + LOB_CHUNK_SIZE + 1);

/*
 *  The first time round, "amount_* = 0" tells OCILobRead to read the whole LOB.
 *  On subsequent reads, the amount_* parameters are ignored.
 *  After the call, "amount_byte" contains the number of bytes read.
 */
    resultlob = OCILobRead (
                  session->connp->svchp,
                  session->envp->errhp,
                  locp,
                  &amount_char,
                  (ub4) 1, 
                  (dvoid *) (*value + *value_len), 
                  (ub4) LOB_CHUNK_SIZE,
                  (dvoid *) 0, 
                  NULL,
                  (ub2) 0, 
                  (ub1) SQLCS_IMPLICIT
                  );
    result = checkerr (resultlob,(dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__);

    if (result == OCI_ERROR) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error fetching result: OCILobRead failed to read LOB chunk", db2Message);
    }

    /* update LOB length */
    *value_len += (long) amount_char;
  }
  while (result == OCI_NEED_DATA);

  /* string end for CLOBs */
  (*value)[*value_len] = '\0';
}

/*
 * db2ClientVersion
 * 		Returns the five components of the client version.
 */
void
db2ClientVersion (int *major, int *minor, int *update, int *patch, int *port_patch)
{
  OCIClientVersion (major, minor, update, patch, port_patch);
}

/*
 * db2ServerVersion
 * 		Returns the five components of the server version.
 */
void
db2ServerVersion (const char *connectstring, char *user, char *password, char * version, int len)
{
  OraText vers[10000];
  OCIEnv *envhp = NULL;
  OCIError *errhp = NULL;
  OCISvcCtx *svchp = NULL;


  memset (vers,0x00,len);
  /* create environment handle */
  if (checkerr (OCIEnvCreate ((OCIEnv **) & envhp, (ub4) OCI_OBJECT,
        (dvoid *) 0, (dvoid * (*)(dvoid *, size_t)) 0,
        (dvoid * (*)(dvoid *, dvoid *, size_t)) 0, (void (*)(dvoid *, dvoid *)) 0, (size_t) 0, (dvoid **) 0), (dvoid *) envhp, OCI_HTYPE_ENV,__LINE__, __FILE__) != 0) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIEnvCreate failed to create environment handle", db2Message);
  }
  /* allocate error handle */
  if (checkerr (OCIHandleAlloc ((dvoid *) envhp, (dvoid **) & errhp, (ub4) OCI_HTYPE_ERROR, (size_t) 0, (dvoid **) 0), (dvoid *) envhp, OCI_HTYPE_ENV,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIHandleAlloc failed to allocate error handle", db2Message);
  }

  if (checkerr (OCILogon (envhp,
                            errhp,
                            &svchp,
                            (DB2Text *) user,
                            strlen ((char *) user),
                            (DB2Text *) password,
                            strlen ((char *) password), (DB2Text *) connectstring, strlen ((char *) connectstring)), (dvoid *) envhp, OCI_HTYPE_ENV,__LINE__, __FILE__) != OCI_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection User: %s %s", user, db2Message);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection to foreign DB2 server %s", db2Message);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection Password: %s %s", password, db2Message);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection Connectstring: %s %s", connectstring, db2Message);
  }

  /* get version information from remote server */
  if (checkerr (OCIServerVersion (svchp, errhp, vers, len, OCI_HTYPE_SVCCTX ), (dvoid *) errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error getting server version:"," OCIServerVersion failed to retrieve version", db2Message);
  }

  strcpy (version,(char *)vers);
  /* disconnect from the database */
  if (checkerr ( OCILogoff( svchp, errhp ), (dvoid *) envhp, OCI_HTYPE_ENV,__LINE__, __FILE__) != OCI_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot Logoff from DB2 server","%s", db2Message);
  }

  /* free connection handle */
  if (checkerr (OCIHandleFree( svchp, OCI_HTYPE_SVCCTX ), (dvoid *) envhp, OCI_HTYPE_ENV,__LINE__, __FILE__) != OCI_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot free connection Handle","%s", db2Message);
  }

  /* free error handle */
  if (checkerr (OCIHandleFree( errhp, OCI_HTYPE_ERROR),(dvoid *) envhp, OCI_HTYPE_ENV,__LINE__, __FILE__) != OCI_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot free error handle","%s", db2Message);
  }

  /* free environment handle */
  if (checkerr (OCIHandleFree( envhp, OCI_HTYPE_ENV),(dvoid *) envhp, OCI_HTYPE_ENV,__LINE__, __FILE__) != OCI_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot free environment handle","%s", db2Message);
  }

  (void)OCITerminate( OCI_DEFAULT );
}

/*
 * db2GetImportColumn
 * 		Get the next element in the ordered list of tables and their columns for "schema".
 * 		Returns 0 if there are no more columns, -1 if the remote schema does not exist, else 1.
 */
int
db2GetImportColumn (db2Session * session, char *schema, char **tabname, char **colname, db2Type * type, int *charlen, int *typeprec, int *typescale, int *nullable, int *key)
{
  /* the static variables will contain data returned to the caller */
  static char s_tabname[129], s_colname[129];
  char typename[129] = { '\0' }, isnull[2] = { '\0'};
  int count = 0;
  const char *const schema_query = "select count(*) from SYSIBM.SYSSCHEMATA where name=:nsp";
  const char *const column_query = 
    "select A.TABLE_NAME,B.NAME,COLTYPE,LENGTH,SCALE,B.NULLS,coalesce(B.KEYSEQ,0) as key \n"
    "from sysibm.tables a,sysibm.SYSCOLUMNS b \n" 
    "where table_schema=:nsp \n"
    "and (TABLE_TYPE like 'BASE TABLE%' OR TABLE_TYPE like 'VIEW') \n"
    "and A.TABLE_NAME = B.TBNAME \n" 
    "and A.TABLE_SCHEMA=B.TBCREATOR \n"
    "order by TABLE_NAME,COLNO";
  OCIBind *bndhp = NULL;
  sb2 ind = 0, ind_tabname, ind_colname, ind_typename, ind_charlen = OCI_IND_NOTNULL, ind_precision = OCI_IND_NOTNULL, ind_scale = OCI_IND_NOTNULL, ind_isnull, ind_key;
  OCIDefine *defnhp_tabname = NULL, *defnhp_colname = NULL, *defnhp_typename = NULL,
    *defnhp_charlen = NULL, *defnhp_scale = NULL, *defnhp_isnull = NULL, *defnhp_key = NULL, *defnhp_count = NULL;
  ub2 len_tabname, len_colname, len_typename, len_charlen, len_scale, len_isnull, len_key, len_count;
  ub4 prefetch_rows = 200;
  sword result;

  /* return a pointer to the static variables */
  *tabname = s_tabname;
  *colname = s_colname;

  /* when first called, check if the schema does exist */
  if (session->stmthp == NULL) {
    /* create statement handle */
    allocHandle ((void **) &(session->stmthp), OCI_HTYPE_STMT, 0, session->envp->envhp, session->connp,
		 FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIHandleAlloc failed to allocate statement handle");

    /* prepare the query */
    if (checkerr (OCIStmtPrepare (session->stmthp, session->envp->errhp, (text *) schema_query, (ub4) strlen (schema_query),
				  (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIStmtPrepare failed to prepare schema query", db2Message);
    }

    /* bind the parameter */
    if (checkerr (OCIBindByName (session->stmthp, &bndhp, session->envp->errhp, (text *) ":nsp",
				 (sb4) 4, (dvoid *) schema, (sb4) (strlen (schema) + 1),
				 SQLT_STR, (dvoid *) & ind, NULL, NULL, (ub4) 0, NULL, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIBindByName failed to bind parameter", db2Message);
    }

    /* define the result value */
    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp_count, session->envp->errhp, (ub4) 1,
				  (dvoid *) & count, (sb4) sizeof (int),
				  SQLT_INT, (dvoid *) & ind, (ub2 *) & len_count, NULL, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIDefineByPos failed to define result", db2Message);
    }

    /* execute the query and get the first result row */
    if (checkerr (OCIStmtExecute (session->connp->svchp, session->stmthp, session->envp->errhp, (ub4) 1, (ub4) 0,
				  (CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIStmtExecute failed to execute schema query", db2Message);
    }

    /* free the statement handle */
    freeHandle (session->stmthp, session->connp);
    session->stmthp = NULL;

    /* return -1 if the remote schema does not exist */
    if (count == 0)
      return -1;
  }

  if (session->stmthp == NULL) {
    /* create statement handle */
    allocHandle ((void **) &(session->stmthp), OCI_HTYPE_STMT, 0, session->envp->envhp, session->connp,
		 FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIHandleAlloc failed to allocate statement handle");

    /* set prefetch options */
    if (checkerr (OCIAttrSet ((dvoid *) session->stmthp, OCI_HTYPE_STMT, (dvoid *) & prefetch_rows, 0,
			      OCI_ATTR_PREFETCH_ROWS, session->envp->errhp), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIAttrSet failed to set number of prefetched rows in statement handle", db2Message);
    }

    /* prepare the query */
    if (checkerr (OCIStmtPrepare (session->stmthp, session->envp->errhp, (text *) column_query, (ub4) strlen (column_query),
				  (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIStmtPrepare failed to prepare remote query", db2Message);
    }

    /* bind the parameter */
    if (checkerr (OCIBindByName (session->stmthp, &bndhp, session->envp->errhp, (text *) ":nsp", (sb4) 4, (dvoid *) schema, (sb4) (strlen (schema) + 1),
				 SQLT_STR, (dvoid *) & ind, NULL, NULL, (ub4) 0, NULL, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIBindByName failed to bind parameter", db2Message);
    }

    /* define result values */
    s_tabname[128] = '\0';
    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp_tabname, session->envp->errhp, (ub4) 1, (dvoid *) s_tabname, (sb4) 129,
				  SQLT_STR, (dvoid *) & ind_tabname, (ub2 *) & len_tabname, NULL, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIDefineByPos failed to define result for table name", db2Message);
    }

    s_colname[128] = '\0';
    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp_colname, session->envp->errhp, (ub4) 2, (dvoid *) s_colname, (sb4) 129,
				  SQLT_STR, (dvoid *) & ind_colname, (ub2 *) & len_colname, NULL, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIDefineByPos failed to define result for column name", db2Message);
    }

    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp_typename, session->envp->errhp, (ub4) 3, (dvoid *) typename, (sb4) 129,
				  SQLT_STR, (dvoid *) & ind_typename, (ub2 *) & len_typename, NULL, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIDefineByPos failed to define result for type name", db2Message);
    }


    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp_charlen, session->envp->errhp, (ub4) 4, (dvoid *) charlen, (sb4) sizeof (int),
				  SQLT_INT, (dvoid *) & ind_charlen, (ub2 *) & len_charlen, NULL, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIDefineByPos failed to define result for character length", db2Message);
    }


    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp_scale, session->envp->errhp, (ub4) 5, (dvoid *) typescale, (sb4) sizeof (int),
				  SQLT_INT, (dvoid *) & ind_scale, (ub2 *) & len_scale, NULL, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIDefineByPos failed to define result for type scale", db2Message);
    }

    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp_isnull, session->envp->errhp, (ub4) 6, (dvoid *) isnull, (sb4) 2,
				  SQLT_STR, (dvoid *) & ind_isnull, (ub2 *) & len_isnull, NULL, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIDefineByPos failed to define result for nullability", db2Message);
    }

    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp_key, session->envp->errhp, (ub4) 7, (dvoid *) key, (sb4) sizeof (int),
				  SQLT_INT, (dvoid *) & ind_key, (ub2 *) & len_key, NULL, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIDefineByPos failed to define result for primary key", db2Message);
    }

    /* execute the query and get the first result row */
    result = checkerr (OCIStmtExecute (session->connp->svchp, session->stmthp, session->envp->errhp, (ub4) 1, (ub4) 0,
				       (CONST OCISnapshot *) NULL, (OCISnapshot *) NULL, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__);

    if (result != OCI_SUCCESS && result != OCI_NO_DATA) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIStmtExecute failed to execute column query", db2Message);
    }
  }
  else {
    /* fetch the next result row */
    result = checkerr (OCIStmtFetch2 (session->stmthp, session->envp->errhp, 1, OCI_FETCH_NEXT, 0, OCI_DEFAULT), (dvoid *) session->envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__);

    if (result != OCI_SUCCESS && result != OCI_NO_DATA) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIStmtFetch2 failed to fetch next result row", db2Message);
    }
  }

  if (result == OCI_NO_DATA) {
    /* free the statement handle */
    freeHandle (session->stmthp, session->connp);
    session->stmthp = NULL;

    return 0;
  }
  else {
    /* set nullable to 1 if isnull is 'Y', else 0 */
    *nullable = (isnull[0] == 'Y');

    /* figure out correct data type */
    if (strcmp (typename, "VARCHAR ") == 0)
      *type = SQL_TYPE_VARCHAR;
    else if (strcmp (typename, "CHAR    ") == 0)
      *type = SQL_TYPE_CHAR;
    else if (strcmp (typename, "SMALLINT") == 0)
      *type = SQL_TYPE_SMALL;
    else if (strcmp (typename, "INTEGER ") == 0)
      *type = SQL_TYPE_INTEGER;
    else if (strcmp (typename, "BIGINT  ") == 0)
      *type = SQL_TYPE_BIG;
    else if (strcmp (typename, "DATE    ") == 0)
      *type = SQL_TYPE_DATE;
    else if (strcmp (typename, "TIMESTMP") == 0)
      *type = SQL_TYPE_STAMP;
    else if (strcmp (typename, "TIME    ") == 0)
      *type = SQL_TYPE_TIME;
    else if (strcmp (typename, "XML     ") == 0) {
      db2Debug2("typename: <%s>",typename);
      *type = SQL_TYPE_XML;
    }
    else if (strcmp (typename, "BLOB    ") == 0)
      *type = SQL_TYPE_BLOB;
    else if (strcmp (typename, "CLOB    ") == 0)
      *type = SQL_TYPE_CLOB;
    else if (strcmp (typename, "DECIMAL ") == 0)
      *type = SQL_TYPE_DECIMAL;
    else if (strcmp (typename, "GRAPHIC ") == 0)
      *type = SQL_TYPE_GRAPHIC;
    else if (strcmp (typename, "VARGRAPH") == 0)
      *type = SQL_TYPE_VARGRAPHIC;
    else if (strcmp (typename, "DOUBLE  ") == 0)
      *type = SQL_TYPE_DOUBLE;
    else if (strcmp (typename, "REAL    ") == 0)
      *type = SQL_TYPE_REAL;
    else if (strcmp (typename, "FLOAT   ") == 0)
      *type = SQL_TYPE_FLOAT;
    else if (strcmp (typename, "BOOLEAN ") == 0)
      *type = SQL_TYPE_BOOLEAN;
    else {
      db2Debug2(" OTHER typename: <%s>",typename);
      *type = SQL_TYPE_OTHER;
    }

    /* set character length, precision and scale to 0 if it was a NULL value */
    if (ind_charlen != OCI_IND_NOTNULL)
      *charlen = 0;
    if (ind_precision != OCI_IND_NOTNULL)
      *typeprec = 0;
    if (ind_scale != OCI_IND_NOTNULL)
      *typescale = 0;
  }

  return 1;
}

/*
 * checkerr
 * 		Call OCIErrorGet to get error message and error code.
 */
sword checkerr (sword status, dvoid * handle, ub4 handleType,int line, char * file)
{
  char message[1024 + 1];
  char submessage[200];
  char sqlstate[5 + 1];
  sb4 sqlcode;
  ub4 i = 1;


  memset (db2Message,0x00,sizeof(db2Message));
  switch (status)
  {
    case OCI_SUCCESS:
      break;
    case OCI_INVALID_HANDLE:
      sprintf(db2Message,"-CI INVALID HANDLE-----\nline=%d\nfile=%s\n",line,file);
      break;
    case OCI_ERROR:
      memset (submessage,0x00,sizeof(submessage));
      memset (message,0x00,sizeof(message));
      while (OCIErrorGet ( handle, i, (text *)sqlstate, &sqlcode, (text *) message, sizeof(message), handleType) == OCI_SUCCESS)  {
        sprintf(submessage,"SQLSTATE = %s  SQLCODE = %d\nline=%d\nfile=%s\n", sqlstate,sqlcode,line,file);
        if ((sizeof(db2Message) - strlen(db2Message))> strlen(submessage)+1){
          strcat (db2Message,submessage);
        }
        if ((sizeof(db2Message) - strlen(db2Message))> strlen(message)+2){
          strcat (db2Message,message);
          strcat (db2Message,"\n");
        }
        i++;
        memset (submessage,0x00,sizeof(submessage));
        memset (message,0x00,sizeof(message));
      }
      break;
    case OCI_SUCCESS_WITH_INFO:
      status = OCI_SUCCESS;
      break;
    case OCI_NEED_DATA:
      break;
    case OCI_NO_DATA:
      strcpy (db2Message, "SQL0100: no data found");
      err_code = (sb4) 100;
      break;
    default:
      break;
  }

  return status;
}

/*
 * copyDB2Text
 * 		Returns a palloc'ed string containing a (possibly quoted) copy of "string".
 * 		If the string starts with "(" and ends with ")", no quoting will take place
 * 		even if "quote" is true.
 */
char * copyDB2Text (const char *string, int size, int quote)
{
  int resultsize = (quote ? size + 2 : size);
  register int i, j = -1;
  char *result;

  /* if "string" is parenthized, return a copy */
  if (string[0] == '(' && string[size - 1] == ')') {
    result = db2Alloc (size + 1);
    memcpy (result, string, size);
    result[size] = '\0';
    return result;
  }

  if (quote) {
    for (i = 0; i < size; ++i) {
      if (string[i] == '"')
	++resultsize;
    }
  }

  result = db2Alloc (resultsize + 1);
  if (quote)
    result[++j] = '"';
  for (i = 0; i < size; ++i) {
    result[++j] = string[i];
    if (quote && string[i] == '"')
      result[++j] = '"';
  }
  if (quote)
    result[++j] = '"';
  result[j + 1] = '\0';

  return result;
}

/*
 * closeSession
 * 		Close the session and remove it from the cache.
 * 		If "disconnect" is true, close the server connection when appropriate.
 */
void closeSession (OCIEnv * envhp, OCIServer * srvhp, OCISession * userhp, int disconnect)
{
  struct envEntry *envp;
  struct srvEntry *srvp;
  struct connEntry *connp;
  OCITrans *txnhp = NULL;

  /* search environment handle in cache */
  envp = findenvEntryHandle (rootenvEntry,envhp);

  if (envp == NULL) {
    if (silent) return;
    else db2Error (FDW_ERROR, "closeSession internal error: environment handle not found in cache");
  }

  /* search server handle in cache */
  srvp = findsrvEntryHandle (envp->srvlist,srvhp);

  if (srvp == NULL) {
    if (silent) return;
    else db2Error (FDW_ERROR, "closeSession internal error: server handle not found in cache");
  }

  /* search connection in cache */
  connp = findconnEntryHandle (srvp->connlist,userhp);

  if (connp == NULL) {
    if (silent) return;
    else db2Error (FDW_ERROR, "closeSession internal error: user handle not found in cache");
  }

  /* terminate the session */
  if (checkerr (OCISessionEnd (connp->svchp, envp->errhp, connp->userhp, OCI_DEFAULT), (dvoid *) envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS && !silent) {
    db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error closing session: OCISessionEnd failed to terminate session", db2Message);
  }

  /* free the session handle */
  (void) OCIHandleFree ((dvoid *) connp->userhp, OCI_HTYPE_SESSION);

  /* get the transaction handle */
  if (checkerr (OCIAttrGet ((dvoid *) connp->svchp, (ub4) OCI_HTYPE_SVCCTX,
			    (dvoid *) & txnhp, (ub4 *) 0, (ub4) OCI_ATTR_TRANS, envp->errhp), (dvoid *) envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS && !silent) {
    db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error closing session: OCIAttrGet failed to get transaction handle", db2Message);
  }

  /* free the service handle */
  (void) OCIHandleFree ((dvoid *) connp->svchp, OCI_HTYPE_SVCCTX);

  /* free the transaction handle */
  (void) OCIHandleFree ((dvoid *) txnhp, OCI_HTYPE_TRANS);

  /* remove the session handle from the cache */
  deleteconnEntry(srvp->connlist, connp);
  srvp->connlist = NULL;

  /* close the server session if desired and this is the last session */
  if (disconnect && srvp->left->connlist == NULL)
    disconnectServer (envhp, srvhp);

  /* unregister callback for rolled back transactions */
  db2UnregisterCallback (connp);

  /* free the memory */
/*  free (connp->user);
  free (connp);*/
}

/*
 * disconnectServer
 * 		Disconnect from the server and remove it from the cache.
 */
void disconnectServer (OCIEnv * envhp, OCIServer * srvhp)
{
  struct envEntry *envp;
  struct srvEntry *srvp;

  /* search environment handle in cache */
  envp = findenvEntryHandle (rootenvEntry,envhp);
  if (envp == NULL) {
    if (silent) return;
    else db2Error (FDW_ERROR, "disconnectServer internal error: environment handle not found in cache");
  }

  /* search server handle in cache */
  srvp = findsrvEntryHandle (envp->srvlist,srvhp);

  if (srvp == NULL) {
    if (silent) return;
    else db2Error (FDW_ERROR, "disconnectServer internal error: server handle not found in cache");
  }

  /* disconnect server */
  if (checkerr (OCIServerDetach (srvp->srvhp, envp->errhp, OCI_DEFAULT), (dvoid *) envp->errhp, OCI_HTYPE_ERROR,__LINE__, __FILE__) != OCI_SUCCESS && !silent) {
    db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error closing session: OCIServerDetach failed to detach from server", db2Message);
  }

  /* free the server handle */
  (void) OCIHandleFree ((dvoid *) srvp->srvhp, OCI_HTYPE_SERVER);

  /* remove server entry from the linked list */
  deletesrvEntry(envp->srvlist,srvp);
  envp->srvlist = NULL;
}

/*
 * removeEnvironment
 * 		Deallocate environment and error handle and remove cache entry.
 */
void removeEnvironment (OCIEnv * envhp)
{
  struct envEntry *envp;

  /* search environment handle in cache */
  envp = findenvEntryHandle (rootenvEntry,envhp);

  if (envp == NULL) {
    if (silent)
      return;
    else
      db2Error (FDW_ERROR, "removeEnvironment internal error: environment handle not found in cache");
  }

  /* free the error handle */
  (void) OCIHandleFree ((dvoid *) envp->errhp, OCI_HTYPE_ERROR);

  /* free the environment handle */
  (void) OCIHandleFree ((dvoid *) envp->envhp, OCI_HTYPE_ENV);

  /* remove environment entry from the linked list */
  deleteenvEntry(rootenvEntry,envp);
  envp = NULL;
}

/*
 * allocHandle
 * 		Allocate an DB2 handle or descriptor, keep it in the cached list.
 */

void allocHandle (dvoid ** handlepp, ub4 type, int isDescriptor, OCIEnv * envhp, struct connEntry *connp, db2error error, const char *errmsg)
{
  struct handleEntry *entry;
  sword rc;
  printstruct();

  /* create entry for linked list */
  if ((entry = malloc (sizeof (struct handleEntry))) == NULL) {
    db2Error_d (FDW_OUT_OF_MEMORY, "error allocating handle:"," failed to allocate %d bytes of memory", sizeof (struct handleEntry));
  }

  if (isDescriptor)
    rc = OCIDescriptorAlloc ((const dvoid *) envhp, handlepp, type, (size_t) 0, NULL);
  else
    rc = OCIHandleAlloc ((const dvoid *) envhp, handlepp, type, (size_t) 0, NULL);

  if (rc != OCI_SUCCESS) {
    free (entry);
    db2Error (error, errmsg);
  }

  /* add handle to linked list */
  entry->handlep = *handlepp;
  entry->type = type;
  entry->isDescriptor = isDescriptor;
  entry->next = connp->handlelist;
  connp->handlelist = entry;
}

/*
 * freeHandle
 * 		Free an DB2 handle or descriptor, remove it from the cached list.
 */

void
freeHandle (dvoid * handlep, struct connEntry *connp)
{
  struct handleEntry *entry, *preventry = NULL;

  /* find it in the linked list */
  for (entry = connp->handlelist; entry != NULL; entry = entry->next) {
    if (entry->handlep == handlep)
      break;

    preventry = entry;
  }

  if (entry == NULL)
    db2Error (FDW_ERROR, "internal error freeing handle: not found in cache");

  /* free the handle */
  if (entry->isDescriptor)
    (void) OCIDescriptorFree (handlep, entry->type);
  else
    (void) OCIHandleFree (handlep, entry->type);

  /* remove it */
  if (preventry == NULL)
    connp->handlelist = entry->next;
  else
    preventry->next = entry->next;

  free (entry);
}

/*
 * getDB2Type
 * 		Find db2's name for a given db2Type.
 */

ub2
getDB2Type (db2Type arg)
{
  switch (arg) {
  case SQL_TYPE_BLOB:
    return SQLT_BLOB;
  case SQL_TYPE_CLOB:
    return SQLT_CLOB;
  case SQL_TYPE_BIG:
    return SQLT_LVC;
  default:
    /* all other columns are converted to strings */
    return SQLT_STR;
  }
}

/*
 * bind_out_callback
 * 		Point DB2 to where it should write the value for the output parameter.
 */

sb4
bind_out_callback (void *octxp, OCIBind * bindp, ub4 iter, ub4 index, void **bufpp, ub4 ** alenp, ub1 * piecep, void **indp, ub2 ** rcodep)
{
  struct db2Column *column = (struct db2Column *) octxp;

  if (column->db2type == SQL_TYPE_BLOB || column->db2type == SQL_TYPE_CLOB) {
    /* for LOBs, data should be written to the LOB locator */
    *bufpp = *((OCILobLocator **) column->val);
    *indp = &(column->val_null);
  }
  else {
    /* for other types, data should be written directly to the buffer */
    *bufpp = column->val;
    *indp = &(column->val_null);
  }
  column->val_len4 = (unsigned int) column->val_size;
  *alenp = &(column->val_len4);
  *rcodep = NULL;

  if (*piecep == OCI_ONE_PIECE)
    return OCI_CONTINUE;
  else
    return OCI_ERROR;
}

/*
 * bind_in_callback
 * 		Provide a NULL value.
 * 		This is necessary for output parameters to keep DB2 from crashing.
 */

sb4
bind_in_callback (void *ictxp, OCIBind * bindp, ub4 iter, ub4 index, void **bufpp, ub4 * alenp, ub1 * piecep, void **indpp)
{
  struct db2Column *column = (struct db2Column *) ictxp;

  *piecep = OCI_ONE_PIECE;

  column->val_null = -1;
  *indpp = &(column->val_null);

  return OCI_CONTINUE;
}
