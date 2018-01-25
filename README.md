Foreign Data Wrapper for DB2
===============================

db2_fdw is a PostgreSQL extension that provides a Foreign Data Wrapper for
easy and efficient access to DB2 databases, including pushdown of WHERE
conditions and required columns as well as comprehensive EXPLAIN support.

This README contains the following sections:

1. [Cookbook](#1-cookbook)
2. [Objects created by the extension](#2-objects-created-by-the-extension)
3. [Options](#3-options)
4. [Usage](#4-usage)
5. [Installation Requirements](#5-installation-requirements)
6. [Installation](#6-installation)
7. [Internals](#7-internals)
8. [Problems](#8-problems)
9. [Support](#9-support)

db2_fdw was written by Wolfgang Brandl, with notable contributions from
Laurenz Alba from Austria.

1 Cookbook
==========

This is a simple example how to use db2_fdw.
More detailed information will be provided in the sections
[Options](#3-options) and [Usage](#4-usage).  You should also read the

[PostgreSQL documentation on foreign data](https://www.postgresql.org/docs/current/static/ddl-foreign-data.html)

and the commands referenced there.
A free distribution of DB2 can be found at:

[IBM Db2 Express-C: Available at no charge](https://www.ibm.com/developerworks/downloads/im/db2express/)

For the Installation of DB2 look at:

[An overview of installing DB2 database servers](https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.1.0/com.ibm.db2.luw.qb.server.doc/doc/t0008921.html)

For the sake of this example, let's assume you can connect as operating system
user `postgres` (or whoever starts the PostgreSQL server) with the following
command:

    db2 connect to SAMPLE

That means that the DB2 client and the environment is set up correctly.
We also assume that the SAMPLE database provided in the DB2 package
installation was built with:

    db2sample

Please look at:

[DB2 Verify Installation using command line processor](https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.1.0/com.ibm.db2.luw.qb.server.doc/doc/t0006839.html)

I also assume that db2_fdw has been compiled and installed (see the
[Installation](#6-installation) section).

We want to access the tables defined in the SAMPLE database:

    db2 describe table DB2INST1.EMPLOYEE
 
                                    Data type                     Column
    Column name                     schema    Data type name      Length     Scale Nulls
    ------------------------------- --------- ------------------- ---------- ----- ------
    EMPNO                           SYSIBM    CHARACTER                    6     0 No
    FIRSTNME                        SYSIBM    VARCHAR                     12     0 No
    MIDINIT                         SYSIBM    CHARACTER                    1     0 Yes
    LASTNAME                        SYSIBM    VARCHAR                     15     0 No
    WORKDEPT                        SYSIBM    CHARACTER                    3     0 Yes
    PHONENO                         SYSIBM    CHARACTER                    4     0 Yes
    HIREDATE                        SYSIBM    DATE                         4     0 Yes
    JOB                             SYSIBM    CHARACTER                    8     0 Yes
    EDLEVEL                         SYSIBM    SMALLINT                     2     0 No
    SEX                             SYSIBM    CHARACTER                    1     0 Yes
    BIRTHDATE                       SYSIBM    DATE                         4     0 Yes
    SALARY                          SYSIBM    DECIMAL                      9     2 Yes
    BONUS                           SYSIBM    DECIMAL                      9     2 Yes
    COMM                            SYSIBM    DECIMAL                      9     2 Yes


Then configure db2_fdw as PostgreSQL superuser like this:

    pgdb=# CREATE EXTENSION db2_fdw;
    pgdb=# CREATE SERVER sample FOREIGN DATA WRAPPER db2_fdw OPTIONS (dbserver 'SAMPLE');
    pgdb=# GRANT USAGE ON FOREIGN SERVER sample TO pguser;


(You can use other naming methods or local connections, see the description of
the option **dbserver** below.)

Then you can connect to PostgreSQL as `pguser` and define:

    pgdb=> CREATE USER MAPPING FOR PUBLIC SERVER sample OPTIONS (user '', password '');



    pgdb=> IMPORT FOREIGN SCHEMA "DB2INST1" FROM SERVER sample INTO public;


(Remember that table and schema name -- the latter is optional -- must
normally be in uppercase.)

Now you can use the table like a regular PostgreSQL table.

2 Objects created by the extension
==================================

    FUNCTION db2_fdw_handler() RETURNS fdw_handler
    FUNCTION db2_fdw_validator(text[], oid) RETURNS void

These functions are the handler and the validator function necessary to create
a foreign data wrapper.

    FOREIGN DATA WRAPPER db2_fdw HANDLER db2_fdw_handler VALIDATOR db2_fdw_validator

The extension automatically creates a foreign data wrapper named `db2_fdw`.
Normally that's all you need, and you can proceed to define foreign servers.
You can create additional DB2 foreign data wrappers, for example if you
need to set the **nls_lang** option (you can alter the existing `db2_fdw`
wrapper, but all modifications will be lost after a dump/restore).

    FUNCTION db2_close_connections() RETURNS void

This function can be used to close all open DB2 connections in this session.
See the [Usage](#4-usage) section for further description.

    FUNCTION db2_diag(name DEFAULT NULL) RETURNS text

This function is useful for diagnostic purposes only.
It will return the versions of db2_fdw, PostgreSQL server and DB2 client.
If called with no argument or NULL, it will additionally return the values of
some environment variables used for establishing DB2 connections.
If called with the name of a foreign server, it will additionally return
the DB2 server version.

3 Options
=========

Foreign data wrapper options
----------------------------

(Caution: If you modify the default foreign data wrapper `db2_fdw`,
any changes will be lost upon dump/restore.  Create a new foreign data wrapper
if you want the options to be persistent.  The SQL script shipped with the
software contains a CREATE FOREIGN DATA WRAPPER statement you can use.)

- **nls_lang** (optional)

  Sets the DB2CODEPAGE registry variable to the code page the database is setup.
  To verfy the DB2 database codepage execute the command:

      db2 get db cfg for SAMPLE|grep -E "Database code page|Database code set"

  Then set the registry variable for the client to:

      db2set DB2CODEPAGE=1208

  When this value is not set, db2_fdw will automatically do the right
  thing if it can and issue a warning if it cannot. Set this only if you
  know what you are doing.  See the [Problems](#8-problems) section.

Foreign server options
----------------------

- **dbserver** (required)

  The DB2 database connection string for the remote database.
  This can be in any of the forms that DB2 supports as long as your
  DB2 client is configured accordingly.

User mapping options
--------------------

- **user** (required)

  The DB2 user name for the session.
  Set this to an empty string for *external authentication* if you don't
  want to store DB2 credentials in the PostgreSQL database (one simple way
  is to use an *external password store*).

- **password** (required)

  The password for the DB2 user.

Foreign table options
---------------------

- **table** (required)

  The DB2 table name.  This name must be written exactly as it occurs in
  DB2's system catalog, so normally consist of uppercase letters only.

  To define a foreign table based on an arbitrary DB2 query, set this
  option to the query enclosed in parentheses, e.g.

      OPTIONS (table '(SELECT col FROM tab WHERE val = ''string'')')

  Do not set the **schema** option in this case.
  INSERT, UPDATE and DELETE will work on foreign tables defined on simple
  queries; if you want to avoid that (or confusing DB2 error messages
  for more complicated queries), use the table option **readonly**.

- **schema** (optional)

  The table's schema (or owner).  Useful to access tables that do not belong
  to the connecting DB2 user.  This name must be written exactly as it
  occurs in DB2's system catalog, so normally consist of uppercase letters
  only.

- **max_long** (optional, defaults to "32767")

  The maximal length of any LONG or LONG RAW columns in the DB2 table.
  Possible values are integers between 1 and 1073741823 (the maximal size of a
  `bytea` in PostgreSQL).  This amount of memory will be allocated at least
  twice, so large values will consume a lot of memory.
  If **max_long** is less than the length of the longest value retrieved,
  you will receive the error message `ORA-01406: fetched column value was
  truncated`.

- **readonly** (optional, defaults to "false")

  INSERT, UPDATE and DELETE is only allowed on tables where this option is
  not set to yes/on/true.  Since these statements can only be executed from
  PostgreSQL 9.3 on, setting this option has no effect on earlier versions.
  It might still be a good idea to set it in PostgreSQL 9.2 and earlier
  on tables that you do not wish to be changed, to be prepared for an upgrade
  to PostgreSQL 9.3 or later.

- **sample_percent** (optional, defaults to "100")

  This option only influences ANALYZE processing and can be useful to
  ANALYZE very large tables in a reasonable time.

  The value must be between 0.000001 and 100 and defines the percentage of
  DB2 table blocks that will be randomly selected to calculate PostgreSQL
  table statistics.  This is accomplished using the `SAMPLE BLOCK (x)`
  clause in DB2.

  ANALYZE will fail with ORA-00933 for tables defined with DB2 queries and
  may fail with ORA-01446 for tables defined with complex DB2 views.

- **prefetch** (optional, defaults to "200")

  Sets the number of rows that will be fetched with a single round-trip between
  PostgreSQL and DB2 during a foreign table scan.  This is implemented using
  DB2 row prefetching.  The value must be between 0 and 10240, where a value
  of zero disables prefetching.

  Higher values can speed up performance, but will use more memory on the
  PostgreSQL server.

Column options (from PostgreSQL 9.2 on)
---------------------------------------

- **key** (optional, defaults to "false")

  If set to yes/on/true, the corresponding column on the foreign DB2 table
  is considered a primary key column.
  For UPDATE and DELETE to work, you must set this option on all columns
  that belong to the table's primary key.

4 Usage
=======

DB2 permissions
------------------

The DB2 user will obviously need CONNECT privilege and the right
to select from the table or view in question.


Connections
-----------

db2_fdw caches DB2 connections because it is expensive to create an
DB2 session for each individual query.  All connections are automatically
closed when the PostgreSQL session ends.

The function `DB2_close_connections()` can be used to close all cached
DB2 connections.  This can be useful for long-running sessions that don't
access foreign tables all the time and want to avoid blocking the resources
needed by an open DB2 connection.
You cannot call this function inside a transaction that modifies DB2 data.

Columns
-------

When you define a foreign table, the columns of the DB2 table are mapped
to the PostgreSQL columns in the order of their definition.

db2_fdw will only include those columns in the DB2 query that are
actually needed by the PostgreSQL query.

The PostgreSQL table can have more or less columns than the DB2 table.
If it has more columns, and these columns are used, you will receive a warning
and NULL values will be returned.

If you want to UPDATE or DELETE, make sure that the `key` option is set on all
columns that belong to the table's primary key.  Failure to do so will result
in errors.

Data types
----------

You must define the PostgreSQL columns with data types that db2_fdw can
translate (see the conversion table below).  This restriction is only enforced
if the column actually gets used, so you can define "dummy" columns for
untranslatable data types as long as you don't access them (this trick only
works with SELECT, not when modifying foreign data).  If an DB2 value
exceeds the size of the PostgreSQL column (e.g., the length of a varchar
column or the maximal integer value), you will receive a runtime error.

These conversions are automatically handled by db2_fdw:

    DB2 type                 | Possible PostgreSQL types
    -------------------------+--------------------------------------------------
    CHAR                     | char
    VARCHAR                  | character varying
    CLOB                     | text
    VARGRAPHIC               | text
    GRAPHIC                  | text
    BLOB                     | bytea
    SMALLINT                 | smallint
    INTEGER                  | integer
    BIGINT                   | bigint
    DOUBLE                   | numeric,float
    DATE                     | date
    TIMESTAMP                | timestamp
    TIME                     | time

This part is still under development. Restrictions will arise in further testing.

WHERE conditions and ORDER BY clauses
-------------------------------------


Joins between foreign tables
----------------------------



Modifying foreign data
----------------------


EXPLAIN
-------
For the explain the db2expln CLI command is called. Therefore the bin path of DB2_HOME has to be include into the PATH environment variable.



Support for IMPORT FOREIGN SCHEMA
---------------------------------

From PostgreSQL 10.1 on, IMPORT FOREIGN SCHEMA is supported to bulk import
table definitions for all tables in an DB2 schema.
In addition to the documentation of IMPORT FOREIGN SCHEMA, consider the
following:

- IMPORT FOREIGN SCHEMA will create foreign tables for all objects found in
  ALL_TAB_COLUMNS.  That includes tables, views and materialized views,
  but not synonyms.

- There are two supported options for IMPORT FOREIGN SCHEMA:
  - **case**: controls case folding for table and column names during import.
    The possible values are:
    - `keep`: leave the names as they are in DB2, usually in upper case.
    - `lower`: translate all table and column names to lower case.
    - `smart`: only translate names that are all upper case in DB2
               (this is the default).
  - **readonly** (boolean): controls if imported tables can be modified.
    If set to `true`, all imported tables are created with the foreign
    table option **readonly** set to `true` (see the [Options](#3-options)
    section).
    The default is `false`.

- The DB2 schema name must be written exactly as it is in DB2, so
  normally in upper case.  Since PostgreSQL translates names to lower case
  before processing, you must protect the schema name with double quotes
  (for example `"SCOTT"`).

- Table names in the LIMIT TO or EXCEPT clause must be written as they
  will appear in PostgreSQL after the case folding described above.

Note that IMPORT FOREIGN SCHEMA does not work with DB2 server 8i;
see the [Problems](#8-problems) section for details.

5 Installation Requirements
===========================

db2_fdw should compile and run on any platform supported by PostgreSQL and
DB2 client, although I could only test it on Linux and Windows.

PostgreSQL 10.1 or better is required.
Support for INSERT, UPDATE and DELETE is available from PostgreSQL 9.3 on.

DB2 client version 11.1 or better is required.
db2_fdw can be built and used with DB2 Instant Client as well as with
DB2 Client and Server installations installed with Universal Installer.
Binaries compiled with DB2 Client 10 can be used with later client versions
without recompilation or relink.

The supported DB2 server versions depend on the used client version (see the
DB2 Client/Server Interoperability Matrix in support document 207303.1).
For maximum coverage use DB2 Client 11.1, as this will allow you to
connect to every server version from 8.1.7 to 12.1.0 except 9.0.1.
PostgreSQL and DB2 need to have the same architecture, for example you
cannot have 32-bit software for the one and 64-bit software for the other.

It is advisable to use the latest Patch Set on both DB2 client and server,
particularly with desupported DB2 versions.
For a list of DB2 bugs that are known to affect db2_fdw's usability,
see the [Problems](#8-problems) section.
Consult the db2_fdw Wiki (https://github.com/laurenz/db2_fdw/wiki)
for tips about DB2 installation and configuration and share your own
knowledge there.

6 Installation
==============

If you use a binary distribution of db2_fdw, skip to "Installing the
extension" below.

Building db2_fdw:
--------------------

db2_fdw has been written as a PostgreSQL extension and uses the Extension
Building Infrastructure PGXS.  It should be easy to install.

You will need PostgreSQL headers and PGXS installed (if your PostgreSQL was
installed with packages, install the development package).
You need to install DB2's C header files as well (SDK package for Instant
Client).  If you use the Instant Client ZIP files provided by DB2 and you
are not on Windows, you will have to create a symbolic link from `libclntsh.so`
to the actual shared library file yourself.

Make sure that PostgreSQL is configured `--without-ldap` (at least the server).
See the [Problems](#8-problems) section.

Make sure that `pg_config` is in the PATH (test with `pg_config --pgxs`).
Set the environment variable DB2_HOME to the location of the DB2
installation.

Unpack the source code of db2_fdw and change into the directory.
Then the software installation should be as simple as:

    $ make
    $ make install

For the second step you need write permission on the directories where
PostgreSQL is installed.

If you want to build db2_fdw in a source tree of PostgreSQL, use

    $ make NO_PGXS=1

Installing the extension:
-------------------------

Make sure that the db2_fdw shared library is installed in the PostgreSQL
library directory and that db2_fdw.control and the SQL files are in
the PostgreSQL extension directory.

Since the DB2 client shared library is probably not in the standard
library path, you have to make sure that the PostgreSQL server will be able
to find it.  How this is done varies from operating system to operating
system; on Linux you can set LD_LIBRARY_PATH or use `/etc/ld.so.conf`.

Make sure that all necessary DB2 environment variables are set in the
environment of the PostgreSQL server process (DB2_HOME if you don't use
Instant Client, TNS_ADMIN if you have configuration files, etc.)

To install the extension in a database, connect as superuser and

    CREATE EXTENSION db2_fdw;

That will define the required functions and create a foreign data wrapper.

To upgrade from an db2_fdw version before 1.0.0, use

    ALTER EXTENSION db2_fdw UPDATE;

Note that the extension version as shown by the psql command `\x` or the
system catalog `pg_available_extensions` is *not* the installed version
of db2_fdw.  To get the db2_fdw version, use the function `DB2_diag`.

Running the regression tests:
-----------------------------

Unless you are developing db2_fdw or want to test its functionality
on an exotic platform, you don't have to do this.

For the regression tests to work, you must have a PostgreSQL cluster
(10.1 or better) and an DB2 server (11.1 or better with Locator or Spatial)
running, and the db2_fdw binaries must be installed.
The regression tests will create a database called `contrib_regression` and
run a number of tests.

The DB2 database must be prepared as follows:
- The sample database 'SAMPLE' has to be created.
A operating system user with password authentication hast to be created 
and for the sake of simplification the rights DBADM granted on the SAMPLE 
database.

The regression tests are run as follows:

    $ make installcheck

7 Internals
===========

db2_fdw sets the MODULE of the DB2 session to `postgres` and the
ACTION to the backend process number.  This can help identifying the DB2
session and allows you to trace it with DBMS_MONITOR.SERV_MOD_ACT_TRACE_ENABLE.




The isolation level is directly defined in the database. Per default the 
SAMPLE database is create with the isolation level 'currently commited'

To check the isolation level execute:
     db2 get db cfg for SAMPLE|grep CUR_COMMIT

If this is set to OFF the default is cursor stability.


8 Problems
==========
will follow.


9 Support
=========

If you want to report a problem with db2_fdw, and the name of the
foreign server is (for example) "sample", please include the output of

    SELECT DB2_diag('sample');

in your problem report.
If that causes an error, please also include the output of

    SELECT DB2_diag();

If you have a problem or question or any kind of feedback, the preferred
option is to open an issue on [GitHub](https://github.com/wolfgangbrandl/db2_fdw)
This requires a GitHub account.
