CREATE FUNCTION db2_fdw_handler() RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION db2_fdw_handler()
IS 'DB2 foreign data wrapper handler';

CREATE FUNCTION db2_fdw_validator(text[], oid) RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION db2_fdw_validator(text[], oid)
IS 'DB2 foreign data wrapper options validator';

CREATE FUNCTION db2_close_connections() RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION db2_close_connections()
IS 'closes all open DB2 connections';

CREATE FUNCTION db2_diag(name DEFAULT NULL) RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE CALLED ON NULL INPUT;

COMMENT ON FUNCTION db2_diag(name)
IS 'shows the version of db2_fdw, PostgreSQL, DB2 client and DB2 server';

CREATE FOREIGN DATA WRAPPER db2_fdw
  HANDLER db2_fdw_handler
  VALIDATOR db2_fdw_validator;

COMMENT ON FOREIGN DATA WRAPPER db2_fdw
IS 'DB2 foreign data wrapper';
