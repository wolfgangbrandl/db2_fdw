/*
 * Author: The maintainer's name
 * Created at: 2018-01-16 09:59:40 +0100
 *
 */

--
-- This is a example code genereted automaticaly
-- by pgxn-utils.

SET client_min_messages = warning;

-- If your extension will create a type you can
-- do somenthing like this
CREATE TYPE db2_fdw AS ( a text, b text );

-- Maybe you want to create some function, so you can use
-- this as an example
CREATE OR REPLACE FUNCTION db2_fdw (text, text)
RETURNS db2_fdw LANGUAGE SQL AS 'SELECT ROW($1, $2)::db2_fdw';

-- Sometimes it is common to use special operators to
-- work with your new created type, you can create
-- one like the command bellow if it is applicable
-- to your case

CREATE OPERATOR #? (
	LEFTARG   = text,
	RIGHTARG  = text,
	PROCEDURE = db2_fdw
);
