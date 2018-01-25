/*
 * Author: The maintainer's name
 * Created at: 2018-01-16 09:59:40 +0100
 *
 */

--
-- This is a example code genereted automaticaly
-- by pgxn-utils.

SET client_min_messages = warning;

BEGIN;

-- You can use this statements as
-- template for your extension.

DROP OPERATOR #? (text, text);
DROP FUNCTION db2_fdw(text, text);
DROP EXTENSION db2_fdw CASCADE;
COMMIT;
