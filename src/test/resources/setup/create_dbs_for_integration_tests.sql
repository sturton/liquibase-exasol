
/*
 * IMPORTANT!
 *
 * If you want to run this script outside of the CI environment, you must replace :path_for_tablespace with an
 * absolute path on your filesystem. This path must already exist and it should be empty.
 */

--DROP DATABASE IF EXISTS liquibase;
--DROP TABLESPACE IF EXISTS liquibase2;
DROP SCHEMA IF EXISTS lbschem2 CASCADE;
DROP SCHEMA IF EXISTS lbcat2 CASCADE;
DROP USER IF EXISTS lbuser;

CREATE USER lbuser IDENTIFIED BY lbuser;

COMMENT ON USER lbuser IS 'Integration test user for Liquibase';

--GRANT CONNECT TO lbuser;


--GRANT ALL ON DATABASE liquibase TO lbuser;

--\c liquibase

/************************************************************************************************* Schema: LBSCHEM2 */
CREATE SCHEMA lbschem2 ;

COMMENT ON SCHEMA lbschem2 IS 'Testing schema for integration tests';

GRANT ALL ON SCHEMA lbschem2 TO lbuser;

/******************************************************************************************** Tablespace: liquibase2 */

/*
CREATE TABLESPACE liquibase2 LOCATION :path_for_tablespace;

ALTER TABLESPACE liquibase2 OWNER TO lbuser;

COMMENT ON TABLESPACE liquibase2 IS 'A testing tablespace for integration tests';

GRANT CREATE ON TABLESPACE liquibase2 TO lbuser;
*/

/**************************************************************************************************** Schema: LBCAT2 */

CREATE SCHEMA lbcat2 ;

COMMENT ON SCHEMA lbcat2 IS 'Testing schema for integration tests';

GRANT ALL ON SCHEMA lbcat2 TO lbuser;


