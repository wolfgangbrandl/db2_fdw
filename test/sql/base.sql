\set ECHO all
CREATE EXTENSION db2_fdw;
CREATE SERVER sample FOREIGN DATA WRAPPER db2_fdw OPTIONS (dbserver 'SAMPLE');
CREATE USER MAPPING FOR PUBLIC SERVER sample OPTIONS (user 'dbtest', password 'start123');
-- CREATE USER MAPPING FOR PUBLIC SERVER sample OPTIONS (user '', password '');
IMPORT FOREIGN SCHEMA "DB2INST1" FROM SERVER sample INTO public;
\c contrib_regression
drop foreign table org;
CREATE FOREIGN TABLE org (
                  DEPTNUMB SMALLINT OPTIONS (key 'yes') NOT NULL ,
                  DEPTNAME VARCHAR(14) ,
                  MANAGER SMALLINT ,
                  DIVISION VARCHAR(10) ,
                  LOCATION VARCHAR(13)
                   )
      SERVER sample OPTIONS (schema 'DB2INST1',table 'ORG');

delete from org;
insert into org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(10,'Head Office',160,'Corporate','New York');
insert into org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(15,'New England',50,'Eastern','Boston');
insert into org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(20,'Mid Atlantic',10,'Eastern','Washington');
insert into org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(38,'South Atlantic',30,'Eastern','Atlanta');
insert into org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(42,'Great Lakes',100,'Midwest','Chicago');
insert into org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(51,'Plains',140,'Midwest','Dallas');
insert into org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(66,'Pacific',270,'Western','San Francisco');
insert into org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(84,'Mountain',290,'Western','Denver');
select * from org;
select * from sales;
select * from employee a, sales b where a.lastname = b.sales_person;
create table orgcopy as select * from org;
drop table orgcopy;
