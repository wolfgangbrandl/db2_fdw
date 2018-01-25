\set echo off
DROP EXTENSION db2_fdw cascade;
DROP USER MAPPING FOR PUBLIC SERVER sample;
DROP FOREIGN TABLE TYPETEST1;
DROP FOREIGN TABLE EMPSM;
\set ECHO all
CREATE EXTENSION db2_fdw;

CREATE SERVER sample FOREIGN DATA WRAPPER db2_fdw OPTIONS (dbserver 'SAMPLE');
CREATE USER MAPPING FOR PUBLIC SERVER sample OPTIONS (user '', password '');
IMPORT FOREIGN SCHEMA "DB2INST1" FROM SERVER sample INTO public;
select * from employee;
select * from sales;
select * from employee a, sales b where a.lastname = b.sales_person;

