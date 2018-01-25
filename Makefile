EXTENSION    = db2_fdw
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
MODULE_big   = db2_fdw
OBJS         = db2_fdw.o db2_utils.o

DATA         = $(filter-out $(wildcard sql/*--*.sql),$(wildcard sql/*.sql))
DOCS         = $(wildcard doc/*.md)
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test
#
# Uncoment the MODULES line if you are adding C files
# to your extention.
#
#MODULES      = $(patsubst %.c,%,$(wildcard src/*.c))
PG_CPPFLAGS  = -g -fPIC -I$(DB2_HOME)/include
SHLIB_LINK   = -fPIC -L$(DB2_HOME)/lib64 -L$(DB2_HOME)/bin  -ldb2ci
PG_CONFIG    = pg_config

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)


checkin:
	git remote set-url origin git@github.com:wolfgangbrandl/db2_fdw.git
#	git remote set-url origin https://github.com/wolfgangbrandl/db2_fdw.git
#	git remote set-url origin https://brandlw@git.brz.gv.at/bitbucket/scm/izsdbpost/db2_fdw.git
	git add --all
	git commit -m "`date`"
	git push -u origin master

