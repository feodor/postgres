# contrib/hstore/Makefile

MODULE_big = hstore
OBJS = hstore_io.o hstore_op.o hstore_gist.o hstore_gin.o hstore_compat.o \
		hstore_gram.o

EXTENSION = hstore
DATA = hstore--1.3.sql hstore--1.0--1.1.sql hstore--1.1--1.2.sql \
	   hstore--1.2--1.3.sql hstore--unpackaged--1.0.sql

REGRESS = hstore nested types

EXTRA_CLEAN = y.tab.c y.tab.h \
				hstore_gram.c hstore_scan.c hstore_gram.h

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/hstore
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

hstore_gram.o: hstore_scan.c

hstore_gram.c: BISONFLAGS += -d

distprep: hstore_gram.c hstore_scan.c

maintainer-clean:
	rm -f hstore_gram.c hstore_scan.c hstore_gram.h

