# contrib/anyarray/Makefile

MODULE_big = anyarray
OBJS = anyarray.o anyarray_util.o anyarray_guc.o \
		anyarray_gist.o anyarray_gin.o

EXTENSION = anyarray
DATA = anyarray--1.0.sql
PGFILEDESC = "anyarray - functions and operators for one-dimensional arrays"

REGRESS = init anyarray int2 int4 int8 \
	float4 float8 numeric text varchar bytea \
	interval money oid

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/anyarray
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
