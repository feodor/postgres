/*-------------------------------------------------------------------------
 * 
 * anyarray.h
 *        anyarray common declarations
 *
 * Copyright (c) 2010-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        contrib/anyarray/anyarray.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef _ANYARRAY_H_
#define _ANYARRAY_H_
#include "postgres.h"
#include "utils/array.h"

typedef struct AnyArrayTypeInfo
{
	Oid			typid;
	Oid			hashFuncOid;
	Oid			cmpFuncOid;
	int16		typlen;
	bool		typbyval;
	char		typalign;
} AnyArrayTypeInfo;

typedef struct SimpleArray
{
	Datum				*elems;
	int32				nelems;
	AnyArrayTypeInfo	*info;
} SimpleArray;

#define NDIM 1
#define ARRISVOID(x)  ((x) == NULL || ARRNELEMS(x) == 0)
#define ARRNELEMS(x)  ArrayGetNItems(ARR_NDIM(x), ARR_DIMS(x))

/* reject arrays we can't handle; but allow a NULL or empty array */
#define CHECKARRVALID(x) \
	do { \
		if (x) { \
			if (ARR_NDIM(x) != NDIM && ARR_NDIM(x) != 0) \
				ereport(ERROR, \
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), \
						 errmsg("array must be one-dimensional"))); \
			if (ARR_HASNULL(x) && array_contains_nulls(x)) \
				ereport(ERROR, \
						(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), \
						 errmsg("array must not contain nulls"))); \
		} \
	} while(0)

#endif

