/*-------------------------------------------------------------------------
 * 
 * pg_prewarm.c
 *		prewarming utilities
 *
 * Copyright (c) 2010-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/anyarray/anyarray.c
 *
 *-------------------------------------------------------------------------
 */

#include "anyarray.h"

#include <access/hash.h>
#include <access/htup_details.h>
#include <access/nbtree.h>
#include <catalog/pg_am.h>
#include <catalog/pg_cast.h>
#include <catalog/pg_collation.h>
#include <commands/defrem.h>
#include <utils/catcache.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

PG_MODULE_MAGIC;

static Oid
getAMProc(Oid amOid, Oid typid)
{
	Oid		opclassOid = GetDefaultOpClass(typid, amOid);

	Assert(amOid == BTREE_AM_OID || amOid == HASH_AM_OID);

	if (!OidIsValid(opclassOid))
	{
		typid = getBaseType(typid);
		opclassOid = GetDefaultOpClass(typid, amOid);

		if (!OidIsValid(opclassOid))
		{
			CatCList	*catlist;
			int			i;

			/*
			 * Search binary-coercible type
			 */
			catlist = SearchSysCacheList(CASTSOURCETARGET, 1,
										 ObjectIdGetDatum(typid),
										 0, 0, 0);
			for (i = 0; i < catlist->n_members; i++)
			{
				HeapTuple		tuple = &catlist->members[i]->tuple;
				Form_pg_cast	castForm = (Form_pg_cast)GETSTRUCT(tuple);

				if (castForm->castfunc == InvalidOid && castForm->castcontext == COERCION_CODE_IMPLICIT)
				{
					typid = castForm->casttarget;
					opclassOid = GetDefaultOpClass(typid, amOid);
					if(OidIsValid(opclassOid))
						break;
				}
			}

			ReleaseSysCacheList(catlist);
		}
	}

	if (!OidIsValid(opclassOid))
		return InvalidOid;

	return get_opfamily_proc(get_opclass_family(opclassOid),
							 typid, typid,
							 (amOid == BTREE_AM_OID) ? BTORDER_PROC : HASHPROC); 
}

static AnyArrayTypeInfo*
getAnyArrayTypeInfo(AnyArrayTypeInfo *info, Oid typid)
{
	if (info == NULL)
		info = palloc(sizeof(*info));

	info->typid = typid;
	info->cmpFuncOid = InvalidOid;
	info->hashFuncOid = InvalidOid;

	get_typlenbyvalalign(typid, &info->typlen, &info->typbyval, &info->typalign);

	return info;
}

static SimpleArray*
Array2SimpleArray(AnyArrayTypeInfo	*info, ArrayType *a)
{
	SimpleArray	*s = palloc(sizeof(SimpleArray));

	CHECKARRVALID(a);

	s->info = info;
	if (ARRISVOID(a))
	{
		s->elems = NULL;
		s->nelems = 0;
	}
	else
	{
		deconstruct_array(a, info->typid,
						  info->typlen, info->typbyval, info->typalign,
						  &s->elems, NULL, &s->nelems);
	}

	return s;
}

static ArrayType*
SimpleArray2Array(SimpleArray *s)
{
	return construct_array(s->elems, s->nelems, 
						   s->info->typid, 
						   s->info->typlen, 
						   s->info->typbyval, 
						   s->info->typalign);
}

static int
cmpAscArrayElem(const void *a, const void *b, void *arg)
{
	FmgrInfo	*cmpFunc = (FmgrInfo*)arg;

	return DatumGetInt32(FunctionCall2Coll(cmpFunc, DEFAULT_COLLATION_OID, *(Datum*)a, *(Datum*)b));
}

static int
cmpDescArrayElem(const void *a, const void *b, void *arg)
{
	FmgrInfo	*cmpFunc = (FmgrInfo*)arg;

	return -DatumGetInt32(FunctionCall2Coll(cmpFunc, DEFAULT_COLLATION_OID, *(Datum*)a, *(Datum*)b));
}

static void
sortSimpleArray(SimpleArray *s, int32 direction)
{
	AnyArrayTypeInfo	*info = s->info;
	FmgrInfo			cmpFunc;

	if (!OidIsValid(info->cmpFuncOid))
	{
		info->cmpFuncOid = getAMProc(BTREE_AM_OID, info->typid);

		if (!OidIsValid(info->cmpFuncOid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not find compare function")));
	}

	if (s->nelems > 1)
	{
		fmgr_info(info->cmpFuncOid, &cmpFunc);

		qsort_arg(s->elems, s->nelems, sizeof(Datum), 
				  (direction > 0) ? cmpAscArrayElem : cmpDescArrayElem,
				  &cmpFunc);
	}
}

static void
uniqSimpleArray(SimpleArray *s)
{
	AnyArrayTypeInfo	*info = s->info;
	FmgrInfo			cmpFunc;

	if (!OidIsValid(info->cmpFuncOid))
	{
		info->cmpFuncOid = getAMProc(BTREE_AM_OID, info->typid);

		if (!OidIsValid(info->cmpFuncOid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not find compare function")));
	}

	if (s->nelems > 1)
	{
		Datum	*tmp, *dr;
		int32	cmp, num =  s->nelems;

		fmgr_info(info->cmpFuncOid, &cmpFunc);

		tmp = dr = s->elems;
		while (tmp - s->elems < num)
		{
			cmp = (tmp == dr) ? 0 : cmpAscArrayElem(tmp, dr, &cmpFunc);

			if ( cmp != 0 )
				*(++dr) = *tmp++;
			else
				tmp++;
		}

		s->nelems = dr + 1 - s->elems;
	}
}

PG_FUNCTION_INFO_V1(aa_set);
Datum
aa_set(PG_FUNCTION_ARGS)
{
	Datum 				a = PG_GETARG_DATUM(0);
	Oid 				typid = get_fn_expr_argtype(fcinfo->flinfo, 0);
	AnyArrayTypeInfo	info;
	SimpleArray			s;
	ArrayType			*r;

	getAnyArrayTypeInfo(&info, typid);
	s.nelems = 1;
	s.elems = &a;
	s.info = &info;

	r = SimpleArray2Array(&s);

	PG_RETURN_POINTER(r);
}

PG_FUNCTION_INFO_V1(aa_icount);
Datum
aa_icount(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	int32		count;

	CHECKARRVALID(a);
	count = ARRNELEMS(a);
	PG_FREE_IF_COPY(a, 0);

	PG_RETURN_INT32(count);
}

PG_FUNCTION_INFO_V1(aa_sort);
Datum
aa_sort(PG_FUNCTION_ARGS)
{
	ArrayType   		*a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType			*r;
	text				*dirstr = (fcinfo->nargs == 2) ? PG_GETARG_TEXT_P(1) : NULL;
	int32				direction = 1;
	SimpleArray			*s;
	AnyArrayTypeInfo	*info;

	CHECKARRVALID(a);

	if (dirstr != NULL)
	{
		if (VARSIZE_ANY_EXHDR(dirstr) == 3 &&
			pg_strncasecmp(VARDATA_ANY(dirstr), "asc", 3) == 0)
			direction = 1;
		else if (VARSIZE_ANY_EXHDR(dirstr) == 4 &&
				 pg_strncasecmp(VARDATA_ANY(dirstr), "desc", 4) == 0)
			direction = -1;
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("second parameter must be \"ASC\" or \"DESC\"")));
	}

	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

	if (info == NULL || info->typid != ARR_ELEMTYPE(a))
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(*info));
		info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;
		info = getAnyArrayTypeInfo(info, ARR_ELEMTYPE(a));
	}

	s = Array2SimpleArray(info, a);
	sortSimpleArray(s, direction);
	r = SimpleArray2Array(s);

	PG_FREE_IF_COPY(a, 0);
	if (dirstr)
		PG_FREE_IF_COPY(dirstr, 1);
	PG_RETURN_POINTER(r);
}

PG_FUNCTION_INFO_V1(aa_sort_asc);
Datum
aa_sort_asc(PG_FUNCTION_ARGS)
{
	ArrayType   		*a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType			*r;
	SimpleArray			*s;
	AnyArrayTypeInfo	*info;

	CHECKARRVALID(a);

	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

	if (info == NULL || info->typid != ARR_ELEMTYPE(a))
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(*info));
		info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;
		info = getAnyArrayTypeInfo(info, ARR_ELEMTYPE(a));
	}

	s = Array2SimpleArray(info, a);
	sortSimpleArray(s, 1);
	r = SimpleArray2Array(s);

	PG_FREE_IF_COPY(a, 0);
	PG_RETURN_POINTER(r);

}

PG_FUNCTION_INFO_V1(aa_sort_desc);
Datum
aa_sort_desc(PG_FUNCTION_ARGS)
{
	ArrayType   		*a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType			*r;
	SimpleArray			*s;
	AnyArrayTypeInfo	*info;

	CHECKARRVALID(a);

	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

	if (info == NULL || info->typid != ARR_ELEMTYPE(a))
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(*info));
		info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;
		info = getAnyArrayTypeInfo(info, ARR_ELEMTYPE(a));
	}

	s = Array2SimpleArray(info, a);
	sortSimpleArray(s, -1);
	r = SimpleArray2Array(s);

	PG_FREE_IF_COPY(a, 0);
	PG_RETURN_POINTER(r);
}

PG_FUNCTION_INFO_V1(aa_uniq);
Datum
aa_uniq(PG_FUNCTION_ARGS)
{
	ArrayType   		*a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType			*r;
	SimpleArray			*s;
	AnyArrayTypeInfo	*info;

	CHECKARRVALID(a);

	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

	if (info == NULL || info->typid != ARR_ELEMTYPE(a))
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(*info));
		info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;
		info = getAnyArrayTypeInfo(info, ARR_ELEMTYPE(a));
	}

	s = Array2SimpleArray(info, a);
	uniqSimpleArray(s);
	r = SimpleArray2Array(s);

	PG_FREE_IF_COPY(a, 0);
	PG_RETURN_POINTER(r);
}

PG_FUNCTION_INFO_V1(aa_idx);
Datum
aa_idx(PG_FUNCTION_ARGS)
{
	ArrayType   		*a = PG_GETARG_ARRAYTYPE_P(0);
	Datum				e = PG_GETARG_DATUM(1);
	SimpleArray			*s;
	AnyArrayTypeInfo	*info;
	FmgrInfo			cmpFunc;
	int					i;

	CHECKARRVALID(a);

	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

	if (info == NULL || info->typid != ARR_ELEMTYPE(a))
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(*info));
		info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;
		info = getAnyArrayTypeInfo(info, ARR_ELEMTYPE(a));
	}

	if (get_fn_expr_argtype(fcinfo->flinfo, 1) != ARR_ELEMTYPE(a))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("array type doesn't match search type")));

	s = Array2SimpleArray(info, a);

	if (!OidIsValid(info->cmpFuncOid))
	{
		info->cmpFuncOid = getAMProc(BTREE_AM_OID, info->typid);

		if (!OidIsValid(info->cmpFuncOid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not find compare function")));
	}

	if (info->typlen < 0)
		e = PointerGetDatum(PG_DETOAST_DATUM(e));

	fmgr_info(info->cmpFuncOid, &cmpFunc);
	for(i=0; i<s->nelems; i++)
	{
		if (cmpAscArrayElem(s->elems + i, &e, &cmpFunc) == 0)
			break;
	}

	PG_FREE_IF_COPY(a, 0);
	if (info->typlen < 0)
		PG_FREE_IF_COPY(DatumGetPointer(e), 1);
	PG_RETURN_INT32( (i < s->nelems) ? (i+1) : 0 );
}

PG_FUNCTION_INFO_V1(aa_subarray);
Datum
aa_subarray(PG_FUNCTION_ARGS)
{
	ArrayType   		*a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType			*r;
	int32				start = PG_GETARG_INT32(1);
	int32				len = (fcinfo->nargs == 3) ? PG_GETARG_INT32(2) : 0;
	int32				end = 0;
	SimpleArray			*s;
	AnyArrayTypeInfo	*info;

	CHECKARRVALID(a);

	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

	if (info == NULL || info->typid != ARR_ELEMTYPE(a))
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(*info));
		info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;
		info = getAnyArrayTypeInfo(info, ARR_ELEMTYPE(a));
	}

	s = Array2SimpleArray(info, a);

	if (s->nelems == 0)
	{
		r = SimpleArray2Array(s);

		PG_FREE_IF_COPY(a, 0);
		PG_RETURN_POINTER(r);
	}

	start = (start > 0) ? start - 1 : start;

	if (start < 0)
		start = s->nelems + start;

	if (len < 0)
		end = s->nelems + len;
	else if (len == 0)
		end = s->nelems;
	else
		end = start + len;

	if (end > s->nelems)
		end = s->nelems;

	if (start < 0)
		start = 0;

	if (!(start >= end || end <= 0))
	{
		if (end - start > 0 && start != 0)
			memcpy(s->elems, s->elems + start, (end - start) * sizeof(*s->elems));
		s->nelems = end - start;
	}

	r = SimpleArray2Array(s);

	PG_FREE_IF_COPY(a, 0);
	PG_RETURN_POINTER(r);
}

PG_FUNCTION_INFO_V1(aa_union_elem);
Datum
aa_union_elem(PG_FUNCTION_ARGS)
{
	ArrayType   		*a = PG_GETARG_ARRAYTYPE_P(0);
	Datum				e = PG_GETARG_DATUM(1);
	SimpleArray			*s;
	AnyArrayTypeInfo	*info;
	ArrayType			*r;
	int 				i;
	FmgrInfo			cmpFunc;

	CHECKARRVALID(a);

	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

	if (info == NULL || info->typid != ARR_ELEMTYPE(a))
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(*info));
		info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;
		info = getAnyArrayTypeInfo(info, ARR_ELEMTYPE(a));
	}

	if (get_fn_expr_argtype(fcinfo->flinfo, 1) != ARR_ELEMTYPE(a))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("array type doesn't match search type")));

	s = Array2SimpleArray(info, a);

	if (!OidIsValid(info->cmpFuncOid))
	{
		info->cmpFuncOid = getAMProc(BTREE_AM_OID, info->typid);

		if (!OidIsValid(info->cmpFuncOid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not find compare function")));
	}

	if (info->typlen < 0)
		e = PointerGetDatum(PG_DETOAST_DATUM(e));

	if (s->nelems == 0)
		s->elems = palloc(sizeof(*s->elems));
	else
		s->elems = repalloc(s->elems, (s->nelems + 1) * sizeof(*s->elems));

	fmgr_info(info->cmpFuncOid, &cmpFunc);
	for(i=0; i<s->nelems; i++)
	{
		if (cmpAscArrayElem(s->elems + i, &e, &cmpFunc) == 0)
			break;
	}

	if (s->nelems == 0 || i>=s->nelems) {
		s->elems[s->nelems] = e;
		s->nelems++;
	}

	r = SimpleArray2Array(s);

	PG_FREE_IF_COPY(a, 0);
	if (info->typlen < 0)
		PG_FREE_IF_COPY(DatumGetPointer(e), 1);
	PG_RETURN_POINTER(r);
}


