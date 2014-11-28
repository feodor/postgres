/*-------------------------------------------------------------------------
 * 
 * anyarray.c
 *		various functions and operatins for 1-D arrays
 *
 * Copyright (c) 2010-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/anyarray/anyarray.c
 *
 *-------------------------------------------------------------------------
 */

#include "anyarray.h"
#include <math.h>

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
getAnyArrayTypeInfo(MemoryContext ctx, Oid typid)
{
	AnyArrayTypeInfo	*info;

	info = MemoryContextAlloc(ctx, sizeof(*info));

	info->typid = typid;
	info->cmpFuncOid = InvalidOid;
	info->hashFuncOid = InvalidOid;
	info->cmpFuncInited = false;
	info->hashFuncInited = false;
	info->funcCtx = ctx;

	get_typlenbyvalalign(typid, &info->typlen, &info->typbyval, &info->typalign);

	return info;
}

static void
cmpFuncInit(AnyArrayTypeInfo* info)
{
	if (info->cmpFuncInited == false)
	{
		if (!OidIsValid(info->cmpFuncOid))
		{
			info->cmpFuncOid = getAMProc(BTREE_AM_OID, info->typid);

			if (!OidIsValid(info->cmpFuncOid))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not find compare function")));
		}

		fmgr_info_cxt(info->cmpFuncOid, &info->cmpFunc, info->funcCtx);
		info->cmpFuncInited = true;
	}
}

/*
static void
hashFuncInit(AnyArrayTypeInfo* info)
{
	if (info->hashFuncInited == false)
	{
		if (!OidIsValid(info->hashFuncOid))
		{
			info->hashFuncOid = getAMProc(HASH_AM_OID, info->typid);

			if (!OidIsValid(info->hashFuncOid))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not find hash function")));
		}

		fmgr_info_cxt(info->hashFuncOid, &hashFunc, info->funcCtx);
		info->hashFuncInited = true;
	}
}
*/

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

	cmpFuncInit(info);

	if (s->nelems > 1)
	{
		qsort_arg(s->elems, s->nelems, sizeof(Datum), 
				  (direction > 0) ? cmpAscArrayElem : cmpDescArrayElem,
				  &info->cmpFunc);
	}
}

static void
uniqSimpleArray(SimpleArray *s, bool onlyDuplicate)
{
	AnyArrayTypeInfo	*info = s->info;

	cmpFuncInit(info);

	if (s->nelems > 1)
	{
		Datum	*tmp, *dr;
		int32	num =  s->nelems;

		if (onlyDuplicate)
		{
			Datum	*head = s->elems;

			dr = s->elems;
			tmp = s->elems + 1;

			while (tmp - s->elems < num)
			{
				while (tmp - s->elems < num && cmpAscArrayElem(tmp, dr, &info->cmpFunc) == 0)
					tmp++;

				if (tmp - dr > 1)
				{
					*head = *dr;
					head++;
				}
				dr = tmp;
			}

			s->nelems = head - s->elems;
		}
		else
		{
			dr = s->elems;
			tmp = s->elems + 1;

			while (tmp - s->elems < num)
			{
				if (cmpAscArrayElem(tmp, dr, &info->cmpFunc) != 0 )
					*(++dr) = *tmp++;
				else
					tmp++;
			}

			s->nelems = dr + 1 - s->elems;
		}
	}
	else if (onlyDuplicate)
	{
		s->nelems = 0;
	}
}

PG_FUNCTION_INFO_V1(aa_set);
Datum
aa_set(PG_FUNCTION_ARGS)
{
	Datum 				a = PG_GETARG_DATUM(0);
	Oid 				typid = get_fn_expr_argtype(fcinfo->flinfo, 0);
	AnyArrayTypeInfo	*info;
	SimpleArray			s;
	ArrayType			*r;

	if (fcinfo->flinfo->fn_extra == NULL)
		fcinfo->flinfo->fn_extra = getAnyArrayTypeInfo(fcinfo->flinfo->fn_mcxt, typid);
	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

	s.nelems = 1;
	s.elems = &a;
	s.info = info;

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

	if (fcinfo->flinfo->fn_extra == NULL)
		fcinfo->flinfo->fn_extra = getAnyArrayTypeInfo(fcinfo->flinfo->fn_mcxt, ARR_ELEMTYPE(a));
	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

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

	if (fcinfo->flinfo->fn_extra == NULL)
		fcinfo->flinfo->fn_extra = getAnyArrayTypeInfo(fcinfo->flinfo->fn_mcxt, ARR_ELEMTYPE(a));
	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

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

	if (fcinfo->flinfo->fn_extra == NULL)
		fcinfo->flinfo->fn_extra = getAnyArrayTypeInfo(fcinfo->flinfo->fn_mcxt, ARR_ELEMTYPE(a));
	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

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

	if (fcinfo->flinfo->fn_extra == NULL)
		fcinfo->flinfo->fn_extra = getAnyArrayTypeInfo(fcinfo->flinfo->fn_mcxt, ARR_ELEMTYPE(a));
	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

	s = Array2SimpleArray(info, a);
	uniqSimpleArray(s, false);
	r = SimpleArray2Array(s);

	PG_FREE_IF_COPY(a, 0);
	PG_RETURN_POINTER(r);
}

PG_FUNCTION_INFO_V1(aa_uniqd);
Datum
aa_uniqd(PG_FUNCTION_ARGS)
{
	ArrayType   		*a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType			*r;
	SimpleArray			*s;
	AnyArrayTypeInfo	*info;

	CHECKARRVALID(a);

	if (fcinfo->flinfo->fn_extra == NULL)
		fcinfo->flinfo->fn_extra = getAnyArrayTypeInfo(fcinfo->flinfo->fn_mcxt, ARR_ELEMTYPE(a));
	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

	s = Array2SimpleArray(info, a);
	uniqSimpleArray(s, true);
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
	int					i;

	CHECKARRVALID(a);

	if (get_fn_expr_argtype(fcinfo->flinfo, 1) != ARR_ELEMTYPE(a))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("array type doesn't match search type")));

	if (fcinfo->flinfo->fn_extra == NULL)
		fcinfo->flinfo->fn_extra = getAnyArrayTypeInfo(fcinfo->flinfo->fn_mcxt, ARR_ELEMTYPE(a));
	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;
	cmpFuncInit(info);

	s = Array2SimpleArray(info, a);

	if (info->typlen < 0)
		e = PointerGetDatum(PG_DETOAST_DATUM(e));

	for(i=0; i<s->nelems; i++)
	{
		if (cmpAscArrayElem(s->elems + i, &e, &info->cmpFunc) == 0)
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

	if (fcinfo->flinfo->fn_extra == NULL)
		fcinfo->flinfo->fn_extra = getAnyArrayTypeInfo(fcinfo->flinfo->fn_mcxt, ARR_ELEMTYPE(a));
	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

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

	CHECKARRVALID(a);

	if (get_fn_expr_argtype(fcinfo->flinfo, 1) != ARR_ELEMTYPE(a))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("array type doesn't match new element type")));

	if (fcinfo->flinfo->fn_extra == NULL)
		fcinfo->flinfo->fn_extra = getAnyArrayTypeInfo(fcinfo->flinfo->fn_mcxt, ARR_ELEMTYPE(a));
	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;
	cmpFuncInit(info);

	s = Array2SimpleArray(info, a);

	if (info->typlen < 0)
		e = PointerGetDatum(PG_DETOAST_DATUM(e));

	if (s->nelems == 0)
		s->elems = palloc(sizeof(*s->elems));
	else
		s->elems = repalloc(s->elems, (s->nelems + 1) * sizeof(*s->elems));

	for(i=0; i<s->nelems; i++)
	{
		if (cmpAscArrayElem(s->elems + i, &e, &info->cmpFunc) == 0)
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

PG_FUNCTION_INFO_V1(aa_union_array);
Datum
aa_union_array(PG_FUNCTION_ARGS)
{
	ArrayType           *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType           *b = PG_GETARG_ARRAYTYPE_P(1);
	SimpleArray			*sb, *sa;
	AnyArrayTypeInfo	*info;
	ArrayType			*r;

	CHECKARRVALID(a);
	CHECKARRVALID(b);

	if (ARR_ELEMTYPE(a) != ARR_ELEMTYPE(b))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("array types aren't matched")));

	if (fcinfo->flinfo->fn_extra == NULL)
		fcinfo->flinfo->fn_extra = getAnyArrayTypeInfo(fcinfo->flinfo->fn_mcxt, ARR_ELEMTYPE(a));
	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

	sa = Array2SimpleArray(info, a);
	sb = Array2SimpleArray(info, b);

	if (sa->nelems == 0)
	{
		sa = sb;
		goto out;
	}
	else if (sb->nelems == 0)
	{
		goto out;
	}

	sa->elems = repalloc(sa->elems, sizeof(*sa->elems) * (sa->nelems + sb->nelems));
	memcpy(sa->elems + sa->nelems, sb->elems, sizeof(*sa->elems) * sb->nelems);
	sa->nelems += sb->nelems;
	sortSimpleArray(sa, 1);
	uniqSimpleArray(sa, false);

out:
	r = SimpleArray2Array(sa);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_POINTER(r);
}

PG_FUNCTION_INFO_V1(aa_intersect_array);
Datum
aa_intersect_array(PG_FUNCTION_ARGS)
{
	ArrayType           *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType           *b = PG_GETARG_ARRAYTYPE_P(1);
	SimpleArray			*sb, *sa;
	AnyArrayTypeInfo	*info;
	ArrayType			*r;

	CHECKARRVALID(a);
	CHECKARRVALID(b);

	if (ARR_ELEMTYPE(a) != ARR_ELEMTYPE(b))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("array types aren't matched")));

	if (fcinfo->flinfo->fn_extra == NULL)
		fcinfo->flinfo->fn_extra = getAnyArrayTypeInfo(fcinfo->flinfo->fn_mcxt, ARR_ELEMTYPE(a));
	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

	sa = Array2SimpleArray(info, a);
	sb = Array2SimpleArray(info, b);

	if (sa->nelems == 0 || sb->nelems == 0)
	{
		sa->nelems = 0;
		goto out;
	}

	sa->elems = repalloc(sa->elems, sizeof(*sa->elems) * (sa->nelems + sb->nelems));
	memcpy(sa->elems + sa->nelems, sb->elems, sizeof(*sa->elems) * sb->nelems);
	sa->nelems += sb->nelems;
	sortSimpleArray(sa, 1);
	uniqSimpleArray(sa, true);

out:
	r = SimpleArray2Array(sa);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_POINTER(r);
}

PG_FUNCTION_INFO_V1(aa_subtract_array);
Datum
aa_subtract_array(PG_FUNCTION_ARGS)
{
	ArrayType           *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType           *b = PG_GETARG_ARRAYTYPE_P(1);
	SimpleArray			*sb, *sa, sr;
	AnyArrayTypeInfo	*info;
	ArrayType			*r;
	Datum				*pa, *pb, *pr;
	int					cmp;

	CHECKARRVALID(a);
	CHECKARRVALID(b);

	if (ARR_ELEMTYPE(a) != ARR_ELEMTYPE(b))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("array types aren't matched")));

	if (fcinfo->flinfo->fn_extra == NULL)
		fcinfo->flinfo->fn_extra = getAnyArrayTypeInfo(fcinfo->flinfo->fn_mcxt, ARR_ELEMTYPE(a));
	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

	sa = Array2SimpleArray(info, a);
	sb = Array2SimpleArray(info, b);

	sr = *sa;
	if (sb->nelems == 0)
		goto out;

	sortSimpleArray(sa, 1);
	uniqSimpleArray(sa, false);
	sortSimpleArray(sb, 1);
	uniqSimpleArray(sb, false);

	sr.elems = palloc(sizeof(*sr.elems) * sr.nelems);
	
	pa = sa->elems;
	pb = sb->elems;
	pr = sr.elems;

	while(pa - sa->elems < sa->nelems)
	{
		if (pb - sb->elems >= sb->nelems)
			cmp = -1;
		else
			cmp = cmpAscArrayElem(pa, pb, &info->cmpFunc);

		if (cmp < 0)
		{
			*pr = *pa;
			pr++;
			pa++;
		}
		else 
		{
			if (cmp == 0)
				pa++;
			pb++;
		}
	}

	sr.nelems = pr - sr.elems;

out:
	r = SimpleArray2Array(&sr);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_POINTER(r);
}

static int
numOfIntersect(SimpleArray *a, SimpleArray *b)
{
	int					cnt = 0,
						cmp;
	Datum				*aptr = a->elems,
						*bptr = b->elems;
	AnyArrayTypeInfo	*info = a->info;

	cmpFuncInit(info);

	while(aptr - a->elems < a->nelems && bptr - b->elems < b->nelems)
	{
		cmp = cmpAscArrayElem(aptr, bptr, &info->cmpFunc);

		if (cmp < 0)
			aptr++;
		else if (cmp > 0)
			bptr++;
		else
		{
			cnt++;
			aptr++;
			bptr++;
		}
	}

	return cnt;
}

static double
getSimilarity(SimpleArray *sa, SimpleArray *sb)
{
	int			inter;
	double		result = 0.0;

	sortSimpleArray(sa, 1);
	uniqSimpleArray(sa, false);
	sortSimpleArray(sb, 1);
	uniqSimpleArray(sb, false);

	inter = numOfIntersect(sa, sb);

	switch(SmlType)
	{
		case AA_Cosine:
			result = ((double)inter) / sqrt(((double)sa->nelems) * ((double)sb->nelems));
			break;
		case AA_Overlap:
			result = ((double)inter) / (((double)sa->nelems) + ((double)sb->nelems) - ((double)inter));
			break;
		default:
			elog(ERROR, "unknown similarity type");
	}

	return result;
}

PG_FUNCTION_INFO_V1(aa_similarity);
Datum
aa_similarity(PG_FUNCTION_ARGS)
{
	ArrayType			*a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType			*b = PG_GETARG_ARRAYTYPE_P(1);
	AnyArrayTypeInfo	*info;
	SimpleArray			*sa, *sb;
	double				result = 0.0;
	
	CHECKARRVALID(a);
	CHECKARRVALID(b);

	if (ARR_ELEMTYPE(a) != ARR_ELEMTYPE(b))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("array types aren't matched")));

	if (ARRISVOID(a) || ARRISVOID(b))
		PG_RETURN_FLOAT4(0.0);

	if (fcinfo->flinfo->fn_extra == NULL)
		fcinfo->flinfo->fn_extra = getAnyArrayTypeInfo(fcinfo->flinfo->fn_mcxt, ARR_ELEMTYPE(a));
	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

	sa = Array2SimpleArray(info, a);
	sb = Array2SimpleArray(info, b);

	result = getSimilarity(sa, sb);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_FLOAT4(result);
}

PG_FUNCTION_INFO_V1(aa_similarity_op);
Datum
aa_similarity_op(PG_FUNCTION_ARGS)
{
	ArrayType			*a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType			*b = PG_GETARG_ARRAYTYPE_P(1);
	AnyArrayTypeInfo	*info;
	SimpleArray			*sa, *sb;
	double				result = 0.0;
	
	CHECKARRVALID(a);
	CHECKARRVALID(b);

	if (ARR_ELEMTYPE(a) != ARR_ELEMTYPE(b))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("array types aren't matched")));

	if (ARRISVOID(a) || ARRISVOID(b))
		PG_RETURN_BOOL(false);

	if (fcinfo->flinfo->fn_extra == NULL)
		fcinfo->flinfo->fn_extra = getAnyArrayTypeInfo(fcinfo->flinfo->fn_mcxt, ARR_ELEMTYPE(a));
	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

	sa = Array2SimpleArray(info, a);
	sb = Array2SimpleArray(info, b);

	result = getSimilarity(sa, sb);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(result >= SmlLimit);
}

