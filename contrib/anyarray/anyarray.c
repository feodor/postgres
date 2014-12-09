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

#include <catalog/pg_collation.h>
#include <utils/builtins.h>


#define	CMP(a, b, cmpFunc) \
	DatumGetInt32(FunctionCall2Coll((cmpFunc), DEFAULT_COLLATION_OID, (a), (b)))

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

	freeSimpleArray(s);
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

	freeSimpleArray(s);
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

	freeSimpleArray(s);
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

	freeSimpleArray(s);
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
		if (CMP(s->elems[i], e, &info->cmpFunc) == 0)
			break;
	}

	freeSimpleArray(s);
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
			s->elems += start;
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
		if (CMP(s->elems[i], e, &info->cmpFunc) == 0)
			break;
	}

	if (s->nelems == 0 || i>=s->nelems) {
		s->elems[s->nelems] = e;
		s->nelems++;
	}

	r = SimpleArray2Array(s);

	freeSimpleArray(s);
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

	freeSimpleArray(sb);
	freeSimpleArray(sa);
	PG_FREE_IF_COPY(b, 1);
	PG_FREE_IF_COPY(a, 0);
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
			cmp = CMP(*pa, *pb, &info->cmpFunc);

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

	freeSimpleArray(sb);
	freeSimpleArray(sa);
	PG_FREE_IF_COPY(b, 1);
	PG_FREE_IF_COPY(a, 0);
	PG_RETURN_POINTER(r);
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

	freeSimpleArray(sb);
	freeSimpleArray(sa);
	PG_FREE_IF_COPY(b, 1);
	PG_FREE_IF_COPY(a, 0);
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

	freeSimpleArray(sb);
	freeSimpleArray(sa);
	PG_FREE_IF_COPY(b, 1);
	PG_FREE_IF_COPY(a, 0);
	PG_RETURN_BOOL(result >= SmlLimit);
}

PG_FUNCTION_INFO_V1(aa_distance);
Datum
aa_distance(PG_FUNCTION_ARGS)
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
	if (result == 0.0)
		result = get_float4_infinity();
	else
		result = 1.0/result;

	freeSimpleArray(sb);
	freeSimpleArray(sa);
	PG_FREE_IF_COPY(b, 1);
	PG_FREE_IF_COPY(a, 0);
	PG_RETURN_FLOAT4(result);
}

