/*
 * contrib/hstore/hstore_op.c
 */
#include "postgres.h"

#include "access/hash.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/builtins.h"

#include "hstore.h"

/* old names for C functions */
HSTORE_POLLUTE(hstore_fetchval, fetchval);
HSTORE_POLLUTE(hstore_exists, exists);
HSTORE_POLLUTE(hstore_defined, defined);
HSTORE_POLLUTE(hstore_delete, delete);
HSTORE_POLLUTE(hstore_concat, hs_concat);
HSTORE_POLLUTE(hstore_contains, hs_contains);
HSTORE_POLLUTE(hstore_contained, hs_contained);
HSTORE_POLLUTE(hstore_akeys, akeys);
HSTORE_POLLUTE(hstore_avals, avals);
HSTORE_POLLUTE(hstore_skeys, skeys);
HSTORE_POLLUTE(hstore_svals, svals);
HSTORE_POLLUTE(hstore_each, each);

static HStoreValue*
arrayToHStoreSortedArray(ArrayType *a)
{
	Datum	 		*key_datums;
	bool	 		*key_nulls;
	int				key_count;
	HStoreValue		*v;
	int				i,
					j;
	bool			hasNonUniq = false;

	deconstruct_array(a,
					  TEXTOID, -1, false, 'i',
					  &key_datums, &key_nulls, &key_count);

	if (key_count == 0)
		return NULL;

	v = palloc(sizeof(*v));
	v->type = hsvArray;
	v->array.scalar = false;

	v->array.elems = palloc(sizeof(*v->hash.pairs) * key_count);

	for (i = 0, j = 0; i < key_count; i++)
	{
		if (!key_nulls[i])
		{
			v->array.elems[j].type = hsvString;
			v->array.elems[j].string.val = VARDATA(key_datums[i]);
			v->array.elems[j].string.len = VARSIZE(key_datums[i]) - VARHDRSZ;
			j++;
		}
	}
	v->array.nelems = j;

	if (v->array.nelems > 1)
		qsort_arg(v->array.elems, v->array.nelems, sizeof(*v->array.elems),
				  compareJsonbStringValue /* compareHStoreStringValue */, &hasNonUniq);

	if (hasNonUniq)
	{
		HStoreValue	*ptr = v->array.elems + 1,
					*res = v->array.elems;

		while (ptr - v->array.elems < v->array.nelems)
		{
			if (!(ptr->string.len == res->string.len &&
				  memcmp(ptr->string.val, res->string.val, ptr->string.len) == 0))
			{
				res++;
				*res = *ptr;
			}

			ptr++;
		}

		v->array.nelems = res + 1 - v->array.elems;
	}

	return v;
}

static HStoreValue*
findInHStoreSortedArray(HStoreValue *a, uint32 *lowbound,
						char *key, uint32 keylen)
{
	HStoreValue		*stopLow = a->array.elems + ((lowbound) ? *lowbound : 0),
					*stopHigh = a->array.elems + a->array.nelems,
					*stopMiddle;

	while (stopLow < stopHigh)
	{
		int diff;

		stopMiddle = stopLow + (stopHigh - stopLow) / 2;

		if (keylen == stopMiddle->string.len)
			diff = memcmp(stopMiddle->string.val, key, keylen);
		else
			diff = (stopMiddle->string.len > keylen) ? 1 : -1;

		if (diff == 0)
		{
			if (lowbound)
				*lowbound = (stopMiddle - a->array.elems) + 1;
			return stopMiddle;
		}
		else if (diff < 0)
		{
			stopLow = stopMiddle + 1;
		}
		else
		{
			stopHigh = stopMiddle;
		}
	}

	if (lowbound)
		*lowbound = (stopLow - a->array.elems) + 1;

	return NULL;
}

PG_FUNCTION_INFO_V1(hstore_fetchval);
Datum		hstore_fetchval(PG_FUNCTION_ARGS);
Datum
hstore_fetchval(PG_FUNCTION_ARGS)
{
	HStore	   	*hs = PG_GETARG_HS(0);
	text	   	*key = PG_GETARG_TEXT_PP(1);
	HStoreValue	*v = NULL;
	text		*out;

	if (!HS_ISEMPTY(hs))
		v = findUncompressedHStoreValue(VARDATA(hs),
										HS_FLAG_HASH | HS_FLAG_ARRAY,
										NULL,
										VARDATA_ANY(key),
										VARSIZE_ANY_EXHDR(key));

	if ((out = HStoreValueToText(v)) == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_TEXT_P(out);
}

PG_FUNCTION_INFO_V1(hstore_fetchval_numeric);
Datum		hstore_fetchval_numeric(PG_FUNCTION_ARGS);
Datum
hstore_fetchval_numeric(PG_FUNCTION_ARGS)
{
	HStore	   	*hs = PG_GETARG_HS(0);
	text	   	*key = PG_GETARG_TEXT_PP(1);
	HStoreValue	*v = NULL;

	if (!HS_ISEMPTY(hs))
		v = findUncompressedHStoreValue(VARDATA(hs),
										HS_FLAG_HASH | HS_FLAG_ARRAY,
										NULL,
										VARDATA_ANY(key),
										VARSIZE_ANY_EXHDR(key));

	if (v && v->type == hsvNumeric)
	{
		Numeric		out = palloc(VARSIZE_ANY(v->numeric));

		memcpy(out, v->numeric, VARSIZE_ANY(v->numeric));
		PG_RETURN_NUMERIC(out);
	}

	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(hstore_fetchval_boolean);
Datum		hstore_fetchval_boolean(PG_FUNCTION_ARGS);
Datum
hstore_fetchval_boolean(PG_FUNCTION_ARGS)
{
	HStore	   	*hs = PG_GETARG_HS(0);
	text	   	*key = PG_GETARG_TEXT_PP(1);
	HStoreValue	*v = NULL;

	if (!HS_ISEMPTY(hs))
		v = findUncompressedHStoreValue(VARDATA(hs),
										HS_FLAG_HASH | HS_FLAG_ARRAY,
										NULL,
										VARDATA_ANY(key),
										VARSIZE_ANY_EXHDR(key));

	if (v && v->type == hsvBool)
		PG_RETURN_BOOL(v->boolean);

	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(hstore_fetchval_n);
Datum		hstore_fetchval_n(PG_FUNCTION_ARGS);
Datum
hstore_fetchval_n(PG_FUNCTION_ARGS)
{
	HStore	   	*hs = PG_GETARG_HS(0);
	int	   		i = PG_GETARG_INT32(1);
	HStoreValue	*v = NULL;
	text		*out;

	if (!HS_ISEMPTY(hs))
		v = getHStoreValue(VARDATA(hs), HS_FLAG_HASH | HS_FLAG_ARRAY, i);

	if ((out = HStoreValueToText(v)) == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_TEXT_P(out);
}

PG_FUNCTION_INFO_V1(hstore_fetchval_n_numeric);
Datum		hstore_fetchval_n_numeric(PG_FUNCTION_ARGS);
Datum
hstore_fetchval_n_numeric(PG_FUNCTION_ARGS)
{
	HStore	   	*hs = PG_GETARG_HS(0);
	int	   		i = PG_GETARG_INT32(1);
	HStoreValue	*v = NULL;

	if (!HS_ISEMPTY(hs))
		v = getHStoreValue(VARDATA(hs), HS_FLAG_HASH | HS_FLAG_ARRAY, i);

	if (v && v->type == hsvNumeric)
	{
		Numeric		out = palloc(VARSIZE_ANY(v->numeric));

		memcpy(out, v->numeric, VARSIZE_ANY(v->numeric));
		PG_RETURN_NUMERIC(out);
	}

	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(hstore_fetchval_n_boolean);
Datum		hstore_fetchval_n_boolean(PG_FUNCTION_ARGS);
Datum
hstore_fetchval_n_boolean(PG_FUNCTION_ARGS)
{
	HStore	   	*hs = PG_GETARG_HS(0);
	int	   		i = PG_GETARG_INT32(1);
	HStoreValue	*v = NULL;

	if (!HS_ISEMPTY(hs))
		v = getHStoreValue(VARDATA(hs), HS_FLAG_HASH | HS_FLAG_ARRAY, i);

	if (v && v->type == hsvBool)
		PG_RETURN_BOOL(v->boolean);

	PG_RETURN_NULL();
}

static bool
h_atoi(char *c, int l, int *acc)
{
	bool	negative = false;
	char 	*p = c;

	*acc = 0;

	while(isspace(*p) && p - c < l)
		p++;

	if (p - c >= l)
		return false;

	if (*p == '-')
	{
		negative = true;
		p++;
	}
	else if (*p == '+')
	{
		p++;
	}

	if (p - c >= l)
		return false;


	while(p - c < l)
	{
		if (!isdigit(*p))
			return false;

		*acc *= 10;
		*acc += (*p - '0');
		p++;
	}

	if (negative)
		*acc = - *acc;

	return true;
}

static HStoreValue*
hstoreDeepFetch(HStore *in, ArrayType *path)
{
	HStoreValue			*v = NULL;
	static HStoreValue 	init /* could be returned */;
	Datum				*path_elems;
	bool				*path_nulls;
	int					path_len, i;

	Assert(ARR_ELEMTYPE(path) == TEXTOID);

	if (ARR_NDIM(path) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (HS_ROOT_COUNT(in) == 0)
		return NULL;

	deconstruct_array(path, TEXTOID, -1, false, 'i',
					  &path_elems, &path_nulls, &path_len);

	init.type = hsvBinary;
	init.size = VARSIZE(in);
	init.binary.data = VARDATA(in);
	init.binary.len = VARSIZE_ANY_EXHDR(in);

	v = &init;

	if (path_len == 0)
		return v;

	for(i=0; v != NULL && i<path_len; i++)
	{
		uint32	header;

		if (v->type != hsvBinary || path_nulls[i])
			return NULL;

		header = *(uint32*)v->binary.data;

		if (header & HS_FLAG_HASH)
		{
			v = findUncompressedHStoreValue(v->binary.data, HS_FLAG_HASH,
											NULL,
											VARDATA_ANY(path_elems[i]),
											VARSIZE_ANY_EXHDR(path_elems[i]));
		}
		else if (header & HS_FLAG_ARRAY)
		{
			int ith;

			if (h_atoi(VARDATA_ANY(path_elems[i]),
					   VARSIZE_ANY_EXHDR(path_elems[i]), &ith) == false)
				return NULL;

			if (ith < 0)
			{
				if (-ith > (int)(header & HS_COUNT_MASK))
					return NULL;
				else
					ith = ((int)(header & HS_COUNT_MASK)) + ith;
			}
			else
			{
				if (ith >= (int)(header & HS_COUNT_MASK))
					return NULL;
			}

			v = getHStoreValue(v->binary.data, HS_FLAG_ARRAY, ith);
		}
		else
		{
			elog(PANIC,"wrong header type: %08x", header);
		}
	}

	return v;
}

PG_FUNCTION_INFO_V1(hstore_fetchval_path);
Datum		hstore_fetchval_path(PG_FUNCTION_ARGS);
Datum
hstore_fetchval_path(PG_FUNCTION_ARGS)
{
	HStore	   	*hs = PG_GETARG_HS(0);
	ArrayType	*path = PG_GETARG_ARRAYTYPE_P(1);
	text		*out;

	if ((out = HStoreValueToText(hstoreDeepFetch(hs, path))) == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_TEXT_P(out);
}

PG_FUNCTION_INFO_V1(hstore_fetchval_path_numeric);
Datum		hstore_fetchval_path_numeric(PG_FUNCTION_ARGS);
Datum
hstore_fetchval_path_numeric(PG_FUNCTION_ARGS)
{
	HStore	   	*hs = PG_GETARG_HS(0);
	ArrayType	*path = PG_GETARG_ARRAYTYPE_P(1);
	HStoreValue	*v = NULL;

	if (!HS_ISEMPTY(hs))
		v = hstoreDeepFetch(hs, path);

	if (v && v->type == hsvNumeric)
	{
		Numeric		out = palloc(VARSIZE_ANY(v->numeric));

		memcpy(out, v->numeric, VARSIZE_ANY(v->numeric));
		PG_RETURN_NUMERIC(out);
	}

	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(hstore_fetchval_path_boolean);
Datum		hstore_fetchval_path_boolean(PG_FUNCTION_ARGS);
Datum
hstore_fetchval_path_boolean(PG_FUNCTION_ARGS)
{
	HStore	   	*hs = PG_GETARG_HS(0);
	ArrayType	*path = PG_GETARG_ARRAYTYPE_P(1);
	HStoreValue	*v = NULL;

	if (!HS_ISEMPTY(hs))
		v = hstoreDeepFetch(hs, path);

	if (v && v->type == hsvBool)
		PG_RETURN_BOOL(v->boolean);

	PG_RETURN_NULL();
}

static HStore *
HStoreValueToHStore(HStoreValue *v)
{
	HStore			*out;

	if (v == NULL || v->type == hsvNull)
	{
		out = NULL;
	}
	else if (v->type == hsvString || v->type == hsvBool ||
			 v->type == hsvNumeric)
	{
		ToHStoreState	*state = NULL;
		HStoreValue		*res;
		int				r;
		HStoreValue		scalarArray;

		scalarArray.type = hsvArray;
		scalarArray.array.scalar = true;
		scalarArray.array.nelems = 1;

		pushHStoreValue(&state, WHS_BEGIN_ARRAY, &scalarArray);
		pushHStoreValue(&state, WHS_ELEM, v);
		res = pushHStoreValue(&state, WHS_END_ARRAY, NULL);

		out = palloc(VARHDRSZ + res->size);
		SET_VARSIZE(out, VARHDRSZ + res->size);
		r = compressHStore(res, VARDATA(out));
		Assert(r <= res->size);
		SET_VARSIZE(out, r + VARHDRSZ);
	}
	else
	{
		out = palloc(VARHDRSZ + v->size);

		Assert(v->type == hsvBinary);
		SET_VARSIZE(out, VARHDRSZ + v->binary.len);
		memcpy(VARDATA(out), v->binary.data, v->binary.len);
	}

	return out;
}

PG_FUNCTION_INFO_V1(hstore_fetchval_hstore);
Datum		hstore_fetchval_hstore(PG_FUNCTION_ARGS);
Datum
hstore_fetchval_hstore(PG_FUNCTION_ARGS)
{
	HStore	   	*hs = PG_GETARG_HS(0);
	text	   	*key = PG_GETARG_TEXT_PP(1);
	HStoreValue	*v = NULL;
	HStore		*out;

	if (!HS_ISEMPTY(hs))
		v = findUncompressedHStoreValue(VARDATA(hs),
										HS_FLAG_HASH | HS_FLAG_ARRAY,
										NULL,
										VARDATA_ANY(key),
										VARSIZE_ANY_EXHDR(key));

	if ((out = HStoreValueToHStore(v)) == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_fetchval_n_hstore);
Datum		hstore_fetchval_n_hstore(PG_FUNCTION_ARGS);
Datum
hstore_fetchval_n_hstore(PG_FUNCTION_ARGS)
{
	HStore	   	*hs = PG_GETARG_HS(0);
	int	   		i = PG_GETARG_INT32(1);
	HStoreValue	*v = NULL;
	HStore		*out;

	if (!HS_ISEMPTY(hs))
		v = getHStoreValue(VARDATA(hs), HS_FLAG_HASH | HS_FLAG_ARRAY, i);

	if ((out = HStoreValueToHStore(v)) == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_fetchval_path_hstore);
Datum		hstore_fetchval_path_hstore(PG_FUNCTION_ARGS);
Datum
hstore_fetchval_path_hstore(PG_FUNCTION_ARGS)
{
	HStore	   	*hs = PG_GETARG_HS(0);
	ArrayType	*path = PG_GETARG_ARRAYTYPE_P(1);
	HStore		*out;

	if ((out = HStoreValueToHStore(hstoreDeepFetch(hs, path))) == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_exists);
Datum		hstore_exists(PG_FUNCTION_ARGS);
Datum
hstore_exists(PG_FUNCTION_ARGS)
{
	HStore	   	*hs = PG_GETARG_HS(0);
	text		*key = PG_GETARG_TEXT_PP(1);
	HStoreValue	*v = NULL;

	if (!HS_ISEMPTY(hs))
		v = findUncompressedHStoreValue(VARDATA(hs),
										HS_FLAG_HASH | HS_FLAG_ARRAY,
										NULL,
										VARDATA_ANY(key),
										VARSIZE_ANY_EXHDR(key));

	PG_RETURN_BOOL(v != NULL);
}


PG_FUNCTION_INFO_V1(hstore_exists_idx);
Datum		hstore_exists_idx(PG_FUNCTION_ARGS);
Datum
hstore_exists_idx(PG_FUNCTION_ARGS)
{
	HStore	   	*hs = PG_GETARG_HS(0);
	int			ith = PG_GETARG_INT32(1);
	HStoreValue	*v = NULL;

	if (!HS_ISEMPTY(hs))
		v = getHStoreValue(VARDATA(hs), HS_FLAG_HASH | HS_FLAG_ARRAY, ith);

	PG_RETURN_BOOL(v != NULL);
}

PG_FUNCTION_INFO_V1(hstore_exists_path);
Datum		hstore_exists_path(PG_FUNCTION_ARGS);
Datum
hstore_exists_path(PG_FUNCTION_ARGS)
{
	HStore	   	*hs = PG_GETARG_HS(0);
	ArrayType	*path = PG_GETARG_ARRAYTYPE_P(1);

	PG_RETURN_BOOL(hstoreDeepFetch(hs, path) != NULL);
}



PG_FUNCTION_INFO_V1(hstore_exists_any);
Datum		hstore_exists_any(PG_FUNCTION_ARGS);
Datum
hstore_exists_any(PG_FUNCTION_ARGS)
{
	HStore		   	*hs = PG_GETARG_HS(0);
	ArrayType	  	*keys = PG_GETARG_ARRAYTYPE_P(1);
	HStoreValue		*v = arrayToHStoreSortedArray(keys);
	int				i;
	uint32			*plowbound = NULL, lowbound = 0;
	bool			res = false;

	if (HS_ISEMPTY(hs) || v == NULL || v->hash.npairs == 0)
		PG_RETURN_BOOL(false);

	if (HS_ROOT_IS_HASH(hs))
		plowbound = &lowbound;
	/*
	 * we exploit the fact that the pairs list is already sorted into strictly
	 * increasing order to narrow the findUncompressedHStoreValue search; each search can
	 * start one entry past the previous "found" entry, or at the lower bound
	 * of the last search.
	 */
	for (i = 0; i < v->array.nelems; i++)
	{
		if (findUncompressedHStoreValueByValue(VARDATA(hs), HS_FLAG_HASH | HS_FLAG_ARRAY, plowbound,
											   v->array.elems + i) != NULL)
		{
			res = true;
			break;
		}
	}

	PG_RETURN_BOOL(res);
}


PG_FUNCTION_INFO_V1(hstore_exists_all);
Datum		hstore_exists_all(PG_FUNCTION_ARGS);
Datum
hstore_exists_all(PG_FUNCTION_ARGS)
{
	HStore		   	*hs = PG_GETARG_HS(0);
	ArrayType	  	*keys = PG_GETARG_ARRAYTYPE_P(1);
	HStoreValue		*v = arrayToHStoreSortedArray(keys);
	int				i;
	uint32			*plowbound = NULL, lowbound = 0;
	bool			res = true;

	if (HS_ISEMPTY(hs) || v == NULL || v->array.nelems == 0)
	{

		if (v == NULL || v->array.nelems == 0)
			PG_RETURN_BOOL(true); /* compatibility */
		else
			PG_RETURN_BOOL(false);
	}

	if (HS_ROOT_IS_HASH(hs))
		plowbound = &lowbound;
	/*
	 * we exploit the fact that the pairs list is already sorted into strictly
	 * increasing order to narrow the findUncompressedHStoreValue search;
	 * each search can start one entry past the previous "found" entry,
	 * or at the lower bound of the last search.
	 */
	for (i = 0; i < v->array.nelems; i++)
	{
		if (findUncompressedHStoreValueByValue(VARDATA(hs),
											   HS_FLAG_HASH | HS_FLAG_ARRAY,
											   plowbound,
											   v->array.elems + i) == NULL)
		{
			res = false;
			break;
		}
	}

	PG_RETURN_BOOL(res);
}


PG_FUNCTION_INFO_V1(hstore_defined);
Datum		hstore_defined(PG_FUNCTION_ARGS);
Datum
hstore_defined(PG_FUNCTION_ARGS)
{
	HStore	   	*hs = PG_GETARG_HS(0);
	text		*key = PG_GETARG_TEXT_PP(1);
	HStoreValue	*v = NULL;

	if (!HS_ISEMPTY(hs))
		v = findUncompressedHStoreValue(VARDATA(hs),
										HS_FLAG_HASH | HS_FLAG_ARRAY,
										NULL,
										VARDATA_ANY(key),
										VARSIZE_ANY_EXHDR(key));

	PG_RETURN_BOOL(!(v == NULL || v->type == hsvNull));
}


PG_FUNCTION_INFO_V1(hstore_delete);
Datum		hstore_delete(PG_FUNCTION_ARGS);
Datum
hstore_delete(PG_FUNCTION_ARGS)
{
	HStore	   		*in = PG_GETARG_HS(0);
	text	   		*key = PG_GETARG_TEXT_PP(1);
	char	   		*keyptr = VARDATA_ANY(key);
	int				keylen = VARSIZE_ANY_EXHDR(key);
	HStore	   		*out = palloc(VARSIZE(in));
	ToHStoreState	*toState = NULL;
	HStoreIterator	*it;
	uint32			r;
	HStoreValue		v, *res = NULL;
	bool			skipNested = false;

	SET_VARSIZE(out, VARSIZE(in));

	if (HS_ISEMPTY(in))
		PG_RETURN_POINTER(out);

	it = HStoreIteratorInit(VARDATA(in));

	while((r = HStoreIteratorGet(&it, &v, skipNested)) != 0)
	{
		skipNested = true;

		if ((r == WHS_ELEM || r == WHS_KEY) &&
			(v.type == hsvString && keylen == v.string.len &&
			 memcmp(keyptr, v.string.val, keylen) == 0))
		{
			if (r == WHS_KEY)
				/* skip corresponding value */
				HStoreIteratorGet(&it, &v, true);

			continue;
		}

		res = pushHStoreValue(&toState, r, &v);
	}

	if (res == NULL || (res->type == hsvArray && res->array.nelems == 0) ||
					   (res->type == hsvHash && res->hash.npairs == 0) )
	{
		SET_VARSIZE(out, VARHDRSZ);
	}
	else
	{
		r = compressHStore(res, VARDATA(out));
		SET_VARSIZE(out, r + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_delete_array);
Datum		hstore_delete_array(PG_FUNCTION_ARGS);
Datum
hstore_delete_array(PG_FUNCTION_ARGS)
{
	HStore	   		*in = PG_GETARG_HS(0);
	HStore	   		*out = palloc(VARSIZE(in));
	HStoreValue 	*a = arrayToHStoreSortedArray(PG_GETARG_ARRAYTYPE_P(1));
	HStoreIterator	*it;
	ToHStoreState	*toState = NULL;
	uint32			r, i = 0;
	HStoreValue		v, *res = NULL;
	bool			skipNested = false;
	bool			isHash = false;


	if (HS_ISEMPTY(in) || a == NULL || a->array.nelems == 0)
	{
		memcpy(out, in, VARSIZE(in));
		PG_RETURN_POINTER(out);
	}

	it = HStoreIteratorInit(VARDATA(in));

	while((r = HStoreIteratorGet(&it, &v, skipNested)) != 0)
	{

		if (skipNested == false)
		{
			Assert(v.type == hsvArray || v.type == hsvHash);
			isHash = (v.type == hsvArray) ? false : true;
			skipNested = true;
		}

		if ((r == WHS_ELEM || r == WHS_KEY) && v.type == hsvString &&
			i < a->array.nelems)
		{
			int diff;

			if (isHash)
			{
				do {
					diff = compareHStoreStringValue(&v, a->array.elems + i,
													NULL);

					if (diff >= 0)
						i++;
				} while(diff > 0 && i < a->array.nelems);
			}
			else
			{
				diff = (findInHStoreSortedArray(a, NULL,
												v.string.val,
												v.string.len) == NULL) ? 1 : 0;
			}

			if (diff == 0)
			{
				if (r == WHS_KEY)
					/* skip corresponding value */
					HStoreIteratorGet(&it, &v, true);

				continue;
			}
		}

		res = pushHStoreValue(&toState, r, &v);
	}

	if (res == NULL || (res->type == hsvArray && res->array.nelems == 0) ||
					   (res->type == hsvHash && res->hash.npairs == 0) )
	{
		SET_VARSIZE(out, VARHDRSZ);
	}
	else
	{
		r = compressHStore(res, VARDATA(out));
		SET_VARSIZE(out, r + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}


PG_FUNCTION_INFO_V1(hstore_delete_hstore);
Datum		hstore_delete_hstore(PG_FUNCTION_ARGS);
Datum
hstore_delete_hstore(PG_FUNCTION_ARGS)
{
	HStore	   		*hs1 = PG_GETARG_HS(0);
	HStore	   		*hs2 = PG_GETARG_HS(1);
	HStore	   		*out = palloc(VARSIZE(hs1));
	HStoreIterator	*it1, *it2;
	ToHStoreState	*toState = NULL;
	uint32			r1, r2;
	HStoreValue		v1, v2, *res = NULL;
	bool			isHash1, isHash2;

	if (HS_ISEMPTY(hs1) || HS_ISEMPTY(hs2))
	{
		memcpy(out, hs1, VARSIZE(hs1));
		PG_RETURN_POINTER(out);
	}

	it1 = HStoreIteratorInit(VARDATA(hs1));
	r1 = HStoreIteratorGet(&it1, &v1, false);
	isHash1 = (v1.type == hsvArray) ? false : true;

	it2 = HStoreIteratorInit(VARDATA(hs2));
	r2 = HStoreIteratorGet(&it2, &v2, false);
	isHash2 = (v2.type == hsvArray) ? false : true;

	res = pushHStoreValue(&toState, r1, &v1);

	if (isHash1 == true && isHash2 == true)
	{
		bool			fin2 = false,
						keyIsDef = false;

		while((r1 = HStoreIteratorGet(&it1, &v1, true)) != 0)
		{
			if (r1 == WHS_KEY && fin2 == false)
			{
				int diff  = 1;

				if (keyIsDef)
					r2 = WHS_KEY;

				while(keyIsDef ||
					  (r2 = HStoreIteratorGet(&it2, &v2, true)) != 0)
				{
					if (r2 != WHS_KEY)
						continue;

					diff = compareHStoreStringValue(&v1, &v2, NULL);

					if (diff > 0 && keyIsDef)
						keyIsDef = false;
					if (diff <= 0)
						break;
				}

				if (r2 == 0)
				{
					fin2 = true;
				}
				else if (diff == 0)
				{
					HStoreValue		vk;

					keyIsDef = false;

					r1 = HStoreIteratorGet(&it1, &vk, true);
					r2 = HStoreIteratorGet(&it2, &v2, true);

					Assert(r1 == WHS_VALUE && r2 == WHS_VALUE);

					if (compareHStoreValue(&vk, &v2) != 0)
					{
						res = pushHStoreValue(&toState, WHS_KEY, &v1);
						res = pushHStoreValue(&toState, WHS_VALUE, &vk);
					}

					continue;
				}
				else
				{
					keyIsDef = true;
				}
			}

			res = pushHStoreValue(&toState, r1, &v1);
		}
	}
	else
	{
		while((r1 = HStoreIteratorGet(&it1, &v1, true)) != 0)
		{

			if (r1 == WHS_ELEM || r1 == WHS_KEY)
			{
				int diff = 1;

				it2 = HStoreIteratorInit(VARDATA(hs2));

				r2 = HStoreIteratorGet(&it2, &v2, false);

				while(diff && (r2 = HStoreIteratorGet(&it2, &v2, true)) != 0)
				{
					if (r2 == WHS_KEY || r2 == WHS_VALUE || r2 == WHS_ELEM)
						diff = compareHStoreValue(&v1, &v2);
				}

				if (diff == 0)
				{
					if (r1 == WHS_KEY)
						HStoreIteratorGet(&it1, &v1, true);
					continue;
				}
			}

			res = pushHStoreValue(&toState, r1, &v1);
		}
	}

	if (res == NULL || (res->type == hsvArray && res->array.nelems == 0) ||
					   (res->type == hsvHash && res->hash.npairs == 0) )
	{
		SET_VARSIZE(out, VARHDRSZ);
	}
	else
	{
		int r = compressHStore(res, VARDATA(out));
		SET_VARSIZE(out, r + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}

static HStoreValue*
deletePathDo(HStoreIterator **it, Datum	*path_elems,
			 bool *path_nulls, int path_len,
			 ToHStoreState	**st, int level)
{
	HStoreValue	v, *res = NULL;
	int			r;

	r = HStoreIteratorGet(it, &v, false);

	if (r == WHS_BEGIN_ARRAY)
	{
		int 	skipIdx, i;
		uint32	n = v.array.nelems;

		skipIdx = n;
		if (level >= path_len || path_nulls[level] ||
			h_atoi(VARDATA_ANY(path_elems[level]),
				   VARSIZE_ANY_EXHDR(path_elems[level]), &skipIdx) == false)
		{
			skipIdx = n;
		}
		else if (skipIdx < 0)
		{
			if (-skipIdx > n)
				skipIdx = n;
			else
				skipIdx = n + skipIdx;
		}

		if (skipIdx > n)
			skipIdx = n;

		if (skipIdx == 0 && n == 1)
		{
			r = HStoreIteratorGet(it, &v, true);
			Assert(r == WHS_ELEM);
			r = HStoreIteratorGet(it, &v, true);
			Assert(r == WHS_END_ARRAY);
			return NULL;
		}

		pushHStoreValue(st, r, &v);

		for(i=0; i<skipIdx; i++) {
			r = HStoreIteratorGet(it, &v, true);
			Assert(r == WHS_ELEM);
			res = pushHStoreValue(st, r, &v);
		}

		if (level >= path_len || skipIdx == n) {
			r = HStoreIteratorGet(it, &v, true);
			Assert(r == WHS_END_ARRAY);
			res = pushHStoreValue(st, r, &v);
			return res;
		}

		if (level == path_len - 1)
		{
			/* last level in path, skip all elem */
			r = HStoreIteratorGet(it, &v, true);
			Assert(r == WHS_ELEM);
		}
		else
		{
			res = deletePathDo(it, path_elems, path_nulls, path_len, st,
							   level + 1);
		}

		for(i = skipIdx + 1; i<n; i++) {
			r = HStoreIteratorGet(it, &v, true);
			Assert(r == WHS_ELEM);
			res = pushHStoreValue(st, r, &v);
		}

		r = HStoreIteratorGet(it, &v, true);
		Assert(r == WHS_END_ARRAY);
		res = pushHStoreValue(st, r, &v);
	}
	else if (r == WHS_BEGIN_HASH)
	{
		int			i;
		uint32		n = v.hash.npairs;
		HStoreValue	k;
		bool		done = false;

		if (n == 1 && level == path_len - 1)
		{
			r = HStoreIteratorGet(it, &k, false);
			Assert(r == WHS_KEY);

			if ( path_nulls[level] == false &&
				 k.string.len == VARSIZE_ANY_EXHDR(path_elems[level]) &&
				 memcmp(k.string.val, VARDATA_ANY(path_elems[level]),
						k.string.len) == 0)
			{
				r = HStoreIteratorGet(it, &v, true);
				Assert(r == WHS_VALUE);
				r = HStoreIteratorGet(it, &v, true);
				Assert(r == WHS_END_HASH);
				return NULL;
			}

			pushHStoreValue(st, WHS_BEGIN_HASH, &v);
			pushHStoreValue(st, WHS_KEY, &k);
			r = HStoreIteratorGet(it, &v, true);
			Assert(r == WHS_VALUE);
			pushHStoreValue(st, r, &v);
			r = HStoreIteratorGet(it, &v, true);
			Assert(r == WHS_END_HASH);
			return pushHStoreValue(st, r, &v);
		}

		pushHStoreValue(st, WHS_BEGIN_HASH, &v);

		if (level >= path_len || path_nulls[level])
			done = true;

		for(i=0; i<n; i++)
		{
			r = HStoreIteratorGet(it, &k, false);
			Assert(r == WHS_KEY);

			if (done == false &&
				k.string.len == VARSIZE_ANY_EXHDR(path_elems[level]) &&
				memcmp(k.string.val, VARDATA_ANY(path_elems[level]),
					   k.string.len) == 0)
			{
				done = true;

				if (level == path_len - 1)
				{
					r = HStoreIteratorGet(it, &v, true);
					Assert(r == WHS_VALUE);
				}
				else
				{
					pushHStoreValue(st, r, &k);
					res = deletePathDo(it, path_elems, path_nulls, path_len,
									   st, level + 1);
					if (res == NULL)
					{
						v.type = hsvNull;
						pushHStoreValue(st, WHS_VALUE, &v);
					}
				}

				continue;
			}

			pushHStoreValue(st, r, &k);
			r = HStoreIteratorGet(it, &v, true);
			Assert(r == WHS_VALUE);
			pushHStoreValue(st, r, &v);
		}

		r = HStoreIteratorGet(it, &v, true);
		Assert(r == WHS_END_HASH);
		res = pushHStoreValue(st, r, &v);
	}
	else if (r == WHS_ELEM || r == WHS_VALUE) /* just a string or null */
	{
		pushHStoreValue(st, r, &v);
		res = (void*)0x01; /* dummy value */
	}
	else
	{
		elog(PANIC, "impossible state");
	}

	return res;
}


PG_FUNCTION_INFO_V1(hstore_delete_path);
Datum		hstore_delete_path(PG_FUNCTION_ARGS);
Datum
hstore_delete_path(PG_FUNCTION_ARGS)
{
	HStore	   		*in = PG_GETARG_HS(0);
	HStore			*out = palloc(VARSIZE(in));
	ArrayType		*path = PG_GETARG_ARRAYTYPE_P(1);
	HStoreValue		*res = NULL;
	Datum			*path_elems;
	bool			*path_nulls;
	int				path_len;
	HStoreIterator	*it;
	ToHStoreState	*st = NULL;

	Assert(ARR_ELEMTYPE(path) == TEXTOID);

	if (ARR_NDIM(path) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (HS_ROOT_COUNT(in) == 0)
	{
		memcpy(out, in, VARSIZE(in));
		PG_RETURN_POINTER(out);
	}

	deconstruct_array(path, TEXTOID, -1, false, 'i',
					  &path_elems, &path_nulls, &path_len);

	if (path_len == 0)
	{
		memcpy(out, in, VARSIZE(in));
		PG_RETURN_POINTER(out);
	}

	it = HStoreIteratorInit(VARDATA(in));

	res = deletePathDo(&it, path_elems, path_nulls, path_len, &st, 0);

	if (res == NULL)
	{
		SET_VARSIZE(out, VARHDRSZ);
	}
	else
	{
		int				sz;

		sz = compressHStore(res, VARDATA(out));
		SET_VARSIZE(out, sz + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_delete_idx);
Datum		hstore_delete_idx(PG_FUNCTION_ARGS);
Datum
hstore_delete_idx(PG_FUNCTION_ARGS)
{
	HStore	   		*in = PG_GETARG_HS(0);
	int	   			idx = PG_GETARG_INT32(1);
	HStore	   		*out = palloc(VARSIZE(in));
	ToHStoreState	*toState = NULL;
	HStoreIterator	*it;
	uint32			r, i = 0, n;
	HStoreValue		v, *res = NULL;

	if (HS_ISEMPTY(in))
	{
		memcpy(out, in, VARSIZE(in));
		PG_RETURN_POINTER(out);
	}

	it = HStoreIteratorInit(VARDATA(in));

	r = HStoreIteratorGet(&it, &v, false);
	if (r == WHS_BEGIN_ARRAY)
		n = v.array.nelems;
	else
		n = v.hash.npairs;

	if (idx < 0)
	{
		if (-idx > n)
			idx = n;
		else
			idx = n + idx;
	}

	if (idx >= n)
	{
		memcpy(out, in, VARSIZE(in));
		PG_RETURN_POINTER(out);
	}

	pushHStoreValue(&toState, r, &v);

	while((r = HStoreIteratorGet(&it, &v, true)) != 0)
	{
		if (r == WHS_ELEM || r == WHS_KEY)
		{
			if (i++ == idx)
			{
				if (r == WHS_KEY)
					HStoreIteratorGet(&it, &v, true); /* skip value */
				continue;
			}
		}

		res = pushHStoreValue(&toState, r, &v);
	}

	if (res == NULL || (res->type == hsvArray && res->array.nelems == 0) ||
					   (res->type == hsvHash && res->hash.npairs == 0) )
	{
		SET_VARSIZE(out, VARHDRSZ);
	}
	else
	{
		r = compressHStore(res, VARDATA(out));
		SET_VARSIZE(out, r + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}

static void
convertScalarToString(HStoreValue *v)
{
	switch(v->type) {
		case hsvNull:
			elog(ERROR, "key in hstore type could not be a NULL");
			break;
		case hsvBool:
			v->type = hsvString;
			v->string.val = pnstrdup((v->boolean) ? "t" : "f", 1);
			v->string.len = 1;
			v->size = sizeof(HEntry) + v->string.len;
			break;
		case hsvNumeric:
			v->type = hsvString;
			v->string.val = DatumGetCString(
							DirectFunctionCall1(numeric_out,
												PointerGetDatum(v->numeric)));
			v->string.len = strlen(v->string.val);
			v->size = sizeof(HEntry) + v->string.len;
			break;
		case hsvString:
			break;
		default:
			elog(PANIC,"Could not convert to string");
	}
}

static HStoreValue *
IteratorConcat(HStoreIterator **it1, HStoreIterator **it2,
			   ToHStoreState **toState)
{
	uint32			r1, r2, rk1, rk2;
	HStoreValue		v1, v2, *res = NULL;

	r1 = rk1 = HStoreIteratorGet(it1, &v1, false);
	r2 = rk2 = HStoreIteratorGet(it2, &v2, false);

	if (rk1 == WHS_BEGIN_HASH && rk2 == WHS_BEGIN_HASH)
	{
		bool			fin2 = false,
						keyIsDef = false;

		res = pushHStoreValue(toState, r1, &v1);

		for(;;)
		{
			r1 = HStoreIteratorGet(it1, &v1, true);

			Assert(r1 == WHS_KEY || r1 == WHS_VALUE || r1 == WHS_END_HASH);

			if (r1 == WHS_KEY && fin2 == false)
			{
				int diff  = 1;

				if (keyIsDef)
					r2 = WHS_KEY;

				while(keyIsDef || (r2 = HStoreIteratorGet(it2, &v2, true)) != 0)
				{
					if (r2 != WHS_KEY)
						continue;

					diff = compareHStoreStringValue(&v1, &v2, NULL);

					if (diff > 0)
					{
						if (keyIsDef)
							keyIsDef = false;

						pushHStoreValue(toState, r2, &v2);
						r2 = HStoreIteratorGet(it2, &v2, true);
						Assert(r2 == WHS_VALUE);
						pushHStoreValue(toState, r2, &v2);
					}
					else if (diff <= 0)
					{
						break;
					}
				}

				if (r2 == 0)
				{
					fin2 = true;
				}
				else if (diff == 0)
				{
					keyIsDef = false;

					pushHStoreValue(toState, r1, &v1);

					r1 = HStoreIteratorGet(it1, &v1, true); /* ignore */
					r2 = HStoreIteratorGet(it2, &v2, true); /* new val */

					Assert(r1 == WHS_VALUE && r2 == WHS_VALUE);
					pushHStoreValue(toState, r2, &v2);

					continue;
				}
				else
				{
					keyIsDef = true;
				}
			}
			else if (r1 == WHS_END_HASH)
			{
				if (r2 != 0)
				{
					if (keyIsDef)
						r2 = WHS_KEY;

					while(keyIsDef ||
						  (r2 = HStoreIteratorGet(it2, &v2, true)) != 0)
					{
						if (r2 != WHS_KEY)
							continue;

						pushHStoreValue(toState, r2, &v2);
						r2 = HStoreIteratorGet(it2, &v2, true);
						Assert(r2 == WHS_VALUE);
						pushHStoreValue(toState, r2, &v2);
						keyIsDef = false;
					}
				}

				res = pushHStoreValue(toState, r1, &v1);
				break;
			}

			res = pushHStoreValue(toState, r1, &v1);
		}
	}
	else if ((rk1 == WHS_BEGIN_HASH || rk1 == WHS_BEGIN_ARRAY) &&
			 (rk2 == WHS_BEGIN_HASH || rk2 == WHS_BEGIN_ARRAY))
	{
		if (rk1 == WHS_BEGIN_HASH && rk2 == WHS_BEGIN_ARRAY &&
			v2.array.nelems % 2 != 0)
			elog(ERROR, "hstore's array must have even number of elements");

		res = pushHStoreValue(toState, r1, &v1);

		for(;;)
		{
			r1 = HStoreIteratorGet(it1, &v1, true);
			if (r1 == WHS_END_HASH || r1 == WHS_END_ARRAY)
				break;
			Assert(r1 == WHS_KEY || r1 == WHS_VALUE || r1 == WHS_ELEM);
			pushHStoreValue(toState, r1, &v1);
		}

		while((r2 = HStoreIteratorGet(it2, &v2, true)) != 0)
		{
			if (!(r2 == WHS_END_HASH || r2 == WHS_END_ARRAY))
			{
				if (rk1 == WHS_BEGIN_HASH)
				{
					convertScalarToString(&v2);
					pushHStoreValue(toState, WHS_KEY, &v2);
					r2 = HStoreIteratorGet(it2, &v2, true);
					Assert(r2 == WHS_ELEM);
					pushHStoreValue(toState, WHS_VALUE, &v2);
				}
				else
				{
					pushHStoreValue(toState, WHS_ELEM, &v2);
				}
			}
		}

		res = pushHStoreValue(toState,
							  (rk1 == WHS_BEGIN_HASH) ? WHS_END_HASH : WHS_END_ARRAY,
							  NULL/* signal to sort */);
	}
	else if ((rk1 & (WHS_VALUE | WHS_ELEM)) != 0)
	{
		if (v2.type == hsvArray && v2.array.scalar)
		{
			Assert(v2.array.nelems == 1);
			r2 = HStoreIteratorGet(it2, &v2, false);
			pushHStoreValue(toState, r1, &v2);
		}
		else
		{
			res = pushHStoreValue(toState, r2, &v2);
			while((r2 = HStoreIteratorGet(it2, &v2, true)) != 0)
				res = pushHStoreValue(toState, r2, &v2);
		}
	}
	else
	{
		elog(ERROR, "invalid concatnation of hstores");
	}

	return res;
}

PG_FUNCTION_INFO_V1(hstore_concat);
Datum		hstore_concat(PG_FUNCTION_ARGS);
Datum
hstore_concat(PG_FUNCTION_ARGS)
{
	HStore	   		*hs1 = PG_GETARG_HS(0);
	HStore	   		*hs2 = PG_GETARG_HS(1);
	HStore	   		*out = palloc(VARSIZE(hs1) + VARSIZE(hs2));
	ToHStoreState	*toState = NULL;
	HStoreValue		*res;
	HStoreIterator	*it1, *it2;

	if (HS_ISEMPTY(hs1))
	{
		memcpy(out, hs2, VARSIZE(hs2));
		PG_RETURN_POINTER(out);
	}
	else if (HS_ISEMPTY(hs2))
	{
		memcpy(out, hs1, VARSIZE(hs1));
		PG_RETURN_POINTER(out);
	}

	it1 = HStoreIteratorInit(VARDATA(hs1));
	it2 = HStoreIteratorInit(VARDATA(hs2));

	res = IteratorConcat(&it1, &it2, &toState);

	if (res == NULL || (res->type == hsvArray && res->array.nelems == 0) ||
					   (res->type == hsvHash && res->hash.npairs == 0) )
	{
		SET_VARSIZE(out, VARHDRSZ);
	}
	else
	{
		uint32 r;

		if (res->type == hsvArray && res->array.nelems > 1)
			res->array.scalar = false;

		r = compressHStore(res, VARDATA(out));
		SET_VARSIZE(out, r + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}


PG_FUNCTION_INFO_V1(hstore_slice_to_array);
Datum		hstore_slice_to_array(PG_FUNCTION_ARGS);
Datum
hstore_slice_to_array(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HS(0);
	ArrayType  *key_array = PG_GETARG_ARRAYTYPE_P(1);
	ArrayType  *aout;
	Datum	   *key_datums;
	bool	   *key_nulls;
	Datum	   *out_datums;
	bool	   *out_nulls;
	int			key_count;
	int			i;

	deconstruct_array(key_array,
					  TEXTOID, -1, false, 'i',
					  &key_datums, &key_nulls, &key_count);

	if (key_count == 0 || HS_ISEMPTY(hs))
	{
		aout = construct_empty_array(TEXTOID);
		PG_RETURN_POINTER(aout);
	}

	out_datums = palloc(sizeof(Datum) * key_count);
	out_nulls = palloc(sizeof(bool) * key_count);

	for (i = 0; i < key_count; ++i)
	{
		text	   *key = (text *) DatumGetPointer(key_datums[i]);
		HStoreValue	*v = NULL;

		if (key_nulls[i] == false)
			v = findUncompressedHStoreValue(VARDATA(hs),
											HS_FLAG_HASH | HS_FLAG_ARRAY,
											NULL,
											VARDATA(key),
											VARSIZE(key) - VARHDRSZ);

		out_datums[i] = PointerGetDatum(HStoreValueToText(v));
		out_nulls[i] = (DatumGetPointer(out_datums[i]) == NULL) ? true : false;
	}

	aout = construct_md_array(out_datums, out_nulls,
							  ARR_NDIM(key_array),
							  ARR_DIMS(key_array),
							  ARR_LBOUND(key_array),
							  TEXTOID, -1, false, 'i');

	PG_RETURN_POINTER(aout);
}


PG_FUNCTION_INFO_V1(hstore_slice_to_hstore);
Datum		hstore_slice_to_hstore(PG_FUNCTION_ARGS);
Datum
hstore_slice_to_hstore(PG_FUNCTION_ARGS)
{
	HStore		   *hs = PG_GETARG_HS(0);
	HStoreValue	   *a = arrayToHStoreSortedArray(PG_GETARG_ARRAYTYPE_P(1));
	uint32			lowbound = 0,
				   *plowbound;
	HStoreValue		*res = NULL;
	ToHStoreState	*state = NULL;
	text			*out;
	uint32			i;

	out = palloc(VARSIZE(hs));

	if (a == NULL || a->array.nelems == 0 || HS_ISEMPTY(hs))
	{
		memcpy(out, hs, VARSIZE(hs));
		PG_RETURN_POINTER(out);
	}

	if (HS_ROOT_IS_HASH(hs))
	{
		plowbound = &lowbound;
		pushHStoreValue(&state, WHS_BEGIN_HASH, NULL);
	}
	else
	{
		plowbound = NULL;
		pushHStoreValue(&state, WHS_BEGIN_ARRAY, NULL);
	}

	for (i = 0; i < a->array.nelems; ++i)
	{
		HStoreValue	*v = findUncompressedHStoreValueByValue(VARDATA(hs),
															HS_FLAG_HASH | HS_FLAG_ARRAY,
															plowbound,
															a->array.elems + i);

		if (v)
		{
			if (plowbound)
			{
				pushHStoreValue(&state, WHS_KEY, a->array.elems + i);
				pushHStoreValue(&state, WHS_VALUE, v);
			}
			else
			{
				pushHStoreValue(&state, WHS_ELEM, v);
			}
		}
	}

	if (plowbound)
		res = pushHStoreValue(&state, WHS_END_HASH, a /* any non-null value */);
	else
		res = pushHStoreValue(&state, WHS_END_ARRAY, NULL);


	if (res == NULL || (res->type == hsvArray && res->array.nelems == 0) ||
						(res->type == hsvHash && res->hash.npairs == 0) )
	{
		SET_VARSIZE(out, VARHDRSZ);
	}
	else
	{
		int r = compressHStore(res, VARDATA(out));
		SET_VARSIZE(out, r + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}

static HStoreValue*
replacePathDo(HStoreIterator **it, Datum *path_elems,
			  bool *path_nulls, int path_len,
			  ToHStoreState  **st, int level, HStoreValue *newval)
{
	HStoreValue v, *res = NULL;
	int			r;

	r = HStoreIteratorGet(it, &v, false);

	if (r == WHS_BEGIN_ARRAY)
	{
		int		idx, i;
		uint32	n = v.array.nelems;

		idx = n;
		if (level >= path_len || path_nulls[level] ||
			h_atoi(VARDATA_ANY(path_elems[level]),
				   VARSIZE_ANY_EXHDR(path_elems[level]), &idx) == false)
		{
			idx = n;
		}
		else if (idx < 0)
		{
			if (-idx > n)
				idx = n;
			else
				idx = n + idx;
		}

		if (idx > n)
			idx = n;

		pushHStoreValue(st, r, &v);

		for(i=0; i<n; i++)
		{
			if (i == idx && level < path_len)
			{
				if (level == path_len - 1)
				{
					r = HStoreIteratorGet(it, &v, true); /* skip */
					Assert(r == WHS_ELEM);
					res = pushHStoreValue(st, r, newval);
				}
				else
				{
					res = replacePathDo(it, path_elems, path_nulls, path_len,
										st, level + 1, newval);
				}
			}
			else
			{
				r = HStoreIteratorGet(it, &v, true);
				Assert(r == WHS_ELEM);
				res = pushHStoreValue(st, r, &v);
			}
		}

		r = HStoreIteratorGet(it, &v, true);
		Assert(r == WHS_END_ARRAY);
		res = pushHStoreValue(st, r, &v);
	}
	else if (r == WHS_BEGIN_HASH)
	{
		int			i;
		uint32		n = v.hash.npairs;
		HStoreValue	k;
		bool		done = false;

		pushHStoreValue(st, WHS_BEGIN_HASH, &v);

		if (level >= path_len || path_nulls[level])
			done = true;

		for(i=0; i<n; i++)
		{
			r = HStoreIteratorGet(it, &k, false);
			Assert(r == WHS_KEY);
			res = pushHStoreValue(st, r, &k);

			if (done == false &&
				k.string.len == VARSIZE_ANY_EXHDR(path_elems[level]) &&
				memcmp(k.string.val, VARDATA_ANY(path_elems[level]),
					   k.string.len) == 0)
			{
				if (level == path_len - 1)
				{
					r = HStoreIteratorGet(it, &v, true); /* skip */
					Assert(r == WHS_VALUE);
					res = pushHStoreValue(st, r, newval);
				}
				else
				{
					res = replacePathDo(it, path_elems, path_nulls, path_len,
										st, level + 1, newval);
				}
			}
			else
			{
				r = HStoreIteratorGet(it, &v, true);
				Assert(r == WHS_VALUE);
				res = pushHStoreValue(st, r, &v);
			}
		}

		r = HStoreIteratorGet(it, &v, true);
		Assert(r == WHS_END_HASH);
		res = pushHStoreValue(st, r, &v);
	}
	else if (r == WHS_ELEM || r == WHS_VALUE)
	{
		pushHStoreValue(st, r, &v);
		res = (void*)0x01; /* dummy value */
	}
	else
	{
		elog(PANIC, "impossible state");
	}

	return res;
}

PG_FUNCTION_INFO_V1(hstore_replace);
Datum		hstore_replace(PG_FUNCTION_ARGS);
Datum
hstore_replace(PG_FUNCTION_ARGS)
{
	HStore	   		*in = PG_GETARG_HS(0);
	ArrayType		*path = PG_GETARG_ARRAYTYPE_P(1);
	HStore	   		*newval = PG_GETARG_HS(2);
	HStore			*out = palloc(VARSIZE(in) + VARSIZE(newval));
	HStoreValue		*res = NULL;
	HStoreValue		value;
	Datum			*path_elems;
	bool			*path_nulls;
	int				path_len;
	HStoreIterator	*it;
	ToHStoreState	*st = NULL;

	Assert(ARR_ELEMTYPE(path) == TEXTOID);

	if (ARR_NDIM(path) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (HS_ROOT_COUNT(in) == 0)
	{
		memcpy(out, in, VARSIZE(in));
		PG_RETURN_POINTER(out);
	}

	deconstruct_array(path, TEXTOID, -1, false, 'i',
					  &path_elems, &path_nulls, &path_len);

	if (path_len == 0)
	{
		memcpy(out, in, VARSIZE(in));
		PG_RETURN_POINTER(out);
	}

	if (HS_ROOT_COUNT(newval) == 0)
	{
		value.type = hsvNull;
		value.size = sizeof(HEntry);
	}
	else
	{
		value.type = hsvBinary;
		value.binary.data = VARDATA(newval);
		value.binary.len = VARSIZE_ANY_EXHDR(newval);
		value.size = value.binary.len + sizeof(HEntry);
	}

	it = HStoreIteratorInit(VARDATA(in));

	res = replacePathDo(&it, path_elems, path_nulls, path_len, &st, 0, &value);

	if (res == NULL)
	{
		SET_VARSIZE(out, VARHDRSZ);
	}
	else
	{
		int				sz;

		sz = compressHStore(res, VARDATA(out));
		SET_VARSIZE(out, sz + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}

static HStoreValue*
concatPathDo(HStoreIterator **it, Datum *path_elems,
			 bool *path_nulls, int path_len,
			 ToHStoreState  **st, int level, HStoreIterator	*toConcat)
{
	HStoreValue v, *res = NULL;
	int			r;

	r = HStoreIteratorGet(it, &v, false);

	if (r == WHS_BEGIN_ARRAY)
	{
		int		idx, i;
		uint32	n = v.array.nelems;

		idx = n;
		if (level >= path_len || path_nulls[level] ||
			h_atoi(VARDATA_ANY(path_elems[level]),
				   VARSIZE_ANY_EXHDR(path_elems[level]), &idx) == false)
		{
			idx = n;
		}
		else if (idx < 0)
		{
			if (-idx > n)
				idx = n;
			else
				idx = n + idx;
		}

		if (idx > n)
			idx = n;

		pushHStoreValue(st, r, &v);

		for(i=0; i<n; i++)
		{
			if (i == idx && level < path_len)
			{
				if (level == path_len - 1)
					res = IteratorConcat(it, &toConcat, st);
				else
					res = concatPathDo(it, path_elems, path_nulls, path_len,
									   st, level + 1, toConcat);
			}
			else
			{
				r = HStoreIteratorGet(it, &v, true);
				Assert(r == WHS_ELEM);
				res = pushHStoreValue(st, r, &v);
			}
		}

		r = HStoreIteratorGet(it, &v, true);
		Assert(r == WHS_END_ARRAY);
		res = pushHStoreValue(st, r, &v);
	}
	else if (r == WHS_BEGIN_HASH)
	{
		int			i;
		uint32		n = v.hash.npairs;
		HStoreValue	k;
		bool		done = false;

		pushHStoreValue(st, WHS_BEGIN_HASH, &v);

		if (level >= path_len || path_nulls[level])
			done = true;

		for(i=0; i<n; i++)
		{
			r = HStoreIteratorGet(it, &k, false);
			Assert(r == WHS_KEY);
			res = pushHStoreValue(st, r, &k);

			if (done == false && level < path_len &&
				k.string.len == VARSIZE_ANY_EXHDR(path_elems[level]) &&
				memcmp(k.string.val, VARDATA_ANY(path_elems[level]),
					   k.string.len) == 0)
			{
				if (level == path_len - 1)
					res = IteratorConcat(it, &toConcat, st);
				else
					res = concatPathDo(it, path_elems, path_nulls, path_len,
									   st, level + 1, toConcat);
			}
			else
			{
				r = HStoreIteratorGet(it, &v, true);
				Assert(r == WHS_VALUE);
				res = pushHStoreValue(st, r, &v);
			}
		}

		r = HStoreIteratorGet(it, &v, true);
		Assert(r == WHS_END_HASH);
		res = pushHStoreValue(st, r, &v);
	}
	else if (r == WHS_ELEM || r == WHS_VALUE)
	{
		pushHStoreValue(st, r, &v);
		res = (void*)0x01; /* dummy value */
	}
	else
	{
		elog(PANIC, "impossible state");
	}

	return res;
}

PG_FUNCTION_INFO_V1(hstore_deep_concat);
Datum		hstore_deep_concat(PG_FUNCTION_ARGS);
Datum
hstore_deep_concat(PG_FUNCTION_ARGS)
{
	HStore	   		*in = PG_GETARG_HS(0);
	ArrayType		*path = PG_GETARG_ARRAYTYPE_P(1);
	HStore	   		*newval = PG_GETARG_HS(2);
	HStore			*out = palloc(VARSIZE(in) + VARSIZE(newval));
	HStoreValue		*res = NULL;
	Datum			*path_elems;
	bool			*path_nulls;
	int				path_len;
	HStoreIterator	*it1, *it2;
	ToHStoreState	*st = NULL;

	Assert(ARR_ELEMTYPE(path) == TEXTOID);

	if (ARR_NDIM(path) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (HS_ROOT_COUNT(in) == 0 || HS_ROOT_COUNT(newval) == 0)
	{
		memcpy(out, in, VARSIZE(in));
		PG_RETURN_POINTER(out);
	}

	deconstruct_array(path, TEXTOID, -1, false, 'i',
					  &path_elems, &path_nulls, &path_len);

	it1 = HStoreIteratorInit(VARDATA(in));
	it2 = HStoreIteratorInit(VARDATA(newval));

	if (path_len == 0)
		res = IteratorConcat(&it1, &it2, &st);
	else
		res = concatPathDo(&it1, path_elems, path_nulls, path_len, &st, 0, it2);

	if (res == NULL)
	{
		SET_VARSIZE(out, VARHDRSZ);
	}
	else
	{
		int				sz;

		if (res->type == hsvArray && res->array.nelems > 1)
			res->array.scalar = false;

		sz = compressHStore(res, VARDATA(out));
		SET_VARSIZE(out, sz + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_akeys);
Datum		hstore_akeys(PG_FUNCTION_ARGS);
Datum
hstore_akeys(PG_FUNCTION_ARGS)
{
	HStore	   		*hs = PG_GETARG_HS(0);
	Datum	   		*d;
	ArrayType  		*a;
	int				i = 0, r = 0;
	HStoreIterator	*it;
	HStoreValue		v;
	bool			skipNested = false;

	if (HS_ISEMPTY(hs))
	{
		a = construct_empty_array(TEXTOID);
		PG_RETURN_POINTER(a);
	}

	d = (Datum *) palloc(sizeof(Datum) * HS_ROOT_COUNT(hs));

	it = HStoreIteratorInit(VARDATA(hs));

	while((r = HStoreIteratorGet(&it, &v, skipNested)) != 0)
	{
		skipNested = true;

		if ((r == WHS_ELEM && v.type != hsvNull) || r == WHS_KEY)
			d[i++] = PointerGetDatum(HStoreValueToText(&v));
	}

	a = construct_array(d, i,
						TEXTOID, -1, false, 'i');

	PG_RETURN_POINTER(a);
}


PG_FUNCTION_INFO_V1(hstore_avals);
Datum		hstore_avals(PG_FUNCTION_ARGS);
Datum
hstore_avals(PG_FUNCTION_ARGS)
{
	HStore	   		*hs = PG_GETARG_HS(0);
	Datum	   		*d;
	ArrayType  		*a;
	int				i = 0, r = 0;
	HStoreIterator	*it;
	HStoreValue		v;
	bool			skipNested = false;
	bool		   *nulls;
	int				lb = 1;

	if (HS_ISEMPTY(hs))
	{
		a = construct_empty_array(TEXTOID);
		PG_RETURN_POINTER(a);
	}

	d = (Datum *) palloc(sizeof(Datum) * HS_ROOT_COUNT(hs));
	nulls = (bool *) palloc(sizeof(bool) * HS_ROOT_COUNT(hs));

	it = HStoreIteratorInit(VARDATA(hs));

	while((r = HStoreIteratorGet(&it, &v, skipNested)) != 0)
	{
		skipNested = true;

		if (r == WHS_ELEM || r == WHS_VALUE)
		{
			d[i] = PointerGetDatum(HStoreValueToText(&v));
			nulls[i] = (DatumGetPointer(d[i]) == NULL) ? true : false;
			i++;
		}
	}

	a = construct_md_array(d, nulls, 1, &i, &lb,
						   TEXTOID, -1, false, 'i');

	PG_RETURN_POINTER(a);
}


static ArrayType *
hstore_to_array_internal(HStore *hs, int ndims)
{
	int				count = HS_ROOT_COUNT(hs);
	int				out_size[2] = {0, 2};
	int				lb[2] = {1, 1};
	Datum		   *out_datums;
	bool	   		*out_nulls;
	bool			isHash = HS_ROOT_IS_HASH(hs) ? true : false;
	int				i = 0, r = 0;
	HStoreIterator	*it;
	HStoreValue		v;
	bool			skipNested = false;

	Assert(ndims < 3);

	if (count == 0 || ndims == 0)
		return construct_empty_array(TEXTOID);

	if (isHash == false && ndims == 2 && count % 2 != 0)
		elog(ERROR, "hstore's array should have even number of elements");

	out_size[0] = count * (isHash ? 2 : 1) / ndims;
	out_datums = palloc(sizeof(Datum) * count * 2);
	out_nulls = palloc(sizeof(bool) * count * 2);

	it = HStoreIteratorInit(VARDATA(hs));

	while((r = HStoreIteratorGet(&it, &v, skipNested)) != 0)
	{
		skipNested = true;

		switch(r)
		{
			case WHS_ELEM:
				out_datums[i] = PointerGetDatum(HStoreValueToText(&v));
				out_nulls[i] = (DatumGetPointer(out_datums[i]) == NULL) ? true : false;
				i++;
				break;
			case WHS_KEY:
				out_datums[i * 2] = PointerGetDatum(HStoreValueToText(&v));
				out_nulls[i * 2] = (DatumGetPointer(out_datums[i * 2]) == NULL) ? true : false;
				break;
			case WHS_VALUE:
				out_datums[i * 2 + 1] = PointerGetDatum(HStoreValueToText(&v));
				out_nulls[i * 2 + 1] = (DatumGetPointer(out_datums[i * 2 + 1]) == NULL) ? true : false;
				i++;
				break;
			default:
				break;
		}
	}

	return construct_md_array(out_datums, out_nulls,
							  ndims, out_size, lb,
							  TEXTOID, -1, false, 'i');
}

PG_FUNCTION_INFO_V1(hstore_to_array);
Datum		hstore_to_array(PG_FUNCTION_ARGS);
Datum
hstore_to_array(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HS(0);
	ArrayType  *out = hstore_to_array_internal(hs, 1);

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_to_matrix);
Datum		hstore_to_matrix(PG_FUNCTION_ARGS);
Datum
hstore_to_matrix(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HS(0);
	ArrayType  *out = hstore_to_array_internal(hs, 2);

	PG_RETURN_POINTER(out);
}

/*
 * Common initialization function for the various set-returning
 * funcs. fcinfo is only passed if the function is to return a
 * composite; it will be used to look up the return tupledesc.
 * we stash a copy of the hstore in the multi-call context in
 * case it was originally toasted. (At least I assume that's why;
 * there was no explanatory comment in the original code. --AG)
 */

typedef struct SetReturningState
{
	HStore			*hs;
	HStoreIterator	*it;
	MemoryContext	ctx;

	HStoreValue		init;
	int				path_len;
	int				level;
	struct {
		HStoreValue		v;
		Datum           varStr;
		int				varInt;
		enum {
			pathStr,
			pathInt,
			pathAny
		} 				varKind;
		int				i;
	}				*path;
} SetReturningState;

static SetReturningState*
setup_firstcall(FuncCallContext *funcctx, HStore *hs, ArrayType *path,
				FunctionCallInfoData *fcinfo)
{
	MemoryContext 			oldcontext;
	SetReturningState	   *st;

	oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

	st = palloc(sizeof(*st));

	st->ctx = funcctx->multi_call_memory_ctx;

	st->hs = (HStore *) palloc(VARSIZE(hs));
	memcpy(st->hs, hs, VARSIZE(hs));
	if (HS_ISEMPTY(hs) || path)
		st->it = NULL;
	else
		st->it = HStoreIteratorInit(VARDATA(st->hs));

	funcctx->user_fctx = (void *) st;

	if (fcinfo)
	{
		TupleDesc	tupdesc;

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
	}

	st->path_len = st->level = 0;
	if (path)
	{
		Datum		*path_elems;
		bool		*path_nulls;
		int			i;

		Assert(ARR_ELEMTYPE(path) == TEXTOID);
		if (ARR_NDIM(path) > 1)
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("wrong number of array subscripts")));

		deconstruct_array(path, TEXTOID, -1, false, 'i',
						  &path_elems, &path_nulls, &st->path_len);

		st->init.type = hsvBinary;
		st->init.size = VARSIZE(st->hs);
		st->init.binary.data = VARDATA(st->hs);
		st->init.binary.len = VARSIZE_ANY_EXHDR(st->hs);

		if (st->path_len > 0)
		{
			st->path = palloc(sizeof(*st->path) * st->path_len);
			st->path[0].v = st->init;
		}

		for(i=0; i<st->path_len; i++)
		{
			st->path[i].varStr = path_elems[i];
			st->path[i].i = 0;

			if (path_nulls[i])
				st->path[i].varKind = pathAny;
			else if (h_atoi(VARDATA_ANY(path_elems[i]),
							VARSIZE_ANY_EXHDR(path_elems[i]),
							&st->path[i].varInt))
				st->path[i].varKind = pathInt;
			else
				st->path[i].varKind = pathStr;
		}
	}

	MemoryContextSwitchTo(oldcontext);

	return st;
}

static uint32
HStoreIteratorGetCtx(SetReturningState *st, HStoreValue *v, bool skipNested)
{
	int 			r;
	MemoryContext	oldctx;

	oldctx = MemoryContextSwitchTo(st->ctx);
	r = HStoreIteratorGet(&st->it, v, skipNested);
	MemoryContextSwitchTo(oldctx);

	return r;
}

PG_FUNCTION_INFO_V1(hstore_skeys);
Datum		hstore_skeys(PG_FUNCTION_ARGS);
Datum
hstore_skeys(PG_FUNCTION_ARGS)
{
	FuncCallContext 	*funcctx;
	SetReturningState	*st;
	int					r;
	HStoreValue			v;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		st = setup_firstcall(funcctx, PG_GETARG_HS(0), NULL, NULL);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (SetReturningState *) funcctx->user_fctx;

	while(st->it && (r = HStoreIteratorGetCtx(st, &v, true)) != 0)
	{
		if (r == WHS_KEY || r == WHS_ELEM)
		{
			text	   *item = HStoreValueToText(&v);

			if (item == NULL)
				SRF_RETURN_NEXT_NULL(funcctx);
			else
				SRF_RETURN_NEXT(funcctx, PointerGetDatum(item));
		}
	}

	SRF_RETURN_DONE(funcctx);
}

PG_FUNCTION_INFO_V1(hstore_svals);
Datum		hstore_svals(PG_FUNCTION_ARGS);
Datum
hstore_svals(PG_FUNCTION_ARGS)
{
	FuncCallContext 	*funcctx;
	SetReturningState	*st;
	int					r;
	HStoreValue			v;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		st = setup_firstcall(funcctx, PG_GETARG_HS(0), NULL, NULL);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (SetReturningState *) funcctx->user_fctx;

	while(st->it && (r = HStoreIteratorGetCtx(st, &v, true)) != 0)
	{
		if (r == WHS_VALUE || r == WHS_ELEM)
		{
			text	   *item = HStoreValueToText(&v);

			if (item == NULL)
				SRF_RETURN_NEXT_NULL(funcctx);
			else
				SRF_RETURN_NEXT(funcctx, PointerGetDatum(item));
		}
	}

	SRF_RETURN_DONE(funcctx);
}

PG_FUNCTION_INFO_V1(hstore_hvals);
Datum		hstore_hvals(PG_FUNCTION_ARGS);
Datum
hstore_hvals(PG_FUNCTION_ARGS)
{
	FuncCallContext 	*funcctx;
	SetReturningState	*st;
	int					r;
	HStoreValue			v;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		st = setup_firstcall(funcctx, PG_GETARG_HS(0), NULL, NULL);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (SetReturningState *) funcctx->user_fctx;

	while(st->it && (r = HStoreIteratorGetCtx(st, &v, true)) != 0)
	{
		if (r == WHS_VALUE || r == WHS_ELEM)
		{
			HStore	   *item = HStoreValueToHStore(&v);

			if (item == NULL)
				SRF_RETURN_NEXT_NULL(funcctx);
			else
				SRF_RETURN_NEXT(funcctx, PointerGetDatum(item));
		}
	}

	SRF_RETURN_DONE(funcctx);
}

static HStoreValue*
getNextValsPath(SetReturningState *st)
{
	HStoreValue 		*v = NULL;

	if (st->path_len == 0)
	{
		/* empty path */
		if (st->level == 0)
		{
			v = &st->init;
			st->level ++;
		}

		return v;
	}

	while(st->level >= 0)
	{
		uint32	header;

		v = NULL;
		if (st->path[st->level].v.type != hsvBinary)
		{
			st->level--;
			continue;
		}

		header = *(uint32*)st->path[st->level].v.binary.data;

		if (header & HS_FLAG_HASH)
		{
			if (st->path[st->level].varKind == pathAny)
			{
				v = getHStoreValue(st->path[st->level].v.binary.data, 
								   HS_FLAG_HASH, 
								   st->path[st->level].i++);
			}
			else
			{
				v = findUncompressedHStoreValue(st->path[st->level].v.binary.data, 
												HS_FLAG_HASH, NULL, 
												VARDATA_ANY(st->path[st->level].varStr),
												VARSIZE_ANY_EXHDR(st->path[st->level].varStr));
			}
		}
		else if (header & HS_FLAG_ARRAY)
		{
			if (st->path[st->level].varKind == pathAny)
			{
				v = getHStoreValue(st->path[st->level].v.binary.data,
								   HS_FLAG_ARRAY, st->path[st->level].i++);
			}
			else if (st->path[st->level].varKind == pathInt)
			{
				int	ith = st->path[st->level].varInt;

				if (ith < 0)
				{
					if (-ith > (int)(header & HS_COUNT_MASK))
					{
						st->level--;
						continue;
					}
					else
					{
						ith = ((int)(header & HS_COUNT_MASK)) + ith;
					}
				}
				else
				{
					if (ith >= (int)(header & HS_COUNT_MASK))
					{
						st->level--;
						continue;
					}
				}

				v = getHStoreValue(st->path[st->level].v.binary.data,
								   HS_FLAG_ARRAY, ith);
			}
			else
			{
				st->level--;
				continue;
			}
		}
		else
		{
			elog(PANIC, "impossible state");
		}

		if (v == NULL)
		{
			st->level--;
		}
		else if (st->level == st->path_len - 1)
		{
			if (st->path[st->level].varKind != pathAny)
			{
				st->path[st->level].v.type = hsvNull;
				st->level--;
			}
			break;
		}
		else
		{
			if (st->path[st->level].varKind != pathAny)
				st->path[st->level].v.type = hsvNull;
			st->level++;
			st->path[st->level].v = *v;
			st->path[st->level].i = 0;
		}
	}

	return v;
}

PG_FUNCTION_INFO_V1(hstore_svals_path);
Datum		hstore_svals_path(PG_FUNCTION_ARGS);
Datum
hstore_svals_path(PG_FUNCTION_ARGS)
{
	FuncCallContext 	*funcctx;
	SetReturningState	*st;
	HStoreValue			*v;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		st = setup_firstcall(funcctx, PG_GETARG_HS(0), PG_GETARG_ARRAYTYPE_P(1), NULL);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (SetReturningState *) funcctx->user_fctx;

	if ((v = getNextValsPath(st)) != NULL)
	{
		text	*item = HStoreValueToText(v);

		if (item == NULL)
			SRF_RETURN_NEXT_NULL(funcctx);
		else
			SRF_RETURN_NEXT(funcctx, PointerGetDatum(item));
	}

	SRF_RETURN_DONE(funcctx);
}

PG_FUNCTION_INFO_V1(hstore_hvals_path);
Datum		hstore_hvals_path(PG_FUNCTION_ARGS);
Datum
hstore_hvals_path(PG_FUNCTION_ARGS)
{
	FuncCallContext 	*funcctx;
	SetReturningState	*st;
	HStoreValue 		*v;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		st = setup_firstcall(funcctx, PG_GETARG_HS(0),
							 PG_GETARG_ARRAYTYPE_P(1), NULL);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (SetReturningState *) funcctx->user_fctx;

	if ((v = getNextValsPath(st)) != NULL)
	{
		HStore	   *item = HStoreValueToHStore(v);

		if (item == NULL)
			SRF_RETURN_NEXT_NULL(funcctx);
		else
			SRF_RETURN_NEXT(funcctx, PointerGetDatum(item));
	}

	SRF_RETURN_DONE(funcctx);
}

PG_FUNCTION_INFO_V1(hstore_each);
Datum		hstore_each(PG_FUNCTION_ARGS);
Datum
hstore_each(PG_FUNCTION_ARGS)
{
	FuncCallContext 	*funcctx;
	SetReturningState	*st;
	int					r;
	HStoreValue			v;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		st = setup_firstcall(funcctx, PG_GETARG_HS(0), NULL, fcinfo);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (SetReturningState *) funcctx->user_fctx;

	while(st->it && (r = HStoreIteratorGetCtx(st, &v, true)) != 0)
	{
		Datum		res,
					dvalues[2] = {0, 0};
		bool		nulls[2] = {false, false};
		text	   *item;
		HeapTuple	tuple;

		if (r == WHS_ELEM)
		{
			nulls[0] = true;

			item = HStoreValueToText(&v);
			if (item == NULL)
				nulls[1] = true;
			else
				dvalues[1] = PointerGetDatum(item);
		}
		else if (r == WHS_KEY)
		{
			item = HStoreValueToText(&v);
			dvalues[0] = PointerGetDatum(item);

			r = HStoreIteratorGetCtx(st, &v, true);
			Assert(r == WHS_VALUE);
			item = HStoreValueToText(&v);
			if (item == NULL)
				nulls[1] = true;
			else
				dvalues[1] = PointerGetDatum(item);
		}
		else
		{
			continue;
		}

		tuple = heap_form_tuple(funcctx->tuple_desc, dvalues, nulls);
		res = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, PointerGetDatum(res));
	}

	SRF_RETURN_DONE(funcctx);
}

PG_FUNCTION_INFO_V1(hstore_each_hstore);
Datum		hstore_each_hstore(PG_FUNCTION_ARGS);
Datum
hstore_each_hstore(PG_FUNCTION_ARGS)
{
	FuncCallContext 	*funcctx;
	SetReturningState	*st;
	int					r;
	HStoreValue			v;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		st = setup_firstcall(funcctx, PG_GETARG_HS(0), NULL, fcinfo);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (SetReturningState *) funcctx->user_fctx;

	while(st->it && (r = HStoreIteratorGetCtx(st, &v, true)) != 0)
	{
		Datum		res,
					dvalues[2] = {0, 0};
		bool		nulls[2] = {false, false};
		text	   *item;
		HStore		*hitem;
		HeapTuple	tuple;

		if (r == WHS_ELEM)
		{
			nulls[0] = true;

			hitem = HStoreValueToHStore(&v);
			if (hitem == NULL)
				nulls[1] = true;
			else
				dvalues[1] = PointerGetDatum(hitem);
		}
		else if (r == WHS_KEY)
		{
			item = HStoreValueToText(&v);
			dvalues[0] = PointerGetDatum(item);

			r = HStoreIteratorGetCtx(st, &v, true);
			Assert(r == WHS_VALUE);
			hitem = HStoreValueToHStore(&v);
			if (hitem == NULL)
				nulls[1] = true;
			else
				dvalues[1] = PointerGetDatum(hitem);
		}
		else
		{
			continue;
		}

		tuple = heap_form_tuple(funcctx->tuple_desc, dvalues, nulls);
		res = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, PointerGetDatum(res));
	}

	SRF_RETURN_DONE(funcctx);
}

static bool
deepContains(HStoreIterator **it1, HStoreIterator **it2)
{
	uint32			r1, r2;
	HStoreValue		v1, v2;
	bool			res = true;

	r1 = HStoreIteratorGet(it1, &v1, false);
	r2 = HStoreIteratorGet(it2, &v2, false);

	if (r1 != r2)
	{
		res = false;
	}
	else if (r1 == WHS_BEGIN_HASH)
	{
		uint32		lowbound = 0;
		HStoreValue	*v;

		for(;;) {
			r2 = HStoreIteratorGet(it2, &v2, false);
			if (r2 == WHS_END_HASH)
				break;

			Assert(r2 == WHS_KEY);

			v = findUncompressedHStoreValueByValue((*it1)->buffer,
												   HS_FLAG_HASH,
												   &lowbound, &v2);

			if (v == NULL)
			{
				res = false;
				break;
			}

			r2 = HStoreIteratorGet(it2, &v2, true);
			Assert(r2 == WHS_VALUE);

			if (v->type != v2.type)
			{
				res = false;
				break;
			}
			else if (v->type == hsvString || v->type == hsvNull ||
					 v->type == hsvBool || v->type == hsvNumeric)
			{
				if (compareHStoreValue(v, &v2) != 0)
				{
					res = false;
					break;
				}
			}
			else
			{
				HStoreIterator	*it1a, *it2a;

				Assert(v2.type == hsvBinary);
				Assert(v->type == hsvBinary);

				it1a = HStoreIteratorInit(v->binary.data);
				it2a = HStoreIteratorInit(v2.binary.data);

				if ((res = deepContains(&it1a, &it2a)) == false)
					break;
			}
		}
	}
	else if (r1 == WHS_BEGIN_ARRAY)
	{
		HStoreValue		*v;
		HStoreValue		*av = NULL;
		uint32			nelems = v1.array.nelems;

		for(;;) {
			r2 = HStoreIteratorGet(it2, &v2, true);
			if (r2 == WHS_END_ARRAY)
				break;

			Assert(r2 == WHS_ELEM);

			if (v2.type == hsvString || v2.type == hsvNull ||
				v2.type == hsvBool || v2.type == hsvNumeric)
			{
				v = findUncompressedHStoreValueByValue((*it1)->buffer,
													   HS_FLAG_ARRAY, NULL,
													   &v2);
				if (v == NULL)
				{
					res = false;
					break;
				}
			}
			else
			{
				uint32 			i;

				if (av == NULL)
				{
					uint32 			j = 0;

					av = palloc(sizeof(*av) * nelems);

					for(i=0; i<nelems; i++)
					{
						r2 = HStoreIteratorGet(it1, &v1, true);
						Assert(r2 == WHS_ELEM);

						if (v1.type == hsvBinary)
							av[j++] = v1;
					}

					if (j == 0)
					{
						res = false;
						break;
					}

					nelems = j;
				}

				res = false;
				for(i = 0; res == false && i<nelems; i++)
				{
					HStoreIterator	*it1a, *it2a;

					it1a = HStoreIteratorInit(av[i].binary.data);
					it2a = HStoreIteratorInit(v2.binary.data);

					res = deepContains(&it1a, &it2a);
				}

				if (res == false)
					break;
			}
		}
	}
	else
	{
		elog(PANIC, "impossible state");
	}

	return res;
}

PG_FUNCTION_INFO_V1(hstore_contains);
Datum		hstore_contains(PG_FUNCTION_ARGS);
Datum
hstore_contains(PG_FUNCTION_ARGS)
{
	HStore	   		*val = PG_GETARG_HS(0);
	HStore	   		*tmpl = PG_GETARG_HS(1);
	bool			res = true;
	HStoreIterator	*it1, *it2;

	if (HS_ROOT_COUNT(val) < HS_ROOT_COUNT(tmpl) ||
		HS_ROOT_IS_HASH(val) != HS_ROOT_IS_HASH(tmpl))
		PG_RETURN_BOOL(false);

	it1 = HStoreIteratorInit(VARDATA(val));
	it2 = HStoreIteratorInit(VARDATA(tmpl));
	res = deepContains(&it1, &it2);

	PG_RETURN_BOOL(res);
}


PG_FUNCTION_INFO_V1(hstore_contained);
Datum		hstore_contained(PG_FUNCTION_ARGS);
Datum
hstore_contained(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(DirectFunctionCall2(hstore_contains,
										PG_GETARG_DATUM(1),
										PG_GETARG_DATUM(0)
										));
}

/*
 * btree sort order for hstores isn't intended to be useful; we really only
 * care about equality versus non-equality.  we compare the entire string
 * buffer first, then the entry pos array.
 */

PG_FUNCTION_INFO_V1(hstore_cmp);
Datum		hstore_cmp(PG_FUNCTION_ARGS);
Datum
hstore_cmp(PG_FUNCTION_ARGS)
{
	HStore	   		*hs1 = PG_GETARG_HS(0);
	HStore	   		*hs2 = PG_GETARG_HS(1);
	int				res;

	if (HS_ISEMPTY(hs1) || HS_ISEMPTY(hs2))
	{
		if (HS_ISEMPTY(hs1))
		{
			if (HS_ISEMPTY(hs2))
				res = 0;
			else
				res = -1;
		}
		else
		{
			res = 1;
		}
	}
	else
	{
		res = compareHStoreBinaryValue(VARDATA(hs1), VARDATA(hs2));
	}

	/*
	 * this is a btree support function; this is one of the few places where
	 * memory needs to be explicitly freed.
	 */
	PG_FREE_IF_COPY(hs1, 0);
	PG_FREE_IF_COPY(hs2, 1);
	PG_RETURN_INT32(res);
}


PG_FUNCTION_INFO_V1(hstore_eq);
Datum		hstore_eq(PG_FUNCTION_ARGS);
Datum
hstore_eq(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(hstore_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res == 0);
}

PG_FUNCTION_INFO_V1(hstore_ne);
Datum		hstore_ne(PG_FUNCTION_ARGS);
Datum
hstore_ne(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(hstore_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res != 0);
}

PG_FUNCTION_INFO_V1(hstore_gt);
Datum		hstore_gt(PG_FUNCTION_ARGS);
Datum
hstore_gt(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(hstore_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res > 0);
}

PG_FUNCTION_INFO_V1(hstore_ge);
Datum		hstore_ge(PG_FUNCTION_ARGS);
Datum
hstore_ge(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(hstore_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res >= 0);
}

PG_FUNCTION_INFO_V1(hstore_lt);
Datum		hstore_lt(PG_FUNCTION_ARGS);
Datum
hstore_lt(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(hstore_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res < 0);
}

PG_FUNCTION_INFO_V1(hstore_le);
Datum		hstore_le(PG_FUNCTION_ARGS);
Datum
hstore_le(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(hstore_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res <= 0);
}


PG_FUNCTION_INFO_V1(hstore_hash);
Datum		hstore_hash(PG_FUNCTION_ARGS);
Datum
hstore_hash(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HS(0);
	Datum		hval = hash_any((unsigned char *) VARDATA(hs),
								VARSIZE(hs) - VARHDRSZ);

	PG_FREE_IF_COPY(hs, 0);
	PG_RETURN_DATUM(hval);
}

PG_FUNCTION_INFO_V1(hstore_typeof);
Datum		hstore_typeof(PG_FUNCTION_ARGS);
Datum
hstore_typeof(PG_FUNCTION_ARGS)
{
	HStore	   		*hs = PG_GETARG_HS(0);
	HStoreIterator	*it;
	HStoreValue		v;
	uint32			r;

	if (HS_ISEMPTY(hs))
		PG_RETURN_NULL();

	it = HStoreIteratorInit(VARDATA(hs));
	r = HStoreIteratorGet(&it, &v, false);

	switch(r)
	{
		case WHS_BEGIN_ARRAY:
			if (v.array.scalar)
			{
				Assert(v.array.nelems == 1);
				r = HStoreIteratorGet(&it, &v, false);
				Assert(r == WHS_ELEM);

				switch(v.type)
				{
					case hsvNull:
						PG_RETURN_TEXT_P(cstring_to_text("null"));
					case hsvBool:
						PG_RETURN_TEXT_P(cstring_to_text("bool"));
					case hsvNumeric:
						PG_RETURN_TEXT_P(cstring_to_text("numeric"));
					case hsvString:
						PG_RETURN_TEXT_P(cstring_to_text("string"));
					default:
						elog(ERROR, "bogus hstore");
				}
			}
			else
			{
				PG_RETURN_TEXT_P(cstring_to_text("array"));
			}
		case WHS_BEGIN_HASH:
			PG_RETURN_TEXT_P(cstring_to_text("hash"));
		case 0:
			PG_RETURN_NULL();
		default:
			elog(ERROR, "bogus hstore");
	}

	PG_RETURN_NULL();
}

