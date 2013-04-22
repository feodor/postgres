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
				  compareHStoreStringValue, &hasNonUniq);

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
findInHStoreSortedArray(HStoreValue *a, uint32 *lowbound, char *key, uint32 keylen)
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
		v = findUncompressedHStoreValue(VARDATA(hs), HS_FLAG_HSTORE | HS_FLAG_ARRAY, 
										NULL, VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));

	if ((out = HStoreValueToText(v)) == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_TEXT_P(out);
}

PG_FUNCTION_INFO_V1(hstore_fetchval_n);
Datum		hstore_fetchval_n(PG_FUNCTION_ARGS);
Datum
hstore_fetchval_n(PG_FUNCTION_ARGS)
{
	HStore	   	*hs = PG_GETARG_HS(0);
	int	   		i = PG_GETARG_INT32(1);
	HStoreValue	*v;
	text		*out;

	if (!HS_ISEMPTY(hs))
		v = getHStoreValue(VARDATA(hs), HS_FLAG_HSTORE | HS_FLAG_ARRAY, i);

	if ((out = HStoreValueToText(v)) == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_TEXT_P(out);
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

		if (header & HS_FLAG_HSTORE)
		{
			v = findUncompressedHStoreValue(v->binary.data, HS_FLAG_HSTORE,
											NULL, VARDATA_ANY(path_elems[i]), VARSIZE_ANY_EXHDR(path_elems[i]));
		}
		else if (header & HS_FLAG_ARRAY)
		{
			int ith;

			if (h_atoi(VARDATA_ANY(path_elems[i]), VARSIZE_ANY_EXHDR(path_elems[i]), &ith) == false)
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

static HStore *
HStoreValueToHStore(HStoreValue *v)
{
	HStore			*out;

	if (v == NULL || v->type == hsvNullString)
	{
		out = NULL;
	}
	else if (v->type == hsvString)
	{
		ToHStoreState	*state = NULL;
		HStoreValue		*res;
		int				r;

		pushHStoreValue(&state, WHS_BEGIN_ARRAY, NULL);
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
		v = findUncompressedHStoreValue(VARDATA(hs), HS_FLAG_HSTORE | HS_FLAG_ARRAY, 
										NULL, VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));


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
		v = getHStoreValue(VARDATA(hs), HS_FLAG_HSTORE | HS_FLAG_ARRAY, i);

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
		v = findUncompressedHStoreValue(VARDATA(hs), HS_FLAG_HSTORE | HS_FLAG_ARRAY, 
										NULL, VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));

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
		v = getHStoreValue(VARDATA(hs), HS_FLAG_HSTORE | HS_FLAG_ARRAY, ith); 

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
		if (findUncompressedHStoreValue(VARDATA(hs), HS_FLAG_HSTORE | HS_FLAG_ARRAY, plowbound,
										v->array.elems[i].string.val, 
										v->array.elems[i].string.len) != NULL)
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
	 * increasing order to narrow the findUncompressedHStoreValue search; each search can
	 * start one entry past the previous "found" entry, or at the lower bound
	 * of the last search.
	 */
	for (i = 0; i < v->array.nelems; i++)
	{
		if (findUncompressedHStoreValue(VARDATA(hs), HS_FLAG_HSTORE | HS_FLAG_ARRAY, plowbound,
										v->array.elems[i].string.val, 
										v->array.elems[i].string.len) == NULL)
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
		v = findUncompressedHStoreValue(VARDATA(hs), HS_FLAG_HSTORE | HS_FLAG_ARRAY, 
										NULL, VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));

	PG_RETURN_BOOL(!(v == NULL || v->type == hsvNullString));
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
			(v.type == hsvString && keylen == v.string.len && memcmp(keyptr, v.string.val, keylen) == 0))
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

		if ((r == WHS_ELEM || r == WHS_KEY) && v.type == hsvString && i < a->array.nelems)
		{
			int diff;

			if (isHash)
			{
				do {
					diff = compareHStoreStringValue(&v, a->array.elems + i, NULL);

					if (diff >= 0)
						i++;
				} while(diff > 0 && i < a->array.nelems);
			}
			else
			{
				diff = (findInHStoreSortedArray(a, NULL, v.string.val, v.string.len) == NULL) ? 1 : 0;
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

				while(keyIsDef || (r2 = HStoreIteratorGet(&it2, &v2, true)) != 0)
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

/*
PG_FUNCTION_INFO_V1(hstore_delete_path);
Datum		hstore_delete_path(PG_FUNCTION_ARGS);
Datum
hstore_delete_path(PG_FUNCTION_ARGS)
{
	HStore	   	*in = PG_GETARG_HS(0);
	HStore		*out = palloc(sizeof(VARSIZE(in)));
	ArrayType	*path = PG_GETARG_ARRAYTYPE_P(1);
	HStoreValue	*v = NULL;
	Datum		*path_elems;
	bool		*path_nulls;
	int			path_len, i;

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

	for(i=0; v != NULL && i<path_len; i++)
	{
		uint32	header;

		if (v->type != hsvBinary || path_nulls[i])
			return NULL;

		header = *(uint32*)v->binary.data;

		if (header & HS_FLAG_HSTORE)
		{
			v = findUncompressedHStoreValue(v->binary.data, HS_FLAG_HSTORE,
											NULL, VARDATA_ANY(path_elems[i]), VARSIZE_ANY_EXHDR(path_elems[i]));
		}
		else if (header & HS_FLAG_ARRAY)
		{
			int ith;

			if (h_atoi(VARDATA_ANY(path_elems[i]), VARSIZE_ANY_EXHDR(path_elems[i]), &ith) == false)
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
}
*/

PG_FUNCTION_INFO_V1(hstore_concat);
Datum		hstore_concat(PG_FUNCTION_ARGS);
Datum
hstore_concat(PG_FUNCTION_ARGS)
{
	HStore	   		*hs1 = PG_GETARG_HS(0);
	HStore	   		*hs2 = PG_GETARG_HS(1);
	HStore	   		*out = palloc(VARSIZE(hs1) + VARSIZE(hs2));
	HStoreIterator	*it1, *it2;
	ToHStoreState	*toState = NULL;
	uint32			r1, r2;
	HStoreValue		v1, v2, *res = NULL;
	bool			isHash1, isHash2;

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

				while(keyIsDef || (r2 = HStoreIteratorGet(&it2, &v2, true)) != 0)
				{
					if (r2 != WHS_KEY)
						continue;

					diff = compareHStoreStringValue(&v1, &v2, NULL);

					if (diff > 0)
					{	
						if (keyIsDef)
							keyIsDef = false;

						pushHStoreValue(&toState, r2, &v2);
						r2 = HStoreIteratorGet(&it2, &v2, true);
						Assert(r2 == WHS_VALUE);
						pushHStoreValue(&toState, r2, &v2);
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

					pushHStoreValue(&toState, r1, &v1);

					r1 = HStoreIteratorGet(&it1, &v1, true); /* ignore */
					r2 = HStoreIteratorGet(&it2, &v2, true); /* new val */

					Assert(r1 == WHS_VALUE && r2 == WHS_VALUE);
					pushHStoreValue(&toState, r2, &v2);

					continue;
				}
				else
				{
					keyIsDef = true;
				}
			}
			else if (r1 == WHS_END_HASH && r2 != 0) 
			{
				if (keyIsDef)
					r2 = WHS_KEY;

				while(keyIsDef || (r2 = HStoreIteratorGet(&it2, &v2, true)) != 0)
				{
					if (r2 != WHS_KEY)
						continue;

					pushHStoreValue(&toState, r2, &v2);
					r2 = HStoreIteratorGet(&it2, &v2, true);
					Assert(r2 == WHS_VALUE);
					pushHStoreValue(&toState, r2, &v2);
					keyIsDef = false;
				}
			}

			res = pushHStoreValue(&toState, r1, &v1);
		}
	}
	else
	{
		if (isHash1 && v2.array.nelems % 2 != 0)
			elog(ERROR, "hstore's array must have even number of elements");

		while((r1 = HStoreIteratorGet(&it1, &v1, true)) != 0)
		{
			if (!(r1 == WHS_END_HASH || r1 == WHS_END_ARRAY))
				pushHStoreValue(&toState, r1, &v1);
		}

		while((r2 = HStoreIteratorGet(&it2, &v2, true)) != 0)
		{
			if (!(r2 == WHS_END_HASH || r2 == WHS_END_ARRAY))
			{
				if (isHash1)
				{
					pushHStoreValue(&toState, WHS_KEY, &v2);
					r2 = HStoreIteratorGet(&it2, &v2, true);
					Assert(r2 == WHS_ELEM);
					pushHStoreValue(&toState, WHS_VALUE, &v2);
				}
				else
				{
					pushHStoreValue(&toState, WHS_ELEM, &v2);
				}
			}
		}

		res = pushHStoreValue(&toState, isHash1 ? WHS_END_HASH : WHS_END_ARRAY, NULL/* signal to sort */);
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
			v = findUncompressedHStoreValue(VARDATA(hs), HS_FLAG_HSTORE | HS_FLAG_ARRAY, NULL,
											VARDATA(key), VARSIZE(key) - VARHDRSZ);

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
		HStoreValue	*v = findUncompressedHStoreValue(VARDATA(hs), HS_FLAG_HSTORE | HS_FLAG_ARRAY, plowbound,
									  a->array.elems[i].string.val, a->array.elems[i].string.len);

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

		if ((r == WHS_ELEM && v.type != hsvNullString) || r == WHS_KEY)
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
} SetReturningState;

static SetReturningState*
setup_firstcall(FuncCallContext *funcctx, HStore *hs,
				FunctionCallInfoData *fcinfo)
{
	MemoryContext 			oldcontext;
	SetReturningState	   *st;

	oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

	st = palloc(sizeof(*st));

	st->hs = (HStore *) palloc(VARSIZE(hs));
	memcpy(st->hs, hs, VARSIZE(hs));
	if (HS_ISEMPTY(hs))
	{
		st->it = NULL;
	}
	else
	{
		HStoreValue	v;

		st->it = HStoreIteratorInit(VARDATA(hs));
		HStoreIteratorGet(&st->it, &v, false); /* skip initial WHS_BEGIN* */
	}

	funcctx->user_fctx = (void *) st;

	if (fcinfo)
	{
		TupleDesc	tupdesc;

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
	}

	MemoryContextSwitchTo(oldcontext);

	return st;
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
		st = setup_firstcall(funcctx, PG_GETARG_HS(0), NULL);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (SetReturningState *) funcctx->user_fctx;

	while(st->it && (r = HStoreIteratorGet(&st->it, &v, true)) != 0)
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
		st = setup_firstcall(funcctx, PG_GETARG_HS(0), NULL);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (SetReturningState *) funcctx->user_fctx;

	while(st->it && (r = HStoreIteratorGet(&st->it, &v, true)) != 0)
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
		st = setup_firstcall(funcctx, PG_GETARG_HS(0), fcinfo);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (SetReturningState *) funcctx->user_fctx;

	while(st->it && (r = HStoreIteratorGet(&st->it, &v, true)) != 0)
	{
		Datum		res,
					dvalues[2] = {0, 0};
		bool		nulls[2] = {false, false};
		text	   *item;
		HeapTuple	tuple;

		if (r == WHS_ELEM)
		{
			item = HStoreValueToText(&v);
			if (item == NULL)
				nulls[0] = true;
			else
				dvalues[0] = PointerGetDatum(item);
			nulls[1] = true;
		}
		else if (r == WHS_KEY)
		{
			item = HStoreValueToText(&v);
			dvalues[0] = PointerGetDatum(item);
			r = HStoreIteratorGet(&st->it, &v, true);
			Assert(r == WHS_VALUE);
			item = HStoreValueToText(&v);
			if (item == NULL)
				nulls[1] = true;
			else
				dvalues[1] = PointerGetDatum(item);
		}
		else
		{
			Assert(r == WHS_END_ARRAY || r == WHS_END_HASH);
			continue;
		}

		tuple = heap_form_tuple(funcctx->tuple_desc, dvalues, nulls);
		res = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, PointerGetDatum(res));
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * btree sort order for hstores isn't intended to be useful; we really only
 * care about equality versus non-equality.  we compare the entire string
 * buffer first, then the entry pos array.
 */

PG_FUNCTION_INFO_V1(hstore_contains);
Datum		hstore_contains(PG_FUNCTION_ARGS);
Datum
hstore_contains(PG_FUNCTION_ARGS)
{
	HStore	   		*val = PG_GETARG_HS(0);
	HStore	   		*tmpl = PG_GETARG_HS(1);
	bool			res = true;
	HStoreIterator	*it;
	uint32			r;
	uint32			lowbound = 0,
					*plowbound = NULL;
	HStoreValue		*v, t;
	bool			skipNested = false;

	if (HS_ROOT_COUNT(val) < HS_ROOT_COUNT(tmpl) || HS_ROOT_IS_HASH(val) != HS_ROOT_IS_HASH(tmpl))
		PG_RETURN_BOOL(false);

	/*
	 * we exploit the fact that keys in "tmpl" are in strictly increasing
	 * order to narrow the findUncompressedHStoreValue search; each search can start one
	 * entry past the previous "found" entry, or at the lower bound of the
	 * search
	 */

	if (HS_ROOT_IS_HASH(val))
		plowbound = &lowbound;

	it = HStoreIteratorInit(VARDATA(tmpl));

	while(res && (r = HStoreIteratorGet(&it, &t, skipNested)) != 0)
	{
		skipNested = true;

		if (r == WHS_ELEM)
		{
			HStoreIterator	*vit;
			HStoreValue		vv;

			vit = HStoreIteratorInit(VARDATA(val));
			HStoreIteratorGet(&vit, &vv, false); /* skip WHS_BEGIN_* */

			res = false;
			while(res == false && (r = HStoreIteratorGet(&vit, &vv, true)) != 0)
				if (r == WHS_ELEM && compareHStoreValue(&t, &vv) == 0)
					res = true;
		}
		else if (r == WHS_KEY)
		{
			v = findUncompressedHStoreValue(VARDATA(val), HS_FLAG_HSTORE | HS_FLAG_ARRAY, plowbound,
											t.string.val, t.string.len);
			if (v)
			{
				r = HStoreIteratorGet(&it, &t, skipNested);
				Assert(r == WHS_VALUE);
				res = (compareHStoreValue(&t, v) == 0) ? true : false;
			}
			else
			{
				res = false;
			}
		}
	}

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
