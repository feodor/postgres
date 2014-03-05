#include "postgres.h"

#include "access/hash.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"

typedef enum HStoreOutputKind {
	JsonOutput = 0x01,
	LooseOutput = 0x02,
	ArrayCurlyBraces = 0x04,
	RootHashDecorated = 0x08,
	PrettyPrint = 0x10
} HStoreOutputKind;

static text* JsonbValueToText(JsonbValue *v);
static char* HStoreToCString(StringInfo out, char *in, int len /* just estimation */,
				HStoreOutputKind kind);

static JsonbValue*
arrayToJsonbSortedArray(ArrayType *a)
{
	Datum	 		*key_datums;
	bool	 		*key_nulls;
	int				key_count;
	JsonbValue		*v;
	int				i,
					j;
	bool			hasNonUniq = false;

	deconstruct_array(a,
					  TEXTOID, -1, false, 'i',
					  &key_datums, &key_nulls, &key_count);

	if (key_count == 0)
		return NULL;

	/*
	 * A text array uses at least eight bytes per element, so any overflow in
	 * "key_count * sizeof(JsonbPair)" is small enough for palloc() to catch.
	 * However, credible improvements to the array format could invalidate
	 * that assumption.  Therefore, use an explicit check rather than relying
	 * on palloc() to complain.
	 */
	if (key_count > MaxAllocSize / sizeof(JsonbPair))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			  errmsg("number of pairs (%d) exceeds the maximum allowed (%d)",
					 key_count, (int) (MaxAllocSize / sizeof(JsonbPair)))));

	v = palloc(sizeof(*v));
	v->type = jbvArray;
	v->array.scalar = false;
	v->array.elems = palloc(sizeof(*v->hash.pairs) * key_count);

	for (i = 0, j = 0; i < key_count; i++)
	{
		if (!key_nulls[i])
		{
			v->array.elems[j].type = jbvString;
			v->array.elems[j].string.val = VARDATA(key_datums[i]);
			v->array.elems[j].string.len = VARSIZE(key_datums[i]) - VARHDRSZ;
			j++;
		}
	}
	v->array.nelems = j;

	if (v->array.nelems > 1)
		qsort_arg(v->array.elems, v->array.nelems, sizeof(*v->array.elems),
				  compareJsonbStringValue /* compareJsonbStringValue */, &hasNonUniq);

	if (hasNonUniq)
	{
		JsonbValue	*ptr = v->array.elems + 1,
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

static JsonbValue*
findInJsonbSortedArray(JsonbValue *a, uint32 *lowbound,
						char *key, uint32 keylen)
{
	JsonbValue		*stopLow = a->array.elems + ((lowbound) ? *lowbound : 0),
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

Datum
hstore_fetchval(PG_FUNCTION_ARGS)
{
	Jsonb	   	*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	text	   	*key = PG_GETARG_TEXT_PP(1);
	JsonbValue	*v = NULL;
	text		*out;

	if (!JB_ISEMPTY(hs))
		v = findUncompressedJsonbValue(VARDATA(hs),
										JB_FLAG_OBJECT | JB_FLAG_ARRAY,
										NULL,
										VARDATA_ANY(key),
										VARSIZE_ANY_EXHDR(key));

	if ((out = JsonbValueToText(v)) == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_TEXT_P(out);
}

Datum
hstore_fetchval_numeric(PG_FUNCTION_ARGS)
{
	Jsonb	   	*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	text	   	*key = PG_GETARG_TEXT_PP(1);
	JsonbValue	*v = NULL;

	if (!JB_ISEMPTY(hs))
		v = findUncompressedJsonbValue(VARDATA(hs),
										JB_FLAG_OBJECT | JB_FLAG_ARRAY,
										NULL,
										VARDATA_ANY(key),
										VARSIZE_ANY_EXHDR(key));

	if (v && v->type == jbvNumeric)
	{
		Numeric		out = palloc(VARSIZE_ANY(v->numeric));

		memcpy(out, v->numeric, VARSIZE_ANY(v->numeric));
		PG_RETURN_NUMERIC(out);
	}

	PG_RETURN_NULL();
}

Datum
hstore_fetchval_boolean(PG_FUNCTION_ARGS)
{
	Jsonb	   	*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	text	   	*key = PG_GETARG_TEXT_PP(1);
	JsonbValue	*v = NULL;

	if (!JB_ISEMPTY(hs))
		v = findUncompressedJsonbValue(VARDATA(hs),
										JB_FLAG_OBJECT | JB_FLAG_ARRAY,
										NULL,
										VARDATA_ANY(key),
										VARSIZE_ANY_EXHDR(key));

	if (v && v->type == jbvBool)
		PG_RETURN_BOOL(v->boolean);

	PG_RETURN_NULL();
}

Datum
hstore_fetchval_n(PG_FUNCTION_ARGS)
{
	Jsonb	   	*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	int	   		i = PG_GETARG_INT32(1);
	JsonbValue	*v = NULL;
	text		*out;

	if (!JB_ISEMPTY(hs))
		v = getJsonbValue(VARDATA(hs), JB_FLAG_OBJECT | JB_FLAG_ARRAY, i);

	if ((out = JsonbValueToText(v)) == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_TEXT_P(out);
}

Datum
hstore_fetchval_n_numeric(PG_FUNCTION_ARGS)
{
	Jsonb	   	*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	int	   		i = PG_GETARG_INT32(1);
	JsonbValue	*v = NULL;

	if (!JB_ISEMPTY(hs))
		v = getJsonbValue(VARDATA(hs), JB_FLAG_OBJECT | JB_FLAG_ARRAY, i);

	if (v && v->type == jbvNumeric)
	{
		Numeric		out = palloc(VARSIZE_ANY(v->numeric));

		memcpy(out, v->numeric, VARSIZE_ANY(v->numeric));
		PG_RETURN_NUMERIC(out);
	}

	PG_RETURN_NULL();
}

Datum
hstore_fetchval_n_boolean(PG_FUNCTION_ARGS)
{
	Jsonb	   	*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	int	   		i = PG_GETARG_INT32(1);
	JsonbValue	*v = NULL;

	if (!JB_ISEMPTY(hs))
		v = getJsonbValue(VARDATA(hs), JB_FLAG_OBJECT | JB_FLAG_ARRAY, i);

	if (v && v->type == jbvBool)
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

static JsonbValue*
hstoreDeepFetch(Jsonb *in, ArrayType *path)
{
	JsonbValue			*v = NULL;
	static JsonbValue 	init /* could be returned */;
	Datum				*path_elems;
	bool				*path_nulls;
	int					path_len, i;

	Assert(ARR_ELEMTYPE(path) == TEXTOID);

	if (ARR_NDIM(path) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (JB_ROOT_COUNT(in) == 0)
		return NULL;

	deconstruct_array(path, TEXTOID, -1, false, 'i',
					  &path_elems, &path_nulls, &path_len);

	init.type = jbvBinary;
	init.size = VARSIZE(in);
	init.binary.data = VARDATA(in);
	init.binary.len = VARSIZE_ANY_EXHDR(in);

	v = &init;

	if (path_len == 0)
		return v;

	for(i=0; v != NULL && i<path_len; i++)
	{
		uint32	header;

		if (v->type != jbvBinary || path_nulls[i])
			return NULL;

		header = *(uint32*)v->binary.data;

		if (header & JB_FLAG_OBJECT)
		{
			v = findUncompressedJsonbValue(v->binary.data, JB_FLAG_OBJECT,
											NULL,
											VARDATA_ANY(path_elems[i]),
											VARSIZE_ANY_EXHDR(path_elems[i]));
		}
		else if (header & JB_FLAG_ARRAY)
		{
			int ith;

			if (h_atoi(VARDATA_ANY(path_elems[i]),
					   VARSIZE_ANY_EXHDR(path_elems[i]), &ith) == false)
				return NULL;

			if (ith < 0)
			{
				if (-ith > (int)(header & JB_COUNT_MASK))
					return NULL;
				else
					ith = ((int)(header & JB_COUNT_MASK)) + ith;
			}
			else
			{
				if (ith >= (int)(header & JB_COUNT_MASK))
					return NULL;
			}

			v = getJsonbValue(v->binary.data, JB_FLAG_ARRAY, ith);
		}
		else
		{
			elog(ERROR,"wrong hstore container type");
		}
	}

	return v;
}

PG_FUNCTION_INFO_V1(hstore_fetchval_path);
Datum		hstore_fetchval_path(PG_FUNCTION_ARGS);
Datum
hstore_fetchval_path(PG_FUNCTION_ARGS)
{
	Jsonb	   	*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	ArrayType	*path = PG_GETARG_ARRAYTYPE_P(1);
	text		*out;

	if ((out = JsonbValueToText(hstoreDeepFetch(hs, path))) == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_TEXT_P(out);
}

PG_FUNCTION_INFO_V1(hstore_fetchval_path_numeric);
Datum		hstore_fetchval_path_numeric(PG_FUNCTION_ARGS);
Datum
hstore_fetchval_path_numeric(PG_FUNCTION_ARGS)
{
	Jsonb	   	*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	ArrayType	*path = PG_GETARG_ARRAYTYPE_P(1);
	JsonbValue	*v = NULL;

	if (!JB_ISEMPTY(hs))
		v = hstoreDeepFetch(hs, path);

	if (v && v->type == jbvNumeric)
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
	Jsonb	   	*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	ArrayType	*path = PG_GETARG_ARRAYTYPE_P(1);
	JsonbValue	*v = NULL;

	if (!JB_ISEMPTY(hs))
		v = hstoreDeepFetch(hs, path);

	if (v && v->type == jbvBool)
		PG_RETURN_BOOL(v->boolean);

	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(hstore_fetchval_hstore);
Datum		hstore_fetchval_hstore(PG_FUNCTION_ARGS);
Datum
hstore_fetchval_hstore(PG_FUNCTION_ARGS)
{
	Jsonb	   	*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	text	   	*key = PG_GETARG_TEXT_PP(1);
	JsonbValue	*v = NULL;
	Jsonb		*out;

	if (!JB_ISEMPTY(hs))
		v = findUncompressedJsonbValue(VARDATA(hs),
										JB_FLAG_OBJECT | JB_FLAG_ARRAY,
										NULL,
										VARDATA_ANY(key),
										VARSIZE_ANY_EXHDR(key));

	if ((out = JsonbValueToJsonb(v)) == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_fetchval_n_hstore);
Datum		hstore_fetchval_n_hstore(PG_FUNCTION_ARGS);
Datum
hstore_fetchval_n_hstore(PG_FUNCTION_ARGS)
{
	Jsonb	   	*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	int	   		i = PG_GETARG_INT32(1);
	JsonbValue	*v = NULL;
	Jsonb		*out;

	if (!JB_ISEMPTY(hs))
		v = getJsonbValue(VARDATA(hs), JB_FLAG_OBJECT | JB_FLAG_ARRAY, i);

	if ((out = JsonbValueToJsonb(v)) == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_fetchval_path_hstore);
Datum		hstore_fetchval_path_hstore(PG_FUNCTION_ARGS);
Datum
hstore_fetchval_path_hstore(PG_FUNCTION_ARGS)
{
	Jsonb	   	*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	ArrayType	*path = PG_GETARG_ARRAYTYPE_P(1);
	Jsonb		*out;

	if ((out = JsonbValueToJsonb(hstoreDeepFetch(hs, path))) == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_exists);
Datum		hstore_exists(PG_FUNCTION_ARGS);
Datum
hstore_exists(PG_FUNCTION_ARGS)
{
	Jsonb	   	*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	text		*key = PG_GETARG_TEXT_PP(1);
	JsonbValue	*v = NULL;

	if (!JB_ISEMPTY(hs))
		v = findUncompressedJsonbValue(VARDATA(hs),
										JB_FLAG_OBJECT | JB_FLAG_ARRAY,
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
	Jsonb	   	*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	int			ith = PG_GETARG_INT32(1);
	JsonbValue	*v = NULL;

	if (!JB_ISEMPTY(hs))
		v = getJsonbValue(VARDATA(hs), JB_FLAG_OBJECT | JB_FLAG_ARRAY, ith);

	PG_RETURN_BOOL(v != NULL);
}

PG_FUNCTION_INFO_V1(hstore_exists_path);
Datum		hstore_exists_path(PG_FUNCTION_ARGS);
Datum
hstore_exists_path(PG_FUNCTION_ARGS)
{
	Jsonb	   	*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	ArrayType	*path = PG_GETARG_ARRAYTYPE_P(1);

	PG_RETURN_BOOL(hstoreDeepFetch(hs, path) != NULL);
}



PG_FUNCTION_INFO_V1(hstore_exists_any);
Datum		hstore_exists_any(PG_FUNCTION_ARGS);
Datum
hstore_exists_any(PG_FUNCTION_ARGS)
{
	Jsonb	   		*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	ArrayType	  	*keys = PG_GETARG_ARRAYTYPE_P(1);
	JsonbValue		*v = arrayToJsonbSortedArray(keys);
	int				i;
	uint32			*plowbound = NULL, lowbound = 0;
	bool			res = false;

	if (JB_ISEMPTY(hs) || v == NULL || v->hash.npairs == 0)
		PG_RETURN_BOOL(false);

	if (JB_ROOT_IS_OBJECT(hs))
		plowbound = &lowbound;
	/*
	 * we exploit the fact that the pairs list is already sorted into strictly
	 * increasing order to narrow the findUncompressedJsonbValue search; each search can
	 * start one entry past the previous "found" entry, or at the lower bound
	 * of the last search.
	 */
	for (i = 0; i < v->array.nelems; i++)
	{
		if (findUncompressedJsonbValueByValue(VARDATA(hs), JB_FLAG_OBJECT | JB_FLAG_ARRAY, plowbound,
											   v->array.elems + i) != NULL)
		{
			res = true;
			break;
		}
	}

	PG_RETURN_BOOL(res);
}


Datum
hstore_exists_all(PG_FUNCTION_ARGS)
{
	Jsonb			*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	ArrayType	  	*keys = PG_GETARG_ARRAYTYPE_P(1);
	JsonbValue		*v = arrayToJsonbSortedArray(keys);
	int				i;
	uint32			*plowbound = NULL, lowbound = 0;
	bool			res = false;

	if (JB_ISEMPTY(hs) || v == NULL || v->hash.npairs == 0)
		PG_RETURN_BOOL(false);

	if (JB_ROOT_IS_OBJECT(hs))
		plowbound = &lowbound;
	/*
	 * we exploit the fact that the pairs list is already sorted into strictly
	 * increasing order to narrow the findUncompressedJsonbValue search; each search can
	 * start one entry past the previous "found" entry, or at the lower bound
	 * of the last search.
	 */
	for (i = 0; i < v->array.nelems; i++)
	{
		if (findUncompressedJsonbValueByValue(VARDATA(hs), JB_FLAG_OBJECT | JB_FLAG_ARRAY, plowbound,
											   v->array.elems + i) != NULL)
		{
			res = true;
			break;
		}
	}

	PG_RETURN_BOOL(res);
}


static text*
JsonbValueToText(JsonbValue *v)
{
	text		*out;
#if 0

	if (v == NULL || v->type == jsbNull)
	{
		out = NULL;
	}
	else if (v->type == jsbString)
	{
		out = cstring_to_text_with_len(v->string.val, v->string.len);
	}
	else if (v->type == hsvBool)
	{
		out = cstring_to_text_with_len((v->boolean) ? "t" : "f", 1);
	}
	else if (v->type == hsvNumeric)
	{
		out = cstring_to_text(DatumGetCString(
				DirectFunctionCall1(numeric_out, PointerGetDatum(v->numeric))
		));
	}
	else
	{
		StringInfo	str;

		str = makeStringInfo();
		appendBinaryStringInfo(str, "    ", 4); /* VARHDRSZ */

		HStoreToCString(str, v->binary.data, v->binary.len, SET_PRETTY_PRINT_VAR(0));

		out = (text*)str->data;
		SET_VARSIZE(out, str->len);
	}

	return out;
#endif
	return NULL;
}

static char*
HStoreToCString(StringInfo out, char *in, int len /* just estimation */,
				HStoreOutputKind kind)
{
#if 0
	bool			first = true;
	JsonbIterator	*it;
	int				type;
	JsonbValue		v;
	int				level = 0;
	bool			isRootHash = false;

	if (out == NULL)
		out = makeStringInfo();

	if (in == NULL)
	{
		appendStringInfoString(out, "");
		return out->data;
	}

	enlargeStringInfo(out, (len >= 0) ? len : 64);

	it = HStoreIteratorInit(in);

	while((type = HStoreIteratorGet(&it, &v, false)) != 0)
	{
reout:
		switch(type)
		{
			case WHS_BEGIN_ARRAY:
				if (first == false)
				{
					appendBinaryStringInfo(out, ", ", 2);
					printCR(out, kind);
				}
				first = true;

				if (needBrackets(level, true, kind, v.array.scalar))
				{
					printIndent(out, isRootHash, kind, level);
					appendStringInfoChar(out, isArrayBrackets(kind) ? '[' : '{');
					printCR(out, kind);
				}
				level++;
				break;
			case WHS_BEGIN_HASH:
				if (first == false)
				{
					appendBinaryStringInfo(out, ", ", 2);
					printCR(out, kind);
				}
				first = true;

				if (level == 0)
					isRootHash = true;

				if (needBrackets(level, false, kind, false))
				{
					printIndent(out, isRootHash, kind, level);
					appendStringInfoCharMacro(out, '{');
					printCR(out, kind);
				}

				level++;
				break;
			case WHS_KEY:
				if (first == false)
				{
					appendBinaryStringInfo(out, ", ", 2);
					printCR(out, kind);
				}
				first = true;

				printIndent(out, isRootHash, kind, level);
				/* key should not be loose */
				putEscapedValue(out, kind & ~LooseOutput, &v);
				appendBinaryStringInfo(out,
									   (kind & JsonOutput) ? ": " : "=>", 2);

				type = HStoreIteratorGet(&it, &v, false);
				if (type == WHS_VALUE)
				{
					first = false;
					putEscapedValue(out, kind, &v);
				}
				else
				{
					Assert(type == WHS_BEGIN_HASH || type == WHS_BEGIN_ARRAY);
					printCR(out, kind);
					goto reout;
				}
				break;
			case WHS_ELEM:
				if (first == false)
				{
					appendBinaryStringInfo(out, ", ", 2);
					printCR(out, kind);
				}
				else
				{
					first = false;
				}

				printIndent(out, isRootHash, kind, level);
				putEscapedValue(out, kind, &v);
				break;
			case WHS_END_ARRAY:
				level--;
				if (needBrackets(level, true, kind, v.array.scalar))
				{
					printCR(out, kind);
					printIndent(out, isRootHash, kind, level);
					appendStringInfoChar(out, isArrayBrackets(kind) ? ']' : '}');
				}
				first = false;
				break;
			case WHS_END_HASH:
				level--;
				if (needBrackets(level, false, kind, false))
				{
					printCR(out, kind);
					printIndent(out, isRootHash, kind, level);
					appendStringInfoCharMacro(out, '}');
				}
				first = false;
				break;
			default:
				elog(ERROR, "unexpected state of hstore iterator");
		}
	}

	Assert(level == 0);

	return out->data;

#endif
	return NULL;
}

Datum
hstore_defined(PG_FUNCTION_ARGS)
{
	Jsonb		*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	text		*key = PG_GETARG_TEXT_PP(1);
	JsonbValue	*v = NULL;

	if (!JB_ISEMPTY(hs))
		v = findUncompressedJsonbValue(VARDATA(hs),
										JB_FLAG_OBJECT | JB_FLAG_ARRAY,
										NULL,
										VARDATA_ANY(key),
										VARSIZE_ANY_EXHDR(key));

	PG_RETURN_BOOL(!(v == NULL || v->type == jbvNull));
}


PG_FUNCTION_INFO_V1(hstore_delete);
Datum		hstore_delete(PG_FUNCTION_ARGS);
Datum
hstore_delete(PG_FUNCTION_ARGS)
{
	Jsonb			*in = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	text	   		*key = PG_GETARG_TEXT_PP(1);
	char	   		*keyptr = VARDATA_ANY(key);
	int				keylen = VARSIZE_ANY_EXHDR(key);
	Jsonb	   		*out = palloc(VARSIZE(in));
	ToJsonbState	*toState = NULL;
	JsonbIterator	*it;
	uint32			r;
	JsonbValue		v, *res = NULL;
	bool			skipNested = false;

	SET_VARSIZE(out, VARSIZE(in));

	if (JB_ISEMPTY(in))
		PG_RETURN_POINTER(out);

	it = JsonbIteratorInit(VARDATA(in));

	while((r = JsonbIteratorGet(&it, &v, skipNested)) != 0)
	{
		skipNested = true;

		if ((r == WJB_ELEM || r == WJB_KEY) &&
			(v.type == jbvString && keylen == v.string.len &&
			 memcmp(keyptr, v.string.val, keylen) == 0))
		{
			if (r == WJB_KEY)
				/* skip corresponding value */
				JsonbIteratorGet(&it, &v, true);

			continue;
		}

		res = pushJsonbValue(&toState, r, &v);
	}

	if (res == NULL || (res->type == jbvArray && res->array.nelems == 0) ||
					   (res->type == jbvHash && res->hash.npairs == 0) )
	{
		SET_VARSIZE(out, VARHDRSZ);
	}
	else
	{
		r = compressJsonb(res, VARDATA(out));
		SET_VARSIZE(out, r + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_delete_array);
Datum		hstore_delete_array(PG_FUNCTION_ARGS);
Datum
hstore_delete_array(PG_FUNCTION_ARGS)
{
	Jsonb			*in = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	Jsonb	   		*out = palloc(VARSIZE(in));
	JsonbValue 	*a = arrayToJsonbSortedArray(PG_GETARG_ARRAYTYPE_P(1));
	JsonbIterator	*it;
	ToJsonbState	*toState = NULL;
	uint32			r, i = 0;
	JsonbValue		v, *res = NULL;
	bool			skipNested = false;
	bool			isHash = false;


	if (JB_ISEMPTY(in) || a == NULL || a->array.nelems == 0)
	{
		memcpy(out, in, VARSIZE(in));
		PG_RETURN_POINTER(out);
	}

	it = JsonbIteratorInit(VARDATA(in));

	while((r = JsonbIteratorGet(&it, &v, skipNested)) != 0)
	{

		if (skipNested == false)
		{
			Assert(v.type == jbvArray || v.type == jbvHash);
			isHash = (v.type == jbvArray) ? false : true;
			skipNested = true;
		}

		if ((r == WJB_ELEM || r == WJB_KEY) && v.type == jbvString &&
			i < a->array.nelems)
		{
			int diff;

			if (isHash)
			{
				do {
					diff = compareJsonbStringValue(&v, a->array.elems + i,
													NULL);

					if (diff >= 0)
						i++;
				} while(diff > 0 && i < a->array.nelems);
			}
			else
			{
				diff = (findInJsonbSortedArray(a, NULL,
												v.string.val,
												v.string.len) == NULL) ? 1 : 0;
			}

			if (diff == 0)
			{
				if (r == WJB_KEY)
					/* skip corresponding value */
					JsonbIteratorGet(&it, &v, true);

				continue;
			}
		}

		res = pushJsonbValue(&toState, r, &v);
	}

	if (res == NULL || (res->type == jbvArray && res->array.nelems == 0) ||
					   (res->type == jbvHash && res->hash.npairs == 0) )
	{
		SET_VARSIZE(out, VARHDRSZ);
	}
	else
	{
		r = compressJsonb(res, VARDATA(out));
		SET_VARSIZE(out, r + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}


PG_FUNCTION_INFO_V1(hstore_delete_hstore);
Datum		hstore_delete_hstore(PG_FUNCTION_ARGS);
Datum
hstore_delete_hstore(PG_FUNCTION_ARGS)
{
	Jsonb			*hs1 = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	Jsonb			*hs2 = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(1));
	Jsonb	   		*out = palloc(VARSIZE(hs1));
	JsonbIterator	*it1, *it2;
	ToJsonbState	*toState = NULL;
	uint32			r1, r2;
	JsonbValue		v1, v2, *res = NULL;
	bool			isHash1, isHash2;

	if (JB_ISEMPTY(hs1) || JB_ISEMPTY(hs2))
	{
		memcpy(out, hs1, VARSIZE(hs1));
		PG_RETURN_POINTER(out);
	}

	it1 = JsonbIteratorInit(VARDATA(hs1));
	r1 = JsonbIteratorGet(&it1, &v1, false);
	isHash1 = (v1.type == jbvArray) ? false : true;

	it2 = JsonbIteratorInit(VARDATA(hs2));
	r2 = JsonbIteratorGet(&it2, &v2, false);
	isHash2 = (v2.type == jbvArray) ? false : true;

	res = pushJsonbValue(&toState, r1, &v1);

	if (isHash1 == true && isHash2 == true)
	{
		bool			fin2 = false,
						keyIsDef = false;

		while((r1 = JsonbIteratorGet(&it1, &v1, true)) != 0)
		{
			if (r1 == WJB_KEY && fin2 == false)
			{
				int diff  = 1;

				if (keyIsDef)
					r2 = WJB_KEY;

				while(keyIsDef ||
					  (r2 = JsonbIteratorGet(&it2, &v2, true)) != 0)
				{
					if (r2 != WJB_KEY)
						continue;

					diff = compareJsonbStringValue(&v1, &v2, NULL);

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
					JsonbValue		vk;

					keyIsDef = false;

					r1 = JsonbIteratorGet(&it1, &vk, true);
					r2 = JsonbIteratorGet(&it2, &v2, true);

					Assert(r1 == WJB_VALUE && r2 == WJB_VALUE);

					if (compareJsonbValue(&vk, &v2) != 0)
					{
						res = pushJsonbValue(&toState, WJB_KEY, &v1);
						res = pushJsonbValue(&toState, WJB_VALUE, &vk);
					}

					continue;
				}
				else
				{
					keyIsDef = true;
				}
			}

			res = pushJsonbValue(&toState, r1, &v1);
		}
	}
	else
	{
		while((r1 = JsonbIteratorGet(&it1, &v1, true)) != 0)
		{

			if (r1 == WJB_ELEM || r1 == WJB_KEY)
			{
				int diff = 1;

				it2 = JsonbIteratorInit(VARDATA(hs2));

				r2 = JsonbIteratorGet(&it2, &v2, false);

				while(diff && (r2 = JsonbIteratorGet(&it2, &v2, true)) != 0)
				{
					if (r2 == WJB_KEY || r2 == WJB_VALUE || r2 == WJB_ELEM)
						diff = compareJsonbValue(&v1, &v2);
				}

				if (diff == 0)
				{
					if (r1 == WJB_KEY)
						JsonbIteratorGet(&it1, &v1, true);
					continue;
				}
			}

			res = pushJsonbValue(&toState, r1, &v1);
		}
	}

	if (res == NULL || (res->type == jbvArray && res->array.nelems == 0) ||
					   (res->type == jbvHash && res->hash.npairs == 0) )
	{
		SET_VARSIZE(out, VARHDRSZ);
	}
	else
	{
		int r = compressJsonb(res, VARDATA(out));
		SET_VARSIZE(out, r + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}

static JsonbValue*
deletePathDo(JsonbIterator **it, Datum	*path_elems,
			 bool *path_nulls, int path_len,
			 ToJsonbState	**st, int level)
{
	JsonbValue	v, *res = NULL;
	int			r;

	r = JsonbIteratorGet(it, &v, false);

	if (r == WJB_BEGIN_ARRAY)
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
			r = JsonbIteratorGet(it, &v, true);
			Assert(r == WJB_ELEM);
			r = JsonbIteratorGet(it, &v, true);
			Assert(r == WJB_END_ARRAY);
			return NULL;
		}

		pushJsonbValue(st, r, &v);

		for(i=0; i<skipIdx; i++) {
			r = JsonbIteratorGet(it, &v, true);
			Assert(r == WJB_ELEM);
			res = pushJsonbValue(st, r, &v);
		}

		if (level >= path_len || skipIdx == n) {
			r = JsonbIteratorGet(it, &v, true);
			Assert(r == WJB_END_ARRAY);
			res = pushJsonbValue(st, r, &v);
			return res;
		}

		if (level == path_len - 1)
		{
			/* last level in path, skip all elem */
			r = JsonbIteratorGet(it, &v, true);
			Assert(r == WJB_ELEM);
		}
		else
		{
			res = deletePathDo(it, path_elems, path_nulls, path_len, st,
							   level + 1);
		}

		for(i = skipIdx + 1; i<n; i++) {
			r = JsonbIteratorGet(it, &v, true);
			Assert(r == WJB_ELEM);
			res = pushJsonbValue(st, r, &v);
		}

		r = JsonbIteratorGet(it, &v, true);
		Assert(r == WJB_END_ARRAY);
		res = pushJsonbValue(st, r, &v);
	}
	else if (r == WJB_BEGIN_OBJECT)
	{
		int			i;
		uint32		n = v.hash.npairs;
		JsonbValue	k;
		bool		done = false;

		if (n == 1 && level == path_len - 1)
		{
			r = JsonbIteratorGet(it, &k, false);
			Assert(r == WJB_KEY);

			if ( path_nulls[level] == false &&
				 k.string.len == VARSIZE_ANY_EXHDR(path_elems[level]) &&
				 memcmp(k.string.val, VARDATA_ANY(path_elems[level]),
						k.string.len) == 0)
			{
				r = JsonbIteratorGet(it, &v, true);
				Assert(r == WJB_VALUE);
				r = JsonbIteratorGet(it, &v, true);
				Assert(r == WJB_END_OBJECT);
				return NULL;
			}

			pushJsonbValue(st, WJB_BEGIN_OBJECT, &v);
			pushJsonbValue(st, WJB_KEY, &k);
			r = JsonbIteratorGet(it, &v, true);
			Assert(r == WJB_VALUE);
			pushJsonbValue(st, r, &v);
			r = JsonbIteratorGet(it, &v, true);
			Assert(r == WJB_END_OBJECT);
			return pushJsonbValue(st, r, &v);
		}

		pushJsonbValue(st, WJB_BEGIN_OBJECT, &v);

		if (level >= path_len || path_nulls[level])
			done = true;

		for(i=0; i<n; i++)
		{
			r = JsonbIteratorGet(it, &k, false);
			Assert(r == WJB_KEY);

			if (done == false &&
				k.string.len == VARSIZE_ANY_EXHDR(path_elems[level]) &&
				memcmp(k.string.val, VARDATA_ANY(path_elems[level]),
					   k.string.len) == 0)
			{
				done = true;

				if (level == path_len - 1)
				{
					r = JsonbIteratorGet(it, &v, true);
					Assert(r == WJB_VALUE);
				}
				else
				{
					pushJsonbValue(st, r, &k);
					res = deletePathDo(it, path_elems, path_nulls, path_len,
									   st, level + 1);
					if (res == NULL)
					{
						v.type = jbvNull;
						pushJsonbValue(st, WJB_VALUE, &v);
					}
				}

				continue;
			}

			pushJsonbValue(st, r, &k);
			r = JsonbIteratorGet(it, &v, true);
			Assert(r == WJB_VALUE);
			pushJsonbValue(st, r, &v);
		}

		r = JsonbIteratorGet(it, &v, true);
		Assert(r == WJB_END_OBJECT);
		res = pushJsonbValue(st, r, &v);
	}
	else if (r == WJB_ELEM || r == WJB_VALUE) /* just a string or null */
	{
		pushJsonbValue(st, r, &v);
		res = (void*)0x01; /* dummy value */
	}
	else
	{
		elog(ERROR, "impossible state");
	}

	return res;
}


PG_FUNCTION_INFO_V1(hstore_delete_path);
Datum		hstore_delete_path(PG_FUNCTION_ARGS);
Datum
hstore_delete_path(PG_FUNCTION_ARGS)
{
	Jsonb			*in = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	Jsonb			*out = palloc(VARSIZE(in));
	ArrayType		*path = PG_GETARG_ARRAYTYPE_P(1);
	JsonbValue		*res = NULL;
	Datum			*path_elems;
	bool			*path_nulls;
	int				path_len;
	JsonbIterator	*it;
	ToJsonbState	*st = NULL;

	Assert(ARR_ELEMTYPE(path) == TEXTOID);

	if (ARR_NDIM(path) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (JB_ROOT_COUNT(in) == 0)
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

	it = JsonbIteratorInit(VARDATA(in));

	res = deletePathDo(&it, path_elems, path_nulls, path_len, &st, 0);

	if (res == NULL)
	{
		SET_VARSIZE(out, VARHDRSZ);
	}
	else
	{
		int				sz;

		sz = compressJsonb(res, VARDATA(out));
		SET_VARSIZE(out, sz + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_delete_idx);
Datum		hstore_delete_idx(PG_FUNCTION_ARGS);
Datum
hstore_delete_idx(PG_FUNCTION_ARGS)
{
	Jsonb			*in = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	int	   			idx = PG_GETARG_INT32(1);
	Jsonb	   		*out = palloc(VARSIZE(in));
	ToJsonbState	*toState = NULL;
	JsonbIterator	*it;
	uint32			r, i = 0, n;
	JsonbValue		v, *res = NULL;

	if (JB_ISEMPTY(in))
	{
		memcpy(out, in, VARSIZE(in));
		PG_RETURN_POINTER(out);
	}

	it = JsonbIteratorInit(VARDATA(in));

	r = JsonbIteratorGet(&it, &v, false);
	if (r == WJB_BEGIN_ARRAY)
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

	pushJsonbValue(&toState, r, &v);

	while((r = JsonbIteratorGet(&it, &v, true)) != 0)
	{
		if (r == WJB_ELEM || r == WJB_KEY)
		{
			if (i++ == idx)
			{
				if (r == WJB_KEY)
					JsonbIteratorGet(&it, &v, true); /* skip value */
				continue;
			}
		}

		res = pushJsonbValue(&toState, r, &v);
	}

	if (res == NULL || (res->type == jbvArray && res->array.nelems == 0) ||
					   (res->type == jbvHash && res->hash.npairs == 0) )
	{
		SET_VARSIZE(out, VARHDRSZ);
	}
	else
	{
		r = compressJsonb(res, VARDATA(out));
		SET_VARSIZE(out, r + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}

static void
convertScalarToString(JsonbValue *v)
{
	switch(v->type) {
		case jbvNull:
			elog(ERROR, "key in hstore type could not be a NULL");
			break;
		case jbvBool:
			v->type = jbvString;
			v->string.val = pnstrdup((v->boolean) ? "t" : "f", 1);
			v->string.len = 1;
			v->size = sizeof(JEntry) + v->string.len;
			break;
		case jbvNumeric:
			v->type = jbvString;
			v->string.val = DatumGetCString(
							DirectFunctionCall1(numeric_out,
												PointerGetDatum(v->numeric)));
			v->string.len = strlen(v->string.val);
			v->size = sizeof(JEntry) + v->string.len;
			break;
		case jbvString:
			break;
		default:
			elog(ERROR,"wrong hstore scalar type");
	}
}

static JsonbValue *
IteratorConcat(JsonbIterator **it1, JsonbIterator **it2,
			   ToJsonbState **toState)
{
	uint32			r1, r2, rk1, rk2;
	JsonbValue		v1, v2, *res = NULL;

	r1 = rk1 = JsonbIteratorGet(it1, &v1, false);
	r2 = rk2 = JsonbIteratorGet(it2, &v2, false);

	if (rk1 == WJB_BEGIN_OBJECT && rk2 == WJB_BEGIN_OBJECT)
	{
		bool			fin2 = false,
						keyIsDef = false;

		res = pushJsonbValue(toState, r1, &v1);

		for(;;)
		{
			r1 = JsonbIteratorGet(it1, &v1, true);

			Assert(r1 == WJB_KEY || r1 == WJB_VALUE || r1 == WJB_END_OBJECT);

			if (r1 == WJB_KEY && fin2 == false)
			{
				int diff  = 1;

				if (keyIsDef)
					r2 = WJB_KEY;

				while(keyIsDef || (r2 = JsonbIteratorGet(it2, &v2, true)) != 0)
				{
					if (r2 != WJB_KEY)
						continue;

					diff = compareJsonbStringValue(&v1, &v2, NULL);

					if (diff > 0)
					{
						if (keyIsDef)
							keyIsDef = false;

						pushJsonbValue(toState, r2, &v2);
						r2 = JsonbIteratorGet(it2, &v2, true);
						Assert(r2 == WJB_VALUE);
						pushJsonbValue(toState, r2, &v2);
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

					pushJsonbValue(toState, r1, &v1);

					r1 = JsonbIteratorGet(it1, &v1, true); /* ignore */
					r2 = JsonbIteratorGet(it2, &v2, true); /* new val */

					Assert(r1 == WJB_VALUE && r2 == WJB_VALUE);
					pushJsonbValue(toState, r2, &v2);

					continue;
				}
				else
				{
					keyIsDef = true;
				}
			}
			else if (r1 == WJB_END_OBJECT)
			{
				if (r2 != 0)
				{
					if (keyIsDef)
						r2 = WJB_KEY;

					while(keyIsDef ||
						  (r2 = JsonbIteratorGet(it2, &v2, true)) != 0)
					{
						if (r2 != WJB_KEY)
							continue;

						pushJsonbValue(toState, r2, &v2);
						r2 = JsonbIteratorGet(it2, &v2, true);
						Assert(r2 == WJB_VALUE);
						pushJsonbValue(toState, r2, &v2);
						keyIsDef = false;
					}
				}

				res = pushJsonbValue(toState, r1, &v1);
				break;
			}

			res = pushJsonbValue(toState, r1, &v1);
		}
	}
	else if ((rk1 == WJB_BEGIN_OBJECT || rk1 == WJB_BEGIN_ARRAY) &&
			 (rk2 == WJB_BEGIN_OBJECT || rk2 == WJB_BEGIN_ARRAY))
	{
		if (rk1 == WJB_BEGIN_OBJECT && rk2 == WJB_BEGIN_ARRAY &&
			v2.array.nelems % 2 != 0)
			elog(ERROR, "hstore's array must have even number of elements");

		res = pushJsonbValue(toState, r1, &v1);

		for(;;)
		{
			r1 = JsonbIteratorGet(it1, &v1, true);
			if (r1 == WJB_END_OBJECT || r1 == WJB_END_ARRAY)
				break;
			Assert(r1 == WJB_KEY || r1 == WJB_VALUE || r1 == WJB_ELEM);
			pushJsonbValue(toState, r1, &v1);
		}

		while((r2 = JsonbIteratorGet(it2, &v2, true)) != 0)
		{
			if (!(r2 == WJB_END_OBJECT || r2 == WJB_END_ARRAY))
			{
				if (rk1 == WJB_BEGIN_OBJECT)
				{
					convertScalarToString(&v2);
					pushJsonbValue(toState, WJB_KEY, &v2);
					r2 = JsonbIteratorGet(it2, &v2, true);
					Assert(r2 == WJB_ELEM);
					pushJsonbValue(toState, WJB_VALUE, &v2);
				}
				else
				{
					pushJsonbValue(toState, WJB_ELEM, &v2);
				}
			}
		}

		res = pushJsonbValue(toState,
							  (rk1 == WJB_BEGIN_OBJECT) ? WJB_END_OBJECT : WJB_END_ARRAY,
							  NULL/* signal to sort */);
	}
	else if ((rk1 & (WJB_VALUE | WJB_ELEM)) != 0)
	{
		if (v2.type == jbvArray && v2.array.scalar)
		{
			Assert(v2.array.nelems == 1);
			r2 = JsonbIteratorGet(it2, &v2, false);
			pushJsonbValue(toState, r1, &v2);
		}
		else
		{
			res = pushJsonbValue(toState, r2, &v2);
			while((r2 = JsonbIteratorGet(it2, &v2, true)) != 0)
				res = pushJsonbValue(toState, r2, &v2);
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
	Jsonb	   		*hs1 = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	Jsonb	   		*hs2 = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(1));
	Jsonb	   		*out = palloc(VARSIZE(hs1) + VARSIZE(hs2));
	ToJsonbState	*toState = NULL;
	JsonbValue		*res;
	JsonbIterator	*it1, *it2;

	if (JB_ISEMPTY(hs1))
	{
		memcpy(out, hs2, VARSIZE(hs2));
		PG_RETURN_POINTER(out);
	}
	else if (JB_ISEMPTY(hs2))
	{
		memcpy(out, hs1, VARSIZE(hs1));
		PG_RETURN_POINTER(out);
	}

	it1 = JsonbIteratorInit(VARDATA(hs1));
	it2 = JsonbIteratorInit(VARDATA(hs2));

	res = IteratorConcat(&it1, &it2, &toState);

	if (res == NULL || (res->type == jbvArray && res->array.nelems == 0) ||
					   (res->type == jbvHash && res->hash.npairs == 0) )
	{
		SET_VARSIZE(out, VARHDRSZ);
	}
	else
	{
		uint32 r;

		if (res->type == jbvArray && res->array.nelems > 1)
			res->array.scalar = false;

		r = compressJsonb(res, VARDATA(out));
		SET_VARSIZE(out, r + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}


PG_FUNCTION_INFO_V1(hstore_slice_to_array);
Datum		hstore_slice_to_array(PG_FUNCTION_ARGS);
Datum
hstore_slice_to_array(PG_FUNCTION_ARGS)
{
	Jsonb	   *hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
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

	if (key_count == 0 || JB_ISEMPTY(hs))
	{
		aout = construct_empty_array(TEXTOID);
		PG_RETURN_POINTER(aout);
	}

	out_datums = palloc(sizeof(Datum) * key_count);
	out_nulls = palloc(sizeof(bool) * key_count);

	for (i = 0; i < key_count; ++i)
	{
		text	   *key = (text *) DatumGetPointer(key_datums[i]);
		JsonbValue	*v = NULL;

		if (key_nulls[i] == false)
			v = findUncompressedJsonbValue(VARDATA(hs),
											JB_FLAG_OBJECT | JB_FLAG_ARRAY,
											NULL,
											VARDATA(key),
											VARSIZE(key) - VARHDRSZ);

		out_datums[i] = PointerGetDatum(JsonbValueToText(v));
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
	Jsonb	       *hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	JsonbValue	   *a = arrayToJsonbSortedArray(PG_GETARG_ARRAYTYPE_P(1));
	uint32			lowbound = 0,
				   *plowbound;
	JsonbValue		*res = NULL;
	ToJsonbState	*state = NULL;
	text			*out;
	uint32			i;

	out = palloc(VARSIZE(hs));

	if (a == NULL || a->array.nelems == 0 || JB_ISEMPTY(hs))
	{
		memcpy(out, hs, VARSIZE(hs));
		PG_RETURN_POINTER(out);
	}

	if (JB_ROOT_IS_OBJECT(hs))
	{
		plowbound = &lowbound;
		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	}
	else
	{
		plowbound = NULL;
		pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);
	}

	for (i = 0; i < a->array.nelems; ++i)
	{
		JsonbValue	*v = findUncompressedJsonbValueByValue(VARDATA(hs),
															JB_FLAG_OBJECT | JB_FLAG_ARRAY,
															plowbound,
															a->array.elems + i);

		if (v)
		{
			if (plowbound)
			{
				pushJsonbValue(&state, WJB_KEY, a->array.elems + i);
				pushJsonbValue(&state, WJB_VALUE, v);
			}
			else
			{
				pushJsonbValue(&state, WJB_ELEM, v);
			}
		}
	}

	if (plowbound)
		res = pushJsonbValue(&state, WJB_END_OBJECT, a /* any non-null value */);
	else
		res = pushJsonbValue(&state, WJB_END_ARRAY, NULL);


	if (res == NULL || (res->type == jbvArray && res->array.nelems == 0) ||
						(res->type == jbvHash && res->hash.npairs == 0) )
	{
		SET_VARSIZE(out, VARHDRSZ);
	}
	else
	{
		int r = compressJsonb(res, VARDATA(out));
		SET_VARSIZE(out, r + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}

static JsonbValue*
replacePathDo(JsonbIterator **it, Datum *path_elems,
			  bool *path_nulls, int path_len,
			  ToJsonbState  **st, int level, JsonbValue *newval)
{
	JsonbValue v, *res = NULL;
	int			r;

	r = JsonbIteratorGet(it, &v, false);

	if (r == WJB_BEGIN_ARRAY)
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

		pushJsonbValue(st, r, &v);

		for(i=0; i<n; i++)
		{
			if (i == idx && level < path_len)
			{
				if (level == path_len - 1)
				{
					r = JsonbIteratorGet(it, &v, true); /* skip */
					Assert(r == WJB_ELEM);
					res = pushJsonbValue(st, r, newval);
				}
				else
				{
					res = replacePathDo(it, path_elems, path_nulls, path_len,
										st, level + 1, newval);
				}
			}
			else
			{
				r = JsonbIteratorGet(it, &v, true);
				Assert(r == WJB_ELEM);
				res = pushJsonbValue(st, r, &v);
			}
		}

		r = JsonbIteratorGet(it, &v, true);
		Assert(r == WJB_END_ARRAY);
		res = pushJsonbValue(st, r, &v);
	}
	else if (r == WJB_BEGIN_OBJECT)
	{
		int			i;
		uint32		n = v.hash.npairs;
		JsonbValue	k;
		bool		done = false;

		pushJsonbValue(st, WJB_BEGIN_OBJECT, &v);

		if (level >= path_len || path_nulls[level])
			done = true;

		for(i=0; i<n; i++)
		{
			r = JsonbIteratorGet(it, &k, false);
			Assert(r == WJB_KEY);
			res = pushJsonbValue(st, r, &k);

			if (done == false &&
				k.string.len == VARSIZE_ANY_EXHDR(path_elems[level]) &&
				memcmp(k.string.val, VARDATA_ANY(path_elems[level]),
					   k.string.len) == 0)
			{
				if (level == path_len - 1)
				{
					r = JsonbIteratorGet(it, &v, true); /* skip */
					Assert(r == WJB_VALUE);
					res = pushJsonbValue(st, r, newval);
				}
				else
				{
					res = replacePathDo(it, path_elems, path_nulls, path_len,
										st, level + 1, newval);
				}
			}
			else
			{
				r = JsonbIteratorGet(it, &v, true);
				Assert(r == WJB_VALUE);
				res = pushJsonbValue(st, r, &v);
			}
		}

		r = JsonbIteratorGet(it, &v, true);
		Assert(r == WJB_END_OBJECT);
		res = pushJsonbValue(st, r, &v);
	}
	else if (r == WJB_ELEM || r == WJB_VALUE)
	{
		pushJsonbValue(st, r, &v);
		res = (void*)0x01; /* dummy value */
	}
	else
	{
		elog(ERROR, "impossible state");
	}

	return res;
}

PG_FUNCTION_INFO_V1(hstore_replace);
Datum		hstore_replace(PG_FUNCTION_ARGS);
Datum
hstore_replace(PG_FUNCTION_ARGS)
{
	Jsonb	   		*in = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	ArrayType		*path = PG_GETARG_ARRAYTYPE_P(1);
	Jsonb	   		*newval = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(2));
	Jsonb			*out = palloc(VARSIZE(in) + VARSIZE(newval));
	JsonbValue		*res = NULL;
	JsonbValue		value;
	Datum			*path_elems;
	bool			*path_nulls;
	int				path_len;
	JsonbIterator	*it;
	ToJsonbState	*st = NULL;

	Assert(ARR_ELEMTYPE(path) == TEXTOID);

	if (ARR_NDIM(path) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (JB_ROOT_COUNT(in) == 0)
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

	if (JB_ROOT_COUNT(newval) == 0)
	{
		value.type = jbvNull;
		value.size = sizeof(JEntry);
	}
	else
	{
		value.type = jbvBinary;
		value.binary.data = VARDATA(newval);
		value.binary.len = VARSIZE_ANY_EXHDR(newval);
		value.size = value.binary.len + sizeof(JEntry);
	}

	it = JsonbIteratorInit(VARDATA(in));

	res = replacePathDo(&it, path_elems, path_nulls, path_len, &st, 0, &value);

	if (res == NULL)
	{
		SET_VARSIZE(out, VARHDRSZ);
	}
	else
	{
		int				sz;

		sz = compressJsonb(res, VARDATA(out));
		SET_VARSIZE(out, sz + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}

static JsonbValue*
concatPathDo(JsonbIterator **it, Datum *path_elems,
			 bool *path_nulls, int path_len,
			 ToJsonbState  **st, int level, JsonbIterator	*toConcat)
{
	JsonbValue v, *res = NULL;
	int			r;

	r = JsonbIteratorGet(it, &v, false);

	if (r == WJB_BEGIN_ARRAY)
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

		pushJsonbValue(st, r, &v);

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
				r = JsonbIteratorGet(it, &v, true);
				Assert(r == WJB_ELEM);
				res = pushJsonbValue(st, r, &v);
			}
		}

		r = JsonbIteratorGet(it, &v, true);
		Assert(r == WJB_END_ARRAY);
		res = pushJsonbValue(st, r, &v);
	}
	else if (r == WJB_BEGIN_OBJECT)
	{
		int			i;
		uint32		n = v.hash.npairs;
		JsonbValue	k;
		bool		done = false;

		pushJsonbValue(st, WJB_BEGIN_OBJECT, &v);

		if (level >= path_len || path_nulls[level])
			done = true;

		for(i=0; i<n; i++)
		{
			r = JsonbIteratorGet(it, &k, false);
			Assert(r == WJB_KEY);
			res = pushJsonbValue(st, r, &k);

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
				r = JsonbIteratorGet(it, &v, true);
				Assert(r == WJB_VALUE);
				res = pushJsonbValue(st, r, &v);
			}
		}

		r = JsonbIteratorGet(it, &v, true);
		Assert(r == WJB_END_OBJECT);
		res = pushJsonbValue(st, r, &v);
	}
	else if (r == WJB_ELEM || r == WJB_VALUE)
	{
		pushJsonbValue(st, r, &v);
		res = (void*)0x01; /* dummy value */
	}
	else
	{
		elog(ERROR, "impossible state");
	}

	return res;
}

PG_FUNCTION_INFO_V1(hstore_deep_concat);
Datum		hstore_deep_concat(PG_FUNCTION_ARGS);
Datum
hstore_deep_concat(PG_FUNCTION_ARGS)
{
	Jsonb	   		*in = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	ArrayType		*path = PG_GETARG_ARRAYTYPE_P(1);
	Jsonb	   		*newval = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(2));
	Jsonb			*out = palloc(VARSIZE(in) + VARSIZE(newval));
	JsonbValue		*res = NULL;
	Datum			*path_elems;
	bool			*path_nulls;
	int				path_len;
	JsonbIterator	*it1, *it2;
	ToJsonbState	*st = NULL;

	Assert(ARR_ELEMTYPE(path) == TEXTOID);

	if (ARR_NDIM(path) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (JB_ROOT_COUNT(in) == 0 || JB_ROOT_COUNT(newval) == 0)
	{
		memcpy(out, in, VARSIZE(in));
		PG_RETURN_POINTER(out);
	}

	deconstruct_array(path, TEXTOID, -1, false, 'i',
					  &path_elems, &path_nulls, &path_len);

	it1 = JsonbIteratorInit(VARDATA(in));
	it2 = JsonbIteratorInit(VARDATA(newval));

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

		if (res->type == jbvArray && res->array.nelems > 1)
			res->array.scalar = false;

		sz = compressJsonb(res, VARDATA(out));
		SET_VARSIZE(out, sz + VARHDRSZ);
	}

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_akeys);
Datum		hstore_akeys(PG_FUNCTION_ARGS);
Datum
hstore_akeys(PG_FUNCTION_ARGS)
{
	Jsonb	   		*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	Datum	   		*d;
	ArrayType  		*a;
	int				i = 0, r = 0;
	JsonbIterator	*it;
	JsonbValue		v;
	bool			skipNested = false;

	if (JB_ISEMPTY(hs))
	{
		a = construct_empty_array(TEXTOID);
		PG_RETURN_POINTER(a);
	}

	d = (Datum *) palloc(sizeof(Datum) * JB_ROOT_COUNT(hs));

	it = JsonbIteratorInit(VARDATA(hs));

	while((r = JsonbIteratorGet(&it, &v, skipNested)) != 0)
	{
		skipNested = true;

		if ((r == WJB_ELEM && v.type != jbvNull) || r == WJB_KEY)
			d[i++] = PointerGetDatum(JsonbValueToText(&v));
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
	Jsonb	   		*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	Datum	   		*d;
	ArrayType  		*a;
	int				i = 0, r = 0;
	JsonbIterator	*it;
	JsonbValue		v;
	bool			skipNested = false;
	bool		   *nulls;
	int				lb = 1;

	if (JB_ISEMPTY(hs))
	{
		a = construct_empty_array(TEXTOID);
		PG_RETURN_POINTER(a);
	}

	d = (Datum *) palloc(sizeof(Datum) * JB_ROOT_COUNT(hs));
	nulls = (bool *) palloc(sizeof(bool) * JB_ROOT_COUNT(hs));

	it = JsonbIteratorInit(VARDATA(hs));

	while((r = JsonbIteratorGet(&it, &v, skipNested)) != 0)
	{
		skipNested = true;

		if (r == WJB_ELEM || r == WJB_VALUE)
		{
			d[i] = PointerGetDatum(JsonbValueToText(&v));
			nulls[i] = (DatumGetPointer(d[i]) == NULL) ? true : false;
			i++;
		}
	}

	a = construct_md_array(d, nulls, 1, &i, &lb,
						   TEXTOID, -1, false, 'i');

	PG_RETURN_POINTER(a);
}


static ArrayType *
hstore_to_array_internal(Jsonb *hs, int ndims)
{
	int				count = JB_ROOT_COUNT(hs);
	int				out_size[2] = {0, 2};
	int				lb[2] = {1, 1};
	Datum		   *out_datums;
	bool	   		*out_nulls;
	bool			isHash = JB_ROOT_IS_OBJECT(hs) ? true : false;
	int				i = 0, r = 0;
	JsonbIterator	*it;
	JsonbValue		v;
	bool			skipNested = false;

	Assert(ndims < 3);

	if (count == 0 || ndims == 0)
		return construct_empty_array(TEXTOID);

	if (isHash == false && ndims == 2 && count % 2 != 0)
		elog(ERROR, "hstore's array should have even number of elements");

	out_size[0] = count * (isHash ? 2 : 1) / ndims;
	out_datums = palloc(sizeof(Datum) * count * 2);
	out_nulls = palloc(sizeof(bool) * count * 2);

	it = JsonbIteratorInit(VARDATA(hs));

	while((r = JsonbIteratorGet(&it, &v, skipNested)) != 0)
	{
		skipNested = true;

		switch(r)
		{
			case WJB_ELEM:
				out_datums[i] = PointerGetDatum(JsonbValueToText(&v));
				out_nulls[i] = (DatumGetPointer(out_datums[i]) == NULL) ? true : false;
				i++;
				break;
			case WJB_KEY:
				out_datums[i * 2] = PointerGetDatum(JsonbValueToText(&v));
				out_nulls[i * 2] = (DatumGetPointer(out_datums[i * 2]) == NULL) ? true : false;
				break;
			case WJB_VALUE:
				out_datums[i * 2 + 1] = PointerGetDatum(JsonbValueToText(&v));
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
	Jsonb	   *hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	ArrayType  *out = hstore_to_array_internal(hs, 1);

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hstore_to_matrix);
Datum		hstore_to_matrix(PG_FUNCTION_ARGS);
Datum
hstore_to_matrix(PG_FUNCTION_ARGS)
{
	Jsonb	   *hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
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
	Jsonb			*hs;
	JsonbIterator	*it;
	MemoryContext	ctx;

	JsonbValue		init;
	int				path_len;
	int				level;
	struct {
		JsonbValue		v;
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
setup_firstcall(FuncCallContext *funcctx, Jsonb *hs, ArrayType *path,
				FunctionCallInfoData *fcinfo)
{
	MemoryContext 			oldcontext;
	SetReturningState	   *st;

	oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

	st = palloc(sizeof(*st));

	st->ctx = funcctx->multi_call_memory_ctx;

	st->hs = (Jsonb *) palloc(VARSIZE(hs));
	memcpy(st->hs, hs, VARSIZE(hs));
	if (JB_ISEMPTY(hs) || path)
		st->it = NULL;
	else
		st->it = JsonbIteratorInit(VARDATA(st->hs));

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

		st->init.type = jbvBinary;
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
JsonbIteratorGetCtx(SetReturningState *st, JsonbValue *v, bool skipNested)
{
	int 			r;
	MemoryContext	oldctx;

	oldctx = MemoryContextSwitchTo(st->ctx);
	r = JsonbIteratorGet(&st->it, v, skipNested);
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
	JsonbValue			v;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		st = setup_firstcall(funcctx,
							 (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0)),
							 NULL, NULL);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (SetReturningState *) funcctx->user_fctx;

	while(st->it && (r = JsonbIteratorGetCtx(st, &v, true)) != 0)
	{
		if (r == WJB_KEY || r == WJB_ELEM)
		{
			text	   *item = JsonbValueToText(&v);

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
	JsonbValue			v;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		st = setup_firstcall(funcctx, (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0)),
							 NULL, NULL);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (SetReturningState *) funcctx->user_fctx;

	while(st->it && (r = JsonbIteratorGetCtx(st, &v, true)) != 0)
	{
		if (r == WJB_VALUE || r == WJB_ELEM)
		{
			text	   *item = JsonbValueToText(&v);

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
	JsonbValue			v;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		st = setup_firstcall(funcctx, (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0)),
							 NULL, NULL);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (SetReturningState *) funcctx->user_fctx;

	while(st->it && (r = JsonbIteratorGetCtx(st, &v, true)) != 0)
	{
		if (r == WJB_VALUE || r == WJB_ELEM)
		{
			Jsonb	   *item = JsonbValueToJsonb(&v);

			if (item == NULL)
				SRF_RETURN_NEXT_NULL(funcctx);
			else
				SRF_RETURN_NEXT(funcctx, PointerGetDatum(item));
		}
	}

	SRF_RETURN_DONE(funcctx);
}

static JsonbValue*
getNextValsPath(SetReturningState *st)
{
	JsonbValue 		*v = NULL;

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
		if (st->path[st->level].v.type != jbvBinary)
		{
			st->level--;
			continue;
		}

		header = *(uint32*)st->path[st->level].v.binary.data;

		if (header & JB_FLAG_OBJECT)
		{
			if (st->path[st->level].varKind == pathAny)
			{
				v = getJsonbValue(st->path[st->level].v.binary.data,
								   JB_FLAG_OBJECT,
								   st->path[st->level].i++);
			}
			else
			{
				v = findUncompressedJsonbValue(st->path[st->level].v.binary.data,
												JB_FLAG_OBJECT, NULL,
												VARDATA_ANY(st->path[st->level].varStr),
												VARSIZE_ANY_EXHDR(st->path[st->level].varStr));
			}
		}
		else if (header & JB_FLAG_ARRAY)
		{
			if (st->path[st->level].varKind == pathAny)
			{
				v = getJsonbValue(st->path[st->level].v.binary.data,
								   JB_FLAG_ARRAY, st->path[st->level].i++);
			}
			else if (st->path[st->level].varKind == pathInt)
			{
				int	ith = st->path[st->level].varInt;

				if (ith < 0)
				{
					if (-ith > (int)(header & JB_COUNT_MASK))
					{
						st->level--;
						continue;
					}
					else
					{
						ith = ((int)(header & JB_COUNT_MASK)) + ith;
					}
				}
				else
				{
					if (ith >= (int)(header & JB_COUNT_MASK))
					{
						st->level--;
						continue;
					}
				}

				v = getJsonbValue(st->path[st->level].v.binary.data,
								   JB_FLAG_ARRAY, ith);
			}
			else
			{
				st->level--;
				continue;
			}
		}
		else
		{
			elog(ERROR, "wrong hstore container type");
		}

		if (v == NULL)
		{
			st->level--;
		}
		else if (st->level == st->path_len - 1)
		{
			if (st->path[st->level].varKind != pathAny)
			{
				st->path[st->level].v.type = jbvNull;
				st->level--;
			}
			break;
		}
		else
		{
			if (st->path[st->level].varKind != pathAny)
				st->path[st->level].v.type = jbvNull;
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
	JsonbValue			*v;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();

		st = setup_firstcall(funcctx, (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0)),
							 PG_GETARG_ARRAYTYPE_P(1), NULL);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (SetReturningState *) funcctx->user_fctx;

	if ((v = getNextValsPath(st)) != NULL)
	{
		text	*item = JsonbValueToText(v);

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
	JsonbValue 		*v;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();

		st = setup_firstcall(funcctx, (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0)),
							 PG_GETARG_ARRAYTYPE_P(1), NULL);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (SetReturningState *) funcctx->user_fctx;

	if ((v = getNextValsPath(st)) != NULL)
	{
		Jsonb	   *item = JsonbValueToJsonb(v);

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
	JsonbValue			v;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();

		st = setup_firstcall(funcctx,
							 (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0)),
							 NULL, fcinfo);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (SetReturningState *) funcctx->user_fctx;

	while(st->it && (r = JsonbIteratorGetCtx(st, &v, true)) != 0)
	{
		Datum		res,
					dvalues[2] = {0, 0};
		bool		nulls[2] = {false, false};
		text	   *item;
		HeapTuple	tuple;

		if (r == WJB_ELEM)
		{
			nulls[0] = true;

			item = JsonbValueToText(&v);
			if (item == NULL)
				nulls[1] = true;
			else
				dvalues[1] = PointerGetDatum(item);
		}
		else if (r == WJB_KEY)
		{
			item = JsonbValueToText(&v);
			dvalues[0] = PointerGetDatum(item);

			r = JsonbIteratorGetCtx(st, &v, true);
			Assert(r == WJB_VALUE);
			item = JsonbValueToText(&v);
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
	JsonbValue			v;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		st = setup_firstcall(funcctx, (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0)),
							 NULL, fcinfo);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (SetReturningState *) funcctx->user_fctx;

	while(st->it && (r = JsonbIteratorGetCtx(st, &v, true)) != 0)
	{
		Datum		res,
					dvalues[2] = {0, 0};
		bool		nulls[2] = {false, false};
		text	   *item;
		Jsonb		*hitem;
		HeapTuple	tuple;

		if (r == WJB_ELEM)
		{
			nulls[0] = true;

			hitem = JsonbValueToJsonb(&v);
			if (hitem == NULL)
				nulls[1] = true;
			else
				dvalues[1] = PointerGetDatum(hitem);
		}
		else if (r == WJB_KEY)
		{
			item = JsonbValueToText(&v);
			dvalues[0] = PointerGetDatum(item);

			r = JsonbIteratorGetCtx(st, &v, true);
			Assert(r == WJB_VALUE);
			hitem = JsonbValueToJsonb(&v);
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
deepContains(JsonbIterator **it1, JsonbIterator **it2)
{
	uint32			r1, r2;
	JsonbValue		v1, v2;
	bool			res = true;

	r1 = JsonbIteratorGet(it1, &v1, false);
	r2 = JsonbIteratorGet(it2, &v2, false);

	if (r1 != r2)
	{
		res = false;
	}
	else if (r1 == WJB_BEGIN_OBJECT)
	{
		uint32		lowbound = 0;
		JsonbValue	*v;

		for(;;) {
			r2 = JsonbIteratorGet(it2, &v2, false);
			if (r2 == WJB_END_OBJECT)
				break;

			Assert(r2 == WJB_KEY);

			v = findUncompressedJsonbValueByValue((*it1)->buffer,
												   JB_FLAG_OBJECT,
												   &lowbound, &v2);

			if (v == NULL)
			{
				res = false;
				break;
			}

			r2 = JsonbIteratorGet(it2, &v2, true);
			Assert(r2 == WJB_VALUE);

			if (v->type != v2.type)
			{
				res = false;
				break;
			}
			else if (v->type == jbvString || v->type == jbvNull ||
					 v->type == jbvBool || v->type == jbvNumeric)
			{
				if (compareJsonbValue(v, &v2) != 0)
				{
					res = false;
					break;
				}
			}
			else
			{
				JsonbIterator	*it1a, *it2a;

				Assert(v2.type == jbvBinary);
				Assert(v->type == jbvBinary);

				it1a = JsonbIteratorInit(v->binary.data);
				it2a = JsonbIteratorInit(v2.binary.data);

				if ((res = deepContains(&it1a, &it2a)) == false)
					break;
			}
		}
	}
	else if (r1 == WJB_BEGIN_ARRAY)
	{
		JsonbValue		*v;
		JsonbValue		*av = NULL;
		uint32			nelems = v1.array.nelems;

		for(;;) {
			r2 = JsonbIteratorGet(it2, &v2, true);
			if (r2 == WJB_END_ARRAY)
				break;

			Assert(r2 == WJB_ELEM);

			if (v2.type == jbvString || v2.type == jbvNull ||
				v2.type == jbvBool || v2.type == jbvNumeric)
			{
				v = findUncompressedJsonbValueByValue((*it1)->buffer,
													   JB_FLAG_ARRAY, NULL,
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
						r2 = JsonbIteratorGet(it1, &v1, true);
						Assert(r2 == WJB_ELEM);

						if (v1.type == jbvBinary)
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
					JsonbIterator	*it1a, *it2a;

					it1a = JsonbIteratorInit(av[i].binary.data);
					it2a = JsonbIteratorInit(v2.binary.data);

					res = deepContains(&it1a, &it2a);
				}

				if (res == false)
					break;
			}
		}
	}
	else
	{
		elog(ERROR, "wrong hstore container type");
	}

	return res;
}

PG_FUNCTION_INFO_V1(jsonb_contains);
Datum		jsonb_contains(PG_FUNCTION_ARGS);
Datum
jsonb_contains(PG_FUNCTION_ARGS)
{
	Jsonb	   		*val = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	Jsonb	   		*tmpl = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(1));

	bool			res = true;
	JsonbIterator	*it1, *it2;

	if (JB_ROOT_COUNT(val) < JB_ROOT_COUNT(tmpl) ||
		JB_ROOT_IS_OBJECT(val) != JB_ROOT_IS_OBJECT(tmpl))
		PG_RETURN_BOOL(false);

	it1 = JsonbIteratorInit(VARDATA(val));
	it2 = JsonbIteratorInit(VARDATA(tmpl));
	res = deepContains(&it1, &it2);

	PG_RETURN_BOOL(res);
}


PG_FUNCTION_INFO_V1(hstore_contained);
Datum		hstore_contained(PG_FUNCTION_ARGS);
Datum
hstore_contained(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(DirectFunctionCall2(jsonb_contains,
										PG_GETARG_DATUM(1),
										PG_GETARG_DATUM(0)
										));
}

/*
 * btree sort order for hstores isn't intended to be useful; we really only
 * care about equality versus non-equality.  we compare the entire string
 * buffer first, then the entry pos array.
 */

PG_FUNCTION_INFO_V1(jsonb_cmp);
Datum		jsonb_cmp(PG_FUNCTION_ARGS);
Datum
jsonb_cmp(PG_FUNCTION_ARGS)
{
	Jsonb	   		*hs1 = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	Jsonb	   		*hs2 = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(1));

	int				res;

	if (JB_ISEMPTY(hs1) || JB_ISEMPTY(hs2))
	{
		if (JB_ISEMPTY(hs1))
		{
			if (JB_ISEMPTY(hs2))
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
		res = compareJsonbBinaryValue(VARDATA(hs1), VARDATA(hs2));
	}

	/*
	 * this is a btree support function; this is one of the few places where
	 * memory needs to be explicitly freed.
	 */
	PG_FREE_IF_COPY(hs1, 0);
	PG_FREE_IF_COPY(hs2, 1);
	PG_RETURN_INT32(res);
}


PG_FUNCTION_INFO_V1(jsonb_eq);
Datum		jsonb_eq(PG_FUNCTION_ARGS);
Datum
jsonb_eq(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(jsonb_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res == 0);
}

PG_FUNCTION_INFO_V1(jsonb_ne);
Datum		jsonb_ne(PG_FUNCTION_ARGS);
Datum
jsonb_ne(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(jsonb_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res != 0);
}

PG_FUNCTION_INFO_V1(jsonb_gt);
Datum		jsonb_gt(PG_FUNCTION_ARGS);
Datum
jsonb_gt(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(jsonb_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res > 0);
}

PG_FUNCTION_INFO_V1(jsonb_ge);
Datum		jsonb_ge(PG_FUNCTION_ARGS);
Datum
jsonb_ge(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(jsonb_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res >= 0);
}

PG_FUNCTION_INFO_V1(jsonb_lt);
Datum		jsonb_lt(PG_FUNCTION_ARGS);
Datum
jsonb_lt(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(jsonb_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res < 0);
}

PG_FUNCTION_INFO_V1(jsonb_le);
Datum		jsonb_le(PG_FUNCTION_ARGS);
Datum
jsonb_le(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(jsonb_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res <= 0);
}


PG_FUNCTION_INFO_V1(hstore_hash);
Datum		hstore_hash(PG_FUNCTION_ARGS);
Datum
hstore_hash(PG_FUNCTION_ARGS)
{
	Jsonb	   	*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));

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
	Jsonb	   		*hs = (Jsonb*) PG_DETOAST_DATUM(PG_GETARG_POINTER(0));
	JsonbIterator	*it;
	JsonbValue		v;
	uint32			r;

	if (JB_ISEMPTY(hs))
		PG_RETURN_NULL();

	it = JsonbIteratorInit(VARDATA(hs));
	r = JsonbIteratorGet(&it, &v, false);

	switch(r)
	{
		case WJB_BEGIN_ARRAY:
			if (v.array.scalar)
			{
				Assert(v.array.nelems == 1);
				r = JsonbIteratorGet(&it, &v, false);
				Assert(r == WJB_ELEM);

				switch(v.type)
				{
					case jbvNull:
						PG_RETURN_TEXT_P(cstring_to_text("null"));
					case jbvBool:
						PG_RETURN_TEXT_P(cstring_to_text("bool"));
					case jbvNumeric:
						PG_RETURN_TEXT_P(cstring_to_text("numeric"));
					case jbvString:
						PG_RETURN_TEXT_P(cstring_to_text("string"));
					default:
						elog(ERROR, "bogus hstore");
				}
			}
			else
			{
				PG_RETURN_TEXT_P(cstring_to_text("array"));
			}
		case WJB_BEGIN_OBJECT:
			PG_RETURN_TEXT_P(cstring_to_text("hash"));
		case 0:
			PG_RETURN_NULL();
		default:
			elog(ERROR, "bogus hstore");
	}

	PG_RETURN_NULL();
}
