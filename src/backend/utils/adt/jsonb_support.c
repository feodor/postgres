/*-------------------------------------------------------------------------
 *
 * jsonb_support.c
 *	  Support functions for jsonb
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 *
 * src/backend/utils/adt/jsonb_support.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/jsonb.h"

#define JENTRY_ISSTRING		(0x00000000)
#define JENTRY_ISNUMERIC	(0x10000000)
#define JENTRY_ISNEST		(0x20000000)
#define JENTRY_ISNULL		(0x40000000)
#define JENTRY_ISBOOL		(0x10000000 | 0x20000000)
#define JENTRY_ISFALSE		JENTRY_ISBOOL
#define JENTRY_ISTRUE		(0x10000000 | 0x20000000 | 0x40000000)

/* Note possible multiple evaluations, also access to prior array element */
#define JBE_ISFIRST(he_)		(((he_).header & JENTRY_ISFIRST) != 0)
#define JBE_ISSTRING(he_)		(((he_).header & JENTRY_TYPEMASK) == JENTRY_ISSTRING)
#define JBE_ISNUMERIC(he_)		(((he_).header & JENTRY_TYPEMASK) == JENTRY_ISNUMERIC)
#define JBE_ISNEST(he_)			(((he_).header & JENTRY_TYPEMASK) == JENTRY_ISNEST)
#define JBE_ISNULL(he_)			(((he_).header & JENTRY_TYPEMASK) == JENTRY_ISNULL)
#define JBE_ISBOOL(he_)			(((he_).header & JENTRY_TYPEMASK & JENTRY_ISBOOL) == JENTRY_ISBOOL)
#define JBE_ISBOOL_TRUE(he_)	(((he_).header & JENTRY_TYPEMASK) == JENTRY_ISTRUE)
#define JBE_ISBOOL_FALSE(he_)	(JBE_ISBOOL(he_) && !JBE_ISBOOL_TRUE(he_))

/*
 * State used while converting an arbitrary JsonbValue into a Jsonb value
 * (4-byte varlena uncompressed representation of a Jsonb)
 */
typedef struct ConvertState
{
	/* Preallocated buffer in which to form varlena/Jsonb value */
	Jsonb			   *buffer;
	/* Pointer into buffer */
	char			   *ptr;

	struct
	{
		uint32		i;
		uint32	   *header;
		JEntry	   *meta;
		char	   *begin;
	}		   *levelstate,
		*curlptr,
		*prvlptr;

	/* Current size of buffer holding levelstate array */
	Size		levelSz;

}	ConvertState;

static int lexicalCompareJsonbStringValue(const void *a, const void *b);
static Size convertJsonb(JsonbValue * v, Jsonb* buffer);
static void walkJsonbValueConversion(JsonbValue * value, ConvertState * state,
									 uint32 nestlevel);
static void putJsonbValueConversion(ConvertState * state, JsonbValue * value,
									uint32 flags, uint32 level);
static void putStringConversion(ConvertState * state, JsonbValue * value,
								uint32 level, uint32 i);
static void parseBuffer(JsonbIterator * it, char *buffer);
static bool formAnswer(JsonbIterator ** it, JsonbValue * v, JEntry * e,
					   bool skipNested);
static JsonbIterator *freeAndGetNext(JsonbIterator * it);
static ToJsonbState *pushState(ToJsonbState ** state);
static void appendKey(ToJsonbState * state, JsonbValue * v);
static void appendValue(ToJsonbState * state, JsonbValue * v);
static void appendElement(ToJsonbState * state, JsonbValue * v);
static int lengthCompareJsonbStringValue(const void *a, const void *b, void *arg);
static int lengthCompareJsonbPair(const void *a, const void *b, void *arg);
static void uniqueifyJsonbObject(JsonbValue * v);
static void uniqueifyJsonbArray(JsonbValue * v);

/*
 * Turn a JsonbValue into a Jsonb
 */
Jsonb *
JsonbValueToJsonb(JsonbValue * v)
{
	Jsonb	   *out;

	if (v == NULL)
	{
		out = NULL;
	}
	else if (v->type >= jbvNull && v->type < jbvArray)
	{
		/* Scalar value */
		ToJsonbState *state = NULL;
		JsonbValue *res;
		Size		sz;
		JsonbValue	scalarArray;

		scalarArray.type = jbvArray;
		scalarArray.array.scalar = true;
		scalarArray.array.nElems = 1;

		pushJsonbValue(&state, WJB_BEGIN_ARRAY, &scalarArray);
		pushJsonbValue(&state, WJB_ELEM, v);
		res = pushJsonbValue(&state, WJB_END_ARRAY, NULL);

		out = palloc(VARHDRSZ + res->size);
		sz = convertJsonb(res, out);
		Assert(sz <= res->size);
		SET_VARSIZE(out, sz + VARHDRSZ);
	}
	else if (v->type == jbvObject || v->type == jbvArray)
	{
		uint32		sz;

		out = palloc(VARHDRSZ + v->size);
		sz = convertJsonb(v, out);
		Assert(sz <= v->size);
		SET_VARSIZE(out, VARHDRSZ + sz);
	}
	else
	{
		Assert(v->type == jbvBinary);

		out = palloc(VARHDRSZ + v->binary.len);
		SET_VARSIZE(out, VARHDRSZ + v->binary.len);
		memcpy(VARDATA(out), v->binary.data, v->binary.len);
	}

	return out;
}

/*
 * Are two JsonbValues a and b equal?
 *
 * Does not use lexical comparisons.  Therefore, it is essentially that this
 * never be used for anything other than searching for values within a single
 * jsonb.
 *
 * We return bool because we don't want to give anyone any ideas about using
 * this for sorting.  This is just for "contains" style searching.
 */
bool
compareJsonbValue(JsonbValue * a, JsonbValue * b)
{
	int			i;

	check_stack_depth();

	if (a->type == b->type)
	{
		switch (a->type)
		{
			case jbvNull:
				return true;
			case jbvString:
				return lengthCompareJsonbStringValue(a, b, NULL) == 0;
			case jbvBool:
				return a->boolean == b->boolean;
			case jbvNumeric:
				return DatumGetBool(DirectFunctionCall2(numeric_eq,
														PointerGetDatum(a->numeric),
														PointerGetDatum(b->numeric)));
			case jbvArray:
				if (a->array.nElems == b->array.nElems)
				{
					for (i = 0; i < a->array.nElems; i++)
						if (compareJsonbValue(a->array.elems + i,
											  b->array.elems + i))
							return true;
				}
				break;
			case jbvObject:
				if (a->object.nPairs == b->object.nPairs)
				{
					for (i = 0; i < a->object.nPairs; i++)
					{
						if (lengthCompareJsonbStringValue(&a->object.pairs[i].key,
														  &b->object.pairs[i].key,
														  NULL) == 0)
							return true;
						if (compareJsonbValue(&a->object.pairs[i].value,
											  &b->object.pairs[i].value))
							return true;
					}
				}
				break;
			case jbvBinary:
				/* This wastes a few cycles on unneeded lexical comparisons */
				return compareJsonbSuperHeaderValue(a->binary.data, b->binary.data) == 0;
			default:
				elog(ERROR, "invalid jsonb scalar type");
		}
	}

	return false;
}

/*
 * Gives consistent ordering of Jsonb values.
 *
 * Strings are compared lexically, making this suitable as a sort comparator.
 *
 * This is called from B-Tree support function 1.
 */
int
compareJsonbSuperHeaderValue(JsonbSuperHeader a, JsonbSuperHeader b)
{
	JsonbIterator *it1,
			   *it2;
	int			res = 0;

	it1 = JsonbIteratorInit(a);
	it2 = JsonbIteratorInit(b);

	do
	{
		JsonbValue	v1,
					v2;
		int			r1,
					r2;

		r1 = JsonbIteratorNext(&it1, &v1, false);
		r2 = JsonbIteratorNext(&it2, &v2, false);

		if (r1 == r2)
		{
			if (r1 == 0)
				break;			/* equal */

			if (v1.type == v2.type)
			{
				switch (v1.type)
				{
					case jbvString:
						res = lexicalCompareJsonbStringValue(&v1, &v2);
						break;
					case jbvBool:
						if (v1.boolean == v2.boolean)
							res = 0;
						else
							res = (v1.boolean > v2.boolean) ? 1 : -1;
						break;
					case jbvNumeric:
						res = DatumGetInt32(DirectFunctionCall2(numeric_cmp,
																PointerGetDatum(v1.numeric),
																PointerGetDatum(v2.numeric)));
						break;
					case jbvArray:
						if (v1.array.nElems != v2.array.nElems)
							res = (v1.array.nElems > v2.array.nElems) ? 1 : -1;
						break;
					case jbvObject:
						if (v1.object.nPairs != v2.object.nPairs)
							res = (v1.object.nPairs > v2.object.nPairs) ? 1 : -1;
						break;
					default:
						break;
				}
			}
			else
			{
				res = (v1.type > v2.type) ? 1 : -1;		/* dummy order */
			}
		}
		else
		{
			res = (r1 > r2) ? 1 : -1;	/* dummy order */
		}
	}
	while (res == 0);

	return res;
}

/*
 * findJsonbValueFromSuperHeader() wrapper that sets up JsonbValue key.
 */
JsonbValue *
findJsonbValueFromSuperHeaderLen(JsonbSuperHeader sheader, uint32 flags,
								 uint32 *lowbound, char *key, uint32 keylen)
{
	JsonbValue	v;

	if (key == NULL)
	{
		v.type = jbvNull;
	}
	else
	{
		v.type = jbvString;
		v.string.val = key;
		v.string.len = keylen;
	}

	return findJsonbValueFromSuperHeader(sheader, flags, lowbound, &v);
}

/*
 * Find string key in object or element by value in array.
 *
 * Returns palloc()'d copy of value.
 */
JsonbValue *
findJsonbValueFromSuperHeader(JsonbSuperHeader sheader, uint32 flags,
							  uint32 *lowbound, JsonbValue * key)
{
	uint32		superheader = *(uint32 *) sheader;
	JEntry	   *array = (JEntry *) (sheader + sizeof(uint32));
	JsonbValue *r = palloc(sizeof(JsonbValue));

	Assert((superheader & (JB_FLAG_ARRAY | JB_FLAG_OBJECT)) !=
		   (JB_FLAG_ARRAY | JB_FLAG_OBJECT));

	if (flags & JB_FLAG_ARRAY & superheader)
	{
		char	   *data = (char *) (array + (superheader & JB_COUNT_MASK));
		int			i;

		for (i = (lowbound) ? *lowbound : 0; i < (superheader & JB_COUNT_MASK); i++)
		{
			JEntry	   *e = array + i;

			if (JBE_ISNULL(*e) && key->type == jbvNull)
			{
				r->type = jbvNull;
				if (lowbound)
					*lowbound = i;
				r->size = sizeof(JEntry);

				return r;
			}
			else if (JBE_ISSTRING(*e) && key->type == jbvString)
			{
				/* Equivalent to lengthCompareJsonbStringValue() */
				if (key->string.len == JBE_LEN(*e) &&
					memcmp(key->string.val, data + JBE_OFF(*e),
						   key->string.len) == 0)
				{
					r->type = jbvString;
					r->string.val = data + JBE_OFF(*e);
					r->string.len = key->string.len;
					r->size = sizeof(JEntry) + r->string.len;
					if (lowbound)
						*lowbound = i;

					return r;
				}
			}
			else if (JBE_ISBOOL(*e) && key->type == jbvBool)
			{
				if ((JBE_ISBOOL_TRUE(*e) && key->boolean) ||
					(JBE_ISBOOL_FALSE(*e) && !key->boolean))
				{
					/* Deep copy */
					*r = *key;
					r->size = sizeof(JEntry);
					if (lowbound)
						*lowbound = i;

					return r;
				}
			}
			else if (JBE_ISNUMERIC(*e) && key->type == jbvNumeric)
			{
				Numeric entry = (Numeric) (data + INTALIGN(JBE_OFF(*e)));

				if (DatumGetBool(DirectFunctionCall2(numeric_eq,
													 PointerGetDatum(entry),
													 PointerGetDatum(key->numeric))))
				{
					r->type = jbvNumeric;
					r->numeric = entry;

					if (lowbound)
						*lowbound = i;

					return r;
				}
			}
		}
	}
	else if (flags & JB_FLAG_OBJECT & superheader)
	{
		char	   *data = (char *) (array + (superheader & JB_COUNT_MASK) * 2);
		uint32		stopLow = lowbound ? *lowbound : 0,
					stopHigh = (superheader & JB_COUNT_MASK),
					stopMiddle;

		/* Object keys must be strings */
		Assert(key->type == jbvString);

		/*
		 * Binary search for matching jsonb value
		 */
		while (stopLow < stopHigh)
		{
			JEntry *e;
			int		difference;

			stopMiddle = stopLow + (stopHigh - stopLow) / 2;

			e = array + stopMiddle * 2;

			/* Equivalent to lengthCompareJsonbStringValue() */
			if (key->string.len == JBE_LEN(*e))
				difference = memcmp(data + JBE_OFF(*e), key->string.val,
									key->string.len);
			else
				difference = (JBE_LEN(*e) > key->string.len) ? 1 : -1;

			if (difference == 0)
			{
				JEntry	   *v = e + 1;

				if (lowbound)
					*lowbound = stopMiddle + 1;

				if (JBE_ISSTRING(*v))
				{
					r->type = jbvString;
					r->string.val = data + JBE_OFF(*v);
					r->string.len = JBE_LEN(*v);
					r->size = sizeof(JEntry) + r->string.len;
				}
				else if (JBE_ISBOOL(*v))
				{
					r->type = jbvBool;
					r->boolean = (JBE_ISBOOL_TRUE(*v)) != 0;
					r->size = sizeof(JEntry);
				}
				else if (JBE_ISNUMERIC(*v))
				{
					r->type = jbvNumeric;
					r->numeric = (Numeric) (data + INTALIGN(JBE_OFF(*v)));

					r->size = 2 * sizeof(JEntry) + VARSIZE_ANY(r->numeric);
				}
				else if (JBE_ISNULL(*v))
				{
					r->type = jbvNull;
					r->size = sizeof(JEntry);
				}
				else
				{
					r->type = jbvBinary;
					r->binary.data = data + INTALIGN(JBE_OFF(*v));
					r->binary.len = JBE_LEN(*v) -
						(INTALIGN(JBE_OFF(*v)) - JBE_OFF(*v));
					r->size = 2 * sizeof(JEntry) + r->binary.len;
				}

				return r;
			}
			else if (difference < 0)
			{
				stopLow = stopMiddle + 1;
			}
			else
			{
				stopHigh = stopMiddle;
			}
		}

		if (lowbound)
			*lowbound = stopLow;
	}

	return NULL;
}

/*
 * Get i-th value of array or object.
 *
 * Returns palloc()'d copy of value.
 */
JsonbValue *
getIthJsonbValueFromSuperHeader(JsonbSuperHeader sheader, uint32 flags,
								int32 i)
{
	uint32		superheader = *(uint32 *) sheader;
	JsonbValue *r;
	JEntry	   *array,
			   *e;
	char	   *data;

	r = palloc(sizeof(JsonbValue));

	Assert((superheader & (JB_FLAG_ARRAY | JB_FLAG_OBJECT)) !=
		   (JB_FLAG_ARRAY | JB_FLAG_OBJECT));

	Assert(i >= 0);

	if (i >= (superheader & JB_COUNT_MASK))
		return NULL;

	array = (JEntry *) (sheader + sizeof(uint32));

	if (flags & JB_FLAG_ARRAY & superheader)
	{
		e = array + i;
		data = (char *) (array + (superheader & JB_COUNT_MASK));
	}
	else if (flags & JB_FLAG_OBJECT & superheader)
	{
		e = array + i * 2 + 1;
		data = (char *) (array + (superheader & JB_COUNT_MASK) * 2);
	}
	else
	{
		return NULL;
	}

	if (JBE_ISSTRING(*e))
	{
		r->type = jbvString;
		r->string.val = data + JBE_OFF(*e);
		r->string.len = JBE_LEN(*e);
		r->size = sizeof(JEntry) + r->string.len;
	}
	else if (JBE_ISBOOL(*e))
	{
		r->type = jbvBool;
		r->boolean = (JBE_ISBOOL_TRUE(*e)) != 0;
		r->size = sizeof(JEntry);
	}
	else if (JBE_ISNUMERIC(*e))
	{
		r->type = jbvNumeric;
		r->numeric = (Numeric) (data + INTALIGN(JBE_OFF(*e)));

		r->size = 2 * sizeof(JEntry) + VARSIZE_ANY(r->numeric);
	}
	else if (JBE_ISNULL(*e))
	{
		r->type = jbvNull;
		r->size = sizeof(JEntry);
	}
	else
	{
		r->type = jbvBinary;
		r->binary.data = data + INTALIGN(JBE_OFF(*e));
		r->binary.len = JBE_LEN(*e) - (INTALIGN(JBE_OFF(*e)) - JBE_OFF(*e));
		r->size = r->binary.len + 2 * sizeof(JEntry);
	}

	return r;
}

/*
 * Push JsonbValue into ToJsonbState.
 *
 * With r = WJB_END_OBJECT and v = NULL, this function sorts and unique-ifys
 * the passed object's key values.  Otherwise, they are assumed to already be
 * sorted and unique.
 *
 * Initial state of *ToJsonbState is NULL.
 */
JsonbValue *
pushJsonbValue(ToJsonbState ** state, int r, JsonbValue * v)
{
	JsonbValue *h = NULL;

	switch (r)
	{
		case WJB_BEGIN_ARRAY:
			*state = pushState(state);
			h = &(*state)->v;
			(*state)->v.type = jbvArray;
			(*state)->v.size = 3 * sizeof(JEntry);
			(*state)->v.array.nElems = 0;
			(*state)->v.array.scalar = (v && v->array.scalar) != 0;
			(*state)->size = (v && v->type == jbvArray && v->array.nElems > 0)
				? v->array.nElems : 4;
			(*state)->v.array.elems = palloc(sizeof(*(*state)->v.array.elems) *
											 (*state)->size);
			break;
		case WJB_BEGIN_OBJECT:
			*state = pushState(state);
			h = &(*state)->v;
			(*state)->v.type = jbvObject;
			(*state)->v.size = 3 * sizeof(JEntry);
			(*state)->v.object.nPairs = 0;
			(*state)->size = (v && v->type == jbvObject && v->object.nPairs > 0) ?
				v->object.nPairs : 4;
			(*state)->v.object.pairs = palloc(sizeof(*(*state)->v.object.pairs) *
											  (*state)->size);
			break;
		case WJB_KEY:
			Assert(v->type == jbvString);
			appendKey(*state, v);
			break;
		case WJB_VALUE:
			Assert((v->type >= jbvNull && v->type < jbvArray) || v->type == jbvBinary);
			appendValue(*state, v);
			break;
		case WJB_ELEM:
			Assert((v->type >= jbvNull && v->type < jbvArray) || v->type == jbvBinary);
			appendElement(*state, v);
			break;
		case WJB_END_OBJECT:
			h = &(*state)->v;
			/*
			 * When v != NULL and control reaches here, keys should already be
			 * sorted
			 */
			if (v == NULL)
				uniqueifyJsonbObject(h);

			/*
			 * No break statement here - fall through and perform those steps
			 * required for the WJB_END_ARRAY case too.  The end of a jsonb
			 * "object" associative structure may require us to first
			 * unique-ify its values, but values must then be appended to state
			 * in the same fashion as arrays.
			 */
		case WJB_END_ARRAY:
			h = &(*state)->v;

			/*
			 * Pop stack and push current array/"object" as value in parent
			 * array/"object"
			 */
			*state = (*state)->next;
			if (*state)
			{
				switch ((*state)->v.type)
				{
					case jbvArray:
						appendElement(*state, h);
						break;
					case jbvObject:
						appendValue(*state, h);
						break;
					default:
						elog(ERROR, "invalid jsonb container type");
				}
			}
			break;
		default:
			elog(ERROR, "invalid jsonb container type");
	}

	return h;
}

/*
 * Given a Jsonb superheader, expand to JsonbIterator to iterate over items
 * fully expanded to in-memory representation for manipulation.
 */
JsonbIterator *
JsonbIteratorInit(JsonbSuperHeader sheader)
{
	JsonbIterator *it = palloc(sizeof(JsonbIterator));

	parseBuffer(it, sheader);
	it->next = NULL;

	return it;
}

/*
 * Get next JsonbValue while iterating
 */
int
JsonbIteratorNext(JsonbIterator ** it, JsonbValue * v, bool skipNested)
{
	iterState	state;

	check_stack_depth();

	if (*it == NULL)
		return 0;

	state = (*it)->state;

	if ((*it)->containerType == JB_FLAG_ARRAY)
	{
		if (state == jbi_start)
		{
			v->type = jbvArray;
			v->array.nElems = (*it)->nElems;
			v->array.scalar = (*it)->isScalar;
			(*it)->i = 0;
			/* Set state for next call */
			(*it)->state = jbi_elem;
			return WJB_BEGIN_ARRAY;
		}
		else if (state == jbi_elem)
		{
			if ((*it)->i >= (*it)->nElems)
			{
				*it = freeAndGetNext(*it);
				return WJB_END_ARRAY;
			}
			else if (formAnswer(it, v, &(*it)->meta[(*it)->i++], skipNested))
			{
				return JsonbIteratorNext(it, v, skipNested);
			}
			else
			{
				return WJB_ELEM;
			}
		}
	}
	else if ((*it)->containerType == JB_FLAG_OBJECT)
	{
		if (state == jbi_start)
		{
			v->type = jbvObject;
			v->object.nPairs = (*it)->nElems;
			(*it)->i = 0;
			/* Set state for next call */
			(*it)->state = jbi_key;
			return WJB_BEGIN_OBJECT;
		}
		else if (state == jbi_key)
		{
			if ((*it)->i >= (*it)->nElems)
			{
				/* Have caller deal with "next" iterator next call */
				*it = freeAndGetNext(*it);
				/* Set state for next call */
				return WJB_END_OBJECT;
			}
			else
			{
				formAnswer(it, v, &(*it)->meta[(*it)->i * 2], false);
				/* Set state for next call */
				(*it)->state = jbi_value;
				return WJB_KEY;
			}
		}
		else if (state == jbi_value)
		{
			/* Set state for next call (may be recursive) */
			(*it)->state = jbi_key;
			if (formAnswer(it, v, &(*it)->meta[((*it)->i++) * 2 + 1], skipNested))
				return JsonbIteratorNext(it, v, skipNested);
			else
				return WJB_VALUE;
		}
	}

	elog(ERROR, "unknown iterator res");
}

/*
 * Convert a Postgres text array to a JsonbSortedArray, with de-duplicated key
 * elements.
 */
JsonbValue *
arrayToJsonbSortedArray(ArrayType *a)
{
	Datum	   *key_datums;
	bool	   *key_nulls;
	int			key_count;
	JsonbValue *result;
	int			i,
				j;

	/* Extract data for sorting */
	deconstruct_array(a, TEXTOID, -1, false, 'i', &key_datums, &key_nulls,
					  &key_count);

	if (key_count == 0)
		return NULL;

	/*
	 * A text array uses at least eight bytes per element, so any overflow in
	 * "key_count * sizeof(JsonbPair)" is small enough for palloc() to catch.
	 * However, credible improvements to the array format could invalidate that
	 * assumption.  Therefore, use an explicit check rather than relying on
	 * palloc() to complain.
	 */
	if (key_count > MaxAllocSize / sizeof(JsonbPair))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("number of pairs (%d) exceeds the maximum allowed (%zu)",
						key_count, MaxAllocSize / sizeof(JsonbPair))));

	result = palloc(sizeof(JsonbValue));
	result->type = jbvArray;
	result->array.scalar = false;
	result->array.elems = palloc(sizeof(*result->object.pairs) * key_count);

	for (i = 0, j = 0; i < key_count; i++)
	{
		if (!key_nulls[i])
		{
			result->array.elems[j].type = jbvString;
			result->array.elems[j].string.val = VARDATA(key_datums[i]);
			result->array.elems[j].string.len = VARSIZE(key_datums[i]) - VARHDRSZ;
			j++;
		}
	}
	result->array.nElems = j;

	uniqueifyJsonbArray(result);
	return result;
}

/*
 * Standard lexical qsort() comparator of jsonb strings.
 *
 * Sorts strings lexically, using the default database collation.  Used by
 * B-Tree operators, where a lexical sort order is generally expected.
 */
static int
lexicalCompareJsonbStringValue(const void *a, const void *b)
{
	const JsonbValue *va = (const JsonbValue *) a;
	const JsonbValue *vb = (const JsonbValue *) b;

	Assert(va->type == jbvString);
	Assert(vb->type == jbvString);

	return varstr_cmp(va->string.val, va->string.len, vb->string.val,
					  vb->string.len, DEFAULT_COLLATION_OID);
}

/*
 * Put JsonbValue tree into a preallocated buffer Jsonb buffer
 */
static Size
convertJsonb(JsonbValue * v, Jsonb *buffer)
{
	uint32		l = 0;
	ConvertState state;

	/* Should not already have binary representation */
	Assert(v->type != jbvBinary);

	state.buffer = buffer;
	/* Start from superheader */
	state.ptr = VARDATA(state.buffer);
	state.levelSz = 8;
	state.levelstate = palloc(sizeof(*state.levelstate) * state.levelSz);

	walkJsonbValueConversion(v, &state, 0);

	l = state.ptr - VARDATA(state.buffer);

	Assert(l <= v->size);
	return l;
}

/*
 * Walk the tree representation of Jsonb, as part of the process of converting
 * a JsonbValue to a Jsonb
 */
static void
walkJsonbValueConversion(JsonbValue * value, ConvertState * state,
						 uint32 nestlevel)
{
	int			i;

	check_stack_depth();

	if (!value)
		return;

	switch (value->type)
	{
		case jbvArray:
			putJsonbValueConversion(state, value, WJB_BEGIN_ARRAY, nestlevel);
			for (i = 0; i < value->array.nElems; i++)
			{
				if ((value->array.elems[i].type >= jbvNull &&
					value->array.elems[i].type < jbvArray) ||
					value->array.elems[i].type == jbvBinary)
					putJsonbValueConversion(state, value->array.elems + i,
											WJB_ELEM, nestlevel);
				else
					walkJsonbValueConversion(value->array.elems + i, state,
											 nestlevel + 1);
			}
			putJsonbValueConversion(state, value, WJB_END_ARRAY, nestlevel);
			break;
		case jbvObject:
			putJsonbValueConversion(state, value, WJB_BEGIN_OBJECT, nestlevel);

			for (i = 0; i < value->object.nPairs; i++)
			{
				putJsonbValueConversion(state, &value->object.pairs[i].key,
										WJB_KEY, nestlevel);

				if ((value->object.pairs[i].value.type >= jbvNull &&
					value->object.pairs[i].value.type < jbvArray) ||
					value->object.pairs[i].value.type == jbvBinary)
					putJsonbValueConversion(state,
											&value->object.pairs[i].value,
											WJB_VALUE, nestlevel);
				else
					walkJsonbValueConversion(&value->object.pairs[i].value,
											 state, nestlevel + 1);
			}

			putJsonbValueConversion(state, value, WJB_END_OBJECT, nestlevel);
			break;
		default:
			elog(ERROR, "unknown type of jsonb container");
	}
}

/*
 * As part of the process of converting an arbitrary JsonbValue to a Jsonb,
 * copy an arbitrary individual JsonbValue.  This function may copy over any
 * type of value, even containers (Objects/arrays).  However, it is not
 * responsible for recursive aspects of walking the tree (so only top-level
 * Object/array details are handled).  No details about their
 * keys/values/elements are touched.  The function is called separately for the
 * start of an Object/Array, and the end.
 *
 * This is a worker function for walkJsonbValueConversion().
 */
static void
putJsonbValueConversion(ConvertState * state, JsonbValue * value, uint32 flags,
						uint32 level)
{
	if (level == state->levelSz)
	{
		state->levelSz *= 2;
		state->levelstate = repalloc(state->levelstate,
							   sizeof(*state->levelstate) * state->levelSz);
	}

	state->curlptr = state->levelstate + level;

	if (flags & (WJB_BEGIN_ARRAY | WJB_BEGIN_OBJECT))
	{
		short		padlen, p;

		Assert(((flags & WJB_BEGIN_ARRAY) && value->type == jbvArray) ||
			   ((flags & WJB_BEGIN_OBJECT) && value->type == jbvObject));

		state->curlptr->begin = state->ptr;

		padlen = INTALIGN(state->ptr - VARDATA(state->buffer)) -
			(state->ptr - VARDATA(state->buffer));

		/*
		 * Add padding as necessary
		 */
		for (p = padlen; p > 0; p--)
		{
			*state->ptr = '\0';
			state->ptr++;
		}

		state->curlptr->header = (uint32 *) state->ptr;
		/* Advance past header */
		state->ptr += sizeof(* state->curlptr->header);

		state->curlptr->meta = (JEntry *) state->ptr;
		state->curlptr->i = 0;

		if (value->type == jbvArray)
		{
			*state->curlptr->header = value->array.nElems | JB_FLAG_ARRAY;
			state->ptr += sizeof(JEntry) * value->array.nElems;

			if (value->array.scalar)
			{
				Assert(value->array.nElems == 1);
				Assert(level == 0);
				*state->curlptr->header |= JB_FLAG_SCALAR;
			}
		}
		else
		{
			*state->curlptr->header = value->object.nPairs | JB_FLAG_OBJECT;
			state->ptr += sizeof(JEntry) * value->object.nPairs * 2;
		}
	}
	else if (flags & WJB_ELEM)
	{
		putStringConversion(state, value, level, state->curlptr->i);
		state->curlptr->i++;
	}
	else if (flags & WJB_KEY)
	{
		Assert(value->type == jbvString);

		putStringConversion(state, value, level, state->curlptr->i * 2);
	}
	else if (flags & WJB_VALUE)
	{
		putStringConversion(state, value, level, state->curlptr->i * 2 + 1);
		state->curlptr->i++;
	}
	else if (flags & (WJB_END_ARRAY | WJB_END_OBJECT))
	{
		uint32		len,
					i;

		Assert(((flags & WJB_END_ARRAY) && value->type == jbvArray) ||
			   ((flags & WJB_END_OBJECT) && value->type == jbvObject));

		if (level == 0)
			return;

		len = state->ptr - (char *) state->curlptr->begin;

		state->prvlptr = state->curlptr - 1;

		if (*state->prvlptr->header & JB_FLAG_ARRAY)
		{
			i = state->prvlptr->i;

			state->prvlptr->meta[i].header = JENTRY_ISNEST;

			if (i == 0)
				state->prvlptr->meta[0].header |= JENTRY_ISFIRST | len;
			else
				state->prvlptr->meta[i].header |=
					(state->prvlptr->meta[i - 1].header & JENTRY_POSMASK) + len;
		}
		else if (*state->prvlptr->header & JB_FLAG_OBJECT)
		{
			i = 2 * state->prvlptr->i + 1;		/* Value, not key */

			state->prvlptr->meta[i].header = JENTRY_ISNEST;

			state->prvlptr->meta[i].header |=
				(state->prvlptr->meta[i - 1].header & JENTRY_POSMASK) + len;
		}
		else
		{
			elog(ERROR, "invalid jsonb container type");
		}

		Assert(state->ptr - state->curlptr->begin <= value->size);
		state->prvlptr->i++;
	}
	else
	{
		elog(ERROR, "unknown flag encountered during jsonb tree walk");
	}
}

/*
 * As part of the process of converting an arbitrary JsonbValue to a Jsonb,
 * copy a string associated with a scalar value.
 *
 * This is a worker function for putJsonbValueConversion() (itself a worker for
 * walkJsonbValueConversion()), handling aspects of copying strings in respect
 * of all scalar values.  It handles the details with regard to Jentry metadata
 * within convert state.
 */
static void
putStringConversion(ConvertState * state, JsonbValue * value,
					uint32 level, uint32 i)
{
	short		p, padlen;

	state->curlptr = state->levelstate + level;

	if (i == 0)
		state->curlptr->meta[0].header = JENTRY_ISFIRST;
	else
		state->curlptr->meta[i].header = 0;

	switch (value->type)
	{
		case jbvNull:
			state->curlptr->meta[i].header |= JENTRY_ISNULL;

			if (i > 0)
				state->curlptr->meta[i].header |=
					state->curlptr->meta[i - 1].header & JENTRY_POSMASK;
			break;
		case jbvString:
			memcpy(state->ptr, value->string.val, value->string.len);
			state->ptr += value->string.len;

			if (i == 0)
				state->curlptr->meta[i].header |= value->string.len;
			else
				state->curlptr->meta[i].header |=
					(state->curlptr->meta[i - 1].header & JENTRY_POSMASK) +
					value->string.len;
			break;
		case jbvBool:
			state->curlptr->meta[i].header |= (value->boolean) ?
				JENTRY_ISTRUE : JENTRY_ISFALSE;

			if (i > 0)
				state->curlptr->meta[i].header |=
					state->curlptr->meta[i - 1].header & JENTRY_POSMASK;
			break;
		case jbvNumeric:
			{
				int numlen = VARSIZE_ANY(value->numeric);

				padlen = INTALIGN(state->ptr - VARDATA(state->buffer)) -
					(state->ptr - VARDATA(state->buffer));

				/*
				 * Add padding as necessary
				 */
				for (p = padlen; p > 0; p--)
				{
					*state->ptr = '\0';
					state->ptr++;
				}

				memcpy(state->ptr, value->numeric, numlen);
				state->ptr += numlen;

				state->curlptr->meta[i].header |= JENTRY_ISNUMERIC;
				if (i == 0)
					state->curlptr->meta[i].header |= padlen + numlen;
				else
					state->curlptr->meta[i].header |=
						(state->curlptr->meta[i - 1].header & JENTRY_POSMASK) +
						padlen + numlen;
				break;
			}
		default:
			elog(ERROR, "invalid jsonb scalar type");
	}
}

/*
 * Iteration over binary jsonb
 */

/*
 * Initialize iterator from superheader
 */
static void
parseBuffer(JsonbIterator * it, JsonbSuperHeader sheader)
{
	uint32		superheader = *(uint32 *) sheader;

	it->containerType = superheader & (JB_FLAG_ARRAY | JB_FLAG_OBJECT);
	it->nElems = superheader & JB_COUNT_MASK;
	it->buffer = sheader;

	/* Array starts just after header */
	it->meta = (JEntry *) (sheader + sizeof(uint32));
	it->state = jbi_start;

	switch (it->containerType)
	{
		case JB_FLAG_ARRAY:
			it->dataProper =
				(char *) it->meta + it->nElems * sizeof(JEntry);
			it->isScalar = (superheader & JB_FLAG_SCALAR) != 0;
			/* This is either a "raw scalar", or an array */
			Assert(!it->isScalar || it->nElems == 1);
			break;
		case JB_FLAG_OBJECT:
			/*
			 * Offset reflects that nElems indicates JsonbPairs in an object
			 *
			 * Each key and each value have Jentry metadata.
			 */
			it->dataProper =
				(char *) it->meta + it->nElems * sizeof(JEntry) * 2;
			break;
		default:
			elog(ERROR, "unknown type of jsonb container");
	}
}

static bool
formAnswer(JsonbIterator ** it, JsonbValue * v, JEntry * e, bool skipNested)
{
	if (JBE_ISSTRING(*e))
	{
		v->type = jbvString;
		v->string.val = (*it)->dataProper + JBE_OFF(*e);
		v->string.len = JBE_LEN(*e);
		v->size = sizeof(JEntry) + v->string.len;

		return false;
	}
	else if (JBE_ISBOOL(*e))
	{
		v->type = jbvBool;
		v->boolean = JBE_ISBOOL_TRUE(*e) != 0;
		v->size = sizeof(JEntry);

		return false;
	}
	else if (JBE_ISNUMERIC(*e))
	{
		v->type = jbvNumeric;
		v->numeric = (Numeric) ((*it)->dataProper + INTALIGN(JBE_OFF(*e)));

		v->size = 2 * sizeof(JEntry) + VARSIZE_ANY(v->numeric);

		return false;
	}
	else if (JBE_ISNULL(*e))
	{
		v->type = jbvNull;
		v->size = sizeof(JEntry);

		return false;
	}
	else if (skipNested)
	{
		v->type = jbvBinary;
		v->binary.data = (*it)->dataProper + INTALIGN(JBE_OFF(*e));
		v->binary.len = JBE_LEN(*e) - (INTALIGN(JBE_OFF(*e)) - JBE_OFF(*e));
		v->size = v->binary.len + 2 * sizeof(JEntry);

		return false;
	}
	else
	{
		JsonbIterator *nit = palloc(sizeof(*nit));

		parseBuffer(nit, (*it)->dataProper + INTALIGN(JBE_OFF(*e)));
		nit->next = *it;
		*it = nit;

		return true;
	}
}

/*
 * Return it->next, while freeing memory pointed to by it
 */
static JsonbIterator *
freeAndGetNext(JsonbIterator * it)
{
	JsonbIterator *v = it->next;

	pfree(it);

	return v;
}

/*
 * Iteration-like forming jsonb
 */
static ToJsonbState *
pushState(ToJsonbState ** state)
{
	ToJsonbState *ns = palloc(sizeof(ToJsonbState));

	ns->next = *state;
	return ns;
}

static void
appendKey(ToJsonbState * state, JsonbValue * v)
{
	JsonbValue *h = &state->v;

	Assert(h->type == jbvObject);

	if (h->object.nPairs >= state->size)
	{
		state->size *= 2;
		h->object.pairs = repalloc(h->object.pairs,
								   sizeof(*h->object.pairs) * state->size);
	}

	h->object.pairs[h->object.nPairs].key = *v;
	h->object.pairs[h->object.nPairs].order = h->object.nPairs;

	h->size += v->size;
}

static void
appendValue(ToJsonbState * state, JsonbValue * v)
{
	JsonbValue *h = &state->v;

	Assert(h->type == jbvObject);

	h->object.pairs[h->object.nPairs++].value = *v;

	h->size += v->size;
}

static void
appendElement(ToJsonbState * state, JsonbValue * v)
{
	JsonbValue *a = &state->v;

	Assert(a->type == jbvArray);

	if (a->array.nElems >= state->size)
	{
		state->size *= 2;
		a->array.elems = repalloc(a->array.elems,
								  sizeof(*a->array.elems) * state->size);
	}

	a->array.elems[a->array.nElems++] = *v;

	a->size += v->size;
}

/*
 * Compare two jbvString JsonbValue values, a and b.
 *
 * This is a special qsort_arg() comparator used to sort strings in certain
 * internal contexts where it is sufficient to have a well-defined sort order.
 * In particular, objects are sorted according to this criteria to facilitate
 * cheap binary searches where we don't care about lexical sort order.
 *
 * a and b are first sorted based on their length.  If a tie-breaker is
 * required, only then do we consider string binary equality.
 *
 * Third argument 'binequal' may point to a bool. If it's set, *binequal is set
 * to true iff a and b have full binary equality, since some callers have an
 * interest in whether the two values are equal or merely equivalent.
 */
static int
lengthCompareJsonbStringValue(const void *a, const void *b, void *binequal)
{
	const JsonbValue *va = (const JsonbValue *) a;
	const JsonbValue *vb = (const JsonbValue *) b;
	int			res;

	Assert(va->type == jbvString);
	Assert(vb->type == jbvString);

	if (va->string.len == vb->string.len)
	{
		res = memcmp(va->string.val, vb->string.val, va->string.len);
		if (res == 0 && binequal)
			*((bool *) binequal) = true;
	}
	else
	{
		res = (va->string.len > vb->string.len) ? 1 : -1;
	}

	return res;
}

/*
 * qsort_arg() comparator to compare JsonbPair values.
 *
 * Function implemented in terms of lengthCompareJsonbStringValue(), and thus the
 * same "arg setting" hack will be applied here in respect of the pair's key
 * values.
 *
 * N.B: String comparions here are "length-wise"
 *
 * Pairs with equals keys are ordered such that the order field is respected.
 */
static int
lengthCompareJsonbPair(const void *a, const void *b, void *binequal)
{
	const JsonbPair *pa = (const JsonbPair *) a;
	const JsonbPair *pb = (const JsonbPair *) b;
	int			res;

	res = lengthCompareJsonbStringValue(&pa->key, &pb->key, binequal);

	/*
	 * Guarantee keeping order of equal pair. Unique algorithm will prefer
	 * first element as value.
	 */
	if (res == 0)
		res = (pa->order > pb->order) ? -1 : 1;

	return res;
}

/*
 * Sort and unique-ify pairs in JsonbValue (associative "object" data
 * structure)
 */
static void
uniqueifyJsonbObject(JsonbValue * v)
{
	bool		hasNonUniq = false;

	Assert(v->type == jbvObject);

	if (v->object.nPairs > 1)
		qsort_arg(v->object.pairs, v->object.nPairs, sizeof(*v->object.pairs),
				  lengthCompareJsonbPair, &hasNonUniq);

	if (hasNonUniq)
	{
		JsonbPair  *ptr = v->object.pairs + 1,
				   *res = v->object.pairs;

		while (ptr - v->object.pairs < v->object.nPairs)
		{
			/* Avoid copying over binary duplicate */
			if (lengthCompareJsonbStringValue(ptr, res, NULL) == 0)
			{
				v->size -= ptr->key.size + ptr->value.size;
			}
			else
			{
				res++;
				if (ptr != res)
					memcpy(res, ptr, sizeof(*res));
			}
			ptr++;
		}

		v->object.nPairs = res + 1 - v->object.pairs;
	}
}

/*
 * Sort and unique-ify JsonbArray
 */
static void
uniqueifyJsonbArray(JsonbValue * v)
{
	bool hasNonUniq = false;

	Assert(v->type == jbvArray);

	/*
	 * Actually sort values, determining if any were equal on the basis of full
	 * binary equality (rather than just having the same string length).
	 */
	if (v->array.nElems > 1)
		qsort_arg(v->array.elems, v->array.nElems,
				  sizeof(*v->array.elems), lengthCompareJsonbStringValue,
				  &hasNonUniq);

	if (hasNonUniq)
	{
		JsonbValue *ptr = v->array.elems + 1,
				   *res = v->array.elems;

		while (ptr - v->array.elems < v->array.nElems)
		{
			/* Avoid copying over binary duplicate */
			if (lengthCompareJsonbStringValue(ptr, res, NULL) != 0)
			{
				res++;
				*res = *ptr;
			}

			ptr++;
		}

		v->array.nElems = res + 1 - v->array.elems;
	}
}
