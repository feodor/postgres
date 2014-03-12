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

#define JENTRY_ISFIRST		(0x80000000)
#define JENTRY_ISSTRING		(0x00000000)
#define JENTRY_ISNUMERIC	(0x10000000)
#define JENTRY_ISNEST		(0x20000000)
#define JENTRY_ISNULL		(0x40000000)
#define JENTRY_ISBOOL		(0x10000000 | 0x20000000)
#define JENTRY_ISFALSE		JENTRY_ISBOOL
#define JENTRY_ISTRUE		(0x10000000 | 0x20000000 | 0x40000000)

/* Note possible multiple evaluations, also access to prior array element */
#define JBE_ISFIRST(he_)		(((he_).entry & JENTRY_ISFIRST) != 0)
#define JBE_ISSTRING(he_)		(((he_).entry & JENTRY_TYPEMASK) == JENTRY_ISSTRING)
#define JBE_ISNUMERIC(he_)		(((he_).entry & JENTRY_TYPEMASK) == JENTRY_ISNUMERIC)
#define JBE_ISNEST(he_)			(((he_).entry & JENTRY_TYPEMASK) == JENTRY_ISNEST)
#define JBE_ISNULL(he_)			(((he_).entry & JENTRY_TYPEMASK) == JENTRY_ISNULL)
#define JBE_ISBOOL(he_)			(((he_).entry & JENTRY_TYPEMASK & JENTRY_ISBOOL) == JENTRY_ISBOOL)
#define JBE_ISBOOL_TRUE(he_)	(((he_).entry & JENTRY_TYPEMASK) == JENTRY_ISTRUE)
#define JBE_ISBOOL_FALSE(he_)	(JBE_ISBOOL(he_) && !JBE_ISBOOL_TRUE(he_))

#define JBE_ENDPOS(he_) ((he_).entry & JENTRY_POSMASK)
#define JBE_OFF(he_) (JBE_ISFIRST(he_) ? 0 : JBE_ENDPOS((&(he_))[-1]))
#define JBE_LEN(he_) (JBE_ISFIRST(he_)	\
					  ? JBE_ENDPOS(he_) \
					  : JBE_ENDPOS(he_) - JBE_ENDPOS((&(he_))[-1]))

typedef struct CompressState
{
	char	   *begin;
	char	   *ptr;

	struct
	{
		uint32		i;
		uint32	   *header;
		JEntry	   *array;
		char	   *begin;
	}		   *levelstate, *lptr, *pptr;

	uint32		maxlevel;

}	CompressState;

static int lexicalCompareJsonbStringValue(const void *a, const void *b);
static void walkUncompressedJsonb(JsonbValue * value, CompressState * state,
								  uint32 nestlevel);
static void parseBuffer(JsonbIterator * it, char *buffer);
static bool formAnswer(JsonbIterator ** it, JsonbValue * v, JEntry * e,
					   bool skipNested);
static JsonbIterator *up(JsonbIterator * it);
static void compressJsonbValue(CompressState * state, JsonbValue * value,
							   uint32 flags, uint32 level);
static void putJEntryString(CompressState * state, JsonbValue * value,
							uint32 level, uint32 i);
static uint32 compressJsonb(JsonbValue * v, char *buffer);
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
	else if (v->type == jbvString || v->type == jbvBool ||
			 v->type == jbvNumeric || v->type == jbvNull)
	{
		/* Scalar value */
		ToJsonbState *state = NULL;
		JsonbValue *res;
		uint32		sz;
		JsonbValue	scalarArray;

		scalarArray.type = jbvArray;
		scalarArray.array.scalar = true;
		scalarArray.array.nelems = 1;

		pushJsonbValue(&state, WJB_BEGIN_ARRAY, &scalarArray);
		pushJsonbValue(&state, WJB_ELEM, v);
		res = pushJsonbValue(&state, WJB_END_ARRAY, NULL);

		out = palloc(VARHDRSZ + res->size);
		sz = compressJsonb(res, VARDATA(out));
		Assert(sz <= res->size);
		SET_VARSIZE(out, sz + VARHDRSZ);
	}
	else if (v->type == jbvObject || v->type == jbvArray)
	{
		uint32		sz;

		out = palloc(VARHDRSZ + v->size);
		sz = compressJsonb(v, VARDATA(out));
		Assert(sz <= v->size);
		SET_VARSIZE(out, VARHDRSZ + sz);
	}
	else
	{
		out = palloc(VARHDRSZ + v->binary.len);

		Assert(v->type == jbvBinary);
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
				if (a->array.nelems == b->array.nelems)
				{
					for (i = 0; i < a->array.nelems; i++)
						if (compareJsonbValue(a->array.elems + i,
											  b->array.elems + i))
							return true;
				}
				break;
			case jbvObject:
				if (a->object.npairs == b->object.npairs)
				{
					for (i = 0; i < a->object.npairs; i++)
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
				return compareJsonbBinaryValue(a->binary.data, b->binary.data) == 0;
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
compareJsonbBinaryValue(char *a, char *b)
{
	JsonbIterator *it1,
			   *it2;
	int			res = 0;

	it1 = JsonbIteratorInit(a);
	it2 = JsonbIteratorInit(b);

	while (res == 0)
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
						if (v1.array.nelems != v2.array.nelems)
							res = (v1.array.nelems > v2.array.nelems) ? 1 : -1;
						break;
					case jbvObject:
						if (v1.object.npairs != v2.object.npairs)
							res = (v1.object.npairs > v2.object.npairs) ? 1 : -1;
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

	pfree(it1);
	pfree(it2);

	return res;
}

/*
 * Find string key in object or element by value in array
 */
JsonbValue *
findUncompressedJsonbValueByValue(char *buffer, uint32 flags,
								  uint32 *lowbound, JsonbValue * key)
{
	uint32		header = *(uint32 *) buffer;
	static JsonbValue r;

	Assert((header & (JB_FLAG_ARRAY | JB_FLAG_OBJECT)) !=
		   (JB_FLAG_ARRAY | JB_FLAG_OBJECT));

	if (flags & JB_FLAG_ARRAY & header)
	{
		JEntry	   *array = (JEntry *) (buffer + sizeof(header));
		char	   *data = (char *) (array + (header & JB_COUNT_MASK));
		int			i;

		for (i = (lowbound) ? *lowbound : 0; i < (header & JB_COUNT_MASK); i++)
		{
			JEntry	   *e = array + i;

			if (JBE_ISNULL(*e) && key->type == jbvNull)
			{
				r.type = jbvNull;
				if (lowbound)
					*lowbound = i;
				r.size = sizeof(JEntry);

				return &r;
			}
			else if (JBE_ISSTRING(*e) && key->type == jbvString)
			{
				if (key->string.len == JBE_LEN(*e) &&
					memcmp(key->string.val, data + JBE_OFF(*e),
						   key->string.len) == 0)
				{
					r.type = jbvString;
					r.string.val = data + JBE_OFF(*e);
					r.string.len = key->string.len;
					r.size = sizeof(JEntry) + r.string.len;
					if (lowbound)
						*lowbound = i;

					return &r;
				}
			}
			else if (JBE_ISBOOL(*e) && key->type == jbvBool)
			{
				if ((JBE_ISBOOL_TRUE(*e) && key->boolean) ||
					(JBE_ISBOOL_FALSE(*e) && !key->boolean))
				{
					r = *key;
					r.size = sizeof(JEntry);
					if (lowbound)
						*lowbound = i;

					return &r;
				}
			}
			else if (JBE_ISNUMERIC(*e) && key->type == jbvNumeric)
			{
				Numeric entry = (Numeric) (data + INTALIGN(JBE_OFF(*e)));

				if (DatumGetBool(DirectFunctionCall2(numeric_eq,
													 PointerGetDatum(entry),
													 PointerGetDatum(key->numeric))))
				{
					r.type = jbvNumeric;
					r.numeric = entry;

					if (lowbound)
						*lowbound = i;

					return &r;
				}
			}
		}
	}
	else if (flags & JB_FLAG_OBJECT & header)
	{
		JEntry	   *array = (JEntry *) (buffer + sizeof(header));
		char	   *data = (char *) (array + (header & JB_COUNT_MASK) * 2);
		uint32		stopLow = lowbound ? *lowbound : 0,
					stopHigh = (header & JB_COUNT_MASK),
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
					r.type = jbvString;
					r.string.val = data + JBE_OFF(*v);
					r.string.len = JBE_LEN(*v);
					r.size = sizeof(JEntry) + r.string.len;
				}
				else if (JBE_ISBOOL(*v))
				{
					r.type = jbvBool;
					r.boolean = (JBE_ISBOOL_TRUE(*v)) != 0;
					r.size = sizeof(JEntry);
				}
				else if (JBE_ISNUMERIC(*v))
				{
					r.type = jbvNumeric;
					r.numeric = (Numeric) (data + INTALIGN(JBE_OFF(*v)));

					r.size = 2 * sizeof(JEntry) + VARSIZE_ANY(r.numeric);
				}
				else if (JBE_ISNULL(*v))
				{
					r.type = jbvNull;
					r.size = sizeof(JEntry);
				}
				else
				{
					r.type = jbvBinary;
					r.binary.data = data + INTALIGN(JBE_OFF(*v));
					r.binary.len = JBE_LEN(*v) -
						(INTALIGN(JBE_OFF(*v)) - JBE_OFF(*v));
					r.size = 2 * sizeof(JEntry) + r.binary.len;
				}

				return &r;
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
 * findUncompressedJsonbValueByValue() wrapper that sets up JsonbValue key.
 */
JsonbValue *
findUncompressedJsonbValue(char *buffer, uint32 flags, uint32 *lowbound,
						   char *key, uint32 keylen)
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

	return findUncompressedJsonbValueByValue(buffer, flags, lowbound, &v);
}

/*
 * Get i-th value of array or object.  If i < 0, then it counts from the end of
 * array/object. Note: returns pointer to statically allocated JsonbValue.
 */
JsonbValue *
getJsonbValue(char *buffer, uint32 flags, int32 i)
{
	uint32		header = *(uint32 *) buffer;
	static JsonbValue r;
	JEntry	   *array,
			   *e;
	char	   *data;

	Assert((header & (JB_FLAG_ARRAY | JB_FLAG_OBJECT)) !=
		   (JB_FLAG_ARRAY | JB_FLAG_OBJECT));

	if (i >= 0)
	{
		if (i >= (header & JB_COUNT_MASK))
			return NULL;
	}
	else
	{
		if (-i > (header & JB_COUNT_MASK))
			return NULL;

		i = (header & JB_COUNT_MASK) + i;
	}

	array = (JEntry *) (buffer + sizeof(header));

	if (flags & JB_FLAG_ARRAY & header)
	{
		e = array + i;
		data = (char *) (array + (header & JB_COUNT_MASK));
	}
	else if (flags & JB_FLAG_OBJECT & header)
	{
		e = array + i * 2 + 1;
		data = (char *) (array + (header & JB_COUNT_MASK) * 2);
	}
	else
	{
		return NULL;
	}

	if (JBE_ISSTRING(*e))
	{
		r.type = jbvString;
		r.string.val = data + JBE_OFF(*e);
		r.string.len = JBE_LEN(*e);
		r.size = sizeof(JEntry) + r.string.len;
	}
	else if (JBE_ISBOOL(*e))
	{
		r.type = jbvBool;
		r.boolean = (JBE_ISBOOL_TRUE(*e)) != 0;
		r.size = sizeof(JEntry);
	}
	else if (JBE_ISNUMERIC(*e))
	{
		r.type = jbvNumeric;
		r.numeric = (Numeric) (data + INTALIGN(JBE_OFF(*e)));

		r.size = 2 * sizeof(JEntry) + VARSIZE_ANY(r.numeric);
	}
	else if (JBE_ISNULL(*e))
	{
		r.type = jbvNull;
		r.size = sizeof(JEntry);
	}
	else
	{
		r.type = jbvBinary;
		r.binary.data = data + INTALIGN(JBE_OFF(*e));
		r.binary.len = JBE_LEN(*e) - (INTALIGN(JBE_OFF(*e)) - JBE_OFF(*e));
		r.size = r.binary.len + 2 * sizeof(JEntry);
	}

	return &r;
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
			(*state)->v.array.nelems = 0;
			(*state)->v.array.scalar = (v && v->array.scalar) != 0;
			(*state)->size = (v && v->type == jbvArray && v->array.nelems > 0)
				? v->array.nelems : 4;
			(*state)->v.array.elems = palloc(sizeof(*(*state)->v.array.elems) *
											 (*state)->size);
			break;
		case WJB_BEGIN_OBJECT:
			*state = pushState(state);
			h = &(*state)->v;
			(*state)->v.type = jbvObject;
			(*state)->v.size = 3 * sizeof(JEntry);
			(*state)->v.object.npairs = 0;
			(*state)->size = (v && v->type == jbvObject && v->object.npairs > 0) ?
				v->object.npairs : 4;
			(*state)->v.object.pairs = palloc(sizeof(*(*state)->v.object.pairs) *
											  (*state)->size);
			break;
		case WJB_KEY:
			Assert(v->type == jbvString);
			appendKey(*state, v);
			break;
		case WJB_VALUE:
			Assert((v->type >= jbvNull && v->type <= jbvBool) || v->type == jbvBinary);
			appendValue(*state, v);
			break;
		case WJB_ELEM:
			Assert((v->type >= jbvNull && v->type <= jbvBool) || v->type == jbvBinary);
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
 * Given an unparsed varlena buffer, expand to JsonbIterator.
 */
JsonbIterator *
JsonbIteratorInit(char *buffer)
{
	JsonbIterator *it = palloc(sizeof(*it));

	parseBuffer(it, buffer);
	it->next = NULL;

	return it;
}

/*
 * Get next JsonbValue while iterating
 */
int
JsonbIteratorNext(JsonbIterator ** it, JsonbValue * v, bool skipNested)
{
	int			res;

	check_stack_depth();

	if (*it == NULL)
		return 0;


	/*
	 * Encode all possible states in the "type" integer.  This is possible
	 * because enum members of JsonbIterator->state use different bit values
	 * than JB_FLAG_ARRAY/JB_FLAG_OBJECT.  See definition of JsonbIterator
	 */
	switch ((*it)->type | (*it)->state)
	{
		case JB_FLAG_ARRAY | jbi_start:
			(*it)->state = jbi_elem;
			(*it)->i = 0;
			v->type = jbvArray;
			v->array.nelems = (*it)->nelems;
			res = WJB_BEGIN_ARRAY;
			v->array.scalar = (*it)->isScalar;
			break;
		case JB_FLAG_ARRAY | jbi_elem:
			if ((*it)->i >= (*it)->nelems)
			{
				*it = up(*it);
				res = WJB_END_ARRAY;
			}
			else if (formAnswer(it, v, &(*it)->array[(*it)->i++], skipNested))
			{
				res = JsonbIteratorNext(it, v, skipNested);
			}
			else
			{
				res = WJB_ELEM;
			}
			break;
		case JB_FLAG_OBJECT | jbi_start:
			(*it)->state = jbi_key;
			(*it)->i = 0;
			v->type = jbvObject;
			v->object.npairs = (*it)->nelems;
			res = WJB_BEGIN_OBJECT;
			break;
		case JB_FLAG_OBJECT | jbi_key:
			if ((*it)->i >= (*it)->nelems)
			{
				*it = up(*it);
				res = WJB_END_OBJECT;
			}
			else
			{
				formAnswer(it, v, &(*it)->array[(*it)->i * 2], false);
				(*it)->state = jbi_value;
				res = WJB_KEY;
			}
			break;
		case JB_FLAG_OBJECT | jbi_value:
			(*it)->state = jbi_key;
			if (formAnswer(it, v, &(*it)->array[((*it)->i++) * 2 + 1], skipNested))
				res = JsonbIteratorNext(it, v, skipNested);
			else
				res = WJB_VALUE;
			break;
		default:
			elog(ERROR, "unexpected jsonb iterator's state");
	}

	return res;
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
	result->array.nelems = j;

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
 * Walk the tree representation of jsonb
 */
static void
walkUncompressedJsonb(JsonbValue * value, CompressState * state,
					  uint32 nestlevel)
{
	int			i;

	if (!value)
		return;

	switch (value->type)
	{
		case jbvArray:
			compressJsonbValue(state, value, WJB_BEGIN_ARRAY, nestlevel);
			for (i = 0; i < value->array.nelems; i++)
			{
				if (value->array.elems[i].type == jbvNull ||
					value->array.elems[i].type == jbvString ||
					value->array.elems[i].type == jbvBool ||
					value->array.elems[i].type == jbvNumeric ||
					value->array.elems[i].type == jbvBinary)
					compressJsonbValue(state, value->array.elems + i, WJB_ELEM, nestlevel);
				else
					walkUncompressedJsonb(value->array.elems + i, state,
										  nestlevel + 1);
			}
			compressJsonbValue(state, value, WJB_END_ARRAY, nestlevel);
			break;
		case jbvObject:
			compressJsonbValue(state, value, WJB_BEGIN_OBJECT, nestlevel);

			for (i = 0; i < value->object.npairs; i++)
			{
				compressJsonbValue(state, &value->object.pairs[i].key, WJB_KEY, nestlevel);

				if (value->object.pairs[i].value.type == jbvNull ||
					value->object.pairs[i].value.type == jbvString ||
					value->object.pairs[i].value.type == jbvBool ||
					value->object.pairs[i].value.type == jbvNumeric ||
					value->object.pairs[i].value.type == jbvBinary)
					compressJsonbValue(state, &value->object.pairs[i].value,
									   WJB_VALUE, nestlevel);
				else
					walkUncompressedJsonb(&value->object.pairs[i].value,
										  state, nestlevel + 1);
			}

			compressJsonbValue(state, value, WJB_END_OBJECT, nestlevel);
			break;
		default:
			elog(ERROR, "unknown type of jsonb container");
	}
}

/*
 * Iteration over binary jsonb
 */
static void
parseBuffer(JsonbIterator * it, char *buffer)
{
	uint32		header = *(uint32 *) buffer;

	it->type = header & (JB_FLAG_ARRAY | JB_FLAG_OBJECT);
	it->nelems = header & JB_COUNT_MASK;
	it->buffer = buffer;


	buffer += sizeof(uint32);
	it->array = (JEntry *) buffer;

	it->state = jbi_start;

	switch (it->type)
	{
		case JB_FLAG_ARRAY:
			it->data = buffer + it->nelems * sizeof(JEntry);
			it->isScalar = (header & JB_FLAG_SCALAR) != 0;
			Assert(!it->isScalar || it->nelems == 1);
			break;
		case JB_FLAG_OBJECT:
			it->data = buffer + it->nelems * sizeof(JEntry) * 2;
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
		v->string.val = (*it)->data + JBE_OFF(*e);
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
		v->numeric = (Numeric) ((*it)->data + INTALIGN(JBE_OFF(*e)));

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
		v->binary.data = (*it)->data + INTALIGN(JBE_OFF(*e));
		v->binary.len = JBE_LEN(*e) - (INTALIGN(JBE_OFF(*e)) - JBE_OFF(*e));
		v->size = v->binary.len + 2 * sizeof(JEntry);

		return false;
	}
	else
	{
		JsonbIterator *nit = palloc(sizeof(*nit));

		parseBuffer(nit, (*it)->data + INTALIGN(JBE_OFF(*e)));
		nit->next = *it;
		*it = nit;

		return true;
	}
}

static JsonbIterator *
up(JsonbIterator * it)
{
	JsonbIterator *v = it->next;

	pfree(it);

	return v;
}

/*
 * Transformation from tree to binary representation of jsonb
 */
#define curLevelState	state->lptr
#define prevLevelState	state->pptr

static void
compressJsonbValue(CompressState * state, JsonbValue * value, uint32 flags,
				   uint32 level)
{
	if (level == state->maxlevel)
	{
		state->maxlevel *= 2;
		state->levelstate = repalloc(state->levelstate,
							   sizeof(*state->levelstate) * state->maxlevel);
	}

	curLevelState = state->levelstate + level;

	if (flags & (WJB_BEGIN_ARRAY | WJB_BEGIN_OBJECT))
	{
		short		padlen, p;

		Assert(((flags & WJB_BEGIN_ARRAY) && value->type == jbvArray) ||
			   ((flags & WJB_BEGIN_OBJECT) && value->type == jbvObject));

		curLevelState->begin = state->ptr;

		padlen = INTALIGN(state->ptr - state->begin) - (state->ptr -
														state->begin);
		/*
		 * Add padding as necessary
		 */
		for (p = padlen; p > 0; p--)
		{
			*state->ptr = '\0';
			state->ptr++;
		}

		curLevelState->header = (uint32 *) state->ptr;
		state->ptr += sizeof(*curLevelState->header);

		curLevelState->array = (JEntry *) state->ptr;
		curLevelState->i = 0;

		if (value->type == jbvArray)
		{
			*curLevelState->header = value->array.nelems | JB_FLAG_ARRAY;
			state->ptr += sizeof(JEntry) * value->array.nelems;

			if (value->array.scalar)
			{
				Assert(value->array.nelems == 1);
				Assert(level == 0);
				*curLevelState->header |= JB_FLAG_SCALAR;
			}
		}
		else
		{
			*curLevelState->header = value->object.npairs | JB_FLAG_OBJECT;
			state->ptr += sizeof(JEntry) * value->object.npairs * 2;
		}
	}
	else if (flags & WJB_ELEM)
	{
		putJEntryString(state, value, level, curLevelState->i);
		curLevelState->i++;
	}
	else if (flags & WJB_KEY)
	{
		Assert(value->type == jbvString);

		putJEntryString(state, value, level, curLevelState->i * 2);
	}
	else if (flags & WJB_VALUE)
	{
		putJEntryString(state, value, level, curLevelState->i * 2 + 1);
		curLevelState->i++;
	}
	else if (flags & (WJB_END_ARRAY | WJB_END_OBJECT))
	{
		uint32		len,
					i;

		Assert(((flags & WJB_END_ARRAY) && value->type == jbvArray) ||
			   ((flags & WJB_END_OBJECT) && value->type == jbvObject));
		if (level == 0)
			return;

		len = state->ptr - (char *) curLevelState->begin;

		prevLevelState = curLevelState - 1;

		if (*prevLevelState->header & JB_FLAG_ARRAY)
		{
			i = prevLevelState->i;

			prevLevelState->array[i].entry = JENTRY_ISNEST;

			if (i == 0)
				prevLevelState->array[0].entry |= JENTRY_ISFIRST | len;
			else
				prevLevelState->array[i].entry |=
					(prevLevelState->array[i - 1].entry & JENTRY_POSMASK) + len;
		}
		else if (*prevLevelState->header & JB_FLAG_OBJECT)
		{
			i = 2 * prevLevelState->i + 1;		/* VALUE, not a KEY */

			prevLevelState->array[i].entry = JENTRY_ISNEST;

			prevLevelState->array[i].entry |=
				(prevLevelState->array[i - 1].entry & JENTRY_POSMASK) + len;
		}
		else
		{
			elog(ERROR, "invalid jsonb container type");
		}

		Assert(state->ptr - curLevelState->begin <= value->size);
		prevLevelState->i++;
	}
	else
	{
		elog(ERROR, "unknown flag in tree walk");
	}
}

static void
putJEntryString(CompressState * state, JsonbValue * value, uint32 level, uint32 i)
{
	short		p, padlen;

	curLevelState = state->levelstate + level;

	if (i == 0)
		curLevelState->array[0].entry = JENTRY_ISFIRST;
	else
		curLevelState->array[i].entry = 0;

	switch (value->type)
	{
		case jbvNull:
			curLevelState->array[i].entry |= JENTRY_ISNULL;

			if (i > 0)
				curLevelState->array[i].entry |=
					curLevelState->array[i - 1].entry & JENTRY_POSMASK;
			break;
		case jbvString:
			memcpy(state->ptr, value->string.val, value->string.len);
			state->ptr += value->string.len;

			if (i == 0)
				curLevelState->array[i].entry |= value->string.len;
			else
				curLevelState->array[i].entry |=
					(curLevelState->array[i - 1].entry & JENTRY_POSMASK) +
					value->string.len;
			break;
		case jbvBool:
			curLevelState->array[i].entry |= (value->boolean) ?
				JENTRY_ISTRUE : JENTRY_ISFALSE;

			if (i > 0)
				curLevelState->array[i].entry |=
					curLevelState->array[i - 1].entry & JENTRY_POSMASK;
			break;
		case jbvNumeric:
			{
				int numlen = VARSIZE_ANY(value->numeric);

				padlen = INTALIGN(state->ptr - state->begin) -
					(state->ptr - state->begin);

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

				curLevelState->array[i].entry |= JENTRY_ISNUMERIC;
				if (i == 0)
					curLevelState->array[i].entry |= padlen + numlen;
				else
					curLevelState->array[i].entry |=
						(curLevelState->array[i - 1].entry & JENTRY_POSMASK) +
						padlen + numlen;
				break;
			}
		case jbvBinary:
			{
				padlen = INTALIGN(state->ptr - state->begin) - (state->ptr -
																state->begin);
				/*
				 * Add padding as necessary
				 */
				for (p = padlen; p > 0; p--)
				{
					*state->ptr = '\0';
					state->ptr++;
				}

				memcpy(state->ptr, value->binary.data, value->binary.len);
				state->ptr += value->binary.len;

				curLevelState->array[i].entry |= JENTRY_ISNEST;

				if (i == 0)
					curLevelState->array[i].entry |= value->binary.len + padlen;
				else
					curLevelState->array[i].entry |=
						(curLevelState->array[i - 1].entry & JENTRY_POSMASK) +
						value->binary.len + padlen;
			}
			break;
		default:
			elog(ERROR, "invalid jsonb scalar type");
	}
}

/*
 * puts JsonbValue tree into preallocated buffer
 */
static uint32
compressJsonb(JsonbValue * v, char *buffer)
{
	uint32		l = 0;
	CompressState state;

	state.begin = state.ptr = buffer;
	state.maxlevel = 8;
	state.levelstate = palloc(sizeof(*state.levelstate) * state.maxlevel);

	walkUncompressedJsonb(v, &state, 0);

	l = state.ptr - buffer;
	Assert(l <= v->size);

	return l;
}

/*
 * Iteration-like forming jsonb
 */
static ToJsonbState *
pushState(ToJsonbState ** state)
{
	ToJsonbState *ns = palloc(sizeof(*ns));

	ns->next = *state;
	return ns;
}

static void
appendKey(ToJsonbState * state, JsonbValue * v)
{
	JsonbValue *h = &state->v;

	Assert(h->type == jbvObject);

	if (h->object.npairs >= state->size)
	{
		state->size *= 2;
		h->object.pairs = repalloc(h->object.pairs,
								   sizeof(*h->object.pairs) * state->size);
	}

	h->object.pairs[h->object.npairs].key = *v;
	h->object.pairs[h->object.npairs].order = h->object.npairs;

	h->size += v->size;
}

static void
appendValue(ToJsonbState * state, JsonbValue * v)
{
	JsonbValue *h = &state->v;

	Assert(h->type == jbvObject);

	h->object.pairs[h->object.npairs++].value = *v;

	h->size += v->size;
}

static void
appendElement(ToJsonbState * state, JsonbValue * v)
{
	JsonbValue *a = &state->v;

	Assert(a->type == jbvArray);

	if (a->array.nelems >= state->size)
	{
		state->size *= 2;
		a->array.elems = repalloc(a->array.elems,
								  sizeof(*a->array.elems) * state->size);
	}

	a->array.elems[a->array.nelems++] = *v;

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

	if (v->object.npairs > 1)
		qsort_arg(v->object.pairs, v->object.npairs, sizeof(*v->object.pairs),
				  lengthCompareJsonbPair, &hasNonUniq);

	if (hasNonUniq)
	{
		JsonbPair  *ptr = v->object.pairs + 1,
				   *res = v->object.pairs;

		while (ptr - v->object.pairs < v->object.npairs)
		{
			/* Avoid copying over binary duplicate */
			if (ptr->key.string.len == res->key.string.len &&
				memcmp(ptr->key.string.val, res->key.string.val,
					   ptr->key.string.len) == 0)
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

		v->object.npairs = res + 1 - v->object.pairs;
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
	if (v->array.nelems > 1)
		qsort_arg(v->array.elems, v->array.nelems,
				  sizeof(*v->array.elems), lengthCompareJsonbStringValue,
				  &hasNonUniq);

	if (hasNonUniq)
	{
		JsonbValue *ptr = v->array.elems + 1,
				   *res = v->array.elems;

		while (ptr - v->array.elems < v->array.nelems)
		{
			/* Avoid copying over binary duplicate */
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
}
