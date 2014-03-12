/*-------------------------------------------------------------------------
 *
 * jsonb_gin.c
 *	 GIN support functions for jsonb
 *
 * Copyright (c) 2014, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/jsonb_gin.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/gin.h"
#include "access/hash.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"

#define PATH_SEPARATOR ("\0")

typedef struct PathHashStack
{
	uint32	hash_state;
	struct PathHashStack *next;
}	PathHashStack;

static text *make_text_key(const char *str, int len, char flag);
static text *make_scalar_text_key(const JsonbValue * v, char flag);
static void hash_scalar_value(const JsonbValue * v, PathHashStack * stack);

/*
 *
 * jsonb_ops GIN opclass support functions
 *
 */
Datum
gin_extract_jsonb(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = (Jsonb *) PG_GETARG_JSONB(0);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	Datum	   *entries = NULL;
	int			total = 2 * JB_ROOT_COUNT(jb);
	int			i = 0,
				r;
	JsonbIterator *it;
	JsonbValue	v;

	if (total == 0)
	{
		*nentries = 0;
		PG_RETURN_POINTER(NULL);
	}

	entries = (Datum *) palloc(sizeof(Datum) * total);

	it = JsonbIteratorInit(VARDATA(jb));

	while ((r = JsonbIteratorNext(&it, &v, false)) != 0)
	{
		if (i >= total)
		{
			total *= 2;
			entries = (Datum *) repalloc(entries, sizeof(Datum) * total);
		}

		switch (r)
		{
			case WJB_KEY:
				/*
				 * Serialize keys and elements as one.  This is necessary
				 * because array elements must be indexed as keys for the
				 * benefit of JsonbContainsStrategyNumber.
				 */
			case WJB_ELEM:
				entries[i++] = PointerGetDatum(make_scalar_text_key(&v, KEYELEMFLAG));
				break;
			case WJB_VALUE:
				entries[i++] = PointerGetDatum(make_scalar_text_key(&v, VALFLAG));
				break;
			default:
				break;
		}
	}

	*nentries = i;

	PG_RETURN_POINTER(entries);
}

Datum
gin_extract_jsonb_query(PG_FUNCTION_ARGS)
{
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	StrategyNumber strategy = PG_GETARG_UINT16(2);
	int32	   *searchMode = (int32 *) PG_GETARG_POINTER(6);
	Datum	   *entries;

	if (strategy == JsonbContainsStrategyNumber)
	{
		/* Query is a jsonb, so just apply gin_extract_jsonb... */
		entries = (Datum *)
			DatumGetPointer(DirectFunctionCall2(gin_extract_jsonb,
												PG_GETARG_DATUM(0),
												PointerGetDatum(nentries)));
		/* ...although "contains {}" requires a full index scan */
		if (entries == NULL)
			*searchMode = GIN_SEARCH_MODE_ALL;
	}
	else if (strategy == JsonbExistsStrategyNumber)
	{
		text	   *query = PG_GETARG_TEXT_PP(0);
		text	   *item;

		*nentries = 1;
		entries = (Datum *) palloc(sizeof(Datum));
		item = make_text_key(VARDATA_ANY(query), VARSIZE_ANY_EXHDR(query),
						KEYELEMFLAG);
		entries[0] = PointerGetDatum(item);
	}
	else if (strategy == JsonbExistsAnyStrategyNumber ||
			 strategy == JsonbExistsAllStrategyNumber)
	{
		ArrayType  *query = PG_GETARG_ARRAYTYPE_P(0);
		Datum	   *key_datums;
		bool	   *key_nulls;
		int			key_count;
		int			i,
					j;
		text	   *item;

		deconstruct_array(query,
						  TEXTOID, -1, false, 'i',
						  &key_datums, &key_nulls, &key_count);

		entries = (Datum *) palloc(sizeof(Datum) * key_count);

		for (i = 0, j = 0; i < key_count; ++i)
		{
			/* Nulls in the array are ignored */
			if (key_nulls[i])
				continue;
			item = make_text_key(VARDATA(key_datums[i]),
							VARSIZE(key_datums[i]) - VARHDRSZ,
							KEYELEMFLAG);
			entries[j++] = PointerGetDatum(item);
		}

		*nentries = j;
		/* ExistsAll with no keys should match everything */
		if (j == 0 && strategy == JsonbExistsAllStrategyNumber)
			*searchMode = GIN_SEARCH_MODE_ALL;
	}
	else
	{
		elog(ERROR, "unrecognized strategy number: %d", strategy);
		entries = NULL;			/* keep compiler quiet */
	}

	PG_RETURN_POINTER(entries);
}

Datum
gin_consistent_jsonb(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);
	StrategyNumber strategy = PG_GETARG_UINT16(1);

	/* Jsonb	   *query = PG_GETARG_JSONB(2); */
	int32		nkeys = PG_GETARG_INT32(3);

	/* Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);
	bool		res = true;
	int32		i;

	if (strategy == JsonbContainsStrategyNumber)
	{
		/*
		 * Index doesn't have information about correspondence of keys and
		 * values, so invariably we recheck.  However, if all of the keys are
		 * not present, that's sufficient reason to return false and finish
		 * immediately.
		 */
		*recheck = true;
		for (i = 0; i < nkeys; i++)
		{
			if (!check[i])
			{
				res = false;
				break;
			}
		}
	}
	else if (strategy == JsonbExistsStrategyNumber)
	{
		/* Existence of key guaranteed in default search mode */
		*recheck = false;
		res = true;
	}
	else if (strategy == JsonbExistsAnyStrategyNumber)
	{
		/* Existence of key guaranteed in default search mode */
		*recheck = false;
		res = true;
	}
	else if (strategy == JsonbExistsAllStrategyNumber)
	{
		/* Testing for the presence of all keys gives an exact result */
		*recheck = false;
		for (i = 0; i < nkeys; i++)
		{
			if (!check[i])
			{
				res = false;
				break;
			}
		}
	}
	else
		elog(ERROR, "unrecognized strategy number: %d", strategy);

	PG_RETURN_BOOL(res);
}

/*
 *
 * jsonb_hash_ops GIN opclass support functions
 *
 */
Datum
gin_consistent_jsonb_hash(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);
	StrategyNumber strategy = PG_GETARG_UINT16(1);

	/* Jsonb	   *query = PG_GETARG_JSONB(2); */
	int32		nkeys = PG_GETARG_INT32(3);

	/* Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);
	bool		res = true;
	int32		i;

	if (strategy == JsonbContainsStrategyNumber)
	{
		/*
		 * jsonb_hash_ops index doesn't have information about correspondence
		 * of keys and values, so invariably we recheck.  However, if all of
		 * the keys are not present, that's sufficient reason to return false
		 * and finish immediately.
		 */
		*recheck = true;
		for (i = 0; i < nkeys; i++)
		{
			if (!check[i])
			{
				res = false;
				break;
			}
		}
	}
	else
		elog(ERROR, "unrecognized strategy number: %d", strategy);

	PG_RETURN_BOOL(res);
}

Datum
gin_extract_jsonb_hash(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB(0);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	Datum	   *entries = NULL;
	int			total = 2 * JB_ROOT_COUNT(jb);
	int			i = 0,
				r;
	JsonbIterator *it;
	JsonbValue	v;
	PathHashStack tail;
	PathHashStack *stack;

	if (total == 0)
	{
		*nentries = 0;
		PG_RETURN_POINTER(NULL);
	}

	entries = (Datum *) palloc(sizeof(Datum) * total);

	it = JsonbIteratorInit(VARDATA(jb));

	tail.next = NULL;
	tail.hash_state = 0;
	stack = &tail;

	/*
	 * Calculate hashes of all key_1.key_2. ... .key_n.value paths as entries.
	 * The order of array elements doesn't matter, so array keys are empty in
	 * path.  For faster calculation of hashes, use a stack of precalculated
	 * hashes of prefixes.
	 */
	while ((r = JsonbIteratorNext(&it, &v, false)) != 0)
	{
		uint32			temphash;
		PathHashStack  *tmp;

		if (i >= total)
		{
			total *= 2;
			entries = (Datum *) repalloc(entries, sizeof(Datum) * total);
		}

		/*
		 * Keys and values hashed as one.
		 *
		 * Note that we don't hash anything that directly reflects the nesting
		 * structure (e.g. whether a structure is an array or object).  It's
		 * generally assumed that per column jsonb values frequently have a
		 * somewhat homogeneous structure.
		 */
		switch (r)
		{
			case WJB_BEGIN_ARRAY:
			case WJB_BEGIN_OBJECT:
				/* Preserve stack item for key */
				tmp = stack;
				stack = (PathHashStack *) palloc(sizeof(PathHashStack));
				stack->next = tmp;
				break;
			case WJB_KEY:
				/* Calc hash of key and separated into preserved stack item */
				stack->hash_state = stack->next->hash_state;
				hash_scalar_value(&v, stack);
				break;
			case WJB_VALUE:
			case WJB_ELEM:
				stack->hash_state = stack->next->hash_state;
				hash_scalar_value(&v, stack);
				temphash = stack->hash_state;
				entries[i++] = temphash;
				break;
			case WJB_END_ARRAY:
			case WJB_END_OBJECT:
				/* Pop the stack */
				tmp = stack->next;
				pfree(stack);
				stack = tmp;
				break;
			default:
				elog(ERROR, "invalid JsonbIteratorNext rc: %d", r);
		}
	}

	*nentries = i;

	PG_RETURN_POINTER(entries);
}

Datum
gin_extract_jsonb_query_hash(PG_FUNCTION_ARGS)
{
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	StrategyNumber strategy = PG_GETARG_UINT16(2);
	int32	   *searchMode = (int32 *) PG_GETARG_POINTER(6);
	Datum	   *entries;

	if (strategy == JsonbContainsStrategyNumber)
	{
		/* Query is a jsonb, so just apply gin_extract_jsonb... */
		entries = (Datum *)
			DatumGetPointer(DirectFunctionCall2(gin_extract_jsonb_hash,
												PG_GETARG_DATUM(0),
												PointerGetDatum(nentries)));
		/* ...although "contains {}" requires a full index scan */
		if (entries == NULL)
			*searchMode = GIN_SEARCH_MODE_ALL;
	}
	else
	{
		elog(ERROR, "unrecognized strategy number: %d", strategy);
		entries = NULL;			/* keep compiler quiet */
	}

	PG_RETURN_POINTER(entries);
}

/*
 * Build an indexable text value from a cstring and flag
 */
static text *
make_text_key(const char *str, int len, char flag)
{
	text	   *item;

	item = (text *) palloc(VARHDRSZ + len + 1);
	SET_VARSIZE(item, VARHDRSZ + len + 1);

	*VARDATA(item) = flag;

	if (str && len > 0)
		memcpy(VARDATA(item) + 1, str, len);

	return item;
}

/*
 * Create a textual representation of a jsonbValue for GIN storage.
 */
static text *
make_scalar_text_key(const JsonbValue * v, char flag)
{
	text	   *item;
	char	   *cstr;

	switch (v->type)
	{
		case jbvNull:
			item = make_text_key(NULL, 0, NULLFLAG);
			break;
		case jbvBool:
			item = make_text_key((v->boolean) ? "t" : "f", 1, flag);
			break;
		case jbvNumeric:
			/*
			 * A normalized textual representation, free of trailing zeroes is
			 * is required.
			 */
			cstr = numeric_normalize(v->numeric);
			item = make_text_key(cstr, strlen(cstr), flag);
			pfree(cstr);
			break;
		case jbvString:
			item = make_text_key(v->string.val, v->string.len, flag);
			break;
		default:
			elog(ERROR, "invalid jsonb scalar type: %d", v->type);
	}

	return item;
}

/*
 * Hash a JsonbValue scalar value, and push it on to hashing stack
 */
static void
hash_scalar_value(const JsonbValue * v, PathHashStack * stack)
{
	switch (v->type)
	{
		case jbvNull:
			stack->hash_state ^= 0x01;
			break;
		case jbvBool:
			stack->hash_state ^= v->boolean? 0x02:0x04;
			break;
		case jbvNumeric:
			/*
			 * A hash value unaffected by trailing zeroes is required.
			 */
			stack->hash_state ^= DatumGetInt32(DirectFunctionCall1(hash_numeric,
											   NumericGetDatum(v->numeric)));
			break;
		case jbvString:
			stack->hash_state ^= hash_any((unsigned char *) v->string.val,
										  v->string.len);
			break;
		default:
			elog(ERROR, "invalid jsonb scalar type");
			break;
	}
}
