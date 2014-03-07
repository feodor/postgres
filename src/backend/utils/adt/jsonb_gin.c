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
#include "access/skey.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"

#define PATH_SEPARATOR ("\0")

typedef struct PathHashStack
{
	pg_crc32	hash_state;
	struct PathHashStack *next;
}	PathHashStack;

static void hash_value(JsonbValue * v, PathHashStack * stack);
static text *makeitem(char *str, int len, char flag);
static text *makeitemFromValue(JsonbValue * v, char flag);


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

	while ((r = JsonbIteratorGet(&it, &v, false)) != 0)
	{
		if (i >= total)
		{
			total *= 2;
			entries = (Datum *) repalloc(entries, sizeof(Datum) * total);
		}

		switch (r)
		{
			case WJB_KEY:
				entries[i++] = PointerGetDatum(makeitemFromValue(&v, KEYFLAG));
				break;
			case WJB_VALUE:
				entries[i++] = PointerGetDatum(makeitemFromValue(&v, VALFLAG));
				break;
			case WJB_ELEM:
				entries[i++] = PointerGetDatum(makeitemFromValue(&v, ELEMFLAG));
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
		/* ... except that "contains {}" requires a full index scan */
		if (entries == NULL)
			*searchMode = GIN_SEARCH_MODE_ALL;
	}
	else if (strategy == JsonbExistsStrategyNumber)
	{
		text	   *query = PG_GETARG_TEXT_PP(0);
		text	   *item;

		*nentries = 1;
		entries = (Datum *) palloc(sizeof(Datum));
		item = makeitem(VARDATA_ANY(query), VARSIZE_ANY_EXHDR(query), KEYFLAG);
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
			item = makeitem(VARDATA(key_datums[i]),
							VARSIZE(key_datums[i]) - VARHDRSZ, KEYFLAG);
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
		 * values, so we must recheck.	However, if not all of the keys are
		 * present, we can fail immediately.
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
		/* Existence of key is guaranteed in default search mode */
		*recheck = false;
		res = true;
	}
	else if (strategy == JsonbExistsAnyStrategyNumber)
	{
		/* Existence of key is guaranteed in default search mode */
		*recheck = false;
		res = true;
	}
	else if (strategy == JsonbExistsAllStrategyNumber)
	{
		/* Testing for all the keys being present gives an exact result */
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
		 * Index doesn't have information about correspondence of keys and
		 * values, so we must recheck.	However, if not all of the keys are
		 * present, we can fail immediately.
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
	PathHashStack *stack,
			   *tmp;
	pg_crc32	path_crc32;

	if (total == 0)
	{
		*nentries = 0;
		PG_RETURN_POINTER(NULL);
	}

	entries = (Datum *) palloc(sizeof(Datum) * total);

	it = JsonbIteratorInit(VARDATA(jb));

	tail.next = NULL;
	INIT_CRC32(tail.hash_state);
	stack = &tail;

	/*
	 * Calculate hashes of all key_1.key_2. ... .key_n.value paths as entries.
	 * Order of array elements doesn't matter so array keys are empty in path.
	 * For faster calculation of hashes, use a stack of precalculated hashes of
	 * prefixes.
	 */
	while ((r = JsonbIteratorGet(&it, &v, false)) != 0)
	{
		if (i >= total)
		{
			total *= 2;
			entries = (Datum *) repalloc(entries, sizeof(Datum) * total);
		}

		switch (r)
		{
			case WJB_BEGIN_ARRAY:
				tmp = stack;
				stack = (PathHashStack *) palloc(sizeof(PathHashStack));
				stack->next = tmp;
				stack->hash_state = tmp->hash_state;
				COMP_CRC32(stack->hash_state, PATH_SEPARATOR, 1);
				break;
			case WJB_BEGIN_OBJECT:
				/* Preserve stack item for key */
				tmp = stack;
				stack = (PathHashStack *) palloc(sizeof(PathHashStack));
				stack->next = tmp;
				break;
			case WJB_KEY:
				/* Calc hash of key and separated into preserved stack item */
				stack->hash_state = stack->next->hash_state;
				hash_value(&v, stack);
				COMP_CRC32(stack->hash_state, PATH_SEPARATOR, 1);
				break;
			case WJB_VALUE:
			case WJB_ELEM:
				hash_value(&v, stack);
				path_crc32 = stack->hash_state;
				FIN_CRC32(path_crc32);
				entries[i++] = path_crc32;
				break;
			case WJB_END_ARRAY:
			case WJB_END_OBJECT:
				/* Pop stack item */
				tmp = stack->next;
				pfree(stack);
				stack = tmp;
				break;
			default:
				break;
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
		/* ... except that "contains {}" requires a full index scan */
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

static void
hash_value(JsonbValue * v, PathHashStack * stack)
{
	switch (v->type)
	{
		case jbvNull:
			COMP_CRC32(stack->hash_state, "NULL", 5 /* include trailing \0 */ );
			break;
		case jbvBool:
			COMP_CRC32(stack->hash_state, (v->boolean) ? " t" : " f", 2 /* include trailing \0 */ );
			break;
		case jbvNumeric:
			stack->hash_state ^= DatumGetInt32(DirectFunctionCall1(hash_numeric,
											   NumericGetDatum(v->numeric)));
			break;
		case jbvString:
			COMP_CRC32(stack->hash_state, v->string.val, v->string.len);
			break;
		default:
			elog(ERROR, "invalid jsonb scalar type");
			break;
	}
}

/* Build an indexable text value */
static text *
makeitem(char *str, int len, char flag)
{
	text	   *item;

	item = (text *) palloc(VARHDRSZ + len + 1);
	SET_VARSIZE(item, VARHDRSZ + len + 1);

	*VARDATA(item) = flag;

	if (str && len > 0)
		memcpy(VARDATA(item) + 1, str, len);

	return item;
}

static text *
makeitemFromValue(JsonbValue * v, char flag)
{
	text	   *item;
	char	   *cstr;

	switch (v->type)
	{
		case jbvNull:
			item = makeitem(NULL, 0, NULLFLAG);
			break;
		case jbvBool:
			item = makeitem((v->boolean) ? " t" : " f", 2, flag);
			break;
		case jbvNumeric:

			/*
			 * A textual locale and precision independent representation of
			 * numeric is required.  Use the standard hash_numeric for this.
			 * This is sufficient because the recheck flag will be set anyway.
			 */
			cstr = palloc(8 /* hex numbers */ + 1 /* \0 */ );
			snprintf(cstr, 9, "%08x", DatumGetInt32(DirectFunctionCall1(hash_numeric,
											  NumericGetDatum(v->numeric))));
			item = makeitem(cstr, 8, flag);
			pfree(cstr);
			break;
		case jbvString:
			item = makeitem(v->string.val, v->string.len, flag);
			break;
		default:
			elog(ERROR, "invalid jsonb scalar type");
	}

	return item;
}
