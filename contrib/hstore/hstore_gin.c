/*
 * contrib/hstore/hstore_gin.c
 */
#include "postgres.h"

#include "access/gin.h"
#include "access/skey.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"

#include "hstore.h"


PG_FUNCTION_INFO_V1(gin_extract_hstore);
Datum		gin_extract_hstore(PG_FUNCTION_ARGS);

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
makeitemFromValue(HStoreValue *v, char flag)
{
	text		*item;
	char		*cstr;

	switch(v->type)
	{
		case hsvNull:
			item = makeitem(NULL, 0, NULLFLAG);
			break;
		case hsvBool:
			item = makeitem((v->boolean) ? " t" : " f", 2, flag);
			break;
		case hsvNumeric:
			cstr = DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(v->numeric)));
			item = makeitem(cstr, strlen(cstr), flag);
			break;
		case hsvString:
			item = makeitem(v->string.val, v->string.len, flag);
			break;
		default:
			elog(ERROR, "Wrong hstore type");
	}

	return item;
}


Datum
gin_extract_hstore(PG_FUNCTION_ARGS)
{
	HStore	   		*hs = PG_GETARG_HS(0);
	int32	   		*nentries = (int32 *) PG_GETARG_POINTER(1);
	Datum	   		*entries = NULL;
	int				total = 2 * HS_ROOT_COUNT(hs);
	int				i = 0, r;
	HStoreIterator	*it;
	HStoreValue		v;

	if (total == 0)
	{
		*nentries = 0;
		PG_RETURN_POINTER(NULL);
	}

	entries = (Datum *) palloc(sizeof(Datum) * total);

	it = HStoreIteratorInit(VARDATA(hs));

	while((r = HStoreIteratorGet(&it, &v, false)) != 0)
	{
		if (i >= total)
		{
			total *= 2;
			entries = (Datum *) repalloc(entries, sizeof(Datum) * total);
		}

		switch(r)
		{
			case WHS_KEY:
				entries[i++] = PointerGetDatum(makeitemFromValue(&v, KEYFLAG));
				break;
			case WHS_VALUE:
				entries[i++] = PointerGetDatum(makeitemFromValue(&v, VALFLAG));
				break;
			case WHS_ELEM:
				entries[i++] = PointerGetDatum(makeitemFromValue(&v, ELEMFLAG));
				break;
			default:
				break;
		}
	}

	*nentries = i;

	PG_RETURN_POINTER(entries);
}

PG_FUNCTION_INFO_V1(gin_extract_hstore_query);
Datum		gin_extract_hstore_query(PG_FUNCTION_ARGS);

Datum
gin_extract_hstore_query(PG_FUNCTION_ARGS)
{
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	StrategyNumber strategy = PG_GETARG_UINT16(2);
	int32	   *searchMode = (int32 *) PG_GETARG_POINTER(6);
	Datum	   *entries;

	if (strategy == HStoreContainsStrategyNumber)
	{
		/* Query is an hstore, so just apply gin_extract_hstore... */
		entries = (Datum *)
			DatumGetPointer(DirectFunctionCall2(gin_extract_hstore,
												PG_GETARG_DATUM(0),
												PointerGetDatum(nentries)));
		/* ... except that "contains {}" requires a full index scan */
		if (entries == NULL)
			*searchMode = GIN_SEARCH_MODE_ALL;
	}
	else if (strategy == HStoreExistsStrategyNumber)
	{
		text	   *query = PG_GETARG_TEXT_PP(0);
		text	   *item;

		*nentries = 1;
		entries = (Datum *) palloc(sizeof(Datum));
		item = makeitem(VARDATA_ANY(query), VARSIZE_ANY_EXHDR(query), KEYFLAG);
		entries[0] = PointerGetDatum(item);
	}
	else if (strategy == HStoreExistsAnyStrategyNumber ||
			 strategy == HStoreExistsAllStrategyNumber)
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
			/* Nulls in the array are ignored, cf hstoreArrayToPairs */
			if (key_nulls[i])
				continue;
			item = makeitem(VARDATA(key_datums[i]),
							VARSIZE(key_datums[i]) - VARHDRSZ, KEYFLAG);
			entries[j++] = PointerGetDatum(item);
		}

		*nentries = j;
		/* ExistsAll with no keys should match everything */
		if (j == 0 && strategy == HStoreExistsAllStrategyNumber)
			*searchMode = GIN_SEARCH_MODE_ALL;
	}
	else
	{
		elog(ERROR, "unrecognized strategy number: %d", strategy);
		entries = NULL;			/* keep compiler quiet */
	}

	PG_RETURN_POINTER(entries);
}

PG_FUNCTION_INFO_V1(gin_consistent_hstore);
Datum		gin_consistent_hstore(PG_FUNCTION_ARGS);

Datum
gin_consistent_hstore(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);
	StrategyNumber strategy = PG_GETARG_UINT16(1);

	/* HStore	   *query = PG_GETARG_HS(2); */
	int32		nkeys = PG_GETARG_INT32(3);

	/* Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);
	bool		res = true;
	int32		i;

	if (strategy == HStoreContainsStrategyNumber)
	{
		/*
		 * Index doesn't have information about correspondence of keys and
		 * values, so we need recheck.	However, if not all the keys are
		 * present, we can fail at once.
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
	else if (strategy == HStoreExistsStrategyNumber)
	{
		/* Existence of key is guaranteed in default search mode */
		*recheck = false;
		res = true;
	}
	else if (strategy == HStoreExistsAnyStrategyNumber)
	{
		/* Existence of key is guaranteed in default search mode */
		*recheck = false;
		res = true;
	}
	else if (strategy == HStoreExistsAllStrategyNumber)
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

PG_FUNCTION_INFO_V1(gin_consistent_hstore_hash);
Datum		gin_consistent_hstore_hash(PG_FUNCTION_ARGS);

Datum
gin_consistent_hstore_hash(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);
	StrategyNumber strategy = PG_GETARG_UINT16(1);

	/* HStore	   *query = PG_GETARG_HS(2); */
	int32		nkeys = PG_GETARG_INT32(3);

	/* Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);
	bool		res = true;
	int32		i;

	if (strategy == HStoreContainsStrategyNumber)
	{
		/*
		 * Index doesn't have information about correspondence of keys and
		 * values, so we need recheck.	However, if not all the keys are
		 * present, we can fail at once.
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

PG_FUNCTION_INFO_V1(gin_extract_hstore_hash);
Datum		gin_extract_hstore_hash(PG_FUNCTION_ARGS);

typedef struct PathHashStack
{
	pg_crc32			  hash_state;
	struct PathHashStack *next;
} PathHashStack;

#define PATH_SEPARATOR ("\0")

static void
hash_value(HStoreValue *v, PathHashStack *stack)
{
	switch(v->type)
	{
		case hsvNull:
			COMP_CRC32(stack->hash_state, "NULL", 5 /* include trailing \0 */);
			break;
		case hsvBool:
			COMP_CRC32(stack->hash_state, (v->boolean) ? " t" : " f", 2 /* include trailing \0 */);
			break;
		case hsvNumeric:
			COMP_CRC32(stack->hash_state,
					   VARDATA_ANY(v->numeric), VARSIZE_ANY_EXHDR(v->numeric));
			break;
		case hsvString:
			COMP_CRC32(stack->hash_state, v->string.val, v->string.len);
			break;
		default:
			elog(ERROR, "Shouldn't take hash of array");
			break;
	}
}

Datum
gin_extract_hstore_hash(PG_FUNCTION_ARGS)
{
	HStore	   		*hs = PG_GETARG_HS(0);
	int32	   		*nentries = (int32 *) PG_GETARG_POINTER(1);
	Datum	   		*entries = NULL;
	int				total = 2 * HS_ROOT_COUNT(hs);
	int				i = 0, r;
	HStoreIterator	*it;
	HStoreValue		v;
	PathHashStack	tail;
	PathHashStack 	*stack, *tmp;
	pg_crc32		path_crc32;

	if (total == 0)
	{
		*nentries = 0;
		PG_RETURN_POINTER(NULL);
	}

	entries = (Datum *) palloc(sizeof(Datum) * total);

	it = HStoreIteratorInit(VARDATA(hs));

	tail.next = NULL;
	INIT_CRC32(tail.hash_state);
	stack = &tail;

	/*
	 * Calculate hashes of all key_1.key_2. ... .key_n.value paths as entries.
	 * Order of array elements doesn't matter so array keys are empty in path.
	 * For faster calculation of hashes use stack for precalculated hashes
	 * of prefixes.
	 */
	while((r = HStoreIteratorGet(&it, &v, false)) != 0)
	{
		if (i >= total)
		{
			total *= 2;
			entries = (Datum *) repalloc(entries, sizeof(Datum) * total);
		}

		switch(r)
		{
			case WHS_BEGIN_ARRAY:
				tmp = stack;
				stack = (PathHashStack *)palloc(sizeof(PathHashStack));
				stack->next = tmp;
				stack->hash_state = tmp->hash_state;
				COMP_CRC32(stack->hash_state, PATH_SEPARATOR, 1);
				break;
			case WHS_BEGIN_HASH:
				/* Preserve stack item for key */
				tmp = stack;
				stack = (PathHashStack *)palloc(sizeof(PathHashStack));
				stack->next = tmp;
				break;
			case WHS_KEY:
				/* Calc hash of key and separated into preserved stack item */
				stack->hash_state = stack->next->hash_state;
				hash_value(&v, stack);
				COMP_CRC32(stack->hash_state, PATH_SEPARATOR, 1);
				break;
			case WHS_VALUE:
			case WHS_ELEM:
				hash_value(&v, stack);
				path_crc32 = stack->hash_state;
				FIN_CRC32(path_crc32);
				entries[i++] = path_crc32;
				break;
			case WHS_END_ARRAY:
			case WHS_END_HASH:
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

PG_FUNCTION_INFO_V1(gin_extract_hstore_hash_query);
Datum		gin_extract_hstore_hash_query(PG_FUNCTION_ARGS);

Datum
gin_extract_hstore_hash_query(PG_FUNCTION_ARGS)
{
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	StrategyNumber strategy = PG_GETARG_UINT16(2);
	int32	   *searchMode = (int32 *) PG_GETARG_POINTER(6);
	Datum	   *entries;

	if (strategy == HStoreContainsStrategyNumber)
	{
		/* Query is an hstore, so just apply gin_extract_hstore... */
		entries = (Datum *)
			DatumGetPointer(DirectFunctionCall2(gin_extract_hstore_hash,
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
