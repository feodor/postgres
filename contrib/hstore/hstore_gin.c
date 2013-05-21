/*
 * contrib/hstore/hstore_gin.c
 */
#include "postgres.h"

#include "access/gin.h"
#include "access/skey.h"
#include "catalog/pg_type.h"

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

	switch(v->type)
	{
		case hsvNullString:
			item = makeitem(NULL, 0, NULLFLAG);
			break;
		case hsvString:
			item = makeitem(v->string.val, v->string.len, flag);
			break;
		default:
			Assert(v->type == hsvBinary);
			do {
				StringInfo	str;

				str = makeStringInfo();
				appendBinaryStringInfo(str, "    ", 4); /* VARHDRSZ */
				appendStringInfoCharMacro(str, flag);

				hstoreToCString(str, v->binary.data, v->binary.len, HStoreStrictOutput, false);
				item = (text*)str->data;
				SET_VARSIZE(item, str->len);
			} while(0);
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
	bool			skipNested = false;

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
			item = makeitem(VARDATA(key_datums[i]), VARSIZE(key_datums[i]) - VARHDRSZ, KEYFLAG);
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
