/*-------------------------------------------------------------------------
 *
 * jsonb_op.c
 *	 Special operators for jsonb only, used by various index access methods
 *
 * Copyright (c) 2014, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/jsonb_op.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/hash.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"

static bool deepContains(JsonbIterator ** it1, JsonbIterator ** it2);

Datum
jsonb_exists(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB(0);
	text	   *key = PG_GETARG_TEXT_PP(1);
	JsonbValue *v = NULL;

	if (!JB_ISEMPTY(jb))
		v = findJsonbValueFromSuperHeaderLen(VARDATA(jb),
											 JB_FOBJECT | JB_FARRAY,
											 NULL,
											 VARDATA_ANY(key),
											 VARSIZE_ANY_EXHDR(key));

	PG_RETURN_BOOL(v != NULL);
}

Datum
jsonb_exists_any(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB(0);
	ArrayType  *keys = PG_GETARG_ARRAYTYPE_P(1);
	JsonbValue *v = arrayToJsonbSortedArray(keys);
	int			i;
	uint32	   *plowbound = NULL,
				lowbound = 0;

	if (JB_ISEMPTY(jb) || v == NULL || v->object.nPairs == 0)
		PG_RETURN_BOOL(false);

	if (JB_ROOT_IS_OBJECT(jb))
		plowbound = &lowbound;

	/*
	 * We exploit the fact that the pairs list is already sorted into strictly
	 * increasing order to narrow the findJsonbValueFromSuperHeader search;
	 * each search can start one entry past the previous "found" entry, or at
	 * the lower bound of the last search.
	 */
	for (i = 0; i < v->array.nElems; i++)
	{
		if (findJsonbValueFromSuperHeader(VARDATA(jb),
										  JB_FOBJECT | JB_FARRAY,
										  plowbound,
										  v->array.elems + i) != NULL)
			PG_RETURN_BOOL(true);
	}

	PG_RETURN_BOOL(false);
}

Datum
jsonb_exists_all(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB(0);
	ArrayType  *keys = PG_GETARG_ARRAYTYPE_P(1);
	JsonbValue *v = arrayToJsonbSortedArray(keys);
	uint32	   *plowbound = NULL;
	uint32		lowbound = 0;
	int			i;

	if (v == NULL || v->array.nElems == 0)
		PG_RETURN_BOOL(true);

	if (JB_ROOT_IS_OBJECT(jb))
		plowbound = &lowbound;

	/*
	 * We exploit the fact that the pairs list is already sorted into strictly
	 * increasing order to narrow the findJsonbValueFromSuperHeader search;
	 * each search can start one entry past the previous "found" entry, or at
	 * the lower bound of the last search.
	 */
	for (i = 0; i < v->array.nElems; i++)
	{
		if (findJsonbValueFromSuperHeader(VARDATA(jb),
										  JB_FOBJECT | JB_FARRAY,
										  plowbound,
										  v->array.elems + i) == NULL)
			PG_RETURN_BOOL(false);
	}

	PG_RETURN_BOOL(true);
}

Datum
jsonb_contains(PG_FUNCTION_ARGS)
{
	Jsonb	   *val = PG_GETARG_JSONB(0);
	Jsonb	   *tmpl = PG_GETARG_JSONB(1);

	JsonbIterator *it1, *it2;

	if (JB_ROOT_COUNT(val) < JB_ROOT_COUNT(tmpl) ||
		JB_ROOT_IS_OBJECT(val) != JB_ROOT_IS_OBJECT(tmpl))
		PG_RETURN_BOOL(false);

	it1 = JsonbIteratorInit(VARDATA(val));
	it2 = JsonbIteratorInit(VARDATA(tmpl));

	PG_RETURN_BOOL(deepContains(&it1, &it2));
}

Datum
jsonb_contained(PG_FUNCTION_ARGS)
{
	/* Just invert "contains" */
	PG_RETURN_DATUM(DirectFunctionCall2(jsonb_contains,
										PG_GETARG_DATUM(1),
										PG_GETARG_DATUM(0)
										));
}

Datum
jsonb_ne(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(jsonb_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res != 0);
}

/*
 * B-Tree operator class operators, support function
 */
Datum
jsonb_lt(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(jsonb_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res < 0);
}

Datum
jsonb_gt(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(jsonb_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res > 0);
}

Datum
jsonb_le(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(jsonb_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res <= 0);
}

Datum
jsonb_ge(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(jsonb_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res >= 0);
}

Datum
jsonb_eq(PG_FUNCTION_ARGS)
{
	int			res = DatumGetInt32(DirectFunctionCall2(jsonb_cmp,
														PG_GETARG_DATUM(0),
														PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(res == 0);
}

Datum
jsonb_cmp(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb1 = PG_GETARG_JSONB(0);
	Jsonb	   *jb2 = PG_GETARG_JSONB(1);

	int			res;

	if (JB_ISEMPTY(jb1) || JB_ISEMPTY(jb2))
	{
		if (JB_ISEMPTY(jb1))
		{
			if (JB_ISEMPTY(jb2))
				res = 0;
			else
				res = -1;
		}
		else
		{
			res = 1;
		}
	}
	else if (JB_ROOT_IS_SCALAR(jb1) && !JB_ROOT_IS_SCALAR(jb2))
	{
		res = -1;
	}
	else if (JB_ROOT_IS_SCALAR(jb2) && !JB_ROOT_IS_SCALAR(jb1))
	{
		res = 1;
	}
	else
	{
		/*
		 * TODO: Think harder about the memory leaked here
		 */
		res = compareJsonbSuperHeaderValue(VARDATA(jb1), VARDATA(jb2));
	}

	/*
	 * This is a btree support function; this is one of the few places where
	 * memory needs to be explicitly freed.
	 */
	PG_FREE_IF_COPY(jb1, 0);
	PG_FREE_IF_COPY(jb2, 1);
	PG_RETURN_INT32(res);
}

/* Hash operator class jsonb hashing function */
Datum
jsonb_hash(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB(0);
	JsonbIterator *it;
	int32		r;
	JsonbValue	v;
	int			crc;

	if (JB_ROOT_COUNT(jb) == 0)
		PG_RETURN_INT32(0);

	it = JsonbIteratorInit(VARDATA(jb));
	INIT_CRC32(crc);

	while ((r = JsonbIteratorNext(&it, &v, false)) != 0)
	{
		switch (r)
		{
			case WJB_BEGIN_ARRAY:
				COMP_CRC32(crc, "ab", 3);
				COMP_CRC32(crc, &v.array.nElems, sizeof(int));
				COMP_CRC32(crc, &v.array.scalar, sizeof(bool));
				break;
			case WJB_BEGIN_OBJECT:
				COMP_CRC32(crc, "hb", 3);
				COMP_CRC32(crc, &v.object.nPairs, sizeof(int));
				break;
			case WJB_KEY:
				COMP_CRC32(crc, "k", 2);
			case WJB_VALUE:
			case WJB_ELEM:
				switch (v.type)
				{
					case jbvString:
						COMP_CRC32(crc, v.string.val, v.string.len);
						break;
					case jbvNull:
						COMP_CRC32(crc, "N", 2);
						break;
					case jbvBool:
						COMP_CRC32(crc, &v.boolean, sizeof(bool));
						break;
					case jbvNumeric:
						crc ^= DatumGetInt32(DirectFunctionCall1(hash_numeric,
																 NumericGetDatum(v.numeric)));
						break;
					default:
						elog(ERROR, "invalid jsonb iterator type");
				}
				break;
			case WJB_END_ARRAY:
				COMP_CRC32(crc, "ae", 3);
				break;
			case WJB_END_OBJECT:
				COMP_CRC32(crc, "he", 3);
				break;
			default:
				elog(ERROR, "invalid JsonbIteratorNext rc: %d", r);
		}
	}

	FIN_CRC32(crc);

	PG_FREE_IF_COPY(jb, 0);
	PG_RETURN_INT32(crc);
}

/*
 * Work horse for "contains" operator
 */
static bool
deepContains(JsonbIterator ** it1, JsonbIterator ** it2)
{
	uint32		r1,
				r2;
	JsonbValue	v1,
				v2;
	bool		res = true;

	r1 = JsonbIteratorNext(it1, &v1, false);
	r2 = JsonbIteratorNext(it2, &v2, false);

	if (r1 != r2)
	{
		res = false;
	}
	else if (r1 == WJB_BEGIN_OBJECT)
	{
		uint32		lowbound = 0;
		JsonbValue *v;

		for (;;)
		{
			r2 = JsonbIteratorNext(it2, &v2, false);
			if (r2 == WJB_END_OBJECT)
				break;

			Assert(r2 == WJB_KEY);

			v = findJsonbValueFromSuperHeader((*it1)->buffer,
											  JB_FOBJECT,
											  &lowbound,
											  &v2);

			if (v == NULL)
			{
				res = false;
				break;
			}

			r2 = JsonbIteratorNext(it2, &v2, true);
			Assert(r2 == WJB_VALUE);

			if (v->type != v2.type)
			{
				res = false;
				break;
			}
			else if (v->type >= jbvNull && v->type < jbvArray)
			{
				if (!compareJsonbScalarValue(v, &v2))
				{
					res = false;
					break;
				}
			}
			else
			{
				JsonbIterator *it1a,
						   *it2a;

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
		JsonbValue *v;
		JsonbValue *av = NULL;
		uint32		nElems = v1.array.nElems;

		for (;;)
		{
			r2 = JsonbIteratorNext(it2, &v2, true);
			if (r2 == WJB_END_ARRAY)
				break;

			Assert(r2 == WJB_ELEM);

			if (v2.type >= jbvNull && v2.type < jbvArray)
			{
				v = findJsonbValueFromSuperHeader((*it1)->buffer,
												  JB_FARRAY,
												  NULL,
												  &v2);
				if (v == NULL)
				{
					res = false;
					break;
				}
			}
			else
			{
				uint32		i;

				if (av == NULL)
				{
					uint32		j = 0;

					av = palloc(sizeof(JsonbValue) * nElems);

					for (i = 0; i < nElems; i++)
					{
						r2 = JsonbIteratorNext(it1, &v1, true);
						Assert(r2 == WJB_ELEM);

						if (v1.type == jbvBinary)
							av[j++] = v1;
					}

					if (j == 0)
					{
						res = false;
						break;
					}

					nElems = j;
				}

				res = false;
				for (i = 0; res == false && i < nElems; i++)
				{
					JsonbIterator *it1a,
							   *it2a;

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
		elog(ERROR, "invalid jsonb container type");
	}

	return res;
}
