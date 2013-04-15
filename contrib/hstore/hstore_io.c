/*
 * contrib/hstore/hstore_io.c
 */
#include "postgres.h"

#include <ctype.h>

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/json.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#include "hstore.h"

PG_MODULE_MAGIC;

/* old names for C functions */
HSTORE_POLLUTE(hstore_from_text, tconvert);

/* GUC variables */
static bool array_square_brackets = false;
static bool	root_array_decorated = true;
static bool	root_hash_decorated = false;

static void recvHStore(StringInfo buf, HStoreValue *v, uint32 level);

size_t
hstoreCheckKeyLen(size_t len)
{
	if (len > HSTORE_MAX_KEY_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				 errmsg("string too long for hstore key")));
	return len;
}

size_t
hstoreCheckValLen(size_t len)
{
	if (len > HSTORE_MAX_VALUE_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				 errmsg("string too long for hstore value")));
	return len;
}


static HStore*
hstoreDump(HStoreValue *p)
{
	uint32			buflen;
	HStore	 	   *out;

	if (p == NULL || (p->type == hsvArray && p->array.nelems == 0) || (p->type == hsvHash && p->hash.npairs == 0))
	{
		buflen = 0;
		out = palloc(VARHDRSZ);
	}
	else
	{
		buflen = VARHDRSZ + p->size; 
		out = palloc(buflen);
		SET_VARSIZE(out, buflen);

		buflen = compressHStore(p, VARDATA(out));
	}
	SET_VARSIZE(out, buflen + VARHDRSZ);

	return out;
}

PG_FUNCTION_INFO_V1(hstore_in);
Datum		hstore_in(PG_FUNCTION_ARGS);
Datum
hstore_in(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(hstoreDump(parseHStore(PG_GETARG_CSTRING(0))));
}

static void
recvHStoreValue(StringInfo buf, HStoreValue *v, uint32 level, int c)
{
	uint32  hentry = c;

	if (c == -1)
	{
		v->type = hsvNullString;
		v->size = sizeof(HEntry);
	} 
	else if (hentry & (HS_FLAG_ARRAY | HS_FLAG_HSTORE))
	{
		recvHStore(buf, v, level + 1);
	}
	else
	{
		v->type = hsvString;

		v->string.val = pq_getmsgtext(buf, c, &c);
		v->string.len = hstoreCheckKeyLen(c);
		v->size = sizeof(HEntry) + v->string.len;
	}
}

static void
recvHStore(StringInfo buf, HStoreValue *v, uint32 level)
{
	uint32	header = pq_getmsgint(buf, 4);
	uint32	i;

	if (level == 0 && (header & HS_COUNT_MASK) == 0)
	{
		/* empty value */
		v->type = hsvHash;
		v->hash.npairs = 0;
		return;
	}

	if (level == 0 && (header & (HS_FLAG_ARRAY | HS_FLAG_HSTORE)) == 0)
		header |= HS_FLAG_HSTORE; /* old version */

	v->size = 3 * sizeof(HEntry);
	if (header & HS_FLAG_HSTORE)
	{
		v->type = hsvHash;
		v->hash.npairs = header & HS_COUNT_MASK;
		if (v->hash.npairs > 0)
		{
			v->hash.pairs = palloc(sizeof(*v->hash.pairs) * v->hash.npairs);
	
			for(i=0; i<v->hash.npairs; i++)
			{
				recvHStoreValue(buf, &v->hash.pairs[i].key, level, pq_getmsgint(buf, 4));
				if (v->hash.pairs[i].key.type != hsvString)
					elog(ERROR, "hstore's key could be only a string");

				recvHStoreValue(buf, &v->hash.pairs[i].value, level, pq_getmsgint(buf, 4));

				v->size += v->hash.pairs[i].key.size + v->hash.pairs[i].value.size;
			}

			ORDER_PAIRS(v->hash.pairs, v->hash.npairs, v->size -= ptr->key.size + ptr->value.size);
		}
	}
	else if (header & HS_FLAG_ARRAY)
	{
		v->type = hsvArray;
		v->array.nelems = header & HS_COUNT_MASK;
		if (v->array.nelems > 0)
		{
			v->array.elems = palloc(sizeof(*v->array.elems) * v->array.nelems);

			for(i=0; i<v->array.nelems; i++)
			{
				recvHStoreValue(buf, v->array.elems + i, level, pq_getmsgint(buf, 4));
				v->size += v->array.elems[i].size;
			}
		}
	}
	else
	{
			elog(ERROR, "bogus input");
	}
}

PG_FUNCTION_INFO_V1(hstore_recv);
Datum		hstore_recv(PG_FUNCTION_ARGS);
Datum
hstore_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	HStoreValue	v;

	recvHStore(buf, &v, 0);

	PG_RETURN_POINTER(hstoreDump(&v));
}

PG_FUNCTION_INFO_V1(hstore_from_text);
Datum		hstore_from_text(PG_FUNCTION_ARGS);
Datum
hstore_from_text(PG_FUNCTION_ARGS)
{
	text	   	*key;
	text	   	*val = NULL;
	HStoreValue	v;
	HStorePair	pair;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	key = PG_GETARG_TEXT_PP(0);
	pair.key.type = hsvString;
	pair.key.string.val = VARDATA_ANY(key);
	pair.key.string.len = hstoreCheckKeyLen(VARSIZE_ANY_EXHDR(key));
	pair.key.size = pair.key.string.len + sizeof(HEntry);

	if (PG_ARGISNULL(1))
	{
		pair.value.type = hsvNullString;
		pair.value.size = sizeof(HEntry);
	}
	else
	{
		val = PG_GETARG_TEXT_PP(1);
		pair.value.type = hsvString;
		pair.value.string.val = VARDATA_ANY(val);
		pair.value.string.len = hstoreCheckValLen(VARSIZE_ANY_EXHDR(val));
		pair.value.size = pair.value.string.len + sizeof(HEntry);
	}

	v.type = hsvHash;
	v.size = sizeof(HEntry) + pair.key.size + pair.value.size; 
	v.hash.npairs = 1;
	v.hash.pairs = &pair;

	PG_RETURN_POINTER(hstoreDump(&v));
}


PG_FUNCTION_INFO_V1(hstore_from_arrays);
Datum		hstore_from_arrays(PG_FUNCTION_ARGS);
Datum
hstore_from_arrays(PG_FUNCTION_ARGS)
{
	HStoreValue v;
	Datum	   *key_datums;
	bool	   *key_nulls;
	int			key_count;
	Datum	   *value_datums;
	bool	   *value_nulls;
	int			value_count;
	ArrayType  *key_array;
	ArrayType  *value_array;
	int			i;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	key_array = PG_GETARG_ARRAYTYPE_P(0);

	Assert(ARR_ELEMTYPE(key_array) == TEXTOID);

	/*
	 * must check >1 rather than != 1 because empty arrays have 0 dimensions,
	 * not 1
	 */

	if (ARR_NDIM(key_array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	deconstruct_array(key_array,
					  TEXTOID, -1, false, 'i',
					  &key_datums, &key_nulls, &key_count);

	/* value_array might be NULL */

	if (PG_ARGISNULL(1))
	{
		value_array = NULL;
		value_count = key_count;
		value_datums = NULL;
		value_nulls = NULL;
	}
	else
	{
		value_array = PG_GETARG_ARRAYTYPE_P(1);

		Assert(ARR_ELEMTYPE(value_array) == TEXTOID);

		if (ARR_NDIM(value_array) > 1)
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("wrong number of array subscripts")));

		if ((ARR_NDIM(key_array) > 0 || ARR_NDIM(value_array) > 0) &&
			(ARR_NDIM(key_array) != ARR_NDIM(value_array) ||
			 ARR_DIMS(key_array)[0] != ARR_DIMS(value_array)[0] ||
			 ARR_LBOUND(key_array)[0] != ARR_LBOUND(value_array)[0]))
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("arrays must have same bounds")));

		deconstruct_array(value_array,
						  TEXTOID, -1, false, 'i',
						  &value_datums, &value_nulls, &value_count);

		Assert(key_count == value_count);
	}

	v.type = hsvHash;
	v.size = 2 * sizeof(HEntry);
	v.hash.pairs = palloc(key_count * sizeof(*v.hash.pairs));
	v.hash.npairs = key_count;

	for (i = 0; i < key_count; ++i)
	{
		if (key_nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("null value not allowed for hstore key")));

		v.hash.pairs[i].key.type = hsvString;
		v.hash.pairs[i].key.string.val = VARDATA_ANY(key_datums[i]);
		v.hash.pairs[i].key.string.len = hstoreCheckKeyLen(VARSIZE_ANY_EXHDR(key_datums[i]));
		v.hash.pairs[i].key.size = sizeof(HEntry) + v.hash.pairs[i].key.string.len; 

		if (!value_nulls || value_nulls[i])
		{
			v.hash.pairs[i].value.type = hsvNullString;
			v.hash.pairs[i].value.size = sizeof(HEntry);
		}
		else
		{
			v.hash.pairs[i].value.type = hsvString;
			v.hash.pairs[i].value.size = sizeof(HEntry);
			v.hash.pairs[i].value.string.val = VARDATA_ANY(value_datums[i]);
			v.hash.pairs[i].value.string.len = hstoreCheckKeyLen(VARSIZE_ANY_EXHDR(value_datums[i]));
			v.hash.pairs[i].value.size = sizeof(HEntry) + v.hash.pairs[i].value.string.len;
		}

		v.size += v.hash.pairs[i].key.size + v.hash.pairs[i].value.size;
	}

	ORDER_PAIRS(v.hash.pairs, v.hash.npairs, v.size -= ptr->key.size + ptr->value.size);


	PG_RETURN_POINTER(hstoreDump(&v));
}


PG_FUNCTION_INFO_V1(hstore_from_array);
Datum		hstore_from_array(PG_FUNCTION_ARGS);
Datum
hstore_from_array(PG_FUNCTION_ARGS)
{
	ArrayType  *in_array = PG_GETARG_ARRAYTYPE_P(0);
	int			ndims = ARR_NDIM(in_array);
	int			count;
	HStoreValue	v;
	Datum	   *in_datums;
	bool	   *in_nulls;
	int			in_count;
	int			i;

	Assert(ARR_ELEMTYPE(in_array) == TEXTOID);

	switch (ndims)
	{
		case 0:
			PG_RETURN_POINTER(hstoreDump(NULL));

		case 1:
			if ((ARR_DIMS(in_array)[0]) % 2)
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("array must have even number of elements")));
			break;

		case 2:
			if ((ARR_DIMS(in_array)[1]) != 2)
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("array must have two columns")));
			break;

		default:
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("wrong number of array subscripts")));
	}

	deconstruct_array(in_array,
					  TEXTOID, -1, false, 'i',
					  &in_datums, &in_nulls, &in_count);

	count = in_count / 2;

	v.type = hsvHash;
	v.size = 2*sizeof(HEntry);
	v.hash.npairs = count;
	v.hash.pairs = palloc(count * sizeof(HStorePair));

	for (i = 0; i < count; ++i)
	{
		if (in_nulls[i * 2])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("null value not allowed for hstore key")));

		v.hash.pairs[i].key.type = hsvString;
		v.hash.pairs[i].key.string.val = VARDATA_ANY(in_datums[i * 2]);
		v.hash.pairs[i].key.string.len = hstoreCheckKeyLen(VARSIZE_ANY_EXHDR(in_datums[i * 2]));
		v.hash.pairs[i].key.size = sizeof(HEntry) + v.hash.pairs[i].key.string.len; 

		if (in_nulls[i * 2 + 1])
		{
			v.hash.pairs[i].value.type = hsvNullString;
			v.hash.pairs[i].value.size = sizeof(HEntry);
		}
		else
		{
			v.hash.pairs[i].value.type = hsvString;
			v.hash.pairs[i].value.size = sizeof(HEntry);
			v.hash.pairs[i].value.string.val = VARDATA_ANY(in_datums[i * 2 + 1]);
			v.hash.pairs[i].value.string.len = hstoreCheckKeyLen(VARSIZE_ANY_EXHDR(in_datums[i * 2 + 1]));
			v.hash.pairs[i].value.size = sizeof(HEntry) + v.hash.pairs[i].value.string.len;
		}

		v.size += v.hash.pairs[i].key.size + v.hash.pairs[i].value.size;
	}

	ORDER_PAIRS(v.hash.pairs, v.hash.npairs, v.size -= ptr->key.size + ptr->value.size);

	PG_RETURN_POINTER(hstoreDump(&v));
}

/* most of hstore_from_record is shamelessly swiped from record_out */

/*
 * structure to cache metadata needed for record I/O
 */
typedef struct ColumnIOData
{
	Oid			column_type;
	Oid			typiofunc;
	Oid			typioparam;
	FmgrInfo	proc;
} ColumnIOData;

typedef struct RecordIOData
{
	Oid			record_type;
	int32		record_typmod;
	int			ncolumns;
	ColumnIOData columns[1];	/* VARIABLE LENGTH ARRAY */
} RecordIOData;

PG_FUNCTION_INFO_V1(hstore_from_record);
Datum		hstore_from_record(PG_FUNCTION_ARGS);
Datum
hstore_from_record(PG_FUNCTION_ARGS)
{
	HeapTupleHeader rec;
	HStore		   *out;
	HStoreValue	   v;
	Oid				tupType;
	int32			tupTypmod;
	TupleDesc		tupdesc;
	HeapTupleData 	tuple;
	RecordIOData   *my_extra;
	int				ncolumns;
	int				i;
	Datum	   	   *values;
	bool	   	   *nulls;

	if (PG_ARGISNULL(0))
	{
		Oid			argtype = get_fn_expr_argtype(fcinfo->flinfo, 0);

		/*
		 * have no tuple to look at, so the only source of type info is the
		 * argtype. The lookup_rowtype_tupdesc call below will error out if we
		 * don't have a known composite type oid here.
		 */
		tupType = argtype;
		tupTypmod = -1;

		rec = NULL;
	}
	else
	{
		rec = PG_GETARG_HEAPTUPLEHEADER(0);

		/* Extract type info from the tuple itself */
		tupType = HeapTupleHeaderGetTypeId(rec);
		tupTypmod = HeapTupleHeaderGetTypMod(rec);
	}

	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
	ncolumns = tupdesc->natts;

	/*
	 * We arrange to look up the needed I/O info just once per series of
	 * calls, assuming the record type doesn't change underneath us.
	 */
	my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL ||
		my_extra->ncolumns != ncolumns)
	{
		fcinfo->flinfo->fn_extra =
			MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
							   sizeof(RecordIOData) - sizeof(ColumnIOData)
							   + ncolumns * sizeof(ColumnIOData));
		my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
		my_extra->record_type = InvalidOid;
		my_extra->record_typmod = 0;
	}

	if (my_extra->record_type != tupType ||
		my_extra->record_typmod != tupTypmod)
	{
		MemSet(my_extra, 0,
			   sizeof(RecordIOData) - sizeof(ColumnIOData)
			   + ncolumns * sizeof(ColumnIOData));
		my_extra->record_type = tupType;
		my_extra->record_typmod = tupTypmod;
		my_extra->ncolumns = ncolumns;
	}

	v.type = hsvHash;
	v.size = 2*sizeof(HEntry);
	v.hash.npairs = ncolumns;
	v.hash.pairs = palloc(ncolumns * sizeof(HStorePair));

	if (rec)
	{
		/* Build a temporary HeapTuple control structure */
		tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
		ItemPointerSetInvalid(&(tuple.t_self));
		tuple.t_tableOid = InvalidOid;
		tuple.t_data = rec;

		values = (Datum *) palloc(ncolumns * sizeof(Datum));
		nulls = (bool *) palloc(ncolumns * sizeof(bool));

		/* Break down the tuple into fields */
		heap_deform_tuple(&tuple, tupdesc, values, nulls);
	}
	else
	{
		values = NULL;
		nulls = NULL;
	}

	for (i = 0; i < ncolumns; ++i)
	{
		ColumnIOData *column_info = &my_extra->columns[i];
		Oid			column_type = tupdesc->attrs[i]->atttypid;
		char	   *value;

		/* Ignore dropped columns in datatype */
		if (tupdesc->attrs[i]->attisdropped)
			continue;

		v.hash.pairs[i].key.type = hsvString;
		v.hash.pairs[i].key.string.val = NameStr(tupdesc->attrs[i]->attname);
		v.hash.pairs[i].key.string.len = hstoreCheckKeyLen(strlen(v.hash.pairs[i].key.string.val));
		v.hash.pairs[i].key.size = sizeof(HEntry) + v.hash.pairs[i].key.string.len; 

		if (!nulls || nulls[i])
		{
			v.hash.pairs[i].value.type = hsvNullString;
			v.hash.pairs[i].value.size = sizeof(HEntry);
		}
		else
		{
			/*
			 * Convert the column value to text
			 */
			if (column_info->column_type != column_type)
			{
				bool		typIsVarlena;

				getTypeOutputInfo(column_type,
								  &column_info->typiofunc,
								  &typIsVarlena);
				fmgr_info_cxt(column_info->typiofunc, &column_info->proc,
							  fcinfo->flinfo->fn_mcxt);
				column_info->column_type = column_type;
			}

			value = OutputFunctionCall(&column_info->proc, values[i]);

			v.hash.pairs[i].value.type = hsvString;
			v.hash.pairs[i].value.size = sizeof(HEntry);
			v.hash.pairs[i].value.string.val = value;
			v.hash.pairs[i].value.string.len = hstoreCheckValLen(strlen(value));
			v.hash.pairs[i].value.size = sizeof(HEntry) + v.hash.pairs[i].value.string.len;
		}

		v.size += v.hash.pairs[i].key.size + v.hash.pairs[i].value.size;
	}

	ORDER_PAIRS(v.hash.pairs, v.hash.npairs, v.size -= ptr->key.size + ptr->value.size);

	out = hstoreDump(&v);

	ReleaseTupleDesc(tupdesc);

	PG_RETURN_POINTER(out);
}


PG_FUNCTION_INFO_V1(hstore_populate_record);
Datum		hstore_populate_record(PG_FUNCTION_ARGS);
Datum
hstore_populate_record(PG_FUNCTION_ARGS)
{
	Oid			argtype = get_fn_expr_argtype(fcinfo->flinfo, 0);
	HStore	   *hs;
	HeapTupleHeader rec;
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupdesc;
	HeapTupleData tuple;
	HeapTuple	rettuple;
	RecordIOData *my_extra;
	int			ncolumns;
	int			i;
	Datum	   *values;
	bool	   *nulls;

	if (!type_is_rowtype(argtype))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("first argument must be a rowtype")));

	if (PG_ARGISNULL(0))
	{
		if (PG_ARGISNULL(1))
			PG_RETURN_NULL();

		rec = NULL;

		/*
		 * have no tuple to look at, so the only source of type info is the
		 * argtype. The lookup_rowtype_tupdesc call below will error out if we
		 * don't have a known composite type oid here.
		 */
		tupType = argtype;
		tupTypmod = -1;
	}
	else
	{
		rec = PG_GETARG_HEAPTUPLEHEADER(0);

		if (PG_ARGISNULL(1))
			PG_RETURN_POINTER(rec);

		/* Extract type info from the tuple itself */
		tupType = HeapTupleHeaderGetTypeId(rec);
		tupTypmod = HeapTupleHeaderGetTypMod(rec);
	}

	hs = PG_GETARG_HS(1);

	/*
	 * if the input hstore is empty, we can only skip the rest if we were
	 * passed in a non-null record, since otherwise there may be issues with
	 * domain nulls.
	 */

	if (HS_ISEMPTY(hs) && rec)
		PG_RETURN_POINTER(rec);

	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
	ncolumns = tupdesc->natts;

	if (rec)
	{
		/* Build a temporary HeapTuple control structure */
		tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
		ItemPointerSetInvalid(&(tuple.t_self));
		tuple.t_tableOid = InvalidOid;
		tuple.t_data = rec;
	}

	/*
	 * We arrange to look up the needed I/O info just once per series of
	 * calls, assuming the record type doesn't change underneath us.
	 */
	my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL ||
		my_extra->ncolumns != ncolumns)
	{
		fcinfo->flinfo->fn_extra =
			MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
							   sizeof(RecordIOData) - sizeof(ColumnIOData)
							   + ncolumns * sizeof(ColumnIOData));
		my_extra = (RecordIOData *) fcinfo->flinfo->fn_extra;
		my_extra->record_type = InvalidOid;
		my_extra->record_typmod = 0;
	}

	if (my_extra->record_type != tupType ||
		my_extra->record_typmod != tupTypmod)
	{
		MemSet(my_extra, 0,
			   sizeof(RecordIOData) - sizeof(ColumnIOData)
			   + ncolumns * sizeof(ColumnIOData));
		my_extra->record_type = tupType;
		my_extra->record_typmod = tupTypmod;
		my_extra->ncolumns = ncolumns;
	}

	values = (Datum *) palloc(ncolumns * sizeof(Datum));
	nulls = (bool *) palloc(ncolumns * sizeof(bool));

	if (rec)
	{
		/* Break down the tuple into fields */
		heap_deform_tuple(&tuple, tupdesc, values, nulls);
	}
	else
	{
		for (i = 0; i < ncolumns; ++i)
		{
			values[i] = (Datum) 0;
			nulls[i] = true;
		}
	}

	for (i = 0; i < ncolumns; ++i)
	{
		ColumnIOData *column_info = &my_extra->columns[i];
		Oid			column_type = tupdesc->attrs[i]->atttypid;
		HStoreValue	*v = NULL;

		/* Ignore dropped columns in datatype */
		if (tupdesc->attrs[i]->attisdropped)
		{
			nulls[i] = true;
			continue;
		}

		if (!HS_ISEMPTY(hs))
		{
			char *key = NameStr(tupdesc->attrs[i]->attname);

			v = findUncompressedHStoreValue(VARDATA(hs), HS_FLAG_HSTORE, NULL, key, strlen(key));
		}

		/*
		 * we can't just skip here if the key wasn't found since we might have
		 * a domain to deal with. If we were passed in a non-null record
		 * datum, we assume that the existing values are valid (if they're
		 * not, then it's not our fault), but if we were passed in a null,
		 * then every field which we don't populate needs to be run through
		 * the input function just in case it's a domain type.
		 */
		if (v == NULL && rec)
			continue;

		/*
		 * Prepare to convert the column value from text
		 */
		if (column_info->column_type != column_type)
		{
			getTypeInputInfo(column_type,
							 &column_info->typiofunc,
							 &column_info->typioparam);
			fmgr_info_cxt(column_info->typiofunc, &column_info->proc,
						  fcinfo->flinfo->fn_mcxt);
			column_info->column_type = column_type;
		}

		if (v == NULL || v->type == hsvNullString)
		{
			/*
			 * need InputFunctionCall to happen even for nulls, so that domain
			 * checks are done
			 */
			values[i] = InputFunctionCall(&column_info->proc, NULL,
										  column_info->typioparam,
										  tupdesc->attrs[i]->atttypmod);
			nulls[i] = true;
		}
		else
		{
			char *s = NULL;

			if (v->type == hsvString)
				s = pnstrdup(v->string.val, v->string.len);
			else if (v->type == hsvBinary)
				s = hstoreToCString(NULL, v->binary.data, v->binary.len, HStoreOutput);
			else
				elog(PANIC, "Wrong hstore");

			values[i] = InputFunctionCall(&column_info->proc, s,
										  column_info->typioparam,
										  tupdesc->attrs[i]->atttypmod);
			nulls[i] = false;
		}
	}

	rettuple = heap_form_tuple(tupdesc, values, nulls);

	ReleaseTupleDesc(tupdesc);

	PG_RETURN_DATUM(HeapTupleGetDatum(rettuple));
}

static bool
stringIsNumber(char *string, int len) {

	if (!(string[0] == '0' && isdigit((unsigned char) string[1])) && strspn(string, "+-0123456789Ee.") == len)
	{
		char	*endptr = "junk";
		long	lval;
		double	dval;

		lval =  strtol(string, &endptr, 10);
		(void) lval;
		if (*endptr == '\0')
			return true;

		dval = strtod(string, &endptr);
		(void) dval;
		if (*endptr == '\0')
			return true;
	}

	return false;
}

static void
putEscapedString(StringInfo out, HStoreOutputKind kind, char *string, uint32 len)
{
	if (string)
	{
		switch(kind)
		{
			case HStoreOutput:
				do
				{
					char       *ptr = string;

					appendStringInfoCharMacro(out, '"');
					while (ptr - string < len)
					{
						if (*ptr == '"' || *ptr == '\\')
							appendStringInfoCharMacro(out, '\\');
						appendStringInfoCharMacro(out, *ptr);
						ptr++;
					}
					appendStringInfoCharMacro(out, '"');
				} while(0);
				break;
			case JsonOutput:
				escape_json(out, pnstrdup(string, len));
				break;
			case JsonLooseOutput:
				do 
				{
					char *s = pnstrdup(string, len);

					if (len == 1 && *string == 't')
						appendStringInfoString(out, "true");
					else if (len == 1 && *string == 'f')
						appendStringInfoString(out, "false");
					else if (len > 0 && stringIsNumber(s, len))
						appendBinaryStringInfo(out, string, len);
					else
						escape_json(out, s);
				} while(0);
				break;
			default:
				elog(PANIC, "Unknown kind");
		}
	}
	else
	{
		appendBinaryStringInfo(out, (kind == HStoreOutput) ? "NULL" : "null", 4);
	}
}

char*
hstoreToCString(StringInfo out, char *in, int len /* just estimation */,
		  HStoreOutputKind kind)
{
	bool			first = true;
	HStoreIterator	*it;
	int				type;
	HStoreValue		v;
	int				level = 0;


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
		switch(type)
		{
			case WHS_BEGIN_ARRAY:
				if (first == false)
					appendBinaryStringInfo(out, ", ", 2);
				first = true;

				if (!(kind == HStoreOutput && level == 0 && root_array_decorated == false))
					appendStringInfoCharMacro(out, 
										  (kind == HStoreOutput && array_square_brackets == false) ? '{' : '[');
				level++;
				break;
			case WHS_BEGIN_HASH:
				if (first == false)
					appendBinaryStringInfo(out, ", ", 2);
				first = true;

				if (!(kind == HStoreOutput && level == 0 && root_hash_decorated == false))
					appendStringInfoCharMacro(out, '{');

				level++;
				break;
			case WHS_KEY:
				if (first == false)
					appendBinaryStringInfo(out, ", ", 2);
				first = true;

				if (kind == JsonLooseOutput)
				{
					kind = JsonOutput;
					putEscapedString(out, kind, v.string.val,  v.string.len);
					kind = JsonLooseOutput;
				}
				else
				{
					putEscapedString(out, kind, v.string.val,  v.string.len);
				}
				appendBinaryStringInfo(out, (kind == HStoreOutput) ? "=>" : ": ", 2);
				break;
			case WHS_ELEM:
				if (first == false)
					appendBinaryStringInfo(out, ", ", 2);
				else
					first = false;

				putEscapedString(out, kind, (v.type == hsvNullString) ? NULL : v.string.val,  v.string.len);
				break;
			case WHS_VALUE:
				first = false;
				putEscapedString(out, kind, (v.type == hsvNullString) ? NULL : v.string.val,  v.string.len);
				break;
			case WHS_END_ARRAY:
				level--;
				if (!(kind == HStoreOutput && level == 0 && root_array_decorated == false))
					appendStringInfoCharMacro(out, 
										 (kind == HStoreOutput && array_square_brackets == false) ? '}' : ']');
				first = false;
				break;
			case WHS_END_HASH:
				level--;
				if (!(kind == HStoreOutput && level == 0 && root_hash_decorated == false))
					appendStringInfoCharMacro(out, '}');
				first = false;
				break;
			default:
				elog(PANIC, "Wrong flags");
		}
	}

	Assert(level == 0);

	return out->data;
}

text*
HStoreValueToText(HStoreValue *v)
{
	text		*out;

	if (v == NULL || v->type == hsvNullString)
	{
		out = NULL;
	}
	else if (v->type == hsvString)
	{
		out = cstring_to_text_with_len(v->string.val, v->string.len);
	}
	else
	{
		StringInfo	str;

		str = makeStringInfo();
		appendBinaryStringInfo(str, "    ", 4); /* VARHDRSZ */

		hstoreToCString(str, v->binary.data, v->binary.len, HStoreOutput);

		out = (text*)str->data;
		SET_VARSIZE(out, str->len);
	}

	return out;
}

PG_FUNCTION_INFO_V1(hstore_out);
Datum		hstore_out(PG_FUNCTION_ARGS);
Datum
hstore_out(PG_FUNCTION_ARGS)
{
	HStore	*hs = PG_GETARG_HS(0);
	char 	*out;

	out = hstoreToCString(NULL, (HS_ISEMPTY(hs)) ? NULL : VARDATA(hs), VARSIZE(hs), HStoreOutput);

	PG_RETURN_CSTRING(out);
}

PG_FUNCTION_INFO_V1(hstore_send);
Datum		hstore_send(PG_FUNCTION_ARGS);
Datum
hstore_send(PG_FUNCTION_ARGS)
{
	HStore	   		*in = PG_GETARG_HS(0);
	StringInfoData	buf;

	pq_begintypsend(&buf);

	if (HS_ISEMPTY(in))
	{
		pq_sendint(&buf, 0, 4);
	}
	else
	{
		HStoreIterator	*it;
		int				type;
		HStoreValue		v;

		enlargeStringInfo(&buf, VARSIZE_ANY(in) /* just estimation */);

		it = HStoreIteratorInit(VARDATA_ANY(in));
	
		while((type = HStoreIteratorGet(&it, &v, false)) != 0)
		{
			switch(type)
			{
				case WHS_BEGIN_ARRAY:
					pq_sendint(&buf, v.array.nelems | HS_FLAG_ARRAY, 4);
					break;
				case WHS_BEGIN_HASH:
					pq_sendint(&buf, v.hash.npairs | HS_FLAG_HSTORE, 4);
					break;
				case WHS_KEY:
					pq_sendint(&buf, v.string.len, 4);
					pq_sendtext(&buf, v.string.val, v.string.len);
					break;
				case WHS_ELEM:
				case WHS_VALUE:
					if (v.type == hsvNullString)
					{
						pq_sendint(&buf, -1, 4);
					}
					else
					{
						pq_sendint(&buf, v.string.len, 4);
						pq_sendtext(&buf, v.string.val, v.string.len);
					}
					break;
				case WHS_END_ARRAY:
				case WHS_END_HASH:
					break;
				default:
					elog(PANIC, "Wrong flags");
			}
		}
	}

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


/*
 * hstore_to_json_loose
 *
 * This is a heuristic conversion to json which treats
 * 't' and 'f' as booleans and strings that look like numbers as numbers,
 * as long as they don't start with a leading zero followed by another digit
 * (think zip codes or phone numbers starting with 0).
 */
PG_FUNCTION_INFO_V1(hstore_to_json_loose);
Datum		hstore_to_json_loose(PG_FUNCTION_ARGS);
Datum
hstore_to_json_loose(PG_FUNCTION_ARGS)
{
	HStore	   *in = PG_GETARG_HS(0);
	StringInfo	str;
	text		*out;

	str = makeStringInfo();
	appendBinaryStringInfo(str, "    ", 4); /* VARHDRSZ */

	hstoreToCString(str, HS_ISEMPTY(in) ? NULL : VARDATA_ANY(in), VARSIZE_ANY(in), JsonLooseOutput);

	out = (text*)str->data;

	SET_VARSIZE(out, str->len);

	PG_RETURN_TEXT_P(out);
}

PG_FUNCTION_INFO_V1(hstore_to_json);
Datum		hstore_to_json(PG_FUNCTION_ARGS);
Datum
hstore_to_json(PG_FUNCTION_ARGS)
{
	HStore	   *in = PG_GETARG_HS(0);
	StringInfo	str;
	text		*out;

	str = makeStringInfo();
	appendBinaryStringInfo(str, "    ", 4); /* VARHDRSZ */

	hstoreToCString(str, HS_ISEMPTY(in) ? NULL : VARDATA_ANY(in), VARSIZE_ANY(in), JsonOutput);

	out = (text*)str->data;

	SET_VARSIZE(out, str->len);

	PG_RETURN_TEXT_P(out);
}

void _PG_init(void);
void
_PG_init(void)
{
	DefineCustomBoolVariable(
		"hstore.array_square_brackets",
		"[] brackets for array",
		"Use [] brackets for array's decoration",
		&array_square_brackets,
		array_square_brackets,
		PGC_USERSET,
		GUC_NOT_IN_SAMPLE,
		NULL,
		NULL,
		NULL
	);

	DefineCustomBoolVariable(
		"hstore.root_array_decorated",
		"Enables brackets decoration for root array",
		"Enables brackets decoration for root array",
		&root_array_decorated,
		root_array_decorated,
		PGC_USERSET,
		GUC_NOT_IN_SAMPLE,
		NULL,
		NULL,
		NULL
	);

	DefineCustomBoolVariable(
		"hstore.root_hash_decorated",
		"Enables brackets decoration for root hash (hstore)",
		"Enables brackets decoration for root hash (hstore)",
		&root_hash_decorated,
		root_hash_decorated,
		PGC_USERSET,
		GUC_NOT_IN_SAMPLE,
		NULL,
		NULL,
		NULL
	);

}
