/*-------------------------------------------------------------------------
 *
 * jsonb.c
 *		I/O routines for jsonb type
 *
 * Copyright (c) 2014, PostgreSQL Global Development Group
 *
 * src/backend/utils/adt/jsonb_support.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonapi.h"
#include "utils/jsonb.h"

typedef struct JsonbInState
{
	ToJsonbState *state;
	JsonbValue *res;
}	JsonbInState;

static inline Datum deserialize_json_text(char *json, int len);
static size_t checkStringLen(size_t len);
static void jsonb_in_object_start(void *state);
static void jsonb_in_object_end(void *state);
static void jsonb_in_array_start(void *state);
static void jsonb_in_array_end(void *state);
static void jsonb_in_object_field_start(void *state, char *fname, bool isnull);
static void jsonb_put_escaped_value(StringInfo out, JsonbValue * v);
static void jsonb_in_scalar(void *state, char *token, JsonTokenType tokentype);
char *JsonbToCString(StringInfo out, char *in, int estimated_len);

/*
 * jsonb type input function
 */
Datum
jsonb_in(PG_FUNCTION_ARGS)
{
	char	   *json = PG_GETARG_CSTRING(0);

	return deserialize_json_text(json, strlen(json));
}

/*
 * jsonb type recv function
 *
 * the type is sent as text in binary mode, so this is almost the same
 * as the input function, but it's prefixed with a version number so we
 * can change the binary format sent in future if necessary. For now,
 * only version 1 is supported.
 */
Datum
jsonb_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int			version = pq_getmsgint(buf, 1);
	char	   *str;
	int			nbytes;

	if (version == 1)
		str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
	else
		elog(ERROR, "Unsupported jsonb version number %d", version);

	return deserialize_json_text(str, nbytes);
}

/*
 * jsonb type output function
 */
Datum
jsonb_out(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB(0);
	char	   *out;

	out = JsonbToCString(NULL, (JB_ISEMPTY(jb)) ? NULL : VARDATA(jb), VARSIZE(jb));

	PG_RETURN_CSTRING(out);
}

/*
 * jsonb type send function
 *
 * Just send jsonb as a string of text
 */
Datum
jsonb_send(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB(0);
	StringInfoData buf;
	StringInfo	jtext = makeStringInfo();
	int			version = 1;

	(void) JsonbToCString(jtext, (JB_ISEMPTY(jb)) ? NULL : VARDATA(jb),
						  VARSIZE(jb));

	pq_begintypsend(&buf);
	pq_sendint(&buf, version, 1);
	pq_sendtext(&buf, jtext->data, jtext->len);
	pfree(jtext->data);
	pfree(jtext);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * SQL function jsonb_typeof(jsonb) -> text
 *
 * This function is here because the analog json function is in json.c, since
 * it uses the json parser internals not exposed elsewhere.
 */
Datum
jsonb_typeof(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB(0);
	JsonbIterator *it;
	JsonbValue	v;
	char	   *result;

	if (JB_ROOT_IS_OBJECT(in))
		result = "object";
	else if (JB_ROOT_IS_ARRAY(in) && !JB_ROOT_IS_SCALAR(in))
		result = "array";
	else
	{
		Assert(JB_ROOT_IS_SCALAR(in));

		it = JsonbIteratorInit(VARDATA_ANY(in));

		/*
		 * A root scalar is stored as an array of one element, so we get the
		 * array and then its first (and only) member.
		 */
		(void) JsonbIteratorNext(&it, &v, true);
		(void) JsonbIteratorNext(&it, &v, true);
		switch (v.type)
		{
			case jbvNull:
				result = "null";
				break;
			case jbvString:
				result = "string";
				break;
			case jbvBool:
				result = "boolean";
				break;
			case jbvNumeric:
				result = "number";
				break;
			default:
				elog(ERROR, "unknown jsonb scalar type");
		}
	}

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * deserialize_json_text
 *
 * turn json text into a jsonb Datum.
 *
 * uses the json parser with hooks to contruct the jsonb.
 */
static inline Datum
deserialize_json_text(char *json, int len)
{
	JsonLexContext *lex;
	JsonbInState state;
	JsonSemAction sem;

	memset(&state, 0, sizeof(state));
	memset(&sem, 0, sizeof(sem));
	lex = makeJsonLexContextCstringLen(json, len, true);

	sem.semstate = (void *) &state;

	sem.object_start = jsonb_in_object_start;
	sem.array_start = jsonb_in_array_start;
	sem.object_end = jsonb_in_object_end;
	sem.array_end = jsonb_in_array_end;
	sem.scalar = jsonb_in_scalar;
	sem.object_field_start = jsonb_in_object_field_start;

	pg_parse_json(lex, &sem);

	/* after parsing, the item member has the composed jsonb structure */
	PG_RETURN_POINTER(JsonbValueToJsonb(state.res));
}

static size_t
checkStringLen(size_t len)
{
	if (len > JSONB_MAX_STRING_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				 errmsg("string too long for jsonb string")));
	return len;
}

static void
jsonb_in_object_start(void *state)
{
	JsonbInState *_state = (JsonbInState *) state;

	_state->res = pushJsonbValue(&_state->state, WJB_BEGIN_OBJECT, NULL);
}

static void
jsonb_in_object_end(void *state)
{
	JsonbInState *_state = (JsonbInState *) state;

	_state->res = pushJsonbValue(&_state->state, WJB_END_OBJECT, NULL);
}

static void
jsonb_in_array_start(void *state)
{
	JsonbInState *_state = (JsonbInState *) state;

	_state->res = pushJsonbValue(&_state->state, WJB_BEGIN_ARRAY, NULL);
}

static void
jsonb_in_array_end(void *state)
{
	JsonbInState *_state = (JsonbInState *) state;

	_state->res = pushJsonbValue(&_state->state, WJB_END_ARRAY, NULL);
}

static void
jsonb_in_object_field_start(void *state, char *fname, bool isnull)
{
	JsonbInState *_state = (JsonbInState *) state;
	JsonbValue	v;

	v.type = jbvString;
	v.string.len = fname ? checkStringLen(strlen(fname)) : 0;
	v.string.val = fname ? pnstrdup(fname, v.string.len) : NULL;
	v.size = sizeof(JEntry) + v.string.len;

	_state->res = pushJsonbValue(&_state->state, WJB_KEY, &v);
}

static void
jsonb_put_escaped_value(StringInfo out, JsonbValue * v)
{
	switch (v->type)
	{
		case jbvNull:
			appendBinaryStringInfo(out, "null", 4);
			break;
		case jbvString:
			escape_json(out, pnstrdup(v->string.val, v->string.len));
			break;
		case jbvBool:
			if (v->boolean)
				appendBinaryStringInfo(out, "true", 4);
			else
				appendBinaryStringInfo(out, "false", 5);
			break;
		case jbvNumeric:
			appendStringInfoString(out,
								   DatumGetCString(DirectFunctionCall1(numeric_out,
																	   PointerGetDatum(v->numeric))));
			break;
		default:
			elog(ERROR, "unknown jsonb scalar type");
	}
}

/*
 * For jsonb we always want the de-escaped value - that's what's in token
 */
static void
jsonb_in_scalar(void *state, char *token, JsonTokenType tokentype)
{
	JsonbInState *_state = (JsonbInState *) state;
	JsonbValue	v;

	v.size = sizeof(JEntry);

	switch (tokentype)
	{

		case JSON_TOKEN_STRING:
			v.type = jbvString;
			v.string.len = token ? checkStringLen(strlen(token)) : 0;
			v.string.val = token ? pnstrdup(token, v.string.len) : NULL;
			v.size += v.string.len;
			break;
		case JSON_TOKEN_NUMBER:
			v.type = jbvNumeric;
			v.numeric = DatumGetNumeric(DirectFunctionCall3(numeric_in, CStringGetDatum(token), 0, -1));

			v.size += VARSIZE_ANY(v.numeric) +sizeof(JEntry) /* alignment */ ;
			break;
		case JSON_TOKEN_TRUE:
			v.type = jbvBool;
			v.boolean = true;
			break;
		case JSON_TOKEN_FALSE:
			v.type = jbvBool;
			v.boolean = false;
			break;
		case JSON_TOKEN_NULL:
			v.type = jbvNull;
			break;
		default:
			/* nothing else should be here in fact */
			Assert(false);
			break;
	}

	if (_state->state == NULL)
	{
		/* single scalar */
		JsonbValue	va;

		va.type = jbvArray;
		va.array.scalar = true;
		va.array.nelems = 1;

		_state->res = pushJsonbValue(&_state->state, WJB_BEGIN_ARRAY, &va);
		_state->res = pushJsonbValue(&_state->state, WJB_ELEM, &v);
		_state->res = pushJsonbValue(&_state->state, WJB_END_ARRAY, NULL);
	}
	else
	{
		JsonbValue *o = &_state->state->v;

		switch (o->type)
		{
			case jbvArray:
				_state->res = pushJsonbValue(&_state->state, WJB_ELEM, &v);
				break;
			case jbvObject:
				_state->res = pushJsonbValue(&_state->state, WJB_VALUE, &v);
				break;
			default:
				elog(ERROR, "unexpected parent of nested structure");
		}
	}
}

/*
 * JsonbToCString
 *	   Converts jsonb value in C-string. If out argument is not null
 * then resulting C-string is placed in it. Return pointer to string.
 * A typical case for passing the StringInfo in rather than NULL is where
 * the caller wants access to the len attribute without having to call
 * strlen, e.g. if they are converting it to a text* object.
 */
char *
JsonbToCString(StringInfo out, char *in, int estimated_len)
{
	bool		first = true;
	JsonbIterator *it;
	int			type = 0;
	JsonbValue	v;
	int			level = 0;
	bool		redo_switch = false;

	if (out == NULL)
		out = makeStringInfo();

	if (in == NULL)
	{
		appendStringInfoString(out, "");
		return out->data;
	}

	enlargeStringInfo(out, (estimated_len >= 0) ? estimated_len : 64);

	it = JsonbIteratorInit(in);

	while (redo_switch || ((type = JsonbIteratorNext(&it, &v, false)) != 0))
	{
		redo_switch = false;
		switch (type)
		{
			case WJB_BEGIN_ARRAY:
				if (!first)
					appendBinaryStringInfo(out, ", ", 2);
				first = true;

				if (!v.array.scalar)
					appendStringInfoChar(out, '[');
				level++;
				break;
			case WJB_BEGIN_OBJECT:
				if (!first)
					appendBinaryStringInfo(out, ", ", 2);
				first = true;
				appendStringInfoCharMacro(out, '{');

				level++;
				break;
			case WJB_KEY:
				if (!first)
					appendBinaryStringInfo(out, ", ", 2);
				first = true;

				/* json rules guarantee this is a string */
				jsonb_put_escaped_value(out, &v);
				appendBinaryStringInfo(out, ": ", 2);

				type = JsonbIteratorNext(&it, &v, false);
				if (type == WJB_VALUE)
				{
					first = false;
					jsonb_put_escaped_value(out, &v);
				}
				else
				{
					Assert(type == WJB_BEGIN_OBJECT || type == WJB_BEGIN_ARRAY);

					/*
					 * We need to rerun the current switch() since we need to
					 * output the object which we just got from the iterator
					 * before calling the iterator again.
					 */
					redo_switch = true;
				}
				break;
			case WJB_ELEM:
				if (!first)
					appendBinaryStringInfo(out, ", ", 2);
				else
					first = false;

				jsonb_put_escaped_value(out, &v);
				break;
			case WJB_END_ARRAY:
				level--;
				if (!v.array.scalar)
					appendStringInfoChar(out, ']');
				first = false;
				break;
			case WJB_END_OBJECT:
				level--;
				appendStringInfoCharMacro(out, '}');
				first = false;
				break;
			default:
				elog(ERROR, "unknown flag of jsonb iterator");
		}
	}

	Assert(level == 0);

	return out->data;
}
