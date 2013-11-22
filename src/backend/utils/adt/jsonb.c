/*-------------------------------------------------------------------------
 *
 * jsonb.c
 *		I/O for jsonb type 
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 *
 * NOTE. JSONB type is designed to be binary compatible with hstore.
 *
 * src/backend/utils/adt/jsonb_support.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonb.h"

static size_t
checkStringLen(size_t len)
{
	 if (len > JSONB_MAX_STRING_LEN)
		  ereport(ERROR,
					 (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
					  errmsg("string too long for jsonb string")));
	 return len;
}

static Jsonb*
dumpJsonb(JsonbValue *p)
{
	 uint32			 buflen;
	 Jsonb			*out;

	 if (p == NULL || (p->type == jbvArray && p->array.nelems == 0) || (p->type == jbvHash && p->hash.npairs == 0))
	 {
		  buflen = 0;
		  out = palloc(VARHDRSZ);
	 }
	 else
	 {
		  buflen = VARHDRSZ + p->size;
		  out = palloc(buflen);
		  SET_VARSIZE(out, buflen);

		  buflen = compressJsonb(p, VARDATA(out));
	 }
	 SET_VARSIZE(out, buflen + VARHDRSZ);

	 return out;
}

Datum
jsonb_in(PG_FUNCTION_ARGS)
{
	 PG_RETURN_POINTER(dumpJsonb(parseJsonb(PG_GETARG_CSTRING(0), -1, true)));
}

static void recvJsonb(StringInfo buf, JsonbValue *v, uint32 level, uint32 header);

static void
recvJsonbValue(StringInfo buf, JsonbValue *v, uint32 level, int c)
{
	 uint32  hentry = c & JENTRY_TYPEMASK;

	 if (hentry == JENTRY_ISNULL)
	 {
		  v->type = jbvNull;
		  v->size = sizeof(JEntry);
	 }
	 else if (hentry == JENTRY_ISOBJECT || hentry == JENTRY_ISARRAY || hentry == JENTRY_ISCALAR)
	 {
		  recvJsonb(buf, v, level + 1, (uint32)c);
	 }
	 else if (hentry == JENTRY_ISFALSE || hentry == JENTRY_ISTRUE)
	 {
		  v->type = jbvBool;
		  v->size = sizeof(JEntry);
		  v->boolean = (hentry == JENTRY_ISFALSE) ? false : true;
	 }
	 else if (hentry == JENTRY_ISNUMERIC)
	 {
		  v->type = jbvNumeric;
		  v->numeric = DatumGetNumeric(DirectFunctionCall3(numeric_recv, PointerGetDatum(buf),
																			Int32GetDatum(0), Int32GetDatum(-1)));
		  v->size = sizeof(JEntry) * 2 + VARSIZE_ANY(v->numeric);
	 }
	 else if (hentry == JENTRY_ISSTRING)
	 {
		  v->type = jbvString;
		  v->string.val = pq_getmsgtext(buf, c, &c);
		  v->string.len = checkStringLen(c);
		  v->size = sizeof(JEntry) + v->string.len;
	 }
	 else
	 {
		  elog(ERROR, "bogus input");
	 }
}

static void
recvJsonb(StringInfo buf, JsonbValue *v, uint32 level, uint32 header)
{
	 uint32  hentry;
	 uint32  i;

	 hentry = header & JENTRY_TYPEMASK;

	 v->size = 3 * sizeof(JEntry);
	 if (hentry == JENTRY_ISOBJECT)
	 {
		  v->type = jbvHash;
		  v->hash.npairs = header & JB_COUNT_MASK;
		  if (v->hash.npairs > 0)
		  {
				v->hash.pairs = palloc(sizeof(*v->hash.pairs) * v->hash.npairs);

				for(i=0; i<v->hash.npairs; i++)
				{
					 recvJsonbValue(buf, &v->hash.pairs[i].key, level, pq_getmsgint(buf, 4));
					 if (v->hash.pairs[i].key.type != jbvString)
						  elog(ERROR, "jsonb's key could be only a string");

					 recvJsonbValue(buf, &v->hash.pairs[i].value, level, pq_getmsgint(buf, 4));

					 v->size += v->hash.pairs[i].key.size + v->hash.pairs[i].value.size;
				}

				ORDER_PAIRS(v->hash.pairs, v->hash.npairs, v->size -= ptr->key.size + ptr->value.size);
		  }
	 }
	 else if (hentry == JENTRY_ISARRAY || hentry == JENTRY_ISCALAR)
	 {
		  v->type = jbvArray;
		  v->array.nelems = header & JB_COUNT_MASK;
		  v->array.scalar = (hentry == JENTRY_ISCALAR) ? true : false;

		  if (v->array.scalar && v->array.nelems != 1)
				elog(ERROR, "bogus input");

		  if (v->array.nelems > 0)
		  {
				v->array.elems = palloc(sizeof(*v->array.elems) * v->array.nelems);

				for(i=0; i<v->array.nelems; i++)
				{
					 recvJsonbValue(buf, v->array.elems + i, level, pq_getmsgint(buf, 4));
					 v->size += v->array.elems[i].size;
				}
		  }
	 }
	 else
	 {
				elog(ERROR, "bogus input");
	 }
}

Datum
jsonb_recv(PG_FUNCTION_ARGS)
{
	 StringInfo  buf = (StringInfo) PG_GETARG_POINTER(0);
	 JsonbValue v;

	 recvJsonb(buf, &v, 0, pq_getmsgint(buf, 4));

	 PG_RETURN_POINTER(dumpJsonb(&v));
}

static void
putEscapedValue(StringInfo out, JsonbValue *v)
{
	 switch(v->type)
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
				appendStringInfoString(out, DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(v->numeric))));
				break;
		  default:
				elog(PANIC, "Unknown type");
	 }
}

static char*
JsonbToCString(StringInfo out, char *in, int len /* just estimation */)
{
	 bool			first = true;
	 JsonbIterator  *it;
	 int			type;
	 JsonbValue	  	v;
	 int			 level = 0;

	 if (out == NULL)
		  out = makeStringInfo();

	 if (in == NULL)
	 {
		  appendStringInfoString(out, "");
		  return out->data;
	 }

	 enlargeStringInfo(out, (len >= 0) ? len : 64);

	 it = JsonbIteratorInit(in);

	 while((type = JsonbIteratorGet(&it, &v, false)) != 0)
	 {
reout:
		  switch(type)
		  {
				case WJB_BEGIN_ARRAY:
					 if (first == false)
						  appendBinaryStringInfo(out, ", ", 2);
					 first = true;

					 if (v.array.scalar == false)
						 appendStringInfoChar(out, '[');
					 level++;
					 break;
				case WJB_BEGIN_OBJECT:
					 if (first == false)
						  appendBinaryStringInfo(out, ", ", 2);
					 first = true;
					 appendStringInfoCharMacro(out, '{');

					 level++;
					 break;
				case WJB_KEY:
					 if (first == false)
						  appendBinaryStringInfo(out, ", ", 2);
					 first = true;

					 putEscapedValue(out, &v);
					 appendBinaryStringInfo(out, ": ", 2);

					 type = JsonbIteratorGet(&it, &v, false);
					 if (type == WJB_VALUE)
					 {
						  first = false;
						  putEscapedValue(out, &v);
					 }
					 else
					 {
						  Assert(type == WJB_BEGIN_OBJECT || type == WJB_BEGIN_ARRAY);
						  goto reout;
					 }
					 break;
				case WJB_ELEM:
					 if (first == false)
						  appendBinaryStringInfo(out, ", ", 2);
					 else
						  first = false;

					 putEscapedValue(out, &v);
					 break;
				case WJB_END_ARRAY:
					 level--;
					 if (v.array.scalar == false)
						  appendStringInfoChar(out, ']');
					 first = false;
					 break;
				case WJB_END_OBJECT:
					 level--;
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

Datum
jsonb_out(PG_FUNCTION_ARGS)
{
	 Jsonb  *jb = PG_GETARG_JSONB(0);
	 char	 *out;

	 out = JsonbToCString(NULL, (JB_ISEMPTY(jb)) ? NULL : VARDATA(jb), VARSIZE(jb));

	 PG_RETURN_CSTRING(out);
}

Datum
jsonb_send(PG_FUNCTION_ARGS)
{
	 Jsonb			 *in = PG_GETARG_JSONB(0);
	 StringInfoData  buf;

	 pq_begintypsend(&buf);

	 if (JB_ISEMPTY(in))
	 {
		  pq_sendint(&buf, 0, 4);
	 }
	 else
	 {
		  JsonbIterator  *it;
		  int				 type;
		  JsonbValue	  v;
		  uint32			 flag;
		  bytea			  *nbuf;

		  enlargeStringInfo(&buf, VARSIZE_ANY(in) /* just estimation */);

		  it = JsonbIteratorInit(VARDATA_ANY(in));

		  while((type = JsonbIteratorGet(&it, &v, false)) != 0)
		  {
				switch(type)
				{
					 case WJB_BEGIN_ARRAY:
						  flag = (v.array.scalar) ? JENTRY_ISCALAR : JENTRY_ISARRAY;
						  pq_sendint(&buf, v.array.nelems | flag, 4);
						  break;
					 case WJB_BEGIN_OBJECT:
						  pq_sendint(&buf, v.hash.npairs | JENTRY_ISOBJECT, 4);
						  break;
					 case WJB_KEY:
						  pq_sendint(&buf, v.string.len | JENTRY_ISSTRING, 4);
						  pq_sendtext(&buf, v.string.val, v.string.len);
						  break;
					 case WJB_ELEM:
					 case WJB_VALUE:
						  switch(v.type)
						  {
								case jbvNull:
									 pq_sendint(&buf, JENTRY_ISNULL, 4);
									 break;
								case jbvString:
									 pq_sendint(&buf, v.string.len | JENTRY_ISSTRING, 4);
									 pq_sendtext(&buf, v.string.val, v.string.len);
									 break;
								case jbvBool:
									 pq_sendint(&buf, (v.boolean) ? JENTRY_ISTRUE : JENTRY_ISFALSE, 4);
									 break;
								case jbvNumeric:
									 nbuf = DatumGetByteaP(DirectFunctionCall1(numeric_send, NumericGetDatum(v.numeric)));
									 pq_sendint(&buf, VARSIZE_ANY(nbuf) | JENTRY_ISNUMERIC, 4);
									 pq_sendbytes(&buf, (char*)nbuf, VARSIZE_ANY(nbuf));
									 break;
								default:
									 elog(PANIC, "Wrong type: %u", v.type);
						  }
						  break;
					 case WJB_END_ARRAY:
					 case WJB_END_OBJECT:
						  break;
					 default:
						  elog(PANIC, "Wrong flags");
				}
		  }
	 }

	 PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}
