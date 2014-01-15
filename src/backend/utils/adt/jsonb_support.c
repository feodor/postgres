/*-------------------------------------------------------------------------
 *
 * jsonb_support.c
 *    Support functions for jsonb
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 *
 * src/backend/utils/adt/jsonb_support.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"

/* turn a JsonbValue into a Jsonb */

Jsonb *
JsonbValueToJsonb(JsonbValue *v)
{
	Jsonb			*out;

	if (v == NULL)
	{
		out = NULL;
	}
	else if (v->type == jbvString || v->type == jbvBool ||
			 v->type == jbvNumeric || v->type == jbvNull)
	{
		ToJsonbState	*state = NULL;
		JsonbValue		*res;
		int				r;
		JsonbValue		scalarArray;

		scalarArray.type = jbvArray;
		scalarArray.array.scalar = true;
		scalarArray.array.nelems = 1;

		pushJsonbValue(&state, WJB_BEGIN_ARRAY, &scalarArray);
		pushJsonbValue(&state, WJB_ELEM, v);
		res = pushJsonbValue(&state, WJB_END_ARRAY, NULL);

		out = palloc(VARHDRSZ + res->size);
		SET_VARSIZE(out, VARHDRSZ + res->size);
		r = compressJsonb(res, VARDATA(out));
		Assert(r <= res->size);
		SET_VARSIZE(out, r + VARHDRSZ);
	}
	else
	{
		out = palloc(VARHDRSZ + v->size);

		Assert(v->type == jbvBinary);
		SET_VARSIZE(out, VARHDRSZ + v->binary.len);
		memcpy(VARDATA(out), v->binary.data, v->binary.len);
	}

	return out;
}

/*
 * Sort and unique pairs in hash-like JsonbValue
 */
void
uniqueJsonbValue(JsonbValue *v)
{
	bool    hasNonUniq = false;

	Assert(v->type == jbvHash);

	if (v->hash.npairs > 1)
		qsort_arg(v->hash.pairs, v->hash.npairs, sizeof(*v->hash.pairs),
				  compareJsonbPair, &hasNonUniq);

	if (hasNonUniq)
	{
		JsonbPair	*ptr = v->hash.pairs + 1,
					*res = v->hash.pairs;

		while(ptr - v->hash.pairs < v->hash.npairs)
		{
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

		v->hash.npairs = res + 1 - v->hash.pairs;
	}
}

/****************************************************************************
 *                         Compare Functions                                *
 ****************************************************************************/
int
compareJsonbStringValue(const void *a, const void *b, void *arg)
{
	const JsonbValue  *va = a;
	const JsonbValue  *vb = b;
	int					res;

	Assert(va->type == jbvString);
	Assert(vb->type == jbvString);

	if (va->string.len == vb->string.len)
	{
		res = memcmp(va->string.val, vb->string.val, va->string.len);
		if (res == 0 && arg)
			*(bool*)arg = true;
	}
	else
	{
		res = (va->string.len > vb->string.len) ? 1 : -1;
	}

	return res;
}

int
compareJsonbPair(const void *a, const void *b, void *arg)
{
	const 	JsonbPair *pa = a;
	const 	JsonbPair *pb = b;
	int 	res;

	res = compareJsonbStringValue(&pa->key, &pb->key, arg);

	/*
	 * guarantee keeping order of equal pair. Unique algorithm will
	 * prefer first element as value
	 */

	if (res == 0)
		res = (pa->order > pb->order) ? -1 : 1;

	return res;
}

int
compareJsonbValue(JsonbValue *a, JsonbValue *b)
{
	if (a->type == b->type)
	{
		switch(a->type)
		{
			case jbvNull:
				return 0;
			case jbvString:
				return compareJsonbStringValue(a, b, NULL);
			case jbvBool:
				if (a->boolean == b->boolean)
					return 0;
				return (a->boolean > b->boolean) ? 1 : -1;
			case jbvNumeric:
				return DatumGetInt32(DirectFunctionCall2(numeric_cmp,
														 PointerGetDatum(a->numeric),
														 PointerGetDatum(b->numeric)));
			case jbvArray:
				if (a->array.nelems == b->array.nelems)
				{
					int i, r;

					for(i=0; i<a->array.nelems; i++)
						if ((r = compareJsonbValue(a->array.elems + i, 
												   b->array.elems + i)) != 0)
							return r;

					return 0;
				}

				return (a->array.nelems > b->array.nelems) ? 1 : -1;
			case jbvHash:
				if (a->hash.npairs == b->hash.npairs)
				{
					int i, r;

					for(i=0; i<a->hash.npairs; i++)
					{
						if ((r = compareJsonbStringValue(&a->hash.pairs[i].key,
														 &b->hash.pairs[i].key,
														 NULL)) != 0)
							return r;
						if ((r = compareJsonbValue(&a->hash.pairs[i].value, 
												   &b->hash.pairs[i].value)) != 0)
							return r;
					}

					return 0;
				}

				return (a->hash.npairs > b->hash.npairs) ? 1 : -1;
			case jbvBinary:
				return compareJsonbBinaryValue(a->binary.data, b->binary.data);
			default:
				elog(PANIC, "unknown JsonbValue->type: %d", a->type);
		}
	}

	return (a->type > b->type) ? 1 : -1;
}

int
compareJsonbBinaryValue(char *a, char *b)
{
	JsonbIterator	*it1, *it2;
	int				res = 0;

	it1 = JsonbIteratorInit(a);
	it2 = JsonbIteratorInit(b);

	while(res == 0)
	{
		JsonbValue		v1, v2;
		int				r1, r2;

		r1 = JsonbIteratorGet(&it1, &v1, false);
		r2 = JsonbIteratorGet(&it2, &v2, false);

		if (r1 == r2)
		{
			if (r1 == 0)
				break; /* equal */

			if (v1.type == v2.type)
			{
				switch(v1.type)
				{
					case jbvString:
						res = compareJsonbStringValue(&v1, &v2, NULL);
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
					case jbvHash:
						if (v1.hash.npairs != v2.hash.npairs)
							res = (v1.hash.npairs > v2.hash.npairs) ? 1 : -1;
						break;
					default:
						break;
				}
			}
			else
			{
				res = (v1.type > v2.type) ?  1 : -1; /* dummy order */
			}
		}
		else
		{
			res = (r1 > r2) ? 1 : -1; /* dummy order */
		}
	}

	return res;
}

/****************************************************************************
 *                         find string key in hash or array                 *
 ****************************************************************************/
JsonbValue*
findUncompressedJsonbValueByValue(char *buffer, uint32 flags, uint32 *lowbound, JsonbValue *key)
{
	uint32				header = *(uint32*)buffer;
	static JsonbValue 	r;

	Assert((header & (JB_FLAG_ARRAY | JB_FLAG_OBJECT)) != 
		   (JB_FLAG_ARRAY | JB_FLAG_OBJECT));

	if (flags & JB_FLAG_ARRAY & header)
	{
		JEntry	*array = (JEntry*)(buffer + sizeof(header));
		char 	*data = (char*)(array + (header & JB_COUNT_MASK));
		int 	i;

		for(i=(lowbound) ? *lowbound : 0; i<(header & JB_COUNT_MASK); i++) {
			JEntry	*e = array + i;

			if (JBE_ISNULL(*e) && key->type == jbvNull)
			{
				r.type = jbvNull;
				if (lowbound)
					*lowbound = i;
				r.size = sizeof(JEntry);

				return &r;
			}
			else if (JBE_ISSTRING(*e) && key->type == jbvString )
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
				if ((JBE_ISBOOL_TRUE(*e) && key->boolean == true) ||
					(JBE_ISBOOL_FALSE(*e) && key->boolean == false))
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
				if (DatumGetBool(DirectFunctionCall2(numeric_eq, 
								 PointerGetDatum(data + INTALIGN(JBE_OFF(*e))),
								 PointerGetDatum(key->numeric))) == true)
				{
					r.type = jbvNumeric;
					r.numeric = (Numeric)(data + INTALIGN(JBE_OFF(*e)));
					if (lowbound)
						*lowbound = i;

					return &r;
				}
			}
		}
	}
	else if (flags & JB_FLAG_OBJECT & header)
	{
		JEntry  *array = (JEntry*)(buffer + sizeof(header));
		char    *data = (char*)(array + (header & JB_COUNT_MASK) * 2);
		uint32	stopLow = lowbound ? *lowbound : 0,
				stopHigh = (header & JB_COUNT_MASK),
				stopMiddle;

		if (key->type != jbvString)
			return NULL;

		while (stopLow < stopHigh)
		{
			int		difference;
			JEntry	*e;

			stopMiddle = stopLow + (stopHigh - stopLow) / 2;

			e = array + stopMiddle * 2;

			if (key->string.len == JBE_LEN(*e))
				difference = memcmp(data + JBE_OFF(*e), key->string.val,
									key->string.len);
			else
				difference = (JBE_LEN(*e) > key->string.len) ? 1 : -1;

			if (difference == 0)
			{
				JEntry	*v = e + 1;

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
					r.boolean = (JBE_ISBOOL_TRUE(*v)) ? true : false;
					r.size = sizeof(JEntry);
				}
				else if (JBE_ISNUMERIC(*v))
				{
					r.type = jbvNumeric;
					r.numeric = (Numeric)(data + INTALIGN(JBE_OFF(*v)));
					r.size = 2*sizeof(JEntry) + VARSIZE_ANY(r.numeric);
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
					r.size = 2*sizeof(JEntry) + r.binary.len;
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

JsonbValue*
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

JsonbValue*
getJsonbValue(char *buffer, uint32 flags, int32 i)
{
	uint32				header = *(uint32*)buffer;
	static JsonbValue	r;
	JEntry				*array, *e;
	char				*data;

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

	array = (JEntry*)(buffer + sizeof(header));

	if (flags & JB_FLAG_ARRAY & header)
	{
		e = array + i;
		data = (char*)(array + (header & JB_COUNT_MASK));
	}
	else if (flags & JB_FLAG_OBJECT & header)
	{
		e = array + i * 2 + 1;
		data = (char*)(array + (header & JB_COUNT_MASK) * 2);
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
		r.boolean = (JBE_ISBOOL_TRUE(*e)) ? true : false;
		r.size = sizeof(JEntry);
	}
	else if (JBE_ISNUMERIC(*e))
	{
		r.type = jbvNumeric;
		r.numeric = (Numeric)(data + INTALIGN(JBE_OFF(*e)));
		r.size = 2*sizeof(JEntry) + VARSIZE_ANY(r.numeric);
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
		r.size = r.binary.len + 2*sizeof(JEntry);
	}

	return &r;
}

/****************************************************************************
 *                         Walk on tree representation of jsonb             *
 ****************************************************************************/
static void
walkUncompressedJsonbDo(JsonbValue *v, walk_jsonb_cb cb, void *cb_arg, uint32 level) 
{
	int i;

	switch(v->type)
	{
		case jbvArray:
			cb(cb_arg, v, WJB_BEGIN_ARRAY, level);
			for(i=0; i<v->array.nelems; i++)
			{
				if (v->array.elems[i].type == jbvNull ||
					v->array.elems[i].type == jbvString ||
					v->array.elems[i].type == jbvBool ||
					v->array.elems[i].type == jbvNumeric ||
					v->array.elems[i].type == jbvBinary)
					cb(cb_arg, v->array.elems + i, WJB_ELEM, level);
				else
					walkUncompressedJsonbDo(v->array.elems + i, cb, cb_arg,
											level + 1);
			}
			cb(cb_arg, v, WJB_END_ARRAY, level);
			break;
		case jbvHash:
			cb(cb_arg, v, WJB_BEGIN_OBJECT, level);

			for(i=0; i<v->hash.npairs; i++)
			{
				cb(cb_arg, &v->hash.pairs[i].key, WJB_KEY, level);
				
				if (v->hash.pairs[i].value.type == jbvNull ||
					v->hash.pairs[i].value.type == jbvString ||
					v->hash.pairs[i].value.type == jbvBool ||
					v->hash.pairs[i].value.type == jbvNumeric ||
					v->hash.pairs[i].value.type == jbvBinary)
					cb(cb_arg, &v->hash.pairs[i].value, WJB_VALUE, level);
				else 
					walkUncompressedJsonbDo(&v->hash.pairs[i].value, cb, cb_arg,
											level + 1);
			}

			cb(cb_arg, v, WJB_END_OBJECT, level);
			break;
		default:
			elog(PANIC, "impossible JsonbValue->type: %d", v->type);
	}
}

void
walkUncompressedJsonb(JsonbValue *v, walk_jsonb_cb cb, void *cb_arg)
{
	if (v)
		walkUncompressedJsonbDo(v, cb, cb_arg, 0);
}

/****************************************************************************
 *                         Iteration over binary jsonb                      *
 ****************************************************************************/
static void
parseBuffer(JsonbIterator *it, char *buffer)
{
	uint32	header = *(uint32*)buffer;

	it->type = header & (JB_FLAG_ARRAY | JB_FLAG_OBJECT);
	it->nelems = header & JB_COUNT_MASK;
	it->buffer = buffer;


	buffer += sizeof(uint32);
	it->array = (JEntry*)buffer;

	it->state = jbi_start;

	switch(it->type)
	{
		case JB_FLAG_ARRAY:
			it->data = buffer + it->nelems * sizeof(JEntry);
			it->isScalar = (header & JB_FLAG_SCALAR) ? true : false;
			Assert(it->isScalar == false || it->nelems == 1);
			break;
		case JB_FLAG_OBJECT:
			it->data = buffer + it->nelems * sizeof(JEntry) * 2;
			break;
		default:
			elog(PANIC, "impossible type: %08x", it->type);
	}
}

JsonbIterator*
JsonbIteratorInit(char *buffer)
{
	JsonbIterator	*it = palloc(sizeof(*it));

	parseBuffer(it, buffer);
	it->next = NULL;

	return it;
}

static bool
formAnswer(JsonbIterator **it, JsonbValue *v, JEntry *e, bool skipNested)
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
		v->boolean = (JBE_ISBOOL_TRUE(*e)) ? true : false;
		v->size = sizeof(JEntry);

		return false;
	}
	else if (JBE_ISNUMERIC(*e))
	{
		v->type = jbvNumeric;
		v->numeric = (Numeric)((*it)->data + INTALIGN(JBE_OFF(*e)));
		v->size = 2*sizeof(JEntry) + VARSIZE_ANY(v->numeric);

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
		v->size = v->binary.len + 2*sizeof(JEntry);

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

static JsonbIterator*
up(JsonbIterator *it)
{
	JsonbIterator *v = it->next;

	pfree(it);

	return v;
}

int
JsonbIteratorGet(JsonbIterator **it, JsonbValue *v, bool skipNested)
{
	int res;

	if (*it == NULL)
		return 0;

	/*
	 * Encode all possible states by one integer. That's possible
	 * because enum members of JsonbIterator->state uses different bits
	 * than JB_FLAG_ARRAY/JB_FLAG_OBJECT. See definition of JsonbIterator
	 */

	switch((*it)->type | (*it)->state)
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
			else if (formAnswer(it, v, &(*it)->array[ (*it)->i++ ], skipNested))
			{
				res = JsonbIteratorGet(it, v, skipNested);
			}
			else
			{
				res = WJB_ELEM;
			}
			break;
		case JB_FLAG_OBJECT | jbi_start:
			(*it)->state = jbi_key;
			(*it)->i = 0;
			v->type = jbvHash;
			v->hash.npairs = (*it)->nelems;
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
				formAnswer(it, v, &(*it)->array[ (*it)->i * 2 ], false);
				(*it)->state = jbi_value;
				res = WJB_KEY;
			}
			break;
		case JB_FLAG_OBJECT | jbi_value:
			(*it)->state = jbi_key;
			if (formAnswer(it, v, &(*it)->array[ ((*it)->i++) * 2 + 1], skipNested))
				res = JsonbIteratorGet(it, v, skipNested);
			else
				res = WJB_VALUE;
			break;
		default:
			elog(PANIC,"unknown state %08x", (*it)->type & (*it)->state);
	}

	return res;
}

/****************************************************************************
 *        Transformation from tree to binary representation of jsonb        *
 ****************************************************************************/
typedef struct CompressState
{
	char	*begin;
	char	*ptr;

	struct {
		uint32	i;
		uint32	*header;
		JEntry	*array;
		char	*begin;
	} *levelstate, *lptr, *pptr;

	uint32	maxlevel;
	
} CompressState;

#define	curLevelState	state->lptr
#define prevLevelState	state->pptr

static void
putJEntryString(CompressState *state, JsonbValue* value, uint32 level, uint32 i)
{
	curLevelState = state->levelstate + level;

	if (i == 0)
		curLevelState->array[0].entry = JENTRY_ISFIRST;
	else
		curLevelState->array[i].entry = 0;

	switch(value->type)
	{
		case jbvNull:
			curLevelState->array[i].entry |= JENTRY_ISNULL;

			if (i>0)
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

			if (i>0)
				curLevelState->array[i].entry |=
					curLevelState->array[i - 1].entry & JENTRY_POSMASK;
			break;
		case jbvNumeric:
			{
				int addlen = INTALIGN(state->ptr - state->begin) -
								(state->ptr - state->begin);
				int	numlen = VARSIZE_ANY(value->numeric); 

				switch(addlen)
				{
					case 3:
						*state->ptr = '\0'; state->ptr++;
					case 2:
						*state->ptr = '\0'; state->ptr++;
					case 1:
						*state->ptr = '\0'; state->ptr++;
					case 0:
					default:
						break;
				}

				memcpy(state->ptr, value->numeric, numlen);
				state->ptr += numlen;

				curLevelState->array[i].entry |= JENTRY_ISNUMERIC;
				if (i == 0)
					curLevelState->array[i].entry |= addlen + numlen;
				else
					curLevelState->array[i].entry |= 
						(curLevelState->array[i - 1].entry & JENTRY_POSMASK) +
						addlen + numlen;
				break;
			}
		case jbvBinary:
			{
				int addlen = INTALIGN(state->ptr - state->begin) -
								(state->ptr - state->begin);

				switch(addlen)
				{
					case 3:
						*state->ptr = '\0'; state->ptr++;
					case 2:
						*state->ptr = '\0'; state->ptr++;
					case 1:
						*state->ptr = '\0'; state->ptr++;
					case 0:
					default:
						break;
				}

				memcpy(state->ptr, value->binary.data, value->binary.len);
				state->ptr += value->binary.len;

				curLevelState->array[i].entry |= JENTRY_ISNEST;

				if (i == 0)
					curLevelState->array[i].entry |= addlen + value->binary.len;
				else
					curLevelState->array[i].entry |=
						(curLevelState->array[i - 1].entry & JENTRY_POSMASK) +
						addlen + value->binary.len;
			}
			break;
		default:
			elog(PANIC,"Unsupported JsonbValue type: %d", value->type);
	}
}

static void
compressCallback(void *arg, JsonbValue* value, uint32 flags, uint32 level)
{
	CompressState	*state = arg;

	if (level == state->maxlevel) {
		state->maxlevel *= 2;
		state->levelstate = repalloc(state->levelstate,
								 sizeof(*state->levelstate) * state->maxlevel);
	}

	curLevelState = state->levelstate + level;

	if (flags & (WJB_BEGIN_ARRAY | WJB_BEGIN_OBJECT))
	{
		Assert(((flags & WJB_BEGIN_ARRAY) && value->type == jbvArray) ||
			   ((flags & WJB_BEGIN_OBJECT) && value->type == jbvHash));

		curLevelState->begin = state->ptr;

		switch(INTALIGN(state->ptr - state->begin) -
			   (state->ptr - state->begin))
		{
			case 3:
				*state->ptr = '\0'; state->ptr++;
			case 2:
				*state->ptr = '\0'; state->ptr++;
			case 1:
				*state->ptr = '\0'; state->ptr++;
			case 0:
			default:
				break;
		}

		curLevelState->header = (uint32*)state->ptr;
		state->ptr += sizeof(*curLevelState->header);

		curLevelState->array = (JEntry*)state->ptr;
		curLevelState->i = 0;

		if (value->type == jbvArray)
		{
			*curLevelState->header = value->array.nelems | JB_FLAG_ARRAY ;
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
			*curLevelState->header = value->hash.npairs | JB_FLAG_OBJECT ;
			state->ptr += sizeof(JEntry) * value->hash.npairs * 2;
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
		uint32	len, i;

		Assert(((flags & WJB_END_ARRAY) && value->type == jbvArray) ||
			   ((flags & WJB_END_OBJECT) && value->type == jbvHash));
		if (level == 0)
			return;

		len = state->ptr - (char*)curLevelState->begin;

		prevLevelState = curLevelState - 1;

		if (*prevLevelState->header & JB_FLAG_ARRAY) {
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
			i = 2 * prevLevelState->i + 1; /* VALUE, not a KEY */

			prevLevelState->array[i].entry = JENTRY_ISNEST;

			prevLevelState->array[i].entry |=
				(prevLevelState->array[i - 1].entry & JENTRY_POSMASK) + len;
		}
		else
		{
			elog(PANIC, "Wrong parent");
		}

		Assert(state->ptr - curLevelState->begin <= value->size);
		prevLevelState->i++;
	}
	else
	{
		elog(PANIC, "Wrong flags");
	}
}

uint32
compressJsonb(JsonbValue *v, char *buffer) {
	uint32			l = 0;
	CompressState	state;

	state.begin = state.ptr = buffer;
	state.maxlevel = 8;
	state.levelstate = palloc(sizeof(*state.levelstate) * state.maxlevel);

	walkUncompressedJsonb(v, compressCallback, &state);

	l = state.ptr - buffer;
	Assert(l <= v->size);

	return l;
}

/****************************************************************************
 *                  Iteration-like forming jsonb                            *
 *       Note: it believes by default in already sorted keys in hash,       *
 *     although with r == WJB_END_OBJECT and v == NULL  it will sort itself *
 ****************************************************************************/
static ToJsonbState*
pushState(ToJsonbState **state)
{
	ToJsonbState	*ns = palloc(sizeof(*ns));

	ns->next = *state;
	return ns;
}

static void
appendArray(ToJsonbState *state, JsonbValue *v)
{
	JsonbValue	*a = &state->v;

	Assert(a->type == jbvArray);

	if (a->array.nelems >= state->size)
	{
		state->size *= 2;
		a->array.elems = repalloc(a->array.elems,
								   sizeof(*a->array.elems) * state->size);
	}

	a->array.elems[a->array.nelems ++] = *v;

	a->size += v->size;
}

static void
appendKey(ToJsonbState *state, JsonbValue *v)
{
	JsonbValue	*h = &state->v;

	Assert(h->type == jbvHash);

	if (h->hash.npairs >= state->size)
	{
		state->size *= 2;
		h->hash.pairs = repalloc(h->hash.pairs,
									sizeof(*h->hash.pairs) * state->size);
	}

	h->hash.pairs[h->hash.npairs].key = *v;
	h->hash.pairs[h->hash.npairs].order = h->hash.npairs;

	h->size += v->size;
}

static void
appendValue(ToJsonbState *state, JsonbValue *v)
{

	JsonbValue	*h = &state->v;

	Assert(h->type == jbvHash);

	h->hash.pairs[h->hash.npairs++].value = *v;

	h->size += v->size;
}


JsonbValue*
pushJsonbValue(ToJsonbState **state, int r /* WJB_* */, JsonbValue *v) {
	JsonbValue	*h = NULL;

	switch(r)
	{
		case WJB_BEGIN_ARRAY:
			*state = pushState(state);
			h = &(*state)->v;
			(*state)->v.type = jbvArray;
			(*state)->v.size = 3*sizeof(JEntry);
			(*state)->v.array.nelems = 0;
			(*state)->v.array.scalar = (v && v->array.scalar) ? true : false;
			(*state)->size = (v && v->type == jbvArray && v->array.nelems > 0)
								? v->array.nelems : 4;
			(*state)->v.array.elems = palloc(sizeof(*(*state)->v.array.elems) *
											 (*state)->size);
			break;
		case WJB_BEGIN_OBJECT:
			*state = pushState(state);
			h = &(*state)->v;
			(*state)->v.type = jbvHash;
			(*state)->v.size = 3*sizeof(JEntry);
			(*state)->v.hash.npairs = 0;
			(*state)->size = (v && v->type == jbvHash && v->hash.npairs > 0) ?
									v->hash.npairs : 4;
			(*state)->v.hash.pairs = palloc(sizeof(*(*state)->v.hash.pairs) *
											(*state)->size);
			break;
		case WJB_ELEM:
			Assert(v->type == jbvNull || v->type == jbvString ||
				   v->type == jbvBool || v->type == jbvNumeric || 
				   v->type == jbvBinary); 
			appendArray(*state, v);
			break;
		case WJB_KEY:
			Assert(v->type == jbvString);
			appendKey(*state, v);
			break;
		case WJB_VALUE:
			Assert(v->type == jbvNull || v->type == jbvString ||
				   v->type == jbvBool || v->type == jbvNumeric || 
				   v->type == jbvBinary); 
			appendValue(*state, v);
			break;
		case WJB_END_OBJECT:
			h = &(*state)->v;
			if (v == NULL)
				uniqueJsonbValue(h);
		case WJB_END_ARRAY:
			h = &(*state)->v;
			*state = (*state)->next;
			if (*state)
			{
				switch((*state)->v.type)
				{
					case jbvArray:
						appendArray(*state, h);
						break;
					case jbvHash:
						appendValue(*state, h);
						break;
					default:
						elog(PANIC, "wrong parent type: %d", (*state)->v.type);
				}
			}
			break;
		default:
			elog(PANIC, "wrong type: %08x", r);
	}

	return h;
}

