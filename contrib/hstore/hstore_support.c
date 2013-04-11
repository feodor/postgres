#include "postgres.h"

#include "hstore.h"

int
compareHStoreStringValue(const HStoreValue *va, const HStoreValue *vb)
{
	Assert(va->type == hsvString);
	Assert(vb->type == hsvString);

	if (va->string.len == vb->string.len)
		return memcmp(va->string.val, vb->string.val, va->string.len);

	return (va->string.len > vb->string.len) ? 1 : -1;
}

int
compareHStorePair(const void *a, const void *b, void *arg)
{
	const HStorePair *pa = a;
	const HStorePair *pb = b;

	int res = compareHStoreStringValue(&pa->key, &pb->key);

	if (res == 0)
		*(bool*)arg = true;

	return res;
}

HStoreValue*
findUncompressedHStoreValue(char *buffer, uint32 flags, uint32 *lowbound, char *key, uint32 keylen)
{
	uint32				header = *(uint32*)buffer;
	static HStoreValue 	r;

	Assert((header & (HS_FLAG_ARRAY | HS_FLAG_HSTORE)) != (HS_FLAG_ARRAY | HS_FLAG_HSTORE));

	if (flags & HS_FLAG_ARRAY & header)
	{
		HEntry	*array = (HEntry*)(buffer + sizeof(header));
		char 	*data = (char*)(array + (header & HS_COUNT_MASK));
		int 	i;

		for(i=(lowbound) ? *lowbound : 0; i<(header & HS_COUNT_MASK); i++) {
			HEntry	*e = array + i;

			if ((e->entry & HENTRY_ISNULL) && key == NULL) 
			{
				r.type = hsvNullString;
				if (lowbound)
					*lowbound = i;

				return &r;
			} 
			else if ((e->entry & (HENTRY_ISARRAY | HENTRY_ISHSTORE)) == 0 && key != NULL)
			{
				if (keylen == HSE_LEN(*e) && memcmp(key, data + HSE_OFF(*e), keylen) == 0)
				{
					r.type = hsvString;
					r.string.val = data + HSE_OFF(*e);
					r.string.len = keylen;
					if (lowbound)
						*lowbound = i;

					return &r;
				}
			}
		}
	}
	else if (flags & HS_FLAG_HSTORE & header)
	{
		HEntry  *array = (HEntry*)(buffer + sizeof(header));
		char    *data = (char*)(array + (header & HS_COUNT_MASK) * 2);
		uint32	stopLow = lowbound ? *lowbound : 0,
				stopHigh = (header & HS_COUNT_MASK),
				stopMiddle;

		while (stopLow < stopHigh)
		{
			int		difference;
			HEntry	*e;

			stopMiddle = stopLow + (stopHigh - stopLow) / 2;

			e = array + stopMiddle * 2;

			if (keylen == HSE_LEN(*e))
				difference = memcmp(data + HSE_OFF(*e), key, keylen);
			else
				difference = (HSE_LEN(*e) > keylen) ? 1 : -1;

			if (difference == 0)
			{
				HEntry	*v = e + 1;

				if (lowbound)
					*lowbound = stopMiddle + 1;

				if (HSE_ISSTRING(*v))
				{
					if (HSE_ISNULL(*v))
					{
						r.type = hsvNullString;
					}
					else
					{
						r.type = hsvString;
						r.string.val = data + HSE_OFF(*v);
						r.string.len = HSE_LEN(*v);
					}
				}
				else
				{
					r.type = hsvBinary;
					r.dump.data = data + INTALIGN(HSE_OFF(*v));
					r.dump.len = HSE_LEN(*v) - (INTALIGN(HSE_OFF(*v)) - HSE_OFF(*v));
					r.size = r.dump.len + sizeof(HEntry);
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
	}

	return NULL;
}

static void
walkUncompressedHStoreDo(HStoreValue *v, walk_hstore_cb cb, void *cb_arg, uint32 level) 
{
	int i;

	switch(v->type) 
	{
		case hsvArray:
			cb(cb_arg, v, WHS_BEGIN_ARRAY, level);
			for(i=0; i<v->array.nelems; i++)
			{
				if (v->array.elems[i].type == hsvNullString || v->array.elems[i].type == hsvString ||
					v->array.elems[i].type == hsvBinary)
					cb(cb_arg, v->array.elems + i, WHS_ELEM, level);
				else
					walkUncompressedHStoreDo(v->array.elems + i, cb, cb_arg, level + 1);
			}
			cb(cb_arg, v, WHS_END_ARRAY, level);
			break;
		case hsvHash:
			cb(cb_arg, v, WHS_BEGIN_HSTORE, level);

			for(i=0; i<v->hash.npairs; i++)
			{
				cb(cb_arg, &v->hash.pairs[i].key, WHS_KEY, level);
				
				if (v->hash.pairs[i].value.type == hsvNullString || v->hash.pairs[i].value.type == hsvString ||
					v->hash.pairs[i].value.type == hsvBinary)
					cb(cb_arg, &v->hash.pairs[i].value, WHS_VALUE, level);
				else 
					walkUncompressedHStoreDo(&v->hash.pairs[i].value, cb, cb_arg, level + 1);
			}

			cb(cb_arg, v, WHS_END_HSTORE, level);
			break;
		default:
			elog(PANIC, "impossible HStoreValue->type: %d", v->type);
	}
}

void
walkUncompressedHStore(HStoreValue *v, walk_hstore_cb cb, void *cb_arg)
{
	if (v)
		walkUncompressedHStoreDo(v, cb, cb_arg, 0); 
}

static void
parseBuffer(HStoreIterator *it, char *buffer)
{
	it->type = (*(uint32*)buffer) & (HS_FLAG_ARRAY | HS_FLAG_HSTORE);
	it->nelems = (*(uint32*)buffer) & HS_COUNT_MASK;

	buffer += sizeof(uint32);

	it->array = (HEntry*)buffer;

	it->state = hsi_start;

	switch(it->type)
	{
		case HS_FLAG_ARRAY:
			it->data = buffer + it->nelems * sizeof(HEntry);
			break;
		case HS_FLAG_HSTORE:
			it->data = buffer + it->nelems * sizeof(HEntry) * 2;
			break;
		default:
			elog(PANIC, "impossible type: %08x", it->type);
	}
}

HStoreIterator*
HStoreIteratorInit(char *buffer)
{
	HStoreIterator	*it = palloc(sizeof(*it));

	parseBuffer(it, buffer);
	it->next = NULL;

	return it;
}

static bool
formAnswer(HStoreIterator **it, HStoreValue *v, HEntry *e, bool skipNested)
{
	if (HSE_ISSTRING(*e))
	{
		if (HSE_ISNULL(*e))
		{
			v->type = hsvNullString;
			v->size = sizeof(HEntry);
		}
		else
		{
			v->type = hsvString;
			v->string.val = (*it)->data + HSE_OFF(*e);
			v->string.len = HSE_LEN(*e);
			v->size = sizeof(HEntry) + v->string.len;
		}

		return false;
	} 
	else if (skipNested)
	{
		v->type = hsvBinary;
		v->dump.data = (*it)->data + INTALIGN(HSE_OFF(*e));
		v->dump.len = HSE_LEN(*e) - (INTALIGN(HSE_OFF(*e)) - HSE_OFF(*e));
		v->size = v->dump.len + sizeof(HEntry);

		return false;
	}
	else
	{
		HStoreIterator *nit = palloc(sizeof(*nit));

		parseBuffer(nit, (*it)->data + INTALIGN(HSE_OFF(*e)));
		nit->next = *it;
		*it = nit;

		return true;
	}
}

static HStoreIterator*
up(HStoreIterator *it)
{
	HStoreIterator *v = it->next;

	pfree(it);

	return v;
}

int
HStoreIteratorGet(HStoreIterator **it, HStoreValue *v, bool skipNested)
{
	int res;

	if (*it == NULL)
		return 0;

	switch((*it)->type | (*it)->state)
	{
		case HS_FLAG_ARRAY | hsi_start:
			(*it)->state = hsi_elem;
			(*it)->i = 0;
			v->type = hsvArray;
			v->array.nelems = (*it)->nelems;
			res = WHS_BEGIN_ARRAY;
			break;
		case HS_FLAG_ARRAY | hsi_elem:
			if ((*it)->i >= (*it)->nelems)
			{
				*it = up(*it);
				res = WHS_END_ARRAY;
			}
			else if (formAnswer(it, v, &(*it)->array[ (*it)->i++ ], skipNested))
			{
				res = HStoreIteratorGet(it, v, skipNested);
			}
			else
			{
				res = WHS_ELEM;
			}
			break;
		case HS_FLAG_HSTORE | hsi_start:
			(*it)->state = hsi_key;
			(*it)->i = 0;
			v->type = hsvHash;
			v->hash.npairs = (*it)->nelems;
			res = WHS_BEGIN_HSTORE;
			break;
		case HS_FLAG_HSTORE | hsi_key:
			if ((*it)->i >= (*it)->nelems)
			{
				*it = up(*it);
				res = WHS_END_HSTORE;
			}
			else
			{
				formAnswer(it, v, &(*it)->array[ (*it)->i * 2 ], false);
				(*it)->state = hsi_value;
				res = WHS_KEY;
			}
			break;
		case HS_FLAG_HSTORE | hsi_value:
			(*it)->state = hsi_key;
			if (formAnswer(it, v, &(*it)->array[ ((*it)->i++) * 2 + 1], skipNested))
				res = HStoreIteratorGet(it, v, skipNested);
			else
				res = WHS_VALUE;
			break;
		default:
			elog(PANIC,"unknown state %08x", (*it)->type & (*it)->state);
	}

	return res;
}

typedef struct CompressState
{
	char	*begin;
	char	*ptr;

	struct {
		uint32	i;
		uint32	*header;
		HEntry	*array;
		char	*begin;
	} *levelstate, *lptr, *pptr;

	uint32	maxlevel;
	
} CompressState;

#define	curLevelState	state->lptr
#define prevLevelState	state->pptr

static void
putHEntryString(CompressState *state, HStoreValue* value, uint32 level, uint32 i)
{
	curLevelState = state->levelstate + level;

	if (i == 0)
		curLevelState->array[0].entry = HENTRY_ISFIRST;
	else
		curLevelState->array[i].entry = 0;

	switch(value->type)
	{
		case hsvNullString:
			curLevelState->array[i].entry |= HENTRY_ISNULL;

			if (i>0)
				curLevelState->array[i].entry |=
					curLevelState->array[i - 1].entry & HENTRY_POSMASK;
			break;
		case hsvString:
			memcpy(state->ptr, value->string.val, value->string.len);
			state->ptr += value->string.len;

			if (i == 0)
				curLevelState->array[i].entry |= value->string.len;
			else
				curLevelState->array[i].entry |= 
					(curLevelState->array[i - 1].entry & HENTRY_POSMASK) + value->string.len;
			break;
		case hsvBinary:
			{
				int addlen = INTALIGN(state->ptr - state->begin) - (state->ptr - state->begin);

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

				memcpy(state->ptr, value->dump.data, value->dump.len);
				state->ptr += value->dump.len;

				curLevelState->array[i].entry |= (*(uint32*)value->dump.data) & (HS_FLAG_ARRAY | HS_FLAG_HSTORE);

				if (i == 0)
					curLevelState->array[i].entry |= addlen + value->dump.len;
				else
					curLevelState->array[i].entry |=
						(curLevelState->array[i - 1].entry & HENTRY_POSMASK) + addlen + value->dump.len;
			}
			break;
		default:
			elog(PANIC,"Unsupported HStoreValue type: %d", value->type);
	}
}

static void
compressCallback(void *arg, HStoreValue* value, uint32 flags, uint32 level)
{
	CompressState	*state = arg;

	if (level == state->maxlevel) {
		state->maxlevel *= 2;
		state->levelstate = repalloc(state->levelstate, sizeof(*state->levelstate) * state->maxlevel);
	}

	curLevelState = state->levelstate + level;

	if (flags & (WHS_BEGIN_ARRAY | WHS_BEGIN_HSTORE))
	{
		Assert(((flags & WHS_BEGIN_ARRAY) && value->type == hsvArray) ||
			   ((flags & WHS_BEGIN_HSTORE) && value->type == hsvHash));

		curLevelState->begin = state->ptr;

		switch(INTALIGN(state->ptr - state->begin) - (state->ptr - state->begin))
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

		curLevelState->array = (HEntry*)state->ptr;
		curLevelState->i = 0;

		if (value->type == hsvArray)
		{
			state->ptr += sizeof(HEntry) * value->array.nelems;
			*curLevelState->header = value->array.nelems | HENTRY_ISARRAY;
		}
		else
		{
			state->ptr += sizeof(HEntry) * value->hash.npairs * 2;
			*curLevelState->header = value->hash.npairs | HENTRY_ISHSTORE;
		}

		if (level == 0)
			*curLevelState->header |= HS_FLAG_NEWVERSION;
	}
	else if (flags & WHS_ELEM)
	{
		putHEntryString(state, value, level, curLevelState->i); 
		curLevelState->i++;
	}
	else if (flags & WHS_KEY)
	{
		Assert(value->type == hsvString);

		putHEntryString(state, value, level, curLevelState->i * 2); 
	}
	else if (flags & WHS_VALUE)
	{
		putHEntryString(state, value, level, curLevelState->i * 2 + 1);
		curLevelState->i++;
	}
	else if (flags & (WHS_END_ARRAY | WHS_END_HSTORE))
	{
		uint32	len, i;

		Assert(((flags & WHS_END_ARRAY) && value->type == hsvArray) ||
			   ((flags & WHS_END_HSTORE) && value->type == hsvHash));
		if (level == 0)
			return;

		len = state->ptr - (char*)curLevelState->begin;

		prevLevelState = curLevelState - 1;

		if (*prevLevelState->header & HS_FLAG_ARRAY) {
			i = prevLevelState->i;

			prevLevelState->array[i].entry = *curLevelState->header & (HS_FLAG_ARRAY | HS_FLAG_HSTORE);

			if (i == 0)
				prevLevelState->array[0].entry |= HENTRY_ISFIRST | len;
			else
				prevLevelState->array[i].entry |=
					(prevLevelState->array[i - 1].entry & HENTRY_POSMASK) + len;
		}
		else if (*prevLevelState->header & HENTRY_ISHSTORE)
		{
			i = 2 * prevLevelState->i + 1; /* VALUE, not a KEY */

			prevLevelState->array[i].entry = *curLevelState->header & (HS_FLAG_ARRAY | HS_FLAG_HSTORE);

			prevLevelState->array[i].entry |=
				(prevLevelState->array[i - 1].entry & HENTRY_POSMASK) + len;
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
compressHStore(HStoreValue *v, char *buffer) {
	uint32			l = 0;
	CompressState	state;

	state.begin = state.ptr = buffer;
	state.maxlevel = 8;
	state.levelstate = palloc(sizeof(*state.levelstate) * state.maxlevel);

	walkUncompressedHStore(v, compressCallback, &state);

	l = state.ptr - buffer;
	Assert(l <= v->size);

	return l;
}

static ToHStoreState*
pushState(ToHStoreState **state)
{
	ToHStoreState	*ns = palloc(sizeof(*ns));

	ns->next = *state;
	return ns;
}

static void
appendArray(ToHStoreState *state, HStoreValue *v)
{
	HStoreValue	*a = &state->v;

	Assert(a->type == hsvArray);

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
appendKey(ToHStoreState *state, HStoreValue *v)
{
	HStoreValue	*h = &state->v;

	Assert(h->type == hsvHash);

	if (h->hash.npairs >= state->size)
	{
		state->size *= 2;
		h->hash.pairs = repalloc(h->hash.pairs,
											sizeof(*h->hash.pairs) * state->size);
	}

	h->hash.pairs[h->hash.npairs].key = *v;

	h->size += v->size;
}

static void
appendValue(ToHStoreState *state, HStoreValue *v)
{

	HStoreValue	*h = &state->v;

	Assert(h->type == hsvHash);

	h->hash.pairs[h->hash.npairs++].value = *v;

	h->size += v->size;
}


HStoreValue*
pushHStoreValue(ToHStoreState **state, int r /* WHS_* */, HStoreValue *v) {
	HStoreValue	*h = NULL;

	switch(r)
	{
		case WHS_BEGIN_ARRAY:
			*state = pushState(state);
			h = &(*state)->v;
			(*state)->v.type = hsvArray;
			(*state)->v.size = 3*sizeof(HEntry);
			(*state)->v.array.nelems = 0;
			(*state)->size = (v && v->type == hsvArray) ? v->array.nelems : 4;
			(*state)->v.array.elems = palloc(sizeof(*(*state)->v.array.elems) * (*state)->size);
			break;
		case WHS_BEGIN_HSTORE:
			*state = pushState(state);
			h = &(*state)->v;
			(*state)->v.type = hsvHash;
			(*state)->v.size = 3*sizeof(HEntry);
			(*state)->v.hash.npairs = 0;
			(*state)->size = (v && v->type == hsvHash) ? v->hash.npairs : 4;
			(*state)->v.hash.pairs = palloc(sizeof(*(*state)->v.hash.pairs) * (*state)->size);
			break;
		case WHS_ELEM:
			Assert(v->type == hsvNullString || v->type == hsvString || v->type == hsvBinary);
			appendArray(*state, v);
			break;
		case WHS_KEY:
			Assert(v->type == hsvString);
			appendKey(*state, v);
			break;
		case WHS_VALUE:
			Assert(v->type == hsvNullString || v->type == hsvString || v->type == hsvBinary);
			appendValue(*state, v);
			break;
		case WHS_END_ARRAY:
		case WHS_END_HSTORE:
			h = &(*state)->v;
			*state = (*state)->next;
			if (*state)
			{
				switch((*state)->v.type)
				{
					case hsvArray:
						appendArray(*state, h);
						break;
					case hsvHash:
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

