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
					r.type = hsvDumped;
					r.dump.data = data + INTALIGN(HSE_OFF(*v));
					r.dump.len = HSE_LEN(*v) - (INTALIGN(HSE_OFF(*v)) - HSE_OFF(*v));
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
				if (v->array.elems[i].type == hsvNullString || v->array.elems[i].type == hsvString)
					cb(cb_arg, v->array.elems + i, WHS_ELEM, level);
				else
					walkUncompressedHStoreDo(v->array.elems + i, cb, cb_arg, level + 1);
			}
			cb(cb_arg, v, WHS_END_ARRAY, level);
			break;
		case hsvPairs:
			cb(cb_arg, v, WHS_BEGIN_HSTORE, level);

			for(i=0; i<v->hstore.npairs; i++)
			{
				cb(cb_arg, &v->hstore.pairs[i].key, WHS_KEY | WHS_BEFORE, level);
				
				if (v->hstore.pairs[i].value.type == hsvNullString || v->hstore.pairs[i].value.type == hsvString)
					cb(cb_arg, &v->hstore.pairs[i].value, WHS_VALUE, level);
				else 
					walkUncompressedHStoreDo(&v->hstore.pairs[i].value, cb, cb_arg, level + 1);

				cb(cb_arg, &v->hstore.pairs[i].key, WHS_KEY | WHS_AFTER, level);
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
walkCompressedHStoreDo(char *buffer, walk_hstore_cb cb, void *cb_arg, uint32 level)
{
	uint32				type = *(uint32*)buffer,
						nelem,
						i;
	HStoreValue			k, v;
	HEntry				*array;
	char				*data;

	nelem = type & HS_COUNT_MASK;
	type &= (HS_FLAG_ARRAY | HS_FLAG_HSTORE);

	buffer += sizeof(uint32);

	array = (HEntry*)buffer;

	switch(type) 
	{
		case HS_FLAG_ARRAY:
			data = buffer + nelem * sizeof(HEntry);

			k.type = hsvArray;
			k.array.nelems = nelem;

			cb(cb_arg, &k, WHS_BEGIN_ARRAY, level);

			for(i=0; i<nelem; i++)
			{
				if (HSE_ISSTRING(array[i])) 
				{

					if (HSE_ISNULL(array[i]))
					{
						v.type = hsvNullString;
					}
					else
					{
						v.type = hsvString;
						v.string.val = data + HSE_OFF(array[i]);
						v.string.len = HSE_LEN(array[i]);
					}
					cb(cb_arg, &v, WHS_ELEM, level);
				}
				else
				{
					walkCompressedHStoreDo(data + INTALIGN(HSE_OFF(array[i])), cb, cb_arg, level + 1);
				}
			}

			cb(cb_arg, &k, WHS_END_ARRAY, level);

			break;
		case HS_FLAG_HSTORE:
			data = buffer + nelem * sizeof(HEntry) * 2;

			v.type = hsvPairs;
			v.hstore.npairs = nelem;

			cb(cb_arg, &v, WHS_BEGIN_HSTORE, level);

			k.type = v.type = hsvString;

			for(i=0; i<nelem; i++)
			{
				k.string.val = HS_KEY(array, data, i);
				k.string.len = HS_KEYLEN(array, i);

				cb(cb_arg, &k, WHS_KEY | WHS_BEFORE, level);

				if (HS_VALISSTRING(array, i))
				{
					if (HS_VALISNULL(array, i))
					{
						v.type = hsvNullString;
					}
					else
					{
						v.type = hsvString;
						v.string.val = HS_VAL(array, data, i);
						v.string.len = HS_VALLEN(array, i);
					}
					cb(cb_arg, &v, WHS_VALUE, level);
				}
				else
				{
					walkCompressedHStoreDo(data + INTALIGN(HSE_OFF(array[2*i + 1])), cb, cb_arg, level + 1);
				}


				cb(cb_arg, &k, WHS_KEY | WHS_AFTER, level);
			}

			v.type = hsvPairs;
			v.hstore.npairs = nelem;

			cb(cb_arg, &v, WHS_END_HSTORE, level);

			break;
		default:
			elog(PANIC, "impossible type: %08x", type);
	}

}

void
walkCompressedHStore(char *buffer, walk_hstore_cb cb, void *cb_arg) 
{
	if (buffer)
		walkCompressedHStoreDo(buffer, cb, cb_arg, 0);
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
	Assert(value->type == hsvString || value->type == hsvNullString);
	curLevelState = state->levelstate + level;

	if (i == 0)
		curLevelState->array[0].entry = HENTRY_ISFIRST;
	else
		curLevelState->array[i].entry = 0;

	if (value->type == hsvNullString)
	{
		curLevelState->array[i].entry |= HENTRY_ISNULL;

		if (i>0)
			curLevelState->array[i].entry |=
				curLevelState->array[i - 1].entry & HENTRY_POSMASK;
	}
	else
	{
		memcpy(state->ptr, value->string.val, value->string.len);
		state->ptr += value->string.len;

		if (i == 0)
			curLevelState->array[i].entry |= value->string.len;
		else
			curLevelState->array[i].entry |= 
				(curLevelState->array[i - 1].entry & HENTRY_POSMASK) + value->string.len;
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
			   ((flags & WHS_BEGIN_HSTORE) && value->type == hsvPairs));

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
			state->ptr += sizeof(HEntry) * value->hstore.npairs * 2;
			*curLevelState->header = value->hstore.npairs | HENTRY_ISHSTORE;
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
		if (flags & WHS_AFTER)
			return;
		Assert(value->type == hsvString);
		Assert(flags & WHS_BEFORE);

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
			   ((flags & WHS_END_HSTORE) && value->type == hsvPairs));
		if (level == 0)
			return;

		len = state->ptr - (char*)curLevelState->begin;

		prevLevelState = curLevelState - 1;

		if (*prevLevelState->header & HENTRY_ISARRAY) {
			i = prevLevelState->i;

			prevLevelState->array[i].entry =  HENTRY_ISARRAY;

			if (i == 0)
				prevLevelState->array[0].entry |= HENTRY_ISFIRST | len;
			else
				prevLevelState->array[i].entry |=
					(prevLevelState->array[i - 1].entry & HENTRY_POSMASK) + len;
		}
		else if (*prevLevelState->header & HENTRY_ISHSTORE)
		{
			i = 2 * prevLevelState->i + 1; /* VALUE, not a KEY */

			prevLevelState->array[i].entry =  HENTRY_ISHSTORE;

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
