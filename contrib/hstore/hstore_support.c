#include "postgres.h"

#include "hstore.h"

int
compareHStorePair(const void *a, const void *b, void *arg)
{
	const HStorePair *pa = a;
	const HStorePair *pb = b;

	if (pa->key.string.len == pb->key.string.len) {
		int res =  memcmp(pa->key.string.val, pb->key.string.val, pa->key.string.len);

		if (res == 0)
			*(bool*)arg = true;
	}

	return (pa->key.string.len > pb->key.string.len) ? 1 : -1;
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
					cb(cb_arg, v->array.elems + i, 0, level);
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

			v.type = hsvArray;
			v.array.nelems = nelem;

			cb(cb_arg, &v, WHS_BEGIN_ARRAY, level);

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
					cb(cb_arg, &v, 0, level);
				}
				else
				{
					walkCompressedHStoreDo(data + HSE_OFF(array[i]), cb, cb_arg, level + 1);
				}
			}

			cb(cb_arg, &v, WHS_END_ARRAY, level);

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
					walkCompressedHStoreDo(HS_VAL(array, data, i), cb, cb_arg, level + 1);
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
	char	*ptr;
} CompressState;

static void
compressCallback(void *arg, HStoreValue* value, uint32 flags, uint32 level) {
	CompressState	*state = arg;

}

uint32
compressHStore(HStoreValue *v, char *buffer) {
	uint32	l = 0;

	CompressState	state;

	state.ptr = buffer;

	walkUncompressedHStore(v, compressCallback, &state);

	l = state.ptr - buffer;
	Assert(l == v->size);

	return l;
}
