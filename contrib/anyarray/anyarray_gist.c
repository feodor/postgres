/*-------------------------------------------------------------------------
 *
 * anyarray_gist.c
 *	  GiST support functions for anyarray
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  contrib/anyarray/anyarray_gist.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "anyarray.h"
#include "access/gist.h"
#include "access/skey.h"
#include "access/tuptoaster.h"

PG_FUNCTION_INFO_V1(ganyarrayin);
PG_FUNCTION_INFO_V1(ganyarrayout);
PG_FUNCTION_INFO_V1(ganyarray_consistent);
PG_FUNCTION_INFO_V1(ganyarray_compress);
PG_FUNCTION_INFO_V1(ganyarray_decompress);
PG_FUNCTION_INFO_V1(ganyarray_penalty);
PG_FUNCTION_INFO_V1(ganyarray_picksplit);
PG_FUNCTION_INFO_V1(ganyarray_union);
PG_FUNCTION_INFO_V1(ganyarray_same);


#define SIGLENINT  31			/* >121 => key will toast, so it will not work
								 * !!! */

#define SIGLEN	( sizeof(int32) * SIGLENINT )
#define SIGLENBIT (SIGLEN * BITS_PER_BYTE)

typedef char BITVEC[SIGLEN];
typedef char *BITVECP;

#define LOOPBYTE \
			for(i=0;i<SIGLEN;i++)

#define GETBYTE(x,i) ( *( (BITVECP)(x) + (int)( (i) / BITS_PER_BYTE ) ) )
#define GETBITBYTE(x,i) ( ((char)(x)) >> (i) & 0x01 )
#define CLRBIT(x,i)   GETBYTE(x,i) &= ~( 0x01 << ( (i) % BITS_PER_BYTE ) )
#define SETBIT(x,i)   GETBYTE(x,i) |=  ( 0x01 << ( (i) % BITS_PER_BYTE ) )
#define GETBIT(x,i) ( (GETBYTE(x,i) >> ( (i) % BITS_PER_BYTE )) & 0x01 )

#define HASHVAL(val) (((unsigned int)(val)) % SIGLENBIT)
#define HASH(sign, val) SETBIT((sign), HASHVAL(val))

#define GETENTRY(vec,pos) ((SignAnyArray *) DatumGetPointer((vec)->vector[(pos)].key))

/*
 * type of GiST index key
 */

typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		flag;
	char		data[1];
} SignAnyArray;

#define ARRKEY		0x01
#define SIGNKEY		0x02
#define ALLISTRUE	0x04

#define ISARRKEY(x) ( ((SignAnyArray*)(x))->flag & ARRKEY )
#define ISSIGNKEY(x)	( ((SignAnyArray*)(x))->flag & SIGNKEY )
#define ISALLTRUE(x)	( ((SignAnyArray*)(x))->flag & ALLISTRUE )

#define GTHDRSIZE	( VARHDRSZ + sizeof(int32) )
#define CALCGTSIZE(flag, len) ( GTHDRSIZE + ( ( (flag) & ARRKEY ) ? ((len)*sizeof(int32)) : (((flag) & ALLISTRUE) ? 0 : SIGLEN) ) )

#define GETSIGN(x)	( (BITVECP)( (char*)(x)+GTHDRSIZE ) )
#define GETARR(x)	( (int32*)( (char*)(x)+GTHDRSIZE ) )
#define ARRNELEM(x) ( ( VARSIZE(x) - GTHDRSIZE )/sizeof(int32) )

/* Number of one-bits in an unsigned byte */
static const uint8 number_of_ones[256] = {
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
};

static int32 sizebitvec(BITVECP sign);

Datum
ganyarrayin(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("ganyarray_in not implemented")));
	PG_RETURN_DATUM(0);
}

#define SINGOUTSTR	"%d true bits, %d false bits"
#define ARROUTSTR	"%d unique values"
#define EXTRALEN	( 2*13 )

static int	outbuf_maxlen = 0;

Datum
ganyarrayout(PG_FUNCTION_ARGS)
{
	SignAnyArray *key = (SignAnyArray *) DatumGetPointer(PG_DETOAST_DATUM(PG_GETARG_POINTER(0)));
	char	   *outbuf;

	if (outbuf_maxlen == 0)
		outbuf_maxlen = 2 * EXTRALEN + Max(strlen(SINGOUTSTR), strlen(ARROUTSTR)) + 1;
	outbuf = palloc(outbuf_maxlen);

	if (ISARRKEY(key))
		sprintf(outbuf, ARROUTSTR, (int) ARRNELEM(key));
	else
	{
		int			cnttrue = (ISALLTRUE(key)) ? SIGLENBIT : sizebitvec(GETSIGN(key));

		sprintf(outbuf, SINGOUTSTR, cnttrue, (int) SIGLENBIT - cnttrue);
	}

	PG_FREE_IF_COPY(key, 0);
	PG_RETURN_POINTER(outbuf);
}

static int
compareint(const void *va, const void *vb)
{
	int32		a = *((const int32 *) va);
	int32		b = *((const int32 *) vb);

	if (a == b)
		return 0;
	return (a > b) ? 1 : -1;
}

/*
 * Removes duplicates from an array of int32. 'l' is
 * size of the input array. Returns the new size of the array.
 */
static int
uniqueint(int32 *a, int32 l)
{
	int32	   *ptr,
			   *res;

	if (l <= 1)
		return l;

	ptr = res = a;

	qsort((void *) a, l, sizeof(int32), compareint);

	while (ptr - a < l)
		if (*ptr != *res)
			*(++res) = *ptr++;
		else
			ptr++;
	return res + 1 - a;
}

static void
makeSignSimpleArray(BITVECP sign, SimpleArray *s)
{
	int32	i, hash;

	MemSet((void *) sign, 0, sizeof(BITVEC));
	hashFuncInit(s->info);

	for (i = 0; i < s->nelems; i++)
	{
		hash = DatumGetInt32(FunctionCall1(&s->info->hashFunc, s->elems[i]));	

		HASH(sign, hash);
	}
}

static void
makesign(BITVECP sign, SignAnyArray *a)
{
	int32		k,
				len = ARRNELEM(a);
	int32	   *ptr = GETARR(a);

	MemSet((void *) sign, 0, sizeof(BITVEC));
	for (k = 0; k < len; k++)
		HASH(sign, ptr[k]);
}

Datum
ganyarray_compress(PG_FUNCTION_ARGS)
{
	GISTENTRY  *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
	GISTENTRY  *retval = entry;

	if (entry->leafkey)
	{							/* array */
		SignAnyArray 		*res;
		ArrayType			*array = DatumGetArrayTypeP(entry->key);
		AnyArrayTypeInfo	*info;
		SimpleArray			*s;
		int32				i, len,
							*arr;

		if (fcinfo->flinfo->fn_extra == NULL)
			fcinfo->flinfo->fn_extra = getAnyArrayTypeInfo(fcinfo->flinfo->fn_mcxt, ARR_ELEMTYPE(array));
		info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;
		hashFuncInit(info);

		s = Array2SimpleArray(info, array);

		len = CALCGTSIZE(ARRKEY, s->nelems);
		res = (SignAnyArray *) palloc(len);
		SET_VARSIZE(res, len);
		res->flag = ARRKEY;
		arr = GETARR(res);

		for(i=0; i<s->nelems; i++)
			arr[i] = DatumGetInt32(FunctionCall1(&info->hashFunc, s->elems[i]));	

		len = uniqueint(GETARR(res), s->nelems);
		if (len != s->nelems)
		{
			/*
			 * there is a collision of hash-function; len is always less than
			 * val->size
			 */
			len = CALCGTSIZE(ARRKEY, len);
			SET_VARSIZE(res, len);
		}

		/* make signature, if array is too long */
		if (VARSIZE(res) > TOAST_INDEX_TARGET)
		{
			SignAnyArray *ressign;

			len = CALCGTSIZE(SIGNKEY, 0);
			ressign = (SignAnyArray *) palloc(len);
			SET_VARSIZE(ressign, len);
			ressign->flag = SIGNKEY;
			makesign(GETSIGN(ressign), res);
			res = ressign;
		}

		retval = (GISTENTRY *) palloc(sizeof(GISTENTRY));
		gistentryinit(*retval, PointerGetDatum(res),
					  entry->rel, entry->page,
					  entry->offset, FALSE);
	}
	else if (ISSIGNKEY(DatumGetPointer(entry->key)) &&
			 !ISALLTRUE(DatumGetPointer(entry->key)))
	{
		int32		i,
					len;
		SignAnyArray *res;
		BITVECP		sign = GETSIGN(DatumGetPointer(entry->key));

		LOOPBYTE
		{
			if ((sign[i] & 0xff) != 0xff)
				PG_RETURN_POINTER(retval);
		}

		len = CALCGTSIZE(SIGNKEY | ALLISTRUE, 0);
		res = (SignAnyArray *) palloc(len);
		SET_VARSIZE(res, len);
		res->flag = SIGNKEY | ALLISTRUE;

		retval = (GISTENTRY *) palloc(sizeof(GISTENTRY));
		gistentryinit(*retval, PointerGetDatum(res),
					  entry->rel, entry->page,
					  entry->offset, FALSE);
	}
	PG_RETURN_POINTER(retval);
}

Datum
ganyarray_decompress(PG_FUNCTION_ARGS)
{
	GISTENTRY  *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
	SignAnyArray *key = (SignAnyArray *) DatumGetPointer(PG_DETOAST_DATUM(entry->key));

	if (key != (SignAnyArray *) DatumGetPointer(entry->key))
	{
		GISTENTRY  *retval = (GISTENTRY *) palloc(sizeof(GISTENTRY));

		gistentryinit(*retval, PointerGetDatum(key),
					  entry->rel, entry->page,
					  entry->offset, FALSE);

		PG_RETURN_POINTER(retval);
	}

	PG_RETURN_POINTER(entry);
}

static int32
unionkey(BITVECP sbase, SignAnyArray *add)
{
	int32		i;

	if (ISSIGNKEY(add))
	{
		BITVECP		sadd = GETSIGN(add);

		if (ISALLTRUE(add))
			return 1;

		LOOPBYTE
			sbase[i] |= sadd[i];
	}
	else
	{
		int32	   *ptr = GETARR(add);

		for (i = 0; i < ARRNELEM(add); i++)
			HASH(sbase, ptr[i]);
	}
	return 0;
}


Datum
ganyarray_union(PG_FUNCTION_ARGS)
{
	GistEntryVector *entryvec = (GistEntryVector *) PG_GETARG_POINTER(0);
	int		   *size = (int *) PG_GETARG_POINTER(1);
	BITVEC		base;
	int32		i,
				len;
	int32		flag = 0;
	SignAnyArray *result;

	MemSet((void *) base, 0, sizeof(BITVEC));
	for (i = 0; i < entryvec->n; i++)
	{
		if (unionkey(base, GETENTRY(entryvec, i)))
		{
			flag = ALLISTRUE;
			break;
		}
	}

	flag |= SIGNKEY;
	len = CALCGTSIZE(flag, 0);
	result = (SignAnyArray *) palloc(len);
	*size = len;
	SET_VARSIZE(result, len);
	result->flag = flag;
	if (!ISALLTRUE(result))
		memcpy((void *) GETSIGN(result), (void *) base, sizeof(BITVEC));

	PG_RETURN_POINTER(result);
}

Datum
ganyarray_same(PG_FUNCTION_ARGS)
{
	SignAnyArray *a = (SignAnyArray *) PG_GETARG_POINTER(0);
	SignAnyArray *b = (SignAnyArray *) PG_GETARG_POINTER(1);
	bool	   *result = (bool *) PG_GETARG_POINTER(2);

	if (ISSIGNKEY(a))
	{							/* then b also ISSIGNKEY */
		if (ISALLTRUE(a) && ISALLTRUE(b))
			*result = true;
		else if (ISALLTRUE(a))
			*result = false;
		else if (ISALLTRUE(b))
			*result = false;
		else
		{
			int32		i;
			BITVECP		sa = GETSIGN(a),
						sb = GETSIGN(b);

			*result = true;
			LOOPBYTE
			{
				if (sa[i] != sb[i])
				{
					*result = false;
					break;
				}
			}
		}
	}
	else
	{							/* a and b ISARRKEY */
		int32		lena = ARRNELEM(a),
					lenb = ARRNELEM(b);

		if (lena != lenb)
			*result = false;
		else
		{
			int32	   *ptra = GETARR(a),
					   *ptrb = GETARR(b);
			int32		i;

			*result = true;
			for (i = 0; i < lena; i++)
				if (ptra[i] != ptrb[i])
				{
					*result = false;
					break;
				}
		}
	}

	PG_RETURN_POINTER(result);
}

static int32
sizebitvec(BITVECP sign)
{
	int32		size = 0,
				i;

	LOOPBYTE
		size += number_of_ones[(unsigned char) sign[i]];
	return size;
}

static int
hemdistsign(BITVECP a, BITVECP b)
{
	int			i,
				diff,
				dist = 0;

	LOOPBYTE
	{
		diff = (unsigned char) (a[i] ^ b[i]);
		dist += number_of_ones[diff];
	}
	return dist;
}

static int
hemdist(SignAnyArray *a, SignAnyArray *b)
{
	if (ISALLTRUE(a))
	{
		if (ISALLTRUE(b))
			return 0;
		else
			return SIGLENBIT - sizebitvec(GETSIGN(b));
	}
	else if (ISALLTRUE(b))
		return SIGLENBIT - sizebitvec(GETSIGN(a));

	return hemdistsign(GETSIGN(a), GETSIGN(b));
}

Datum
ganyarray_penalty(PG_FUNCTION_ARGS)
{
	GISTENTRY  *origentry = (GISTENTRY *) PG_GETARG_POINTER(0); /* always ISSIGNKEY */
	GISTENTRY  *newentry = (GISTENTRY *) PG_GETARG_POINTER(1);
	float	   *penalty = (float *) PG_GETARG_POINTER(2);
	SignAnyArray *origval = (SignAnyArray *) DatumGetPointer(origentry->key);
	SignAnyArray *newval = (SignAnyArray *) DatumGetPointer(newentry->key);
	BITVECP		orig = GETSIGN(origval);

	*penalty = 0.0;

	if (ISARRKEY(newval))
	{
		BITVEC		sign;

		makesign(sign, newval);

		if (ISALLTRUE(origval))
			*penalty = ((float) (SIGLENBIT - sizebitvec(sign))) / (float) (SIGLENBIT + 1);
		else
			*penalty = hemdistsign(sign, orig);
	}
	else
		*penalty = hemdist(origval, newval);
	PG_RETURN_POINTER(penalty);
}

typedef struct
{
	bool		allistrue;
	BITVEC		sign;
} CACHESIGN;

static void
fillcache(CACHESIGN *item, SignAnyArray *key)
{
	item->allistrue = false;
	if (ISARRKEY(key))
		makesign(item->sign, key);
	else if (ISALLTRUE(key))
		item->allistrue = true;
	else
		memcpy((void *) item->sign, (void *) GETSIGN(key), sizeof(BITVEC));
}

#define WISH_F(a,b,c) (double)( -(double)(((a)-(b))*((a)-(b))*((a)-(b)))*(c) )
typedef struct
{
	OffsetNumber pos;
	int32		cost;
} SPLITCOST;

static int
comparecost(const void *va, const void *vb)
{
	const SPLITCOST *a = (const SPLITCOST *) va;
	const SPLITCOST *b = (const SPLITCOST *) vb;

	if (a->cost == b->cost)
		return 0;
	else
		return (a->cost > b->cost) ? 1 : -1;
}


static int
hemdistcache(CACHESIGN *a, CACHESIGN *b)
{
	if (a->allistrue)
	{
		if (b->allistrue)
			return 0;
		else
			return SIGLENBIT - sizebitvec(b->sign);
	}
	else if (b->allistrue)
		return SIGLENBIT - sizebitvec(a->sign);

	return hemdistsign(a->sign, b->sign);
}

Datum
ganyarray_picksplit(PG_FUNCTION_ARGS)
{
	GistEntryVector *entryvec = (GistEntryVector *) PG_GETARG_POINTER(0);
	GIST_SPLITVEC *v = (GIST_SPLITVEC *) PG_GETARG_POINTER(1);
	OffsetNumber k,
				j;
	SignAnyArray *datum_l,
			   *datum_r;
	BITVECP		union_l,
				union_r;
	int32		size_alpha,
				size_beta;
	int32		size_waste,
				waste = -1;
	int32		nbytes;
	OffsetNumber seed_1 = 0,
				seed_2 = 0;
	OffsetNumber *left,
			   *right;
	OffsetNumber maxoff;
	BITVECP		ptr;
	int			i;
	CACHESIGN  *cache;
	SPLITCOST  *costvector;

	maxoff = entryvec->n - 2;
	nbytes = (maxoff + 2) * sizeof(OffsetNumber);
	v->spl_left = (OffsetNumber *) palloc(nbytes);
	v->spl_right = (OffsetNumber *) palloc(nbytes);

	cache = (CACHESIGN *) palloc(sizeof(CACHESIGN) * (maxoff + 2));
	fillcache(&cache[FirstOffsetNumber], GETENTRY(entryvec, FirstOffsetNumber));

	for (k = FirstOffsetNumber; k < maxoff; k = OffsetNumberNext(k))
	{
		for (j = OffsetNumberNext(k); j <= maxoff; j = OffsetNumberNext(j))
		{
			if (k == FirstOffsetNumber)
				fillcache(&cache[j], GETENTRY(entryvec, j));

			size_waste = hemdistcache(&(cache[j]), &(cache[k]));
			if (size_waste > waste)
			{
				waste = size_waste;
				seed_1 = k;
				seed_2 = j;
			}
		}
	}

	left = v->spl_left;
	v->spl_nleft = 0;
	right = v->spl_right;
	v->spl_nright = 0;

	if (seed_1 == 0 || seed_2 == 0)
	{
		seed_1 = 1;
		seed_2 = 2;
	}

	/* form initial .. */
	if (cache[seed_1].allistrue)
	{
		datum_l = (SignAnyArray *) palloc(CALCGTSIZE(SIGNKEY | ALLISTRUE, 0));
		SET_VARSIZE(datum_l, CALCGTSIZE(SIGNKEY | ALLISTRUE, 0));
		datum_l->flag = SIGNKEY | ALLISTRUE;
	}
	else
	{
		datum_l = (SignAnyArray *) palloc(CALCGTSIZE(SIGNKEY, 0));
		SET_VARSIZE(datum_l, CALCGTSIZE(SIGNKEY, 0));
		datum_l->flag = SIGNKEY;
		memcpy((void *) GETSIGN(datum_l), (void *) cache[seed_1].sign, sizeof(BITVEC));
	}
	if (cache[seed_2].allistrue)
	{
		datum_r = (SignAnyArray *) palloc(CALCGTSIZE(SIGNKEY | ALLISTRUE, 0));
		SET_VARSIZE(datum_r, CALCGTSIZE(SIGNKEY | ALLISTRUE, 0));
		datum_r->flag = SIGNKEY | ALLISTRUE;
	}
	else
	{
		datum_r = (SignAnyArray *) palloc(CALCGTSIZE(SIGNKEY, 0));
		SET_VARSIZE(datum_r, CALCGTSIZE(SIGNKEY, 0));
		datum_r->flag = SIGNKEY;
		memcpy((void *) GETSIGN(datum_r), (void *) cache[seed_2].sign, sizeof(BITVEC));
	}

	union_l = GETSIGN(datum_l);
	union_r = GETSIGN(datum_r);
	maxoff = OffsetNumberNext(maxoff);
	fillcache(&cache[maxoff], GETENTRY(entryvec, maxoff));
	/* sort before ... */
	costvector = (SPLITCOST *) palloc(sizeof(SPLITCOST) * maxoff);
	for (j = FirstOffsetNumber; j <= maxoff; j = OffsetNumberNext(j))
	{
		costvector[j - 1].pos = j;
		size_alpha = hemdistcache(&(cache[seed_1]), &(cache[j]));
		size_beta = hemdistcache(&(cache[seed_2]), &(cache[j]));
		costvector[j - 1].cost = Abs(size_alpha - size_beta);
	}
	qsort((void *) costvector, maxoff, sizeof(SPLITCOST), comparecost);

	for (k = 0; k < maxoff; k++)
	{
		j = costvector[k].pos;
		if (j == seed_1)
		{
			*left++ = j;
			v->spl_nleft++;
			continue;
		}
		else if (j == seed_2)
		{
			*right++ = j;
			v->spl_nright++;
			continue;
		}

		if (ISALLTRUE(datum_l) || cache[j].allistrue)
		{
			if (ISALLTRUE(datum_l) && cache[j].allistrue)
				size_alpha = 0;
			else
				size_alpha = SIGLENBIT - sizebitvec(
													(cache[j].allistrue) ? GETSIGN(datum_l) : GETSIGN(cache[j].sign)
					);
		}
		else
			size_alpha = hemdistsign(cache[j].sign, GETSIGN(datum_l));

		if (ISALLTRUE(datum_r) || cache[j].allistrue)
		{
			if (ISALLTRUE(datum_r) && cache[j].allistrue)
				size_beta = 0;
			else
				size_beta = SIGLENBIT - sizebitvec(
												   (cache[j].allistrue) ? GETSIGN(datum_r) : GETSIGN(cache[j].sign)
					);
		}
		else
			size_beta = hemdistsign(cache[j].sign, GETSIGN(datum_r));

		if (size_alpha < size_beta + WISH_F(v->spl_nleft, v->spl_nright, 0.1))
		{
			if (ISALLTRUE(datum_l) || cache[j].allistrue)
			{
				if (!ISALLTRUE(datum_l))
					MemSet((void *) GETSIGN(datum_l), 0xff, sizeof(BITVEC));
			}
			else
			{
				ptr = cache[j].sign;
				LOOPBYTE
					union_l[i] |= ptr[i];
			}
			*left++ = j;
			v->spl_nleft++;
		}
		else
		{
			if (ISALLTRUE(datum_r) || cache[j].allistrue)
			{
				if (!ISALLTRUE(datum_r))
					MemSet((void *) GETSIGN(datum_r), 0xff, sizeof(BITVEC));
			}
			else
			{
				ptr = cache[j].sign;
				LOOPBYTE
					union_r[i] |= ptr[i];
			}
			*right++ = j;
			v->spl_nright++;
		}
	}

	*right = *left = FirstOffsetNumber;
	v->spl_ldatum = PointerGetDatum(datum_l);
	v->spl_rdatum = PointerGetDatum(datum_r);

	PG_RETURN_POINTER(v);
}

static int32
countIntersections(SimpleArray *query, SignAnyArray *array, bool justOverlap)
{
	int32	nIntersect = 0;

	if (query->nelems == 0)
		return 0;

	if (ISSIGNKEY(array))
	{
		int32	i;

		if (ISALLTRUE(array))
			return query->nelems;

		hashFuncInit(query->info);

		for(i=0; i<query->nelems; i++)
		{
			int32 hash = DatumGetInt32(FunctionCall1(&query->info->hashFunc, query->elems[i]));

			if (GETBIT(GETSIGN(array), HASHVAL(hash)))
			{
				nIntersect++;
				if (justOverlap)
					break;
			}
		}
	}
	else if (ARRNELEM(array) <= 3)
	{
		int32	i, j,
				len = ARRNELEM(array),
				*a = GETARR(array);

		hashFuncInit(query->info);

		for(i=0; i<query->nelems; i++)
		{
			int32 hash = DatumGetInt32(FunctionCall1(&query->info->hashFunc, query->elems[i]));

			for(j=0; j<len; j++)
			{
				if (a[j] == hash)
				{
					nIntersect++;
					if (justOverlap)
						break;
				}
			}
		}
	}
	else if (query->nelems < 5)
	{
		int32	i;

		hashFuncInit(query->info);

		for(i=0; i<query->nelems; i++)
		{
			int32 hash = DatumGetInt32(FunctionCall1(&query->info->hashFunc, query->elems[i]));
			int32	*StopLow  = GETARR(array),
					*StopHigh = GETARR(array) + ARRNELEM(array),
					*StopMiddle;

			while (StopLow < StopHigh)
			{
				StopMiddle = StopLow + (StopHigh - StopLow) / 2;

				if (*StopMiddle < hash)
					StopLow = StopMiddle + 1;
				else if (*StopMiddle > hash)
					StopHigh = StopMiddle;
				else
				{
					nIntersect++;
					break;
				}
			}

			if (nIntersect > 0 && justOverlap)
				break;
		}
	}
	else
	{
		int32			i, j,
						na = ARRNELEM(array),
						*da = GETARR(array),
						nb, 
						*db = palloc(sizeof(int32) * query->nelems);

		hashFuncInit(query->info);

		for(i=0; i<query->nelems; i++)
			db[i] = DatumGetInt32(FunctionCall1(&query->info->hashFunc, query->elems[i]));	

		query->nelems = nb = uniqueint(db, query->nelems);

		i = j = 0;
		
		while(i < na && j < nb)
		{
			if (da[i] < db[j])
				i++;
			else if (da[i] > db[j])
				j++;
			else
			{
				nIntersect++;
				if (justOverlap)
					break;
				i++;
				j++;
			}
		}
	}

	return nIntersect;
}

static bool
anyarrayContains(SignAnyArray *array, SimpleArray *query)
{
	if (ISSIGNKEY(array))
	{
		BITVECP	sign;
		int32	i;

		if (ISALLTRUE(array))
			return true;

		hashFuncInit(query->info);

		sign = GETSIGN(array);
		for(i=0; i<query->nelems; i++)
		{
			int32 hash = DatumGetInt32(FunctionCall1(&query->info->hashFunc, query->elems[i]));

			if (!GETBIT(sign, HASHVAL(hash)))
				return false;
		}
	}
	else if (ARRNELEM(array) <= 3)
	{
		int32	i, j,
				len = ARRNELEM(array),
				*a = GETARR(array);

		hashFuncInit(query->info);

		for(i=0; i<query->nelems; i++)
		{
			int32 hash = DatumGetInt32(FunctionCall1(&query->info->hashFunc, query->elems[i]));

			for(j=0; j<len; j++)
			{
				if (a[j] == hash)
					break;
			}

			if (j >= len)
				return false;
		}
	}
	else
	{
		int32	i;

		hashFuncInit(query->info);

		for(i=0; i<query->nelems; i++)
		{
			int32 hash = DatumGetInt32(FunctionCall1(&query->info->hashFunc, query->elems[i]));
			int32	*StopLow  = GETARR(array),
					*StopHigh = GETARR(array) + ARRNELEM(array),
					*StopMiddle;

			while (StopLow < StopHigh)
			{
				StopMiddle = StopLow + (StopHigh - StopLow) / 2;

				if (*StopMiddle < hash)
					StopLow = StopMiddle + 1;
				else if (*StopMiddle > hash)
					StopHigh = StopMiddle;
				else
					break;
			}

			if (StopLow >= StopHigh)
				return false;
		}
	}

	return true;
}

Datum
ganyarray_consistent(PG_FUNCTION_ARGS)
{
	GISTENTRY			*entry = (GISTENTRY *) PG_GETARG_POINTER(0);
	ArrayType			*query = PG_GETARG_ARRAYTYPE_P(1);
	AnyArrayTypeInfo	*info;
	SimpleArray			*queryArray;
	SignAnyArray		*array = (SignAnyArray*)DatumGetPointer(entry->key);
	StrategyNumber		strategy = (StrategyNumber) PG_GETARG_UINT16(2);
	/* Oid      subtype = PG_GETARG_OID(3); */
	bool				*recheck = (bool *) PG_GETARG_POINTER(4);
	bool				retval = true;
	int					nIntersect;

	CHECKARRVALID(query);

	*recheck = true;
	if (ISSIGNKEY(array) && ISALLTRUE(array))
		PG_RETURN_BOOL(true);
		
	if (fcinfo->flinfo->fn_extra == NULL)
		fcinfo->flinfo->fn_extra = getAnyArrayTypeInfo(fcinfo->flinfo->fn_mcxt, ARR_ELEMTYPE(query));
	info = (AnyArrayTypeInfo*)fcinfo->flinfo->fn_extra;

	queryArray = Array2SimpleArray(info, query);
	
	switch (strategy)
	{
		case RTOverlapStrategyNumber:
			retval = (countIntersections(queryArray, array, true) > 0);
			break;
		case RTSameStrategyNumber:
			if (ISSIGNKEY(array))
			{
				BITVEC	b;
				BITVECP	pb, pa = GETSIGN(array);
				int 	i;

				pb = b;
				makeSignSimpleArray(pb, queryArray);

				if (GIST_LEAF(entry))
				{
					LOOPBYTE
					{
						if (pa[i] != pb[i])
						{
							retval = false;
							break;
						}
					}
				}
				else
				{
					LOOPBYTE
					{
						if ((pa[i] & pb[i]) != pb[i])
						{
							retval = false;
							break;
						}
					}
				}
			}
			else
			{
				nIntersect = countIntersections(queryArray, array, false);
				retval = (nIntersect == queryArray->nelems && nIntersect >= ARRNELEM(array));
			}
			break;
		case RTContainsStrategyNumber:
			retval = anyarrayContains(array, queryArray);
			break;
		case RTContainedByStrategyNumber:
			if (GIST_LEAF(entry))
			{
				if (ISSIGNKEY(array))
				{
					BITVEC	b;
					BITVECP	pb, pa = GETSIGN(array);
					int 	i;

					pb = b;
					makeSignSimpleArray(pb, queryArray);

					LOOPBYTE
					{
						if (pa[i] & ~pb[i])
						{
							retval = false;
							break;
						}
					}
				}
				else
				{
					nIntersect = countIntersections(queryArray, array, false);
					retval = (nIntersect >= ARRNELEM(array));
				}
			}
			else
			{
					retval = (countIntersections(queryArray, array, true) > 0);
			}
			break;
		case AnyAarraySimilarityStrategy:

			if (GIST_LEAF(entry))
			{
				int32	length;

				nIntersect = countIntersections(queryArray, array, false);

				if (ISALLTRUE(array))
					length = SIGLENBIT;
				else if (ISARRKEY(array))
					length = ARRNELEM(array);
				else
					length = sizebitvec(GETSIGN(array));

				switch(SmlType)
				{
					case AA_Cosine:
						if (((double)nIntersect) / 
								sqrt(((double)queryArray->nelems) * ((double)length)) < SmlLimit)
							retval = false;
						break;
					case AA_Overlap:
						if (((double)(nIntersect)) / 
							(((double)queryArray->nelems) + ((double)length) - ((double)nIntersect)) < SmlLimit)
							retval = false;
						break;
					default:
						elog(ERROR, "unknown similarity type");
				}
			}
			else if (!ISALLTRUE(entry))
			{
				int32	length;

				length = sizebitvec(GETSIGN(array));
				nIntersect = countIntersections(queryArray, array, false);

				switch(SmlType)
				{
					case AA_Cosine:
						/* nIntersect / sqrt(queryArray->nelems * nIntersect) */
						if (sqrt(((double)nIntersect) / ((double)queryArray->nelems)) < SmlLimit)
							retval = false;
						break;
					case AA_Overlap:
						if (((double)nIntersect) / ((double)queryArray->nelems) < SmlLimit)
							retval = false;
						break;
					default:
						elog(ERROR, "unknown similarity type");
				}
			}
			break;
		default:
			elog(ERROR, "unknown strategy number");
	}

	PG_RETURN_BOOL(retval);
}

