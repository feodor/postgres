/*-------------------------------------------------------------------------
 *
 * jsonb_gist.c
 *	 GiST support functions for jsonb
 *
 * Copyright (c) 2014, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/jsonb_gist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/gist.h"
#include "access/skey.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/pg_crc.h"

/* bigint defines */
#define BITBYTE			8
#define SIGLENINT  		4			/* > 122 => key be will toasted */
#define SIGLEN			(sizeof(int)*SIGLENINT)
#define SIGLENBIT		(SIGLEN*BITBYTE)

typedef char BITVEC[SIGLEN];
typedef char *BITVECP;

#define SIGPTR(x)  ( (BITVECP) ARR_DATA_PTR(x) )

#define LOOPBYTE \
			for(i=0;i<SIGLEN;i++)

#define LOOPBIT \
			for(i=0;i<SIGLENBIT;i++)

/* Beware of multiple evaluation of arguments to these macros! */
#define GETBYTE(x,i)		(*((BITVECP)(x) + (int)( (i) / BITBYTE )))
#define GETBITBYTE(x,i)		((*((char*)(x)) >> (i)) & 0x01)
#define CLRBIT(x,i)   		GETBYTE(x,i) &= ~( 0x01 << ( (i) % BITBYTE ))
#define SETBIT(x,i)   		GETBYTE(x,i) |=  ( 0x01 << ( (i) % BITBYTE ))
#define GETBIT(x,i) 		((GETBYTE(x,i) >> ( (i) % BITBYTE )) & 0x01)
#define HASHVAL(val)		(((unsigned int)(val)) % SIGLENBIT)
#define HASH(sign, val)		SETBIT((sign), HASHVAL(val))

#define ALLISTRUE			0x04
#define ISALLTRUE(x)		(((GISTTYPE*)x)->flag & ALLISTRUE)
#define GTHDRSIZE			(VARHDRSZ + sizeof(int32))
#define CALCGTSIZE(flag)	(GTHDRSIZE+(((flag) & ALLISTRUE) ? 0 : SIGLEN))
#define GETSIGN(x)			((BITVECP)( (char*)x+GTHDRSIZE))
#define SUMBIT(val) (		\
	GETBITBYTE((val),0) + \
	GETBITBYTE((val),1) + \
	GETBITBYTE((val),2) + \
	GETBITBYTE((val),3) + \
	GETBITBYTE((val),4) + \
	GETBITBYTE((val),5) + \
	GETBITBYTE((val),6) + \
	GETBITBYTE((val),7)   \
)

#define GETENTRY(vec,pos)	((GISTTYPE *) DatumGetPointer((vec)->vector[(pos)].key))
#define WISH_F(a,b,c) 		((double) ( -(double)(((a)-(b))*((a)-(b))*((a)-(b)))*(c)))

typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		flag;
	char		data[1];
} GISTTYPE;

typedef struct
{
	OffsetNumber pos;
	int32		cost;
} SPLITCOST;

static int crc32_JsonbValue(JsonbValue * v, uint32 r);
static int crc32_Key(char *buf, int sz);
static int32 sizebitvec(BITVECP sign);
static int hemdistsign(BITVECP a, BITVECP b);
static int hemdist(GISTTYPE *a, GISTTYPE *b);
static int32 unionkey(BITVECP sbase, GISTTYPE *add);
static int comparecost(const void *a, const void *b);

Datum
gjsonb_in(PG_FUNCTION_ARGS)
{
	elog(ERROR, "not implemented");
	PG_RETURN_DATUM(0);
}

Datum
gjsonb_out(PG_FUNCTION_ARGS)
{
	elog(ERROR, "not implemented");
	PG_RETURN_DATUM(0);
}

Datum
gjsonb_consistent(PG_FUNCTION_ARGS)
{
	GISTTYPE   *entry = (GISTTYPE *) DatumGetPointer(((GISTENTRY *) PG_GETARG_POINTER(0))->key);
	StrategyNumber strategy = (StrategyNumber) PG_GETARG_UINT16(2);

	/* Oid		subtype = PG_GETARG_OID(3); */
	bool	   *recheck = (bool *) PG_GETARG_POINTER(4);
	bool		res = true;
	BITVECP		sign;

	/* All cases served by this function are inexact */
	*recheck = true;

	if (ISALLTRUE(entry))
		PG_RETURN_BOOL(true);

	sign = GETSIGN(entry);

	if (strategy == JsonbContainsStrategyNumber)
	{
		BITVECP		qe;
		int			i;

		qe = fcinfo->flinfo->fn_extra;
		if (qe == NULL)
		{
			Jsonb	   *query = PG_GETARG_JSONB(1);

			qe = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt, sizeof(BITVEC));
			memset(qe, 0, sizeof(BITVEC));

			if (!JB_ISEMPTY(query))
			{
				int			r;
				JsonbIterator *it = JsonbIteratorInit(VARDATA(query));
				JsonbValue	v;

				while ((r = JsonbIteratorGet(&it, &v, false)) != 0)
				{
					if ((r == WJB_ELEM || r == WJB_KEY || r == WJB_VALUE) && v.type != jbvNull)
					{
						int			crc = crc32_JsonbValue(&v, r);

						HASH(qe, crc);
					}
				}
			}

			fcinfo->flinfo->fn_extra = qe;
		}

		LOOPBYTE
		{
			if ((sign[i] & qe[i]) != qe[i])
			{
				res = false;
				break;
			}
		}
	}
	else if (strategy == JsonbExistsStrategyNumber)
	{
		int		   *qval;

		qval = fcinfo->flinfo->fn_extra;
		if (qval == NULL)
		{
			text	   *query = PG_GETARG_TEXT_PP(1);
			int			crc = crc32_Key(VARDATA_ANY(query), VARSIZE_ANY_EXHDR(query));

			qval = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt, sizeof(int));
			*qval = HASHVAL(crc);

			fcinfo->flinfo->fn_extra = qval;
		}

		res = (GETBIT(sign, *qval)) != 0;
	}
	else if (strategy == JsonbExistsAllStrategyNumber ||
			 strategy == JsonbExistsAnyStrategyNumber)
	{
		BITVECP		arrentry;
		int			i;

		arrentry = fcinfo->flinfo->fn_extra;
		if (arrentry == NULL)
		{
			ArrayType  *query = PG_GETARG_ARRAYTYPE_P(1);
			Datum	   *key_datums;
			bool	   *key_nulls;
			int			key_count;

			arrentry = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
										  sizeof(BITVEC));
			memset(arrentry, 0, sizeof(BITVEC));

			deconstruct_array(query,
							  TEXTOID, -1, false, 'i',
							  &key_datums, &key_nulls, &key_count);

			for (i = 0; i < key_count; ++i)
			{
				int			crc;

				if (key_nulls[i])
					continue;
				crc = crc32_Key(VARDATA(key_datums[i]),
								VARSIZE(key_datums[i]) - VARHDRSZ);
				HASH(arrentry, crc);
			}

			fcinfo->flinfo->fn_extra = arrentry;
		}

		if (strategy == JsonbExistsAllStrategyNumber)
		{
			LOOPBYTE
			{
				if ((sign[i] & arrentry[i]) != arrentry[i])
				{
					res = false;
					break;
				}
			}
		}
		else	/* JsonbExistsAnyStrategyNumber */
		{
			res = false;

			LOOPBYTE
			{
				if (sign[i] & arrentry[i])
				{
					res = true;
					break;
				}
			}
		}
	}
	else
		elog(ERROR, "unsupported strategy number: %d", strategy);

	PG_RETURN_BOOL(res);
}

Datum
gjsonb_union(PG_FUNCTION_ARGS)
{
	GistEntryVector *entryvec = (GistEntryVector *) PG_GETARG_POINTER(0);
	int32		len = entryvec->n;

	int		   *size = (int *) PG_GETARG_POINTER(1);
	BITVEC		base;
	int32		i;
	int32		flag = 0;
	GISTTYPE   *result;

	MemSet((void *) base, 0, sizeof(BITVEC));
	for (i = 0; i < len; i++)
	{
		if (unionkey(base, GETENTRY(entryvec, i)))
		{
			flag = ALLISTRUE;
			break;
		}
	}

	len = CALCGTSIZE(flag);
	result = (GISTTYPE *) palloc(len);
	SET_VARSIZE(result, len);
	result->flag = flag;
	if (!ISALLTRUE(result))
		memcpy((void *) GETSIGN(result), (void *) base, sizeof(BITVEC));
	*size = len;

	PG_RETURN_POINTER(result);
}

Datum
gjsonb_compress(PG_FUNCTION_ARGS)
{
	GISTENTRY  *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
	GISTENTRY  *retval = entry;

	if (entry->leafkey)
	{
		GISTTYPE   *res = (GISTTYPE *) palloc0(CALCGTSIZE(0));
		Jsonb	   *val = (Jsonb *) PG_DETOAST_DATUM(entry->key);

		SET_VARSIZE(res, CALCGTSIZE(0));

		if (!JB_ISEMPTY(val))
		{
			JsonbIterator *it = JsonbIteratorInit(VARDATA(val));
			JsonbValue	v;
			int			r;

			while ((r = JsonbIteratorGet(&it, &v, false)) != 0)
			{
				if ((r == WJB_ELEM || r == WJB_KEY || r == WJB_VALUE) &&
					v.type != jbvNull)
				{
					int			h = crc32_JsonbValue(&v, r);

					HASH(GETSIGN(res), h);
				}
			}
		}

		retval = (GISTENTRY *) palloc(sizeof(GISTENTRY));
		gistentryinit(*retval, PointerGetDatum(res),
					  entry->rel, entry->page,
					  entry->offset,
					  FALSE);
	}
	else if (!ISALLTRUE(DatumGetPointer(entry->key)))
	{
		int32		i;
		GISTTYPE   *res;
		BITVECP		sign = GETSIGN(DatumGetPointer(entry->key));

		LOOPBYTE
		{
			if ((sign[i] & 0xff) != 0xff)
				PG_RETURN_POINTER(retval);
		}

		res = (GISTTYPE *) palloc(CALCGTSIZE(ALLISTRUE));
		SET_VARSIZE(res, CALCGTSIZE(ALLISTRUE));
		res->flag = ALLISTRUE;

		retval = (GISTENTRY *) palloc(sizeof(GISTENTRY));
		gistentryinit(*retval, PointerGetDatum(res),
					  entry->rel, entry->page,
					  entry->offset,
					  FALSE);
	}

	PG_RETURN_POINTER(retval);
}

/*
 * Since type gjsonb isn't toastable (and doesn't need to be),
 * this function can be a no-op.
 */
Datum
gjsonb_decompress(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(PG_GETARG_POINTER(0));
}

Datum
gjsonb_penalty(PG_FUNCTION_ARGS)
{
	GISTENTRY  *origentry = (GISTENTRY *) PG_GETARG_POINTER(0); /* always ISSIGNKEY */
	GISTENTRY  *newentry = (GISTENTRY *) PG_GETARG_POINTER(1);
	float	   *penalty = (float *) PG_GETARG_POINTER(2);
	GISTTYPE   *origval = (GISTTYPE *) DatumGetPointer(origentry->key);
	GISTTYPE   *newval = (GISTTYPE *) DatumGetPointer(newentry->key);

	*penalty = hemdist(origval, newval);
	PG_RETURN_POINTER(penalty);
}

Datum
gjsonb_picksplit(PG_FUNCTION_ARGS)
{
	GistEntryVector *entryvec = (GistEntryVector *) PG_GETARG_POINTER(0);
	OffsetNumber maxoff = entryvec->n - 2;

	GIST_SPLITVEC *v = (GIST_SPLITVEC *) PG_GETARG_POINTER(1);
	OffsetNumber k,
				j;
	GISTTYPE   *datum_l,
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
	BITVECP		ptr;
	int			i;
	SPLITCOST  *costvector;
	GISTTYPE   *_k,
			   *_j;

	nbytes = (maxoff + 2) * sizeof(OffsetNumber);
	v->spl_left = (OffsetNumber *) palloc(nbytes);
	v->spl_right = (OffsetNumber *) palloc(nbytes);

	for (k = FirstOffsetNumber; k < maxoff; k = OffsetNumberNext(k))
	{
		_k = GETENTRY(entryvec, k);
		for (j = OffsetNumberNext(k); j <= maxoff; j = OffsetNumberNext(j))
		{
			size_waste = hemdist(_k, GETENTRY(entryvec, j));
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
	if (ISALLTRUE(GETENTRY(entryvec, seed_1)))
	{
		datum_l = (GISTTYPE *) palloc(GTHDRSIZE);
		SET_VARSIZE(datum_l, GTHDRSIZE);
		datum_l->flag = ALLISTRUE;
	}
	else
	{
		datum_l = (GISTTYPE *) palloc(GTHDRSIZE + SIGLEN);
		SET_VARSIZE(datum_l, GTHDRSIZE + SIGLEN);
		datum_l->flag = 0;
		memcpy((void *) GETSIGN(datum_l),
			   (void *) GETSIGN(GETENTRY(entryvec, seed_1)), sizeof(BITVEC));
	}
	if (ISALLTRUE(GETENTRY(entryvec, seed_2)))
	{
		datum_r = (GISTTYPE *) palloc(GTHDRSIZE);
		SET_VARSIZE(datum_r, GTHDRSIZE);
		datum_r->flag = ALLISTRUE;
	}
	else
	{
		datum_r = (GISTTYPE *) palloc(GTHDRSIZE + SIGLEN);
		SET_VARSIZE(datum_r, GTHDRSIZE + SIGLEN);
		datum_r->flag = 0;
		memcpy((void *) GETSIGN(datum_r),
			   (void *) GETSIGN(GETENTRY(entryvec, seed_2)), sizeof(BITVEC));
	}

	maxoff = OffsetNumberNext(maxoff);
	/* sort before ... */
	costvector = (SPLITCOST *) palloc(sizeof(SPLITCOST) * maxoff);
	for (j = FirstOffsetNumber; j <= maxoff; j = OffsetNumberNext(j))
	{
		costvector[j - 1].pos = j;
		_j = GETENTRY(entryvec, j);
		size_alpha = hemdist(datum_l, _j);
		size_beta = hemdist(datum_r, _j);
		costvector[j - 1].cost = abs(size_alpha - size_beta);
	}
	qsort((void *) costvector, maxoff, sizeof(SPLITCOST), comparecost);

	union_l = GETSIGN(datum_l);
	union_r = GETSIGN(datum_r);

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
		_j = GETENTRY(entryvec, j);
		size_alpha = hemdist(datum_l, _j);
		size_beta = hemdist(datum_r, _j);

		if (size_alpha < size_beta + WISH_F(v->spl_nleft, v->spl_nright, 0.0001))
		{
			if (ISALLTRUE(datum_l) || ISALLTRUE(_j))
			{
				if (!ISALLTRUE(datum_l))
					MemSet((void *) union_l, 0xff, sizeof(BITVEC));
			}
			else
			{
				ptr = GETSIGN(_j);
				LOOPBYTE
					union_l[i] |= ptr[i];
			}
			*left++ = j;
			v->spl_nleft++;
		}
		else
		{
			if (ISALLTRUE(datum_r) || ISALLTRUE(_j))
			{
				if (!ISALLTRUE(datum_r))
					MemSet((void *) union_r, 0xff, sizeof(BITVEC));
			}
			else
			{
				ptr = GETSIGN(_j);
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

Datum
gjsonb_same(PG_FUNCTION_ARGS)
{
	GISTTYPE   *a = (GISTTYPE *) PG_GETARG_POINTER(0);
	GISTTYPE   *b = (GISTTYPE *) PG_GETARG_POINTER(1);
	bool	   *result = (bool *) PG_GETARG_POINTER(2);

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
	PG_RETURN_POINTER(result);
}

static int
crc32_JsonbValue(JsonbValue * v, uint32 r)
{
	int			crc;
	char		flag = '\0';

	INIT_CRC32(crc);

	switch (r)
	{
		case WJB_KEY:
			/*
			 * Serialize keys and elements as one.  This is necessary because
			 * array elements must be indexed as keys for the benefit of
			 * JsonbContainsStrategyNumber.
			 */
		case WJB_ELEM:
			flag = KEYELEMFLAG;
			break;
		case WJB_VALUE:
			flag = VALFLAG;
			break;
		default:
			break;
	}

	COMP_CRC32(crc, &flag, 1);

	switch (v->type)
	{
		case jbvString:
			COMP_CRC32(crc, v->string.val, v->string.len);
			break;
		case jbvBool:
			flag = v->boolean ? 't' : 'f';
			COMP_CRC32(crc, &flag, 1);
			break;
		case jbvNumeric:
			crc = DatumGetInt32(DirectFunctionCall1(hash_numeric,
													NumericGetDatum(v->numeric)));
			break;
		default:
			elog(ERROR, "invalid jsonb scalar type");
	}

	FIN_CRC32(crc);
	return crc;
}

static int
crc32_Key(char *buf, int sz)
{
	int			crc;
	char		flag = KEYELEMFLAG;

	INIT_CRC32(crc);

	COMP_CRC32(crc, &flag, 1);
	COMP_CRC32(crc, buf, sz);

	FIN_CRC32(crc);
	return crc;
}

static int32
sizebitvec(BITVECP sign)
{
	int32		size = 0,
				i;

	LOOPBYTE
	{
		size += SUMBIT(sign);
		sign = (BITVECP) (((char *) sign) + 1);
	}

	return size;
}

static int
hemdistsign(BITVECP a, BITVECP b)
{
	int			i,
				dist = 0;

	LOOPBIT
	{
		if (GETBIT(a, i) != GETBIT(b, i))
			dist++;
	}

	return dist;
}

static int
hemdist(GISTTYPE *a, GISTTYPE *b)
{
	if (ISALLTRUE(a))
	{
		if (ISALLTRUE(b))
			return 0;
		else
			return SIGLENBIT - sizebitvec(GETSIGN(b));
	}
	else if (ISALLTRUE(b))
	{
		return SIGLENBIT - sizebitvec(GETSIGN(a));
	}

	return hemdistsign(GETSIGN(a), GETSIGN(b));
}

static int32
unionkey(BITVECP sbase, GISTTYPE *add)
{
	int32		i;
	BITVECP		sadd = GETSIGN(add);

	if (ISALLTRUE(add))
		return 1;

	LOOPBYTE
		sbase[i] |= sadd[i];

	return 0;
}

static int
comparecost(const void *a, const void *b)
{
	return ((const SPLITCOST *) a)->cost - ((const SPLITCOST *) b)->cost;
}
