/*-------------------------------------------------------------------------
 * 
 * anyarray_util.c
 *		various support functions
 *
 * Copyright (c) 2010-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/anyarray/anyarray.c
 *
 *-------------------------------------------------------------------------
 */

#include "anyarray.h"
#include <math.h>

#include <access/hash.h>
#include <access/htup_details.h>
#include <access/nbtree.h>
#include <catalog/pg_am.h>
#include <catalog/pg_cast.h>
#include <catalog/pg_collation.h>
#include <commands/defrem.h>
#include <utils/catcache.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

PG_MODULE_MAGIC;

static Oid
getAMProc(Oid amOid, Oid typid)
{
	Oid		opclassOid = GetDefaultOpClass(typid, amOid);
	Oid		procOid;

	Assert(amOid == BTREE_AM_OID || amOid == HASH_AM_OID);

	if (!OidIsValid(opclassOid))
	{
		typid = getBaseType(typid);
		opclassOid = GetDefaultOpClass(typid, amOid);


		if (!OidIsValid(opclassOid))
		{
			CatCList	*catlist;
			int			i;

			/*
			 * Search binary-coercible type
			 */
			catlist = SearchSysCacheList(CASTSOURCETARGET, 1,
										 ObjectIdGetDatum(typid),
										 0, 0, 0);
			for (i = 0; i < catlist->n_members; i++)
			{
				HeapTuple		tuple = &catlist->members[i]->tuple;
				Form_pg_cast	castForm = (Form_pg_cast)GETSTRUCT(tuple);

				if (castForm->castmethod == COERCION_METHOD_BINARY)
				{
					typid = castForm->casttarget;
					opclassOid = GetDefaultOpClass(typid, amOid);
					if(OidIsValid(opclassOid))
						break;
				}
			}

			ReleaseSysCacheList(catlist);
		}
	}

	if (!OidIsValid(opclassOid))
		return InvalidOid;

	procOid = get_opfamily_proc(get_opclass_family(opclassOid),
							 typid, typid,
							 (amOid == BTREE_AM_OID) ? BTORDER_PROC : HASHPROC);

	if (!OidIsValid(procOid))
	{
		typid = get_opclass_input_type(opclassOid);

		procOid = get_opfamily_proc(get_opclass_family(opclassOid),
								 typid, typid,
								 (amOid == BTREE_AM_OID) ? BTORDER_PROC : HASHPROC);
	}

	return procOid;
}

AnyArrayTypeInfo*
getAnyArrayTypeInfo(MemoryContext ctx, Oid typid)
{
	AnyArrayTypeInfo	*info;

	info = MemoryContextAlloc(ctx, sizeof(*info));

	info->typid = typid;
	info->cmpFuncOid = InvalidOid;
	info->hashFuncOid = InvalidOid;
	info->cmpFuncInited = false;
	info->hashFuncInited = false;
	info->funcCtx = ctx;

	get_typlenbyvalalign(typid, &info->typlen, &info->typbyval, &info->typalign);

	return info;
}

void
cmpFuncInit(AnyArrayTypeInfo* info)
{
	if (info->cmpFuncInited == false)
	{
		if (!OidIsValid(info->cmpFuncOid))
		{
			info->cmpFuncOid = getAMProc(BTREE_AM_OID, info->typid);

			if (!OidIsValid(info->cmpFuncOid))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not find compare function")));
		}

		fmgr_info_cxt(info->cmpFuncOid, &info->cmpFunc, info->funcCtx);
		info->cmpFuncInited = true;
	}
}

void
hashFuncInit(AnyArrayTypeInfo* info)
{
	if (info->hashFuncInited == false)
	{
		if (!OidIsValid(info->hashFuncOid))
		{
			info->hashFuncOid = getAMProc(HASH_AM_OID, info->typid);

			if (!OidIsValid(info->hashFuncOid))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not find hash function")));
		}

		fmgr_info_cxt(info->hashFuncOid, &info->hashFunc, info->funcCtx);
		info->hashFuncInited = true;
	}
}

SimpleArray*
Array2SimpleArray(AnyArrayTypeInfo	*info, ArrayType *a)
{
	SimpleArray	*s = palloc(sizeof(SimpleArray));

	CHECKARRVALID(a);

	s->info = info;
	if (ARRISVOID(a))
	{
		s->elems = NULL;
		s->nelems = 0;
	}
	else
	{
		deconstruct_array(a, info->typid,
						  info->typlen, info->typbyval, info->typalign,
						  &s->elems, NULL, &s->nelems);
	}

	return s;
}

ArrayType*
SimpleArray2Array(SimpleArray *s)
{
	return construct_array(s->elems, s->nelems, 
						   s->info->typid, 
						   s->info->typlen, 
						   s->info->typbyval, 
						   s->info->typalign);
}

static int
cmpAscArrayElem(const void *a, const void *b, void *arg)
{
	FmgrInfo	*cmpFunc = (FmgrInfo*)arg;

	return DatumGetInt32(FunctionCall2Coll(cmpFunc, DEFAULT_COLLATION_OID, *(Datum*)a, *(Datum*)b));
}

static int
cmpDescArrayElem(const void *a, const void *b, void *arg)
{
	FmgrInfo	*cmpFunc = (FmgrInfo*)arg;

	return -DatumGetInt32(FunctionCall2Coll(cmpFunc, DEFAULT_COLLATION_OID, *(Datum*)a, *(Datum*)b));
}

void
sortSimpleArray(SimpleArray *s, int32 direction)
{
	AnyArrayTypeInfo	*info = s->info;

	cmpFuncInit(info);

	if (s->nelems > 1)
	{
		qsort_arg(s->elems, s->nelems, sizeof(Datum), 
				  (direction > 0) ? cmpAscArrayElem : cmpDescArrayElem,
				  &info->cmpFunc);
	}
}

void
uniqSimpleArray(SimpleArray *s, bool onlyDuplicate)
{
	AnyArrayTypeInfo	*info = s->info;

	cmpFuncInit(info);

	if (s->nelems > 1)
	{
		Datum	*tmp, *dr;
		int32	num =  s->nelems;

		if (onlyDuplicate)
		{
			Datum	*head = s->elems;

			dr = s->elems;
			tmp = s->elems + 1;

			while (tmp - s->elems < num)
			{
				while (tmp - s->elems < num && cmpAscArrayElem(tmp, dr, &info->cmpFunc) == 0)
					tmp++;

				if (tmp - dr > 1)
				{
					*head = *dr;
					head++;
				}
				dr = tmp;
			}

			s->nelems = head - s->elems;
		}
		else
		{
			dr = s->elems;
			tmp = s->elems + 1;

			while (tmp - s->elems < num)
			{
				if (cmpAscArrayElem(tmp, dr, &info->cmpFunc) != 0 )
					*(++dr) = *tmp++;
				else
					tmp++;
			}

			s->nelems = dr + 1 - s->elems;
		}
	}
	else if (onlyDuplicate)
	{
		s->nelems = 0;
	}
}

int
numOfIntersect(SimpleArray *a, SimpleArray *b)
{
	int					cnt = 0,
						cmp;
	Datum				*aptr = a->elems,
						*bptr = b->elems;
	AnyArrayTypeInfo	*info = a->info;

	cmpFuncInit(info);

	while(aptr - a->elems < a->nelems && bptr - b->elems < b->nelems)
	{
		cmp = cmpAscArrayElem(aptr, bptr, &info->cmpFunc);

		if (cmp < 0)
			aptr++;
		else if (cmp > 0)
			bptr++;
		else
		{
			cnt++;
			aptr++;
			bptr++;
		}
	}

	return cnt;
}

double
getSimilarity(SimpleArray *sa, SimpleArray *sb)
{
	int			inter;
	double		result = 0.0;

	sortSimpleArray(sa, 1);
	uniqSimpleArray(sa, false);
	sortSimpleArray(sb, 1);
	uniqSimpleArray(sb, false);

	inter = numOfIntersect(sa, sb);

	switch(SmlType)
	{
		case AA_Cosine:
			result = ((double)inter) / sqrt(((double)sa->nelems) * ((double)sb->nelems));
			break;
		case AA_Jaccard:
			result = ((double)inter) / (((double)sa->nelems) + ((double)sb->nelems) - ((double)inter));
			break;
		case AA_Overlap:
			result = inter;
			break;
		default:
			elog(ERROR, "unknown similarity type");
	}

	return result;
}

