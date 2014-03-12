/*-------------------------------------------------------------------------
 *
 * jsonb.h
 *	  Declarations for JSONB data type support.
 *
 * Copyright (c) 1996-2014, PostgreSQL Global Development Group
 *
 * src/include/utils/jsonb.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __JSONB_H__
#define __JSONB_H__

#include "lib/stringinfo.h"
#include "utils/array.h"
#include "utils/numeric.h"

#define JENTRY_POSMASK	(0x0FFFFFFF)
#define JENTRY_ISFIRST	(0x80000000)
#define JENTRY_TYPEMASK (~(JENTRY_POSMASK | JENTRY_ISFIRST))

/*
 * determined by the size of "endpos" (ie JENTRY_POSMASK)
 */
#define JSONB_MAX_STRING_LEN		JENTRY_POSMASK

/*
 * it's not possible to get more than 2^28 items into an jsonb.
 */
#define JB_FLAG_ARRAY			0x40000000
#define JB_FLAG_OBJECT			0x20000000
#define JB_FLAG_SCALAR			0x10000000

#define JB_COUNT_MASK			0x0FFFFFFF

#define JB_ISEMPTY(jbp_)		(VARSIZE(jbp_) <= VARHDRSZ)
#define JB_ROOT_COUNT(jbp_)		(JB_ISEMPTY(jbp_) ? 0 : ( *(uint32*)VARDATA(jbp_) & JB_COUNT_MASK))
#define JB_ROOT_IS_OBJECT(jbp_) (JB_ISEMPTY(jbp_) ? 0 : ( *(uint32*)VARDATA(jbp_) & JB_FLAG_OBJECT))
#define JB_ROOT_IS_ARRAY(jbp_)	(JB_ISEMPTY(jbp_) ? 0 : ( *(uint32*)VARDATA(jbp_) & JB_FLAG_ARRAY))
#define JB_ROOT_IS_SCALAR(jbp_) (JB_ISEMPTY(jbp_) ? 0 : ( *(uint32*)VARDATA(jbp_) & JB_FLAG_SCALAR))

#define WJB_KEY				(0x001)
#define WJB_VALUE			(0x002)
#define WJB_ELEM			(0x004)
#define WJB_BEGIN_ARRAY		(0x008)
#define WJB_END_ARRAY		(0x010)
#define WJB_BEGIN_OBJECT	(0x020)
#define WJB_END_OBJECT		(0x040)

/*
 * When using a GIN index for jsonb, we choose to index both keys and values.
 * The storage format is "text" values, with K, V, or N prepended to the string
 * to indicate key, value, or null values.  (As of 9.1 it might be better to
 * store null values as nulls, but we'll keep it this way for on-disk
 * compatibility.)
 *
 * jsonb Keys and elements are treated equivalently when serialized to text
 * index storage.
 */
#define KEYELEMFLAG 'K'
#define VALFLAG     'V'
#define NULLFLAG    'N'

#define JsonbContainsStrategyNumber		7
#define JsonbExistsStrategyNumber		9
#define JsonbExistsAnyStrategyNumber	10
#define JsonbExistsAllStrategyNumber	11

/* Convenience macros */
#define DatumGetJsonb(d)	((Jsonb*) PG_DETOAST_DATUM(d))
#define JsonbGetDatum(p)	PointerGetDatum(p)
#define PG_GETARG_JSONB(x)	DatumGetJsonb(PG_GETARG_DATUM(x))
#define PG_RETURN_JSONB(x)	PG_RETURN_POINTER(x)

typedef struct JsonbPair JsonbPair;
typedef struct JsonbValue JsonbValue;

/*
 * JEntry: there is one of these for each key _and_ value in a jsonb object
 *
 * The position offset points to the _end_ so that we can get the length by
 * subtraction from the previous entry.	 The JENTRY_ISFIRST flag indicates if
 * there is a previous entry.
 */
typedef struct
{
	uint32		entry;
}	JEntry;

typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	/* header of jsonb object or array */
	/* array of JEntry follows */
} Jsonb;

struct JsonbValue
{
	enum
	{
		/* Scalar types (influences sort order) */
		jbvNull = 0,
		jbvString,
		jbvNumeric,
		jbvBool,
		/* Composite types */
		jbvArray = 4,
		jbvObject,
		/* Binary form of jbvArray/jbvObject */
		jbvBinary
	} type;

	uint32		size;			/* Estimation size of node (including
								 * subnodes) */

	union
	{
		Numeric numeric;
		bool		boolean;
		struct
		{
			uint32		len;
			char	   *val;	/* Not necessarily null-terminated */
		} string;

		struct
		{
			int			nElems;
			JsonbValue *elems;
			bool		scalar; /* Scalar actually shares representation with
								 * array */
		} array;

		struct
		{
			int			nPairs;
			JsonbPair  *pairs;
		} object;		/* Associative data structure */

		struct
		{
			uint32		len;
			char	   *data;
		} binary;
	};

};

struct JsonbPair
{
	JsonbValue	key;
	JsonbValue	value;
	uint32		order;			/* preserves order of pairs with equal keys */
};

typedef struct ToJsonbState
{
	JsonbValue	v;
	uint32		size;
	struct ToJsonbState *next;
} ToJsonbState;

/*
 * JsonbIterator holds details of the type for each iteration. It also stores
 * an unoriginal unparsed varlena buffer, which can be directly accessed
 * without deserialization in some contexts.
 */
typedef struct JsonbIterator
{
	/* Unparsed buffer (not necessarily root) */
	char	   *buffer;

	/* Current item in buffer */
	int			i;

	/* Current value */
	uint32		containerType; /* Never JB_FLAG_SCALAR
								* scalars may appear in pseudo-arrays */
	uint32		nElems;		   /* Number of elements in metaArray
								* (we * 2 for pairs within objects) */
	bool		isScalar;	   /* Pseudo-array scalar value? */
	JEntry	   *metaArray;

	/*
	 * Jentry items.  Note that this points just past metaArray (straight to
	 * items proper).
	 *
	 * char pointer due to alignment considerations
	 */
	char	   *containerData;

	/*
	 * Enum members should be freely OR'ed with JB_FLAG_ARRAY/JB_FLAG_OBJECT
	 * with possibility of decoding.
	 *
	 * See space optimization in JsonbIteratorGet()
	 */
	enum
	{
		jbi_start	= 0x00,
		jbi_key		= 0x01,
		jbi_value	= 0x02,
		jbi_elem	= 0x04
	} state;

	struct JsonbIterator *next;
} JsonbIterator;

/* I/O routines */
extern Datum jsonb_in(PG_FUNCTION_ARGS);
extern Datum jsonb_out(PG_FUNCTION_ARGS);
extern Datum jsonb_recv(PG_FUNCTION_ARGS);
extern Datum jsonb_send(PG_FUNCTION_ARGS);
extern Datum jsonb_typeof(PG_FUNCTION_ARGS);

/* Indexing-related ops */
extern Datum jsonb_exists(PG_FUNCTION_ARGS);
extern Datum jsonb_exists_any(PG_FUNCTION_ARGS);
extern Datum jsonb_exists_all(PG_FUNCTION_ARGS);
extern Datum jsonb_contains(PG_FUNCTION_ARGS);
extern Datum jsonb_contained(PG_FUNCTION_ARGS);
extern Datum jsonb_ne(PG_FUNCTION_ARGS);
extern Datum jsonb_lt(PG_FUNCTION_ARGS);
extern Datum jsonb_gt(PG_FUNCTION_ARGS);
extern Datum jsonb_le(PG_FUNCTION_ARGS);
extern Datum jsonb_ge(PG_FUNCTION_ARGS);
extern Datum jsonb_eq(PG_FUNCTION_ARGS);
extern Datum jsonb_cmp(PG_FUNCTION_ARGS);
extern Datum jsonb_hash(PG_FUNCTION_ARGS);

/* GIN support functions */
extern Datum gin_extract_jsonb(PG_FUNCTION_ARGS);
extern Datum gin_extract_jsonb_query(PG_FUNCTION_ARGS);
extern Datum gin_consistent_jsonb(PG_FUNCTION_ARGS);
/* GIN hash opclass functions */
extern Datum gin_extract_jsonb_hash(PG_FUNCTION_ARGS);
extern Datum gin_extract_jsonb_query_hash(PG_FUNCTION_ARGS);
extern Datum gin_consistent_jsonb_hash(PG_FUNCTION_ARGS);

/* Support functions */
extern int	compareJsonbBinaryValue(char *a, char *b);
extern bool	compareJsonbValue(JsonbValue *a, JsonbValue *b);
extern JsonbValue *findUncompressedJsonbValueByValue(char *buffer, uint32 flags,
								  uint32 *lowbound, JsonbValue *key);
extern JsonbValue *findUncompressedJsonbValue(char *buffer, uint32 flags,
						   uint32 *lowbound, char *key, uint32 keylen);
extern JsonbValue *getJsonbValue(char *buffer, uint32 flags, int32 i);
extern JsonbValue *pushJsonbValue(ToJsonbState ** state, int r, JsonbValue *v);
extern JsonbIterator *JsonbIteratorInit(char *buffer);
extern int JsonbIteratorNext(JsonbIterator **it, JsonbValue *v, bool skipNested);
extern Jsonb *JsonbValueToJsonb(JsonbValue *v);
extern JsonbValue *arrayToJsonbSortedArray(ArrayType *a);

/* jsonb.c support function */
extern char *JsonbToCString(StringInfo out, char *in, int estimated_len);

#endif   /* __JSONB_H__ */
