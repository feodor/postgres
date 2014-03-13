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

#define JENTRY_POSMASK			0x0FFFFFFF
#define JENTRY_ISFIRST			0x80000000
#define JENTRY_TYPEMASK 		(~(JENTRY_POSMASK | JENTRY_ISFIRST))

/*
 * determined by the size of "endpos" (ie JENTRY_POSMASK)
 */
#define JSONB_MAX_STRING_LEN	JENTRY_POSMASK

/*
 * it's not possible to get more than 2^28 items into an jsonb.
 *
 * JB_CMASK is count mask
 */
#define JB_CMASK				0x0FFFFFFF

#define JB_FSCALAR				0x10000000
#define JB_FOBJECT				0x20000000
#define JB_FARRAY				0x40000000

/* Get information on varlena Jsonb */
#define JB_ISEMPTY(jbp_)		(VARSIZE(jbp_) == 0)
#define JB_ROOT_COUNT(jbp_)		(JB_ISEMPTY(jbp_) \
								 ? 0: ( *(uint32*) VARDATA(jbp_) & JB_CMASK))
#define JB_ROOT_IS_SCALAR(jbp_) (JB_ISEMPTY(jbp_) \
								 ? 0: ( *(uint32*) VARDATA(jbp_) & JB_FSCALAR))
#define JB_ROOT_IS_OBJECT(jbp_) (JB_ISEMPTY(jbp_) \
								 ? 0: ( *(uint32*) VARDATA(jbp_) & JB_FOBJECT))
#define JB_ROOT_IS_ARRAY(jbp_)	(JB_ISEMPTY(jbp_) \
								 ? 0: ( *(uint32*) VARDATA(jbp_) & JB_FARRAY))

/* Flags indicating a stage of sequential Jsonb processing */
#define WJB_KEY					0x001
#define WJB_VALUE				0x002
#define WJB_ELEM				0x004
#define WJB_BEGIN_ARRAY			0x008
#define WJB_END_ARRAY			0x010
#define WJB_BEGIN_OBJECT		0x020
#define WJB_END_OBJECT			0x040

/* Get offset for Jentry  */
#define JBE_ENDPOS(he_) 		((he_).header & JENTRY_POSMASK)
#define JBE_OFF(he_) 			(JBE_ISFIRST(he_) ? 0 : JBE_ENDPOS((&(he_))[-1]))
#define JBE_LEN(he_) 			(JBE_ISFIRST(he_)	\
								  ? JBE_ENDPOS(he_) \
								  : JBE_ENDPOS(he_) - JBE_ENDPOS((&(he_))[-1]))

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
typedef	char*  JsonbSuperHeader;


/*
 * Jsonbs are varlena objects, so must meet the varlena convention that the
 * first int32 of the object contains the total object size in bytes.  Be sure
 * to use VARSIZE() and SET_VARSIZE() to access it, though!
 *
 * We have an abstraction called a "superheader".  This is a pointer that
 * conventionally points to the first item after our 4-byte uncompressed
 * varlena header, from which we can read uint32 values through bitwise
 * operations.
 *
 * Sometimes we pass a superheader reference to a function, and it doesn't
 * matter if it points to just after the start of a Jsonb, or to a Jentry.
 * Either way, the type punning works and the superheader/header metadata is
 * used to operate on an underlying JsonbValue.
 *
 * In a few contexts, when passing a superheader, there actually is an
 * assumption that it really does point to just past vl_len_ in a Jsonb.  We
 * assert that it's "superheader sane" in those contexts.  In general, this is
 * expected to work just fine, as care has been taken to make the nested layout
 * consistent to the extent that it matters between the least nested level
 * (Jsonb), and deeper nested levels (Jentry).
 */

typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	uint32		superheader;
	/* (array of JEntry follows, size determined using uint32 superheader) */
} Jsonb;

/*
 * JEntry: there is one of these for each key _and_ value in a jsonb object
 *
 * The position offset points to the _end_ so that we can get the length by
 * subtraction from the previous entry.	 The JENTRY_ISFIRST flag indicates if
 * there is a previous entry.
 */
typedef struct
{
	uint32		header;			/* May be accessed as superheader */
}	JEntry;

struct JsonbValue
{
	enum
	{
		/* Scalar types (influences sort order) */
		jbvNull = 0x0,
		jbvString,
		jbvNumeric,
		jbvBool,
		/* Composite types */
		jbvArray = 0x10,
		jbvObject,
		/* Binary form of jbvArray/jbvObject/scalar */
		jbvBinary
	} type;

	Size		size;			/* Estimation size of node (including
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

/* Pair within an Object */
struct JsonbPair
{
	JsonbValue	key;
	JsonbValue	value;
	uint32		order;			/* preserves order of pairs with equal keys */
};

/* Conversion state used when parsing Jsonb from text, or for type coercion */
typedef struct ToJsonbState
{
	JsonbValue	v;
	Size		size;
	struct ToJsonbState *next;
} ToJsonbState;

/*
 * JsonbIterator holds details of the type for each iteration. It also stores a
 * Jsonb varlena buffer, which can be directly accessed without deserialization
 * in some contexts.
 */
typedef enum
{
	jbi_start = 0x0,
	jbi_key,
	jbi_value,
	jbi_elem
} iterState;

typedef struct JsonbIterator
{
	/* Jsonb varlena buffer (may or may not be root) */
	char	   *buffer;

	/* Current item in buffer */
	int			i;

	/* Current value */
	uint32		containerType; /* Never of value JB_FLAG_SCALAR, since
								* scalars will appear in pseudo-arrays */
	uint32		nElems;		   /* Number of elements in metaArray
								* (we * 2 for pairs within objects) */
	bool		isScalar;	   /* Pseudo-array scalar value? */
	JEntry	   *meta;

	/*
	 * Data proper.  Note that this points just past end of metaArray.  We use
	 * "meta" metadata (Jentrys) with JBE_OFF() macro to find appropriate
	 * offsets into this array.
	 */
	char	   *dataProper;

	iterState state;

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
extern int	compareJsonbSuperHeaderValue(JsonbSuperHeader a,
										 JsonbSuperHeader b);
extern bool	compareJsonbValue(JsonbValue *a, JsonbValue *b);
extern JsonbValue *findJsonbValueFromSuperHeaderLen(JsonbSuperHeader sheader,
													uint32 flags,
													uint32 *lowbound,
													char *key, uint32 keylen);
extern JsonbValue *findJsonbValueFromSuperHeader(JsonbSuperHeader sheader,
												 uint32 flags,
												 uint32 *lowbound,
												 JsonbValue *key);
extern JsonbValue *getIthJsonbValueFromSuperHeader(JsonbSuperHeader sheader,
												   uint32 flags, uint32 i);
extern JsonbValue *pushJsonbValue(ToJsonbState ** state, int r, JsonbValue *v);
extern JsonbIterator *JsonbIteratorInit(JsonbSuperHeader buffer);
extern int JsonbIteratorNext(JsonbIterator **it, JsonbValue *v, bool skipNested);
extern Jsonb *JsonbValueToJsonb(JsonbValue *v);
extern JsonbValue *arrayToJsonbSortedArray(ArrayType *a);

/* jsonb.c support function */
extern char *JsonbToCString(StringInfo out, char *in, int estimated_len);

#endif   /* __JSONB_H__ */
