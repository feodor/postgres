/*-------------------------------------------------------------------------
 *
 * jsonb.h
 *    Declarations for JSONB data type support.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 *
 * NOTE. JSONB type is designed to be binary compatible with hstore.
 *
 * src/include/utils/jsonb.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __JSONB_H__
#define __JSONB_H__

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/array.h"
#include "utils/numeric.h"

/*
 * JEntry: there is one of these for each key _and_ value in an jsonb
 *
 * the position offset points to the _end_ so that we can get the length
 * by subtraction from the previous entry.	the ISFIRST flag lets us tell
 * whether there is a previous entry.
 */
typedef struct
{
	uint32		entry;
} JEntry;

#define JENTRY_ISFIRST		0x80000000
#define JENTRY_ISSTRING 	(0x00000000) /* keep binary compatibility */
#define JENTRY_ISNUMERIC	(0x10000000)
#define JENTRY_ISNEST		(0x20000000)
#define JENTRY_ISNULL		(0x40000000) /* keep binary compatibility */
#define JENTRY_ISBOOL		(0x10000000 | 0x20000000)
#define JENTRY_ISFALSE		JENTRY_ISBOOL
#define JENTRY_ISTRUE		(0x10000000 | 0x20000000 | 0x40000000)

/* JENTRY_ISOBJECT, JENTRY_ISARRAY and JENTRY_ISCALAR is only used in send/recv */
#define JENTRY_ISOBJECT		(0x20000000)
#define JENTRY_ISARRAY		(0x20000000 | 0x40000000)
#define JENTRY_ISCALAR		(0x10000000 | 0x40000000)

#define JENTRY_POSMASK 	0x0FFFFFFF
#define JENTRY_TYPEMASK	(~(JENTRY_POSMASK | JENTRY_ISFIRST))

/* note possible multiple evaluations, also access to prior array element */
#define JBE_ISFIRST(he_) 		(((he_).entry & JENTRY_ISFIRST) != 0)
#define JBE_ISSTRING(he_)		(((he_).entry & JENTRY_TYPEMASK) == JENTRY_ISSTRING)
#define JBE_ISNUMERIC(he_) 		(((he_).entry & JENTRY_TYPEMASK) == JENTRY_ISNUMERIC)
#define JBE_ISNEST(he_) 		(((he_).entry & JENTRY_TYPEMASK) == JENTRY_ISNEST)
#define JBE_ISNULL(he_) 		(((he_).entry & JENTRY_TYPEMASK) == JENTRY_ISNULL)
#define JBE_ISBOOL(he_) 		(((he_).entry & JENTRY_TYPEMASK & JENTRY_ISBOOL) == JENTRY_ISBOOL)
#define JBE_ISBOOL_TRUE(he_) 	(((he_).entry & JENTRY_TYPEMASK) == JENTRY_ISTRUE)
#define JBE_ISBOOL_FALSE(he_) 	(JBE_ISBOOL(he_) && !JBE_ISBOOL_TRUE(he_))

#define JBE_ENDPOS(he_) ((he_).entry & JENTRY_POSMASK)
#define JBE_OFF(he_) (JBE_ISFIRST(he_) ? 0 : JBE_ENDPOS((&(he_))[-1]))
#define JBE_LEN(he_) (JBE_ISFIRST(he_)	\
					  ? JBE_ENDPOS(he_) \
					  : JBE_ENDPOS(he_) - JBE_ENDPOS((&(he_))[-1]))

/*
 * determined by the size of "endpos" (ie JENTRY_POSMASK)
 */
#define JSONB_MAX_STRING_LEN 		JENTRY_POSMASK

typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	/* header of hash or array jsonb type */
	/* array of JEntry follows */
} Jsonb;

/*
 * it's not possible to get more than 2^28 items into an jsonb.
 */
#define JB_FLAG_UNUSED 			0x80000000
#define JB_FLAG_ARRAY			0x40000000
#define JB_FLAG_OBJECT			0x20000000
#define JB_FLAG_SCALAR			0x10000000

#define JB_COUNT_MASK			0x0FFFFFFF

#define JB_ISEMPTY(jbp_)		(VARSIZE(jbp_) <= VARHDRSZ)
#define JB_ROOT_COUNT(jbp_) 	(JB_ISEMPTY(jbp_) ? 0 : ( *(uint32*)VARDATA(jbp_) & JB_COUNT_MASK))
#define JB_ROOT_IS_OBJECT(jbp_) (JB_ISEMPTY(jbp_) ? 0 : ( *(uint32*)VARDATA(jbp_) & JB_FLAG_OBJECT))
#define JB_ROOT_IS_ARRAY(jbp_) 	(JB_ISEMPTY(jbp_) ? 0 : ( *(uint32*)VARDATA(jbp_) & JB_FLAG_ARRAY))
#define JB_ROOT_IS_SCALAR(jbp_) (JB_ISEMPTY(jbp_) ? 0 : ( *(uint32*)VARDATA(jbp_) & JB_FLAG_SCALAR))

#define DatumGetJsonb(d) 	((Jsonb*) PG_DETOAST_DATUM(d))
#define JsonbGetDatum(p)	PointerGetDatum(p)

#define PG_GETARG_JSONB(x) DatumGetJsonb(PG_GETARG_DATUM(x))
#define PG_RETURN_JSONB(x) PG_RETURN_POINTER(x)

typedef struct JsonbPair JsonbPair;
typedef struct JsonbValue JsonbValue;

struct JsonbValue {
	enum {
		jbvNull,
		jbvString,
		jbvNumeric,
		jbvBool,
		jbvArray,
		jbvHash,
		jbvBinary  /* binary form of jbvArray/jbvHash */
	} type;

	uint32		size; /* estimation size of node (including subnodes) */

	union {
		Numeric			numeric;
		bool			boolean;
		struct {
			uint32		len;
			char 		*val; /* could be not null-terminated */
		} string;

		struct {
			int			nelems;
			JsonbValue	*elems;
			bool		scalar; /* scalar actually shares representation with array */
		} array;

		struct {
			int			npairs;
			JsonbPair 	*pairs;
		} hash;

		struct {
			uint32		len;
			char		*data;
		} binary;
	};

}; 

struct JsonbPair {
	JsonbValue	key;
	JsonbValue	value;
	uint32		order; /* to keep order of pairs with equal key */ 
}; 

/*
 * jsonb support functios
 */

#define WJB_KEY         	(0x001)
#define WJB_VALUE       	(0x002)
#define WJB_ELEM       		(0x004)
#define WJB_BEGIN_ARRAY 	(0x008)
#define WJB_END_ARRAY   	(0x010)
#define WJB_BEGIN_OBJECT    (0x020)
#define WJB_END_OBJECT      (0x040)

typedef void (*walk_jsonb_cb)(void* /*arg*/, JsonbValue* /* value */, 
											uint32 /* flags */, uint32 /* level */);
extern void walkUncompressedJsonb(JsonbValue *v, walk_jsonb_cb cb, void *cb_arg);

extern int compareJsonbStringValue(const void *a, const void *b, void *arg);
extern int compareJsonbPair(const void *a, const void *b, void *arg);

extern int compareJsonbBinaryValue(char *a, char *b);
extern int compareJsonbValue(JsonbValue *a, JsonbValue *b);

extern JsonbValue* findUncompressedJsonbValueByValue(char *buffer, uint32 flags, 
												uint32 *lowbound, JsonbValue* key);
extern JsonbValue* findUncompressedJsonbValue(char *buffer, uint32 flags, 
												uint32 *lowbound, char *key, uint32 keylen);

extern JsonbValue* getJsonbValue(char *buffer, uint32 flags, int32 i);

typedef struct ToJsonbState
{
	JsonbValue             v;
	uint32                  size;
	struct ToJsonbState    *next;
} ToJsonbState;

extern JsonbValue* pushJsonbValue(ToJsonbState **state, int r /* WJB_* */, JsonbValue *v);

extern void uniqueJsonbValue(JsonbValue *v);

extern uint32 compressJsonb(JsonbValue *v, char *buffer);

typedef struct JsonbIterator
{
	uint32					type;
	uint32					nelems;
	JEntry					*array;
	bool					isScalar;
	char					*data;
	char					*buffer; /* unparsed buffer */

	int						i;

	/*
	 * enum members should be freely OR'ed with JB_FLAG_ARRAY/JB_FLAG_JSONB 
	 * with possiblity of decoding. See optimization in JsonbIteratorGet()
	 */
	enum {
		jbi_start 	= 0x00,
		jbi_key		= 0x01,
		jbi_value	= 0x02,
		jbi_elem	= 0x04
	} state;

	struct JsonbIterator	*next;
} JsonbIterator;

extern 	JsonbIterator*	JsonbIteratorInit(char *buffer);
extern	int /* WJB_* */	JsonbIteratorGet(JsonbIterator **it, JsonbValue *v, bool skipNested);

extern Datum jsonb_in(PG_FUNCTION_ARGS);
extern Datum jsonb_out(PG_FUNCTION_ARGS);
extern Datum jsonb_recv(PG_FUNCTION_ARGS);
extern Datum jsonb_send(PG_FUNCTION_ARGS);

extern Datum jsonb_typeof(PG_FUNCTION_ARGS);

extern char *JsonbToCString(StringInfo out, char *in, int estimated_len);
extern Jsonb *JsonbValueToJsonb(JsonbValue *v);

#endif   /* __JSONB_H__ */
