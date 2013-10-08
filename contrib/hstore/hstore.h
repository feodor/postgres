/*
 * contrib/hstore/hstore.h
 */
#ifndef __HSTORE_H__
#define __HSTORE_H__

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/array.h"
#include "utils/numeric.h"

/*
 * HEntry: there is one of these for each key _and_ value in an hstore
 *
 * the position offset points to the _end_ so that we can get the length
 * by subtraction from the previous entry.	the ISFIRST flag lets us tell
 * whether there is a previous entry.
 */
typedef struct
{
	uint32		entry;
} HEntry;

#define HENTRY_ISFIRST		0x80000000
#define HENTRY_ISSTRING 	(0x00000000) /* keep binary compatibility */
#define HENTRY_ISNUMERIC	(0x10000000)
#define HENTRY_ISNEST		(0x20000000)
#define HENTRY_ISNULL		(0x40000000) /* keep binary compatibility */
#define HENTRY_ISBOOL		(0x10000000 | 0x20000000)
#define HENTRY_ISFALSE		HENTRY_ISBOOL
#define HENTRY_ISTRUE		(0x10000000 | 0x20000000 | 0x40000000)

/* HENTRY_ISHASH, HENTRY_ISARRAY and HENTRY_ISCALAR is only used in send/recv */
#define HENTRY_ISHASH		(0x20000000)
#define HENTRY_ISARRAY		(0x20000000 | 0x40000000)
#define HENTRY_ISCALAR		(0x10000000 | 0x40000000)

#define HENTRY_POSMASK 	0x0FFFFFFF
#define HENTRY_TYPEMASK	(~(HENTRY_POSMASK | HENTRY_ISFIRST))

/* note possible multiple evaluations, also access to prior array element */
#define HSE_ISFIRST(he_) 		(((he_).entry & HENTRY_ISFIRST) != 0)
#define HSE_ISSTRING(he_)		(((he_).entry & HENTRY_TYPEMASK) == HENTRY_ISSTRING)
#define HSE_ISNUMERIC(he_) 		(((he_).entry & HENTRY_TYPEMASK) == HENTRY_ISNUMERIC)
#define HSE_ISNEST(he_) 		(((he_).entry & HENTRY_TYPEMASK) == HENTRY_ISNEST)
#define HSE_ISNULL(he_) 		(((he_).entry & HENTRY_TYPEMASK) == HENTRY_ISNULL)
#define HSE_ISBOOL(he_) 		(((he_).entry & HENTRY_TYPEMASK & HENTRY_ISBOOL) == HENTRY_ISBOOL)
#define HSE_ISBOOL_TRUE(he_) 	(((he_).entry & HENTRY_TYPEMASK) == HENTRY_ISTRUE)
#define HSE_ISBOOL_FALSE(he_) 	(HSE_ISBOOL(he_) && !HSE_ISBOOL_TRUE(he_))

#define HSE_ENDPOS(he_) ((he_).entry & HENTRY_POSMASK)
#define HSE_OFF(he_) (HSE_ISFIRST(he_) ? 0 : HSE_ENDPOS((&(he_))[-1]))
#define HSE_LEN(he_) (HSE_ISFIRST(he_)	\
					  ? HSE_ENDPOS(he_) \
					  : HSE_ENDPOS(he_) - HSE_ENDPOS((&(he_))[-1]))

/*
 * determined by the size of "endpos" (ie HENTRY_POSMASK)
 */
#define HSTORE_MAX_KEY_LEN 		HENTRY_POSMASK
#define HSTORE_MAX_VALUE_LEN 	HENTRY_POSMASK

typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	/* header of hash or array hstore type */
	/* array of HEntry follows */
} HStore;

/*
 * it's not possible to get more than 2^28 items into an hstore,
 * so we reserve the top few bits of the size field. See hstore_compat.c
 * for one reason why.	Some bits are left for future use here.
 */
#define HS_FLAG_NEWVERSION 		0x80000000
#define HS_FLAG_ARRAY			0x40000000
#define HS_FLAG_HSTORE			0x20000000
#define HS_FLAG_SCALAR			0x10000000

#define HS_COUNT_MASK			0x0FFFFFFF

#define HS_ISEMPTY(hsp_)		(VARSIZE(hsp_) <= VARHDRSZ)
#define HS_ROOT_COUNT(hsp_) 	(HS_ISEMPTY(hsp_) ? 0 : ( *(uint32*)VARDATA(hsp_) & HS_COUNT_MASK))
#define HS_ROOT_IS_HASH(hsp_) 	(HS_ISEMPTY(hsp_) ? 0 : ( *(uint32*)VARDATA(hsp_) & HS_FLAG_HSTORE))
#define HS_ROOT_IS_ARRAY(hsp_) 	(HS_ISEMPTY(hsp_) ? 0 : ( *(uint32*)VARDATA(hsp_) & HS_FLAG_ARRAY))
#define HS_ROOT_IS_SCALAR(hsp_) (HS_ISEMPTY(hsp_) ? 0 : ( *(uint32*)VARDATA(hsp_) & HS_FLAG_SCALAR))

/* DatumGetHStoreP includes support for reading old-format hstore values */
extern HStore *hstoreUpgrade(Datum orig);

#define DatumGetHStoreP(d) hstoreUpgrade(d)

#define PG_GETARG_HS(x) DatumGetHStoreP(PG_GETARG_DATUM(x))

typedef struct HStorePair HStorePair;
typedef struct HStoreValue HStoreValue;

struct HStoreValue {
	enum {
		hsvNull,
		hsvString,
		hsvNumeric,
		hsvBool,
		hsvArray,
		hsvHash,
		hsvBinary  /* binary form of hsvArray/hsvHash */
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
			HStoreValue	*elems;
			bool		scalar; /* scalar actually shares representation with array */
		} array;

		struct {
			int			npairs;
			HStorePair 	*pairs;
		} hash;

		struct {
			uint32		len;
			char		*data;
		} binary;
	};

}; 

struct HStorePair {
	HStoreValue	key;
	HStoreValue	value;
	uint32		order; /* to keep order of pairs with equal key */ 
}; 


extern HStoreValue* parseHStore(const char *str, int len, bool json);

/*
 * hstore support functios
 */

#define WHS_KEY         	(0x001)
#define WHS_VALUE       	(0x002)
#define WHS_ELEM       		(0x004)
#define WHS_BEGIN_ARRAY 	(0x008)
#define WHS_END_ARRAY   	(0x010)
#define WHS_BEGIN_HASH	    (0x020)
#define WHS_END_HASH        (0x040)

typedef void (*walk_hstore_cb)(void* /*arg*/, HStoreValue* /* value */, 
											uint32 /* flags */, uint32 /* level */);
extern void walkUncompressedHStore(HStoreValue *v, walk_hstore_cb cb, void *cb_arg);

extern int compareHStoreStringValue(const void *a, const void *b, void *arg);
extern int compareHStorePair(const void *a, const void *b, void *arg);

extern int compareHStoreBinaryValue(char *a, char *b);
extern int compareHStoreValue(HStoreValue *a, HStoreValue *b);

extern HStoreValue* findUncompressedHStoreValueByValue(char *buffer, uint32 flags, 
												uint32 *lowbound, HStoreValue* key);
extern HStoreValue* findUncompressedHStoreValue(char *buffer, uint32 flags, 
												uint32 *lowbound, char *key, uint32 keylen);

extern HStoreValue* getHStoreValue(char *buffer, uint32 flags, int32 i);

extern bool stringIsNumber(char *string, int len);

typedef enum HStoreOutputKind {
	HStoreOutput,
	HStoreStrictOutput,
	JsonOutput,
	JsonLooseOutput
} HStoreOutputKind;

extern char* hstoreToCString(StringInfo out, char *in,
							 int len /* just estimation */, HStoreOutputKind kind,
							 bool enable_pretty_print);
text* HStoreValueToText(HStoreValue *v);

typedef struct ToHStoreState
{
	HStoreValue             v;
	uint32                  size;
	struct ToHStoreState    *next;
} ToHStoreState;

extern HStoreValue* pushHStoreValue(ToHStoreState **state, int r /* WHS_* */, HStoreValue *v);

/* be aware: size effects for n argument */
#define ORDER_PAIRS(a, n, delaction)												\
	do {																			\
		bool	hasNonUniq = false;													\
																					\
		if ((n) > 1)																\
			qsort_arg((a), (n), sizeof(HStorePair), compareHStorePair, &hasNonUniq);\
																					\
		if (hasNonUniq)																\
		{																			\
			HStorePair	*ptr = (a) + 1,												\
						*res = (a);													\
																					\
			while (ptr - (a) < (n))													\
			{																		\
				if (ptr->key.string.len == res->key.string.len && 					\
					memcmp(ptr->key.string.val, res->key.string.val,				\
						   ptr->key.string.len) == 0)								\
				{																	\
					delaction;														\
				}																	\
				else																\
				{																	\
					res++;															\
					if (ptr != res)													\
						memcpy(res, ptr, sizeof(*res));								\
				}																	\
				ptr++;																\
			}																		\
																					\
			(n) = res + 1 - (a);													\
		}																			\
	} while(0)																		

uint32 compressHStore(HStoreValue *v, char *buffer);


typedef struct HStoreIterator
{
	uint32					type;
	uint32					nelems;
	HEntry					*array;
	bool					isScalar;
	char					*data;
	char					*buffer; /* unparsed buffer */

	int						i;

	/*
	 * enum members should be freely OR'ed with HS_FLAG_ARRAY/HS_FLAG_HSTORE 
	 * with possiblity of decoding. See optimization in HStoreIteratorGet()
	 */
	enum {
		hsi_start 	= 0x00,
		hsi_key		= 0x01,
		hsi_value	= 0x02,
		hsi_elem	= 0x04
	} state;

	struct HStoreIterator	*next;
} HStoreIterator;

extern 	HStoreIterator*	HStoreIteratorInit(char *buffer);
extern	int /* WHS_* */	HStoreIteratorGet(HStoreIterator **it, HStoreValue *v, bool skipNested);

#define HStoreContainsStrategyNumber	7
#define HStoreExistsStrategyNumber		9
#define HStoreExistsAnyStrategyNumber	10
#define HStoreExistsAllStrategyNumber	11
#define HStoreOldContainsStrategyNumber 13		/* backwards compatibility */

/*
 * defining HSTORE_POLLUTE_NAMESPACE=0 will prevent use of old function names;
 * for now, we default to on for the benefit of people restoring old dumps
 */
#ifndef HSTORE_POLLUTE_NAMESPACE
#define HSTORE_POLLUTE_NAMESPACE 1
#endif

#if HSTORE_POLLUTE_NAMESPACE
#define HSTORE_POLLUTE(newname_,oldname_) \
	PG_FUNCTION_INFO_V1(oldname_);		  \
	Datum oldname_(PG_FUNCTION_ARGS);	  \
	Datum newname_(PG_FUNCTION_ARGS);	  \
	Datum oldname_(PG_FUNCTION_ARGS) { return newname_(fcinfo); } \
	extern int no_such_variable
#else
#define HSTORE_POLLUTE(newname_,oldname_) \
	extern int no_such_variable
#endif

/*
 * When using a GIN/GiST index for hstore, we choose to index both keys and values.
 * The storage format is "text" values, with K, V, or N prepended to the string
 * to indicate key, value, or null values.  (As of 9.1 it might be better to
 * store null values as nulls, but we'll keep it this way for on-disk
 * compatibility.)
 */
#define ELEMFLAG    'E'
#define KEYFLAG     'K'
#define VALFLAG     'V'
#define NULLFLAG    'N'



#endif   /* __HSTORE_H__ */
