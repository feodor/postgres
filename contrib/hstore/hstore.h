/*
 * contrib/hstore/hstore.h
 */
#ifndef __HSTORE_H__
#define __HSTORE_H__

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/array.h"

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

#define HENTRY_ISFIRST	0x80000000
#define HENTRY_ISNULL	0x40000000
#define HENTRY_ISARRAY	0x20000000
#define HENTRY_ISHSTORE	0x10000000
#define HENTRY_POSMASK 	0x0FFFFFFF

/* note possible multiple evaluations, also access to prior array element */
#define HSE_ISFIRST(he_) (((he_).entry & HENTRY_ISFIRST) != 0)
#define HSE_ISNULL(he_) (((he_).entry & HENTRY_ISNULL) != 0)
#define HSE_ISARRAY(he_) (((he_).entry & HENTRY_ISARRAY) != 0)
#define HSE_ISHSTORE(he_) (((he_).entry & HENTRY_ISHSTORE) != 0)
#define HSE_ISSTRING(he_) (((he_).entry & (HENTRY_ISHSTORE | HENTRY_ISARRAY)) == 0)
#define HSE_ENDPOS(he_) ((he_).entry & HENTRY_POSMASK)
#define HSE_OFF(he_) (HSE_ISFIRST(he_) ? 0 : HSE_ENDPOS((&(he_))[-1]))
#define HSE_LEN(he_) (HSE_ISFIRST(he_)	\
					  ? HSE_ENDPOS(he_) \
					  : HSE_ENDPOS(he_) - HSE_ENDPOS((&(he_))[-1]))

/*
 * determined by the size of "endpos" (ie HENTRY_POSMASK)
 */
#define HSTORE_MAX_KEY_LEN 		0x0FFFFFFF		/* XXX */
#define HSTORE_MAX_VALUE_LEN 	0x0FFFFFFF		/* XXX */
#define HSTORE_MAX_STRING_LEN	0x0FFFFFFF

typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	uint32		size_;			/* flags and number of items in hstore */
	/* array of HEntry follows */
} HStore;

/*
 * it's not possible to get more than 2^28 items into an hstore,
 * so we reserve the top few bits of the size field. See hstore_compat.c
 * for one reason why.	Some bits are left for future use here.
 */
#define HS_FLAG_NEWVERSION 		0x80000000
#define HS_FLAG_ARRAY			HENTRY_ISARRAY
#define HS_FLAG_HSTORE			HENTRY_ISHSTORE

#define HS_COUNT_MASK			0x0FFFFFFF

#define HS_ISEMPTY(hsp_)		(VARSIZE(hsp_) <= VARHDRSZ)
#define HS_COUNT(hsp_) 			(HS_ISEMPTY(hsp_) ? 0 : (hsp_)->size_ & 0x0FFFFFFF)		/* XXX */
#define HS_SETCOUNT(hsp_,c_) 	((hsp_)->size_ = (c_) | HS_FLAG_NEWVERSION | ((hsp_)->size_ & ~HS_COUNT_MASK))


#define HSHRDSIZE	(sizeof(HStore))
#define CALCDATASIZE(x, lenstr) ( (x) * 2 * sizeof(HEntry) + HSHRDSIZE + (lenstr) )

/* note multiple evaluations of x */
#define ARRPTR(x)		( (HEntry*) ( (HStore*)(x) + 1 ) )
#define STRPTR(x)		( (char*)(ARRPTR(x) + HS_COUNT((HStore*)(x)) * 2) )

/* note multiple/non evaluations */
#define HS_KEY(arr_,str_,i_) ((str_) + HSE_OFF((arr_)[2*(i_)]))
#define HS_VAL(arr_,str_,i_) ((str_) + HSE_OFF((arr_)[2*(i_)+1]))
#define HS_KEYLEN(arr_,i_) (HSE_LEN((arr_)[2*(i_)]))
#define HS_VALLEN(arr_,i_) (HSE_LEN((arr_)[2*(i_)+1]))
#define HS_VALISNULL(arr_,i_) (HSE_ISNULL((arr_)[2*(i_)+1]))
#define HS_VALISSTRING(arr_,i_) (HSE_ISSTRING((arr_)[2*(i_)+1]))
#define HS_VALISARRAY(arr_,i_) (HSE_ISARRAY((arr_)[2*(i_)+1]))
#define HS_VALISHSTORE(arr_,i_) (HSE_ISHSTORE((arr_)[2*(i_)+1]))

/*
 * currently, these following macros are the _only_ places that rely
 * on internal knowledge of HEntry. Everything else should be using
 * the above macros. Exception: the in-place upgrade in hstore_compat.c
 * messes with entries directly.
 */

/*
 * copy one key/value pair (which must be contiguous starting at
 * sptr_) into an under-construction hstore; dent_ is an HEntry*,
 * dbuf_ is the destination's string buffer, dptr_ is the current
 * position in the destination. lots of modification and multiple
 * evaluation here.
 */
#define HS_COPYITEM(dent_,dbuf_,dptr_,sptr_,klen_,vlen_,vnull_)				\
	do {																	\
		memcpy((dptr_), (sptr_), (klen_)+(vlen_));							\
		(dptr_) += (klen_)+(vlen_);											\
		(dent_)++->entry = ((dptr_) - (dbuf_) - (vlen_)) & HENTRY_POSMASK; 	\
		(dent_)++->entry = ((((dptr_) - (dbuf_)) & HENTRY_POSMASK)			\
							 | ((vnull_) ? HENTRY_ISNULL : 0));				\
	} while(0)

/*
 * add one key/item pair, from a Pairs structure, into an
 * under-construction hstore
 */
#define HS_ADDITEM(dent_,dbuf_,dptr_,pair_)											\
	do {																			\
		memcpy((dptr_), (pair_).key, (pair_).keylen);								\
		(dptr_) += (pair_).keylen;													\
		(dent_)++->entry = ((dptr_) - (dbuf_)) & HENTRY_POSMASK;					\
		switch((pair_).valtype) {													\
			case valNull:															\
				(dent_)++->entry = ((((dptr_) - (dbuf_)) & HENTRY_POSMASK)			\
									 | HENTRY_ISNULL);								\
				break;																\
			case valText:															\
				memcpy((dptr_), (pair_).val.text.val, (pair_).val.text.vallen);		\
				(dptr_) += (pair_).val.text.vallen;									\
				(dent_)++->entry = ((dptr_) - (dbuf_)) & HENTRY_POSMASK;			\
				break;																\
			case valFormedArray:													\
				while (INTALIGN((dptr_) - (dbuf_)) != ((dptr_) - (dbuf_))) 			\
				{																	\
					*(dptr_) = '\0';												\
					(dptr_)++;														\
				}																	\
				memcpy((dptr_), (pair_).val.formedArray, (pair_).anyvallen);		\
				(dptr_) += (pair_).anyvallen;										\
				(dent_)++->entry = ((((dptr_) - (dbuf_)) & HENTRY_POSMASK)			\
									 | HENTRY_ISARRAY);								\
				break;																\
			case valFormedHstore:													\
				while (INTALIGN((dptr_) - (dbuf_)) != ((dptr_) - (dbuf_))) 			\
				{																	\
					*(dptr_) = '\0';												\
					(dptr_)++;														\
				}																	\
				memcpy((dptr_), (pair_).val.formedHStore, (pair_).anyvallen);		\
				(dptr_) += (pair_).anyvallen;										\
				(dent_)++->entry = ((((dptr_) - (dbuf_)) & HENTRY_POSMASK)			\
									 | HENTRY_ISHSTORE);							\
				break;																\
			default:																\
				elog(ERROR,"HS_ADDITEM fails for pair type: %d", (pair_).valtype);	\
		}																			\
	} while (0)

/* finalize a newly-constructed hstore */
#define HS_FINALIZE(hsp_,count_,buf_,ptr_)							\
	do {															\
		int buflen = (ptr_) - (buf_);								\
		if ((count_))												\
			ARRPTR(hsp_)[0].entry |= HENTRY_ISFIRST;				\
		if ((count_) != HS_COUNT((hsp_)))							\
		{															\
			HS_SETCOUNT((hsp_),(count_));							\
			memmove(STRPTR(hsp_), (buf_), buflen);					\
		}															\
		SET_VARSIZE((hsp_), CALCDATASIZE((count_), buflen));		\
	} while (0)

/* ensure the varlena size of an existing hstore is correct */
#define HS_FIXSIZE(hsp_,count_)											\
	do {																\
		int bl = (count_) ? HSE_ENDPOS(ARRPTR(hsp_)[2*(count_)-1]) : 0; \
		SET_VARSIZE((hsp_), CALCDATASIZE((count_),bl));					\
	} while (0)

/* DatumGetHStoreP includes support for reading old-format hstore values */
extern HStore *hstoreUpgrade(Datum orig);

#define DatumGetHStoreP(d) hstoreUpgrade(d)

#define PG_GETARG_HS(x) DatumGetHStoreP(PG_GETARG_DATUM(x))


/*
 * Pairs is a "decompressed" representation of one key/value pair.
 * The two strings are not necessarily null-terminated.
 */
typedef struct Pairs
{
	char	   *key;
	size_t		keylen;
	enum		{
		valText 			= 0,
		valArray 			= ~HENTRY_ISARRAY,
		valHstore 			= ~HENTRY_ISHSTORE,
		valNull				= HENTRY_ISNULL,
		valFormedArray 		= HENTRY_ISARRAY,
		valFormedHstore 	= HENTRY_ISHSTORE
	}	valtype;
	union {
		struct {
			char	   		*val;
			size_t			vallen;
		} text;
		struct {
			char			**elems;
			int				*elens;
			int				nelems;
		} array;
		struct {
			struct Pairs* 	pairs;
			int				npairs;
		} hstore;
		ArrayType			*formedArray;
		HStore				*formedHStore;
	} val;
	int32		anyvallen;
	bool		needfree;		/* need to pfree the value? */
} Pairs;

typedef struct HStorePair HStorePair;
typedef struct HStoreValue HStoreValue;

struct HStoreValue {
	enum {
		hsvNullString,
		hsvString,
		hsvArray,
		hsvPairs,
		hsvDumped
	} type;

	uint32		size; /* size of node (including subnodes) */

	union {
		struct {
			uint32		len;
			char 		*val;
		} string;

		struct {
			int			nelems;
			HStoreValue	*elems;
		} array;

		struct {
			int			npairs;
			HStorePair 	*pairs;
		} hstore;

		struct {
			uint32		len;
			char		*data;
		} dump;
	};

}; 

struct HStorePair {
	HStoreValue	key;
	HStoreValue	value;
}; 


extern HStoreValue* parseHStore(const char *str);
extern int	hstoreUniquePairs(Pairs *a, int32 l, int32 *buflen);
extern HStore *hstorePairs(Pairs *pairs, int32 pcount, int32 buflen);

/*
 * hstore support functios
 */

#define WHS_KEY         	(0x001)
#define WHS_VALUE       	(0x002)
#define WHS_ELEM       		(0x004)
#define WHS_BEGIN_ARRAY 	(0x008)
#define WHS_END_ARRAY   	(0x010)
#define WHS_BEGIN_HSTORE    (0x020)
#define WHS_END_HSTORE      (0x040)
#define WHS_BEFORE      	(0x080)
#define WHS_AFTER       	(0x100)

typedef void (*walk_hstore_cb)(void* /*arg*/, HStoreValue* /* value */, 
											uint32 /* flags */, uint32 /* level */);
extern void walkUncompressedHStore(HStoreValue *v, walk_hstore_cb cb, void *cb_arg);

extern void walkCompressedHStore(char *buffer, walk_hstore_cb cb, void *cb_arg);

extern int compareHStoreStringValue(const HStoreValue *va, const HStoreValue *vb);
extern int compareHStorePair(const void *a, const void *b, void *arg);

extern HStoreValue* findUncompressedHStoreValue(char *buffer, uint32 flags, 
												uint32 *lowbound, char *key, uint32 keylen);

extern char*  hstoreToCString(StringInfo str, char *v, int len /* estimation */);

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
			(n) = res + 1 - a;														\
		}																			\
	} while(0)																		

uint32 compressHStore(HStoreValue *v, char *buffer);


typedef struct HStoreIterator
{
	uint32					type;
	uint32					nelems;
	HEntry					*array;
	char					*data;

	int						i;

	enum {
		hsi_start 	= 0x00,
		hsi_key		= 0x01,
		hsi_value	= 0x02,
		hsi_elem	= 0x04
	} state;

	struct HStoreIterator	*next;
} HStoreIterator;

extern 	HStoreIterator*	HStoreIteratorInit(char *buffer);
extern	int /* WHS_* */	HStoreIteratorGet(HStoreIterator **it, HStoreValue *v);

extern size_t hstoreCheckKeyLen(size_t len);
extern size_t hstoreCheckValLen(size_t len);

extern int	hstoreFindKey(HStore *hs, int *lowbound, char *key, int keylen);

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

#endif   /* __HSTORE_H__ */
