/*
 * contrib/hstore/hstore.h
 */
#ifndef __HSTORE_H__
#define __HSTORE_H__

#include "utils/jsonb.h"

/*
 * HEntry: there is one of these for each key _and_ value in an hstore
 *
 * the position offset points to the _end_ so that we can get the length
 * by subtraction from the previous entry.	the ISFIRST flag lets us tell
 * whether there is a previous entry.
 */

typedef JEntry HEntry;

#define HENTRY_ISFIRST		JENTRY_ISFIRST
#define HENTRY_ISSTRING 	JENTRY_ISSTRING
#define HENTRY_ISNUMERIC	JENTRY_ISNUMERIC
#define HENTRY_ISNEST		JENTRY_ISNEST
#define HENTRY_ISNULL		JENTRY_ISNULL
#define HENTRY_ISBOOL		JENTRY_ISBOOL
#define HENTRY_ISFALSE		JENTRY_ISFALSE
#define HENTRY_ISTRUE		JENTRY_ISTRUE

/* HENTRY_ISHASH, HENTRY_ISARRAY and HENTRY_ISCALAR is only used in send/recv */
#define HENTRY_ISHASH		JENTRY_ISOBJECT
#define HENTRY_ISARRAY		JENTRY_ISARRAY
#define HENTRY_ISCALAR		JENTRY_ISCALAR

#define HENTRY_POSMASK 	JENTRY_POSMASK
#define HENTRY_TYPEMASK	JENTRY_TYPEMASK

#define HSE_ISFIRST(he_) 		JBE_ISFIRST(he_)
#define HSE_ISSTRING(he_)		JBE_ISSTRING(he_)
#define HSE_ISNUMERIC(he_) 		JBE_ISNUMERIC(he_)
#define HSE_ISNEST(he_) 		JBE_ISNEST(he_)
#define HSE_ISNULL(he_) 		JBE_ISNULL(he_)
#define HSE_ISBOOL(he_) 		JBE_ISBOOL(he_)
#define HSE_ISBOOL_TRUE(he_) 	JBE_ISBOOL_TRUE(he_)
#define HSE_ISBOOL_FALSE(he_) 	JBE_ISBOOL_FALSE(he_)

#define HSE_ENDPOS(he_) 		JBE_ENDPOS(he_)
#define HSE_OFF(he_) 			JBE_OFF(he_)
#define HSE_LEN(he_) 			JBE_LEN(he_)

/*
 * determined by the size of "endpos" (ie HENTRY_POSMASK)
 */
#define HSTORE_MAX_KEY_LEN 		HENTRY_POSMASK
#define HSTORE_MAX_VALUE_LEN 	HENTRY_POSMASK

typedef Jsonb HStore;

/*
 * it's not possible to get more than 2^28 items into an hstore,
 * so we reserve the top few bits of the size field. See hstore_compat.c
 * for one reason why.	Some bits are left for future use here.
 */
#define HS_FLAG_NEWVERSION 		0x80000000
#define HS_FLAG_ARRAY			JB_FLAG_ARRAY
#define HS_FLAG_HASH			JB_FLAG_OBJECT
#define HS_FLAG_SCALAR			JB_FLAG_SCALAR

#define HS_COUNT_MASK			0x0FFFFFFF

#define HS_ISEMPTY(hsp_)		JB_ISEMPTY(hsp_)
#define HS_ROOT_COUNT(hsp_) 	JB_ROOT_COUNT(hsp_)
#define HS_ROOT_IS_HASH(hsp_) 	JB_ROOT_IS_OBJECT(hsp_)
#define HS_ROOT_IS_ARRAY(hsp_) 	JB_ROOT_IS_ARRAY(hsp_)
#define HS_ROOT_IS_SCALAR(hsp_) JB_ROOT_IS_SCALAR(hsp_)

/* DatumGetHStoreP includes support for reading old-format hstore values */
extern HStore *hstoreUpgrade(Datum orig);

#define DatumGetHStoreP(d) hstoreUpgrade(d)

#define PG_GETARG_HS(x) DatumGetHStoreP(PG_GETARG_DATUM(x))

typedef JsonbPair HStorePair;
typedef JsonbValue HStoreValue;

/* JsonbValue.type renaming */
#define hsvNull		jbvNull
#define hsvString	jbvString
#define hsvNumeric	jbvNumeric
#define hsvBool		jbvBool
#define hsvArray	jbvArray
#define hsvHash		jbvHash
#define hsvBinary	jbvBinary

/*
 * hstore support functions, they are mostly the same as jsonb
 */

#define WHS_KEY         	WJB_KEY
#define WHS_VALUE       	WJB_VALUE
#define WHS_ELEM       		WJB_ELEM
#define WHS_BEGIN_ARRAY 	WJB_BEGIN_ARRAY
#define WHS_END_ARRAY   	WJB_END_ARRAY
#define WHS_BEGIN_HASH	    WJB_BEGIN_OBJECT
#define WHS_END_HASH        WJB_END_OBJECT

#define walkUncompressedHStore(v, cb, cb_arg)		walkUncompressedJsonb((v), (cb), (cb_arg))
#define compareHStoreStringValue(a, b, arg)			compareJsonbStringValue((a), (b), (arg))
#define compareHStorePair(a, b, arg)				compareJsonbPair((a), (b), (arg))

#define compareHStoreBinaryValue(a, b)				compareJsonbBinaryValue((a), (b))
#define compareHStoreValue(a, b)					compareJsonbValue((a), (b))

#define findUncompressedHStoreValueByValue(buffer, flags, lowbound, key)	\
	findUncompressedJsonbValueByValue((buffer), (flags), (lowbound), (key))
#define findUncompressedHStoreValue(buffer, flags, lowbound, key, keylen)	\
	findUncompressedJsonbValue((buffer), (flags), (lowbound), (key), (keylen))

#define getHStoreValue(buffer, flags, i)			getJsonbValue((buffer), (flags), (i))

typedef ToJsonbState ToHStoreState;
#define pushHStoreValue(state, r /* WHS_* */, v)	pushJsonbValue((state), (r), (v))

extern bool stringIsNumber(char *string, int len, bool jsonNumber);

extern uint32 compressHStore(HStoreValue *v, char *buffer);

typedef JsonbIterator HStoreIterator;

#define	HStoreIteratorInit(buffer)					JsonbIteratorInit(buffer)

#define HStoreIteratorGet(it, v, skipNested)	JsonbIteratorGet((it), (v), (skipNested))

text* HStoreValueToText(HStoreValue *v);

extern HStoreValue* parseHStore(const char *str, int len, bool json);

#define uniqueHStoreValue(v) uniqueJsonbValue(v)

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
