%{
#define YYPARSE_PARAM result  /* need this to pass a pointer (void *) to yyparse */

#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"
#include "hstore.h"

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.  Note this only works with
 * bison >= 2.0.  However, in bison 1.875 the default is to use alloca()
 * if possible, so there's not really much problem anyhow, at least if
 * you're building with gcc.
 */
#define YYMALLOC palloc
#define YYFREE   pfree

/* Avoid exit() on fatal scanner errors (a bit ugly -- see yy_fatal_error) */
#undef fprintf
#define fprintf(file, fmt, msg)  fprintf_to_ereport(fmt, msg)

static void
fprintf_to_ereport(const char *fmt, const char *msg)
{
	ereport(ERROR, (errmsg_internal("%s", msg)));
}

/* struct string is shared between scan and gram */
typedef struct string {
	char 	*val;
	int  	len;
	int		total;
} string;
#include <hstore_gram.h>

/* flex 2.5.4 doesn't bother with a decl for this */
int hstore_yylex(YYSTYPE * yylval_param);
int hstore_yyparse(void *result);
void hstore_yyerror(const char *message);

static HStoreValue*
makeHStoreValueString(HStoreValue* v, string *s)
{
	if (v == NULL)
		v = palloc(sizeof(*v));

	if (s == NULL)
	{
		v->type = hsvNullString;
		v->size = sizeof(HEntry);
	}
	else if (s->len > HENTRY_POSMASK)
	{
		elog(ERROR, "string is too long");
	}
	else
	{
		v->type = hsvString;
		v->string.val = s->val;
		v->string.len = s->len;
		v->size = sizeof(HEntry) + s->len;

	}

	return v;
}

static HStoreValue*
makeHStoreValueArray(List *list)
{
	HStoreValue	*v = palloc(sizeof(*v));

	v->type = hsvArray;
	v->array.nelems = list_length(list);
	v->size = sizeof(uint32) /* header */ + sizeof(HEntry) /* parent's entry */ + sizeof(HEntry) - 1 /*alignment*/;

	if (v->array.nelems > 0)
	{
		ListCell	*cell;
		int			i = 0;

		v->array.elems = palloc(sizeof(HStoreValue) * v->array.nelems);

		foreach(cell, list)
		{
			HStoreValue	*s = (HStoreValue*)lfirst(cell);

			v->size += s->size; 

			v->array.elems[i++] = *s;

			if (v->size > HENTRY_POSMASK)
				elog(ERROR, "array is too long");
		}
	}
	else
	{
		v->array.elems = NULL;
	}

	return v;
}

static HStoreValue*
makeHStoreValuePairs(List *list)
{
	HStoreValue	*v = palloc(sizeof(*v));

	v->type = hsvPairs;
	v->hstore.npairs = list_length(list);
	v->size = sizeof(uint32) /* header */ + sizeof(HEntry) /* parent's entry */ + sizeof(HEntry) - 1 /*alignment*/;

	if (v->hstore.npairs > 0)
	{
		ListCell	*cell;
		int			i = 0;

		v->hstore.pairs = palloc(sizeof(HStorePair) * v->hstore.npairs);

		foreach(cell, list)
		{
			HStorePair	*s = (HStorePair*)lfirst(cell);

			v->size += s->key.size + s->value.size; 
			v->hstore.pairs[i++] = *s;

			if (v->size > HENTRY_POSMASK)
				elog(ERROR, "hstore is too long");
		}

		ORDER_PAIRS(v->hstore.pairs, v->hstore.npairs, v->size -= ptr->key.size + ptr->value.size);
	}
	else
	{
		v->hstore.pairs = NULL;
	}

	return v;
}

static HStorePair*
makeHStorePair(string *key, HStoreValue *value) {
	HStorePair	*v = palloc(sizeof(*v));

	makeHStoreValueString(&v->key, key);
	v->value = *value;

	return v;
}

/*
 * See comments below, in result product
 */
static HStoreValue*
makeHStoreValueFinalArray(List *l)
{
	HStoreValue	*v = NULL;

	if (list_length(l) == 1)
	{
		v = ((HStoreValue*)linitial(l));

		if (v->type == hsvString)
			v = makeHStoreValueArray(l);
		else if (v->type == hsvNullString)
			v = NULL;
	}
	else if (list_length(l) > 1)
	{
		v = makeHStoreValueArray(l);
	}

	return v;
}
%}

/* BISON Declarations */
%pure-parser
%expect 0
%name-prefix="hstore_yy"
%error-verbose

%union {
	string 			str;
	List			*elems; 		/* list of HStoreValue */
	List			*pairs; 		/* list of HStorePair */

	HStoreValue		*hvalue;
	HStorePair		*pair;
}

%token	<str>			DELIMITER_P NULL_P STRING_P

%type	<hvalue>		result hstore value 
%type	<str>			key

%type	<pair>			pair

%type	<elems>			value_list
%type 	<pairs>			pair_list

/* Grammar follows */
%%

result: 
	pair_list						{ *((HStoreValue**)result) = makeHStoreValuePairs($1); }
	/* 
	 * hstore product produces reduce/reduce conflict althought we would like to make 'a,b'::hstore and
	 * '{a,b}'::hstore etc the same. In other words it's desirable to make outer {} braces optional.
	 * To make it we just remove root array if it contains only one non-scalar element, see 
	 * makeHStoreValueFinalArray() definition. 
	 */
	| value_list					{ *((HStoreValue**)result) = makeHStoreValueFinalArray($1); }
	| /* EMPTY */					{ *((HStoreValue**)result) = NULL; }
	;

hstore:
	'{' pair_list '}'				{ $$ = makeHStoreValuePairs($2); }
	| '{' value_list '}'			{ $$ = makeHStoreValueArray($2); }
	| '[' value_list ']'			{ $$ = makeHStoreValueArray($2); }
	| '{' '}'						{ $$ = makeHStoreValueString(NULL, NULL); }
	| '[' ']'						{ $$ = makeHStoreValueString(NULL, NULL); }
	;

value:
	NULL_P							{ $$ = makeHStoreValueString(NULL, NULL); }
	| STRING_P						{ $$ = makeHStoreValueString(NULL, &$1); }
	| hstore						{ $$ = $1; } 
	;

value_list:
	value							{ $$ = lappend(NIL, $1); } 
	| value_list ',' value			{ $$ = lappend($1, $3); } 
	;

key:
	STRING_P						{ $$ = $1; }
	| NULL_P						{ $$ = $1; }
	;

pair:
	key DELIMITER_P value			{ $$ = makeHStorePair(&$1, $3); }
	;

pair_list:
	pair							{ $$ = lappend(NIL, $1); }
	| pair_list ',' pair			{ $$ = lappend($1, $3); }
	;

%%

#include "hstore_scan.c"
