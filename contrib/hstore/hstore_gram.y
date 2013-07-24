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
		v->type = hsvNull;
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
makeHStoreValueNumeric(HStoreValue* v, string *s)
{
	Numeric n = NULL;

	PG_TRY();
	{
		n = DatumGetNumeric(DirectFunctionCall3(numeric_in, CStringGetDatum(s->val), 0, -1));
	}
	PG_CATCH();
	{
		n = NULL;
	}
	PG_END_TRY();

	v = makeHStoreValueString(v, s);

	if (/* XXX */ 0 && n != NULL)
	{
		v->type = hsvNumeric;
		v->numeric = n;
		v->size = sizeof(HEntry) + VARSIZE_ANY(n);
	}

	return v;
}

static HStoreValue*
makeHStoreValueBool(bool val) {
	HStoreValue *v = palloc(sizeof(*v));

	v->type = hsvBool;
	v->boolean = val;
	v->size = sizeof(HEntry);

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

	v->type = hsvHash;
	v->hash.npairs = list_length(list);
	v->size = sizeof(uint32) /* header */ + sizeof(HEntry) /* parent's entry */ + sizeof(HEntry) - 1 /*alignment*/;

	if (v->hash.npairs > 0)
	{
		ListCell	*cell;
		int			i = 0;

		v->hash.pairs = palloc(sizeof(HStorePair) * v->hash.npairs);

		foreach(cell, list)
		{
			HStorePair	*s = (HStorePair*)lfirst(cell);

			v->size += s->key.size + s->value.size; 
			v->hash.pairs[i].order = i;
			v->hash.pairs[i++] = *s;

			if (v->size > HENTRY_POSMASK)
				elog(ERROR, "hstore is too long");
		}

		ORDER_PAIRS(v->hash.pairs, v->hash.npairs, v->size -= ptr->key.size + ptr->value.size);
	}
	else
	{
		v->hash.pairs = NULL;
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

%}

/* BISON Declarations */
%pure-parser
%expect 0
%name-prefix="hstore_yy"
%error-verbose

%union {
	string 			str;
	Numeric			numeric;
	List			*elems; 		/* list of HStoreValue */
	List			*pairs; 		/* list of HStorePair */

	HStoreValue		*hvalue;
	HStorePair		*pair;
}

%token	<str>			DELIMITER_P NULL_P STRING_P TRUE_P FALSE_P
						NUMERIC_P

%type	<hvalue>		result hstore value 
%type	<str>			key

%type	<pair>			pair

%type	<elems>			value_list
%type 	<pairs>			pair_list

/* Grammar follows */
%%

result: 
	pair_list						{ *((HStoreValue**)result) = makeHStoreValuePairs($1); }
	| value_list					{ *((HStoreValue**)result) = makeHStoreValueArray($1); }
	| value							{ 	
										if ($1->type == hsvString || $1->type == hsvBool || $1->type == hsvNumeric)
											*((HStoreValue**)result) = makeHStoreValueArray(lappend(NIL, $1));
										else if ($1->type == hsvNull)
											*((HStoreValue**)result) = NULL;
										else
											*((HStoreValue**)result) = $1;
									}
	| /* EMPTY */					{ *((HStoreValue**)result) = NULL; }
	;

hstore:
	'{' pair_list '}'				{ $$ = makeHStoreValuePairs($2); }
	| '{' value_list '}'			{ $$ = makeHStoreValueArray($2); }
	| '[' value_list ']'			{ $$ = makeHStoreValueArray($2); }
	| '{' value '}'					{ $$ = makeHStoreValueArray(lappend(NIL, $2)); }
	| '[' value ']'					{ $$ = makeHStoreValueArray(lappend(NIL, $2)); }
	| '{' '}'						{ $$ = makeHStoreValueString(NULL, NULL); }
	| '[' ']'						{ $$ = makeHStoreValueString(NULL, NULL); }
	;

value:
	NULL_P							{ $$ = makeHStoreValueString(NULL, NULL); }
	| STRING_P						{ $$ = makeHStoreValueString(NULL, &$1); }
	/* XXX | TRUE_P						{ $$ = makeHStoreValueBool(true); }
	| FALSE_P						{ $$ = makeHStoreValueBool(false); } */
	| TRUE_P						{ $$ = makeHStoreValueString(NULL, &$1); }
	| FALSE_P						{ $$ = makeHStoreValueString(NULL, &$1); }
	| NUMERIC_P						{ $$ = makeHStoreValueNumeric(NULL, &$1); }
	| hstore						{ $$ = $1; } 
	;

value_list:
	value ',' value					{ $$ = lappend(lappend(NIL, $1), $3); } 
	| value_list ',' value			{ $$ = lappend($1, $3); } 
	;

/*
 * key is always a string, not a bool or numeric
 */
key:
	STRING_P						{ $$ = $1; }
	| TRUE_P						{ $$ = $1; }
	| FALSE_P						{ $$ = $1; }
	| NUMERIC_P						{ $$ = $1; }
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
