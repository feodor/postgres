/*-------------------------------------------------------------------------
 *
 * jsonb_gram.y
 *    Grammar definition for jsonb
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 *
 * src/backend/utils/adt/jsonb_gram.y
 *
 *-------------------------------------------------------------------------
 */

%{
#define YYPARSE_PARAM result  /* need this to pass a pointer (void *) to yyparse */

#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"

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

static bool inputJSON = false;

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
#include <jsonb_gram.h>

/* flex 2.5.4 doesn't bother with a decl for this */
int jsonb_yylex(YYSTYPE * yylval_param);
int jsonb_yyparse(void *result);
void jsonb_yyerror(const char *message);

static JsonbValue*
makeJsonbValueString(JsonbValue* v, string *s)
{
	if (v == NULL)
		v = palloc(sizeof(*v));

	if (s == NULL)
	{
		v->type = jbvNull;
		v->size = sizeof(JEntry);
	}
	else if (s->len > JENTRY_POSMASK)
	{
		elog(ERROR, "string is too long");
	}
	else
	{
		v->type = jbvString;
		v->string.val = s->val;
		v->string.len = s->len;
		v->size = sizeof(JEntry) + s->len;

	}

	return v;
}

static JsonbValue*
makeJsonbValueNumeric(string *s)
{
	Numeric 	n = NULL;
	JsonbValue	*v;

	PG_TRY();
	{
		n = DatumGetNumeric(DirectFunctionCall3(numeric_in, CStringGetDatum(s->val), 0, -1));
	}
	PG_CATCH();
	{
		n = NULL;
	}
	PG_END_TRY();

	if (n != NULL)
	{
		v = palloc(sizeof(*v));
		v->type = jbvNumeric;
		v->numeric = n;
		v->size = 2*sizeof(JEntry) + VARSIZE_ANY(n);
	}
	else
	{
		v = makeJsonbValueString(NULL, s);
	}

	return v;
}

static JsonbValue*
makeJsonbValueBool(bool val) {
	JsonbValue *v = palloc(sizeof(*v));

	v->type = jbvBool;
	v->boolean = val;
	v->size = sizeof(JEntry);

	return v;
}

static JsonbValue*
makeJsonbValueArray(List *list)
{
	JsonbValue	*v = palloc(sizeof(*v));

	v->type = jbvArray;
	v->array.scalar = false;
	v->array.nelems = list_length(list);
	v->size = sizeof(uint32) /* header */ + sizeof(JEntry) /* parent's entry */ + sizeof(JEntry) - 1 /*alignment*/;

	if (v->array.nelems > 0)
	{
		ListCell	*cell;
		int			i = 0;

		v->array.elems = palloc(sizeof(JsonbValue) * v->array.nelems);

		foreach(cell, list)
		{
			JsonbValue	*s = (JsonbValue*)lfirst(cell);

			v->size += s->size; 

			v->array.elems[i++] = *s;

			if (v->size > JENTRY_POSMASK)
				elog(ERROR, "array is too long");
		}
	}
	else
	{
		v->array.elems = NULL;
	}

	return v;
}

static JsonbValue*
makeJsonbValuePairs(List *list)
{
	JsonbValue	*v = palloc(sizeof(*v));

	v->type = jbvHash;
	v->hash.npairs = list_length(list);
	v->size = sizeof(uint32) /* header */ + sizeof(JEntry) /* parent's entry */ + sizeof(JEntry) - 1 /*alignment*/;

	if (v->hash.npairs > 0)
	{
		ListCell	*cell;
		int			i = 0;

		v->hash.pairs = palloc(sizeof(JsonbPair) * v->hash.npairs);

		foreach(cell, list)
		{
			JsonbPair	*s = (JsonbPair*)lfirst(cell);

			v->size += s->key.size + s->value.size; 
			v->hash.pairs[i].order = i;
			v->hash.pairs[i++] = *s;

			if (v->size > JENTRY_POSMASK)
				elog(ERROR, "%s is too long", inputJSON ? "jsonb" : "hstore");
		}

		ORDER_PAIRS(v->hash.pairs, v->hash.npairs, v->size -= ptr->key.size + ptr->value.size);
	}
	else
	{
		v->hash.pairs = NULL;
	}

	return v;
}

static JsonbPair*
makeJsonbPair(string *key, JsonbValue *value) {
	JsonbPair	*v = palloc(sizeof(*v));

	makeJsonbValueString(&v->key, key);
	v->value = *value;

	return v;
}

%}

/* BISON Declarations */
%pure-parser
%expect 0
%name-prefix="jsonb_yy"
%error-verbose

%union {
	string 			str;
	Numeric			numeric;
	List			*elems; 		/* list of JsonbValue */
	List			*pairs; 		/* list of JsonbPair */

	JsonbValue		*hvalue;
	JsonbPair		*pair;
}

%token	<str>			DELIMITER_P NULL_P STRING_P TRUE_P FALSE_P
						NUMERIC_P

%type	<hvalue>		result jsonb value scalar_value 
%type	<str>			key

%type	<pair>			pair

%type	<elems>			value_list
%type 	<pairs>			pair_list

/* Grammar follows */
%%

result: 
	pair_list						{ 
										if (inputJSON)
											elog(ERROR, "Wrong jsonb representation");
										 *((JsonbValue**)result) = makeJsonbValuePairs($1);
									}
	| jsonb							{ 	
										if ($1->type == jbvNull)
											*((JsonbValue**)result) = NULL;
										else
											*((JsonbValue**)result) = $1;
									}
	| scalar_value					{ 
										*((JsonbValue**)result) = makeJsonbValueArray(lappend(NIL, $1));
										(*((JsonbValue**)result))->array.scalar = true;
									}
	| /* EMPTY */					{ *((JsonbValue**)result) = NULL; }
	;

jsonb:
	'{' pair_list '}'				{ $$ = makeJsonbValuePairs($2); }
	| '[' value_list ']'			{ $$ = makeJsonbValueArray($2); }
	| '[' value ']'					{ $$ = makeJsonbValueArray(lappend(NIL, $2)); }
	| '{' value_list '}'			{ 
										if (inputJSON)
											elog(ERROR, "Wrong jsonb representation");
										$$ = makeJsonbValueArray($2); 
									}
	| '{' value '}'					{ 
										if (inputJSON)
											elog(ERROR, "Wrong jsonb representation");
										$$ = makeJsonbValueArray(lappend(NIL, $2)); 
									}
	| '{' '}'						{ $$ = makeJsonbValueString(NULL, NULL); }
	| '[' ']'						{ $$ = makeJsonbValueString(NULL, NULL); }
	;

scalar_value:
	NULL_P							{ $$ = makeJsonbValueString(NULL, NULL); }
	| STRING_P						{ $$ = makeJsonbValueString(NULL, &$1); }
	| TRUE_P						{ $$ = makeJsonbValueBool(true); }
	| FALSE_P						{ $$ = makeJsonbValueBool(false); }
	| NUMERIC_P						{ $$ = makeJsonbValueNumeric(&$1); }
	;

value:
	scalar_value					{ $$ = $1; }
	| jsonb							{ $$ = $1; } 
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
	key DELIMITER_P value			{ $$ = makeJsonbPair(&$1, $3); }
	;

pair_list:
	pair							{ $$ = lappend(NIL, $1); }
	| pair_list ',' pair			{ $$ = lappend($1, $3); }
	;

%%

#include "jsonb_scan.c"
