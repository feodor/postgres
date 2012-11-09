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
#define fprintf(file, fmt, msg)  ereport(ERROR, (errmsg_internal("%s", msg)))

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

static string *
makeCopyString(string *s) {
	string *d = palloc(sizeof(*d));

	*d = *s;

	return d;
}

static char**
convertListToStrings(List *inlist, int **elens, int *nelems) {
	char	**result = NULL;

	*nelems = list_length(inlist);

	if (*nelems > 0)
	{
		int			i = 0;
		ListCell	*cell;

		result = palloc(sizeof(*result) * *nelems);
		*elens = palloc(sizeof(**elens) * *nelems);

		foreach(cell, inlist) {
			string	*s = (string*)lfirst(cell);

			if (s)
			{
				result[i] = s->val;
				(*elens)[i] = s->len;
			}
			else
			{
				result[i] = NULL;
				(*elens)[i] = 0;
			}
			i++;
		}
	}
	else
	{
		*elens = NULL;
	}

	return result;
}

static Pairs*
convertListToPairs(List *inlist, int *npairs) {
    Pairs       *result = NULL;

	*npairs = list_length(inlist);

	if (*npairs > 0) 
	{
		int         i = 0;
		ListCell    *cell;

		result = palloc(sizeof(*result) * *npairs);

		foreach(cell, inlist)
			result[i++] = *(Pairs*)lfirst(cell);
	}

	return result;
}

%}

/* BISON Declarations */
%pure-parser
%expect 0
%name-prefix="hstore_yy"
%error-verbose

%union {
	string 	str;
	List	*strs; 			/* list of string */

	Pairs	*kv_pair;
	List	*list_kv_pairs; /* list of kv_pair pair */
}

%token	<str>			DELIMITER_P NULL_P STRING_P

%type	<list_kv_pairs>	hstore list_pairs
%type	<kv_pair>		pair
%type	<str>			key
%type	<strs>			array

/* Grammar follows */
%%

hstore: 
	list_pairs						{ *((List**)result) = $1; }
	| /* EMPTY */					{ *((List**)result) = NIL; }
	;

list_pairs:
	pair							{ $$ = lappend(NIL, $1); }
	| list_pairs ',' pair			{ $$ = lappend($1, $3); }
	;

array:
	NULL_P							{ $$ = lappend(NIL, NULL); }
	| STRING_P						{ $$ = lappend(NIL, makeCopyString(&$1)); }
	| array ',' NULL_P				{ $$ = lappend($1, NULL); }
	| array ',' STRING_P			{ $$ = lappend($1, makeCopyString(&$3)); }
	;

pair:
	key DELIMITER_P STRING_P		{
			$$ = palloc(sizeof(*$$));
			$$->key = $1.val;
			$$->keylen = $1.len;
			$$->valtype = valText;
			$$->val.text.val = $3.val;
			$$->val.text.vallen = $3.len;
			$$->needfree = true;
		}
	| key DELIMITER_P NULL_P		{
			$$ = palloc(sizeof(*$$));
			$$->key = $1.val;
			$$->keylen = $1.len;
			$$->valtype = valNull;
			$$->needfree = true;
		}
	| key DELIMITER_P '{' list_pairs '}'		{
			$$ = palloc(sizeof(*$$));
			$$->key = $1.val;
			$$->keylen = $1.len;
			$$->valtype = valHstore;
			$$->val.hstore.pairs =  convertListToPairs($4, &$$->val.hstore.npaires);
			$$->needfree = true;
		}
	| key DELIMITER_P '{' array '}'		{
			$$ = palloc(sizeof(*$$));
			$$->key = $1.val;
			$$->keylen = $1.len;
			$$->valtype = valArray;
			$$->val.array.elems = convertListToStrings($4, &$$->val.array.elens, &$$->val.array.nelems);
			$$->needfree = true;
		}
	;

key:
	STRING_P					{ $$ = $1; }
	| NULL_P					{ $$ = $1; }
	;

%%

#include "hstore_scan.c"
