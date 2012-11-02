%{
#define YYPARSE_PARAM result  /* need this to pass a pointer (void *) to yyparse */

#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"
#include "hstore.h"

#include <hstore_gram.h>
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

/* flex 2.5.4 doesn't bother with a decl for this */
int hstore_yylex(YYSTYPE * yylval_param);
int hstore_yyparse(void *result);
void hstore_yyerror(const char *message);

/* Avoid exit() on fatal scanner errors (a bit ugly -- see yy_fatal_error) */
#undef fprintf
#define fprintf(file, fmt, msg)  ereport(ERROR, (errmsg_internal("%s", msg)))

%}

/* BISON Declarations */
%pure-parser
%expect 0
%name-prefix="hstore_yy"
%error-verbose

%union {
	struct {
		char *val;
		int  len;
	} str;

	Pairs	*kv_pair;
	List	*list_kv_pairs; /* list of kv_pair pair */
}

%token	<str>			DELIMITER_P NULL_P STRING_P

%type	<list_kv_pairs>	hstore list_pairs
%type	<kv_pair>		pair
%type	<str>			key

/* Grammar follows */
%%

hstore: 
	list_pairs		{ *((List**)result) = $1; }
	| /* EMPTY */	{ *((List**)result) = NIL; }
	;

list_pairs:
	pair						{ $$ = lappend(NIL, $1); }
	| list_pairs ',' pair		{ $$ = lappend($1, $3); }
	;

pair:
	key DELIMITER_P STRING_P		{
			$$ = palloc(sizeof(*$$));
			$$->key = $1.val;
			$$->keylen = $1.len;
			$$->val = $3.val;
			$$->vallen = $3.len;
			$$->isnull = false;
			$$->needfree = true;
		}
	| key DELIMITER_P NULL_P		{
			$$ = palloc(sizeof(*$$));
			$$->key = $1.val;
			$$->keylen = $1.len;
			$$->val = NULL;
			$$->vallen = 0;
			$$->isnull = true;
			$$->needfree = true;
		}
	;

key:
	STRING_P					{ $$ = $1; }
	| NULL_P					{ $$ = $1; }
	;

%%


#include "hstore_scan.c"
