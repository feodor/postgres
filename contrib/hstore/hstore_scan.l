/*-------------------------------------------------------------------------
 *
 * hstore_scan.l
 *    Lexer definition for hstore
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 *
 * contrib/hstore/hstore_scan.l
 *
 *-------------------------------------------------------------------------
 */

%{
static string scanstring;

/* No reason to constrain amount of data slurped */
/* #define YY_READ_BUF_SIZE 16777216 */

/* Handles to the buffer that the lexer uses internally */
static YY_BUFFER_STATE scanbufhandle;
static char *scanbuf;
static int	scanbuflen;

static void addstring(bool init, char *s, int l);
static void addchar(bool init, char s);
static int checkSpecialVal(void); /* examine scanstring for the special value */

%}

%option 8bit
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option warn
%option prefix="hstore_yy"
%option bison-bridge

%x xQUOTED
%x xNONQUOTED

any			[^\,\[\]\{\}\"\=\> \t\n\r\f\\\:]


%%

<INITIAL>[\,\{\}\[\]]			{ return *yytext; }

<INITIAL>\=\>					{ 
									if (inputJSON)
										elog(ERROR, "syntax error");
									return DELIMITER_P; 
								}

<INITIAL>\:						{ 
									if (inputJSON)
									{
										return DELIMITER_P;
									}
									else
									{
										addchar(true, ':');
										BEGIN xNONQUOTED;
									}
								}

<INITIAL>[ \t\n\r\f]+			{ /* ignore */ }

<INITIAL>\=/[^\>]				{
									addchar(true, '=');
									BEGIN xNONQUOTED;
								}
									
<INITIAL>\>						{
									addchar(true, yytext[0]);
									BEGIN xNONQUOTED;
								}
<INITIAL>\\.					{
									addchar(true, yytext[1]);
									BEGIN xNONQUOTED;
								}

<INITIAL>({any}|\>)+			{
									addstring(true, yytext, yyleng);
									BEGIN xNONQUOTED;
								}
									
<INITIAL>\" 					{
									addchar(true, '\0');
									BEGIN xQUOTED;
								}

<INITIAL>\=						{	/* =<<EOF>> */
									addchar(true, '=');
									yylval->str = scanstring;
									if (inputJSON)
										elog(ERROR, "syntax error");
									return STRING_P;
								}

<xNONQUOTED>({any}|[\>\"\:])+	{ 
									addstring(false, yytext, yyleng); 
								}

<xNONQUOTED>\=/[^\>]			{ addchar(false, *yytext); }

<xNONQUOTED>[ \t\n\r\f]+		{ 
									yylval->str = scanstring;
									BEGIN INITIAL;
									return checkSpecialVal();
								}

<xNONQUOTED>\=					{	/* =<<EOF>> */
									addchar(false, '=');
									yylval->str = scanstring;
									BEGIN INITIAL;
									if (inputJSON)
										elog(ERROR, "syntax error");
									return STRING_P;
								}

<xNONQUOTED>[\,\{\}\[\]]		{
									yylval->str = scanstring;
									yyless(0);
									BEGIN INITIAL;
									return checkSpecialVal();
								}

<xNONQUOTED><<EOF>>				{ 
									yylval->str = scanstring;
									BEGIN INITIAL;
									return checkSpecialVal();
								}

<xNONQUOTED>\=\>				{
									yylval->str = scanstring;
									yyless(0);
									BEGIN INITIAL;
									return checkSpecialVal();
								}
									

<xNONQUOTED,xQUOTED>\\.  		{ addchar(false, yytext[1]); }

<INITIAL,xNONQUOTED,xQUOTED>\\ 	{ yyerror("Unexpected end after backslesh"); }

<xQUOTED><<EOF>>				{ yyerror("Unexpected end of quoted string"); }

<xQUOTED>\"						{
									yylval->str = scanstring;
									BEGIN INITIAL;
									return STRING_P;
								}

<xQUOTED>[^\\\"]+   			{ addstring(false, yytext, yyleng); }

<INITIAL><<EOF>>				{ yyterminate(); }

%%

void
yyerror(const char *message)
{
	if (*yytext == YY_END_OF_BUFFER_CHAR)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("bad %s representation", inputJSON ? "json" : "hstore"),
				 /* translator: %s is typically "syntax error" */
				 errdetail("%s at end of input", message)));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("bad %s representation", inputJSON ? "json" : "hstore"),
				 /* translator: first %s is typically "syntax error" */
				 errdetail("%s at or near \"%s\"", message, yytext)));
	}
}

static int
checkSpecialVal()
{
	int res = STRING_P;

	if (stringIsNumber(scanstring.val, scanstring.len, inputJSON))
	{
		/* for numeric_in() call we need to make a correct C-string */
		addchar(false, '\0');
		res = NUMERIC_P;
	}
	else if (scanstring.len == 1)
	{
		if (*scanstring.val == 't')
			res = TRUE_P;
		else if (*scanstring.val == 'f')
			res = FALSE_P;
	}
	else if (scanstring.len == 4)
	{
		if (pg_strncasecmp("null", scanstring.val, scanstring.len) == 0)
			res = NULL_P;
		else if (pg_strncasecmp("true", scanstring.val, scanstring.len) == 0)
			res = TRUE_P;
	}
	else if (scanstring.len == 5)
	{
		if (pg_strncasecmp("false", scanstring.val, scanstring.len) == 0)
			res = FALSE_P;
	}

	if (inputJSON && res == STRING_P)
		elog(ERROR, "Syntax error");

	return res;
}
/*
 * Called before any actual parsing is done
 */
static void
hstore_scanner_init(const char *str, int slen)
{
	if (slen <= 0)
		slen = strlen(str);

	/*
	 * Might be left over after ereport()
	 */
	if (YY_CURRENT_BUFFER)
		yy_delete_buffer(YY_CURRENT_BUFFER);

	/*
	 * Make a scan buffer with special termination needed by flex.
	 */

	scanbuflen = slen;
	scanbuf = palloc(slen + 2);
	memcpy(scanbuf, str, slen);
	scanbuf[slen] = scanbuf[slen + 1] = YY_END_OF_BUFFER_CHAR;
	scanbufhandle = yy_scan_buffer(scanbuf, slen + 2);

	BEGIN(INITIAL);
}


/*
 * Called after parsing is done to clean up after hstore_scanner_init()
 */
static void
hstore_scanner_finish(void)
{
	yy_delete_buffer(scanbufhandle);
	pfree(scanbuf);
}

static void
addstring(bool init, char *s, int l) {
	if (init) {
		scanstring.total = 32;
		scanstring.val = palloc(scanstring.total);
		scanstring.len = 0;
	}

	if (s && l) {
		while(scanstring.len + l + 1 >= scanstring.total) {
			scanstring.total *= 2;
			scanstring.val = repalloc(scanstring.val, scanstring.total);
		}

		memcpy(scanstring.val+scanstring.len, s, l);
		scanstring.len+=l;
	}
}

static void
addchar(bool init, char s) {
	if (init)
	{
		scanstring.total = 32;
		scanstring.val = palloc(scanstring.total);
		scanstring.len = 0;
	}
	else if(scanstring.len + 1 >= scanstring.total)
	{
		scanstring.total*=2;
		scanstring.val=repalloc(scanstring.val, scanstring.total);
	}

	scanstring.val[ scanstring.len ] = s;
	if (s != '\0')
		scanstring.len++;
}

HStoreValue* 
parseHStore(const char *str, int len, bool json) {
	HStoreValue		*parseresult;

	inputJSON = json;

	hstore_scanner_init(str, len);

	if (hstore_yyparse((void*)&parseresult) != 0)
		hstore_yyerror("bugus input");

	hstore_scanner_finish();

	return parseresult;
}

