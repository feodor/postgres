/* contrib/hstore/hstore--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION hstore UPDATE TO '1.2'" to load this file. \quit

CREATE FUNCTION fetchval_numeric(hstore,text)
RETURNS numeric
AS 'MODULE_PATHNAME','hstore_fetchval_numeric'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR ^> (
	LEFTARG = hstore,
	RIGHTARG = text,
	PROCEDURE = fetchval_numeric
);

CREATE FUNCTION fetchval_boolean(hstore,text)
RETURNS boolean
AS 'MODULE_PATHNAME','hstore_fetchval_boolean'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR ?> (
	LEFTARG = hstore,
	RIGHTARG = text,
	PROCEDURE = fetchval_boolean
);

CREATE FUNCTION fetchval(hstore,int)
RETURNS text
AS 'MODULE_PATHNAME','hstore_fetchval_n'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR -> (
	LEFTARG = hstore,
	RIGHTARG = int,
	PROCEDURE = fetchval
);

CREATE FUNCTION fetchval_numeric(hstore,int)
RETURNS numeric
AS 'MODULE_PATHNAME','hstore_fetchval_n_numeric'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR ^> (
	LEFTARG = hstore,
	RIGHTARG = int,
	PROCEDURE = fetchval_numeric
);

CREATE FUNCTION fetchval_boolean(hstore,int)
RETURNS boolean
AS 'MODULE_PATHNAME','hstore_fetchval_n_boolean'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR ?> (
	LEFTARG = hstore,
	RIGHTARG = int,
	PROCEDURE = fetchval_boolean
);

CREATE FUNCTION fetchval(hstore,text[])
RETURNS text
AS 'MODULE_PATHNAME','hstore_fetchval_path'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR #> (
	LEFTARG = hstore,
	RIGHTARG = text[],
	PROCEDURE = fetchval
);

CREATE FUNCTION fetchval_numeric(hstore,text[])
RETURNS numeric
AS 'MODULE_PATHNAME','hstore_fetchval_path_numeric'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR #^> (
	LEFTARG = hstore,
	RIGHTARG = text[],
	PROCEDURE = fetchval_numeric
);

CREATE FUNCTION fetchval_boolean(hstore,text[])
RETURNS boolean
AS 'MODULE_PATHNAME','hstore_fetchval_path_boolean'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR #?> (
	LEFTARG = hstore,
	RIGHTARG = text[],
	PROCEDURE = fetchval_boolean
);

CREATE FUNCTION fetchval_hstore(hstore,text)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_fetchval_hstore'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR %> (
	LEFTARG = hstore,
	RIGHTARG = text,
	PROCEDURE = fetchval_hstore
);

CREATE FUNCTION fetchval_hstore(hstore,int)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_fetchval_n_hstore'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR %> (
	LEFTARG = hstore,
	RIGHTARG = int,
	PROCEDURE = fetchval_hstore
);

CREATE FUNCTION fetchval_hstore(hstore,text[])
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_fetchval_path_hstore'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR #%> (
	LEFTARG = hstore,
	RIGHTARG = text[],
	PROCEDURE = fetchval_hstore
);

CREATE FUNCTION json_to_hstore(json)
RETURNS hstore
AS 'MODULE_PATHNAME','json_to_hstore'
LANGUAGE C STRICT IMMUTABLE;

CREATE CAST (json AS hstore)
WITH FUNCTION json_to_hstore(json);

CREATE FUNCTION isexists(hstore,int)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_exists_idx'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION exist(hstore,int)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_exists_idx'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR ? (
	LEFTARG = hstore,
	RIGHTARG = int,
	PROCEDURE = exist,
	RESTRICT = contsel,
	JOIN = contjoinsel
);

CREATE FUNCTION isexists(hstore,text[])
RETURNS bool
AS 'MODULE_PATHNAME','hstore_exists_path'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION exist(hstore,text[])
RETURNS bool
AS 'MODULE_PATHNAME','hstore_exists_path'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR #? (
	LEFTARG = hstore,
	RIGHTARG = text[],
	PROCEDURE = exist,
	RESTRICT = contsel,
	JOIN = contjoinsel
);

CREATE FUNCTION delete_path(hstore,text[])
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_delete_path'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR #- (
	LEFTARG = hstore,
	RIGHTARG = text[],
	PROCEDURE = delete_path
);

CREATE FUNCTION delete(hstore,int)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_delete_idx'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR - (
	LEFTARG = hstore,
	RIGHTARG = int,
	PROCEDURE = delete
);

CREATE FUNCTION replace(hstore,text[],hstore)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_replace'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION svals(hstore, text[])
RETURNS setof text
AS 'MODULE_PATHNAME','hstore_svals_path'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hvals(hstore)
RETURNS setof hstore
AS 'MODULE_PATHNAME','hstore_hvals'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hvals(hstore, text[])
RETURNS setof hstore
AS 'MODULE_PATHNAME','hstore_hvals_path'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION concat_path(hstore,text[],hstore)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_deep_concat'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION each_hstore(IN hs hstore,
	OUT key text,
	OUT value hstore)
RETURNS SETOF record
AS 'MODULE_PATHNAME','hstore_each_hstore'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hstore_typeof(hstore)
RETURNS text 
AS 'MODULE_PATHNAME','hstore_typeof'
LANGUAGE C STRICT IMMUTABLE;
