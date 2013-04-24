/* contrib/hstore/hstore--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION hstore UPDATE TO '1.2'" to load this file. \quit

CREATE FUNCTION fetchval(hstore,int)
RETURNS text
AS 'MODULE_PATHNAME','hstore_fetchval_n'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR -> (
	LEFTARG = hstore,
	RIGHTARG = int,
	PROCEDURE = fetchval
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

CREATE OPERATOR ? (
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

CREATE OPERATOR / (
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

