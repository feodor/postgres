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

CREATE FUNCTION hstore(text,bool)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_from_bool'
LANGUAGE C IMMUTABLE; -- not STRICT; needs to allow (key,NULL)

CREATE FUNCTION hstore(text,numeric)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_from_numeric'
LANGUAGE C IMMUTABLE; -- not STRICT; needs to allow (key,NULL)

CREATE FUNCTION hstore(text)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_scalar_from_text'
LANGUAGE C IMMUTABLE; -- not STRICT; needs to allow (key,NULL)

CREATE FUNCTION hstore(bool)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_scalar_from_bool'
LANGUAGE C IMMUTABLE; -- not STRICT; needs to allow (key,NULL)

CREATE FUNCTION hstore(numeric)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_scalar_from_numeric'
LANGUAGE C IMMUTABLE; -- not STRICT; needs to allow (key,NULL)


-- GIN support: hash based opclass

FUNCTION gin_extract_hstore_hash(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gin_extract_hstore_hash_query(internal, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gin_consistent_hstore_hash(internal, int2, internal, int4, internal, internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR CLASS gin_hstore_hash_ops
FOR TYPE hstore USING gin
AS
	OPERATOR        7       @>,
	FUNCTION        1       btint4cmp(int4,int4),
	FUNCTION        2       gin_extract_hstore_hash(internal, internal),
	FUNCTION        3       gin_extract_hstore_hash_query(internal, internal, int2, internal, internal),
	FUNCTION        4       gin_consistent_hstore_hash(internal, int2, internal, int4, internal, internal),
STORAGE         int4;

CREATE FUNCTION array_to_hstore(anyarray)
RETURNS hstore
AS 'MODULE_PATHNAME','array_to_hstore'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hstore_pretty_print()
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION hstore_array_curly_braces()
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION hstore_json()
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION hstore_root_hash_decorated()
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION hstore_loose()
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION hstore_print(hstore,
							 pretty_print bool DEFAULT false,
							 array_curly_braces bool DEFAULT false,
							 root_hash_decorated bool DEFAULT false,
							 json bool DEFAULT false,
							 loose bool DEFAULT false)
RETURNS text
AS 'MODULE_PATHNAME', 'hstore_print'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION hstore2jsonb(hstore)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'hstore2jsonb'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (hstore AS jsonb)
  WITH FUNCTION hstore2jsonb(hstore);

CREATE FUNCTION jsonb2hstore(jsonb)
RETURNS hstore
AS 'MODULE_PATHNAME', 'jsonb2hstore'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (jsonb AS hstore)
  WITH FUNCTION jsonb2hstore(jsonb);

