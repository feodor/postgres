/* contrib/hstore/hstore--1.3.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION hstore" to load this file. \quit

CREATE TYPE hstore;

CREATE FUNCTION hstore_in(cstring)
RETURNS hstore
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hstore_out(hstore)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hstore_recv(internal)
RETURNS hstore
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hstore_send(hstore)
RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE TYPE hstore (
        INTERNALLENGTH = -1,
        INPUT = hstore_in,
        OUTPUT = hstore_out,
        RECEIVE = hstore_recv,
        SEND = hstore_send,
        STORAGE = extended
);

CREATE FUNCTION hstore_version_diag(hstore)
RETURNS integer
AS 'MODULE_PATHNAME','hstore_version_diag'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION fetchval(hstore,text)
RETURNS text
AS 'MODULE_PATHNAME','hstore_fetchval'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR -> (
	LEFTARG = hstore,
	RIGHTARG = text,
	PROCEDURE = fetchval
);

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

CREATE FUNCTION slice_array(hstore,text[])
RETURNS text[]
AS 'MODULE_PATHNAME','hstore_slice_to_array'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR -> (
	LEFTARG = hstore,
	RIGHTARG = text[],
	PROCEDURE = slice_array
);

CREATE FUNCTION slice(hstore,text[])
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_slice_to_hstore'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION isexists(hstore,text)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_exists'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION exist(hstore,text)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_exists'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR ? (
	LEFTARG = hstore,
	RIGHTARG = text,
	PROCEDURE = exist,
	RESTRICT = contsel,
	JOIN = contjoinsel
);

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

CREATE FUNCTION exists_any(hstore,text[])
RETURNS bool
AS 'MODULE_PATHNAME','hstore_exists_any'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR ?| (
	LEFTARG = hstore,
	RIGHTARG = text[],
	PROCEDURE = exists_any,
	RESTRICT = contsel,
	JOIN = contjoinsel
);

CREATE FUNCTION exists_all(hstore,text[])
RETURNS bool
AS 'MODULE_PATHNAME','hstore_exists_all'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR ?& (
	LEFTARG = hstore,
	RIGHTARG = text[],
	PROCEDURE = exists_all,
	RESTRICT = contsel,
	JOIN = contjoinsel
);

CREATE FUNCTION isdefined(hstore,text)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_defined'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION defined(hstore,text)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_defined'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION delete(hstore,text)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_delete'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION delete(hstore,int)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_delete_idx'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION delete(hstore,text[])
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_delete_array'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION delete(hstore,hstore)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_delete_hstore'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION delete_path(hstore,text[])
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_delete_path'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR - (
	LEFTARG = hstore,
	RIGHTARG = text,
	PROCEDURE = delete
);

CREATE OPERATOR - (
	LEFTARG = hstore,
	RIGHTARG = int,
	PROCEDURE = delete
);

CREATE OPERATOR - (
	LEFTARG = hstore,
	RIGHTARG = text[],
	PROCEDURE = delete
);

CREATE OPERATOR - (
	LEFTARG = hstore,
	RIGHTARG = hstore,
	PROCEDURE = delete
);

CREATE OPERATOR #- (
	LEFTARG = hstore,
	RIGHTARG = text[],
	PROCEDURE = delete_path
);

CREATE FUNCTION replace(hstore,text[],hstore)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_replace'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hs_concat(hstore,hstore)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_concat'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR || (
	LEFTARG = hstore,
	RIGHTARG = hstore,
	PROCEDURE = hs_concat
);

CREATE FUNCTION concat_path(hstore,text[],hstore)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_deep_concat'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hs_contains(hstore,hstore)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_contains'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hs_contained(hstore,hstore)
RETURNS bool
AS 'MODULE_PATHNAME','hstore_contained'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR @> (
	LEFTARG = hstore,
	RIGHTARG = hstore,
	PROCEDURE = hs_contains,
	COMMUTATOR = '<@',
	RESTRICT = contsel,
	JOIN = contjoinsel
);

CREATE OPERATOR <@ (
	LEFTARG = hstore,
	RIGHTARG = hstore,
	PROCEDURE = hs_contained,
	COMMUTATOR = '@>',
	RESTRICT = contsel,
	JOIN = contjoinsel
);

-- obsolete:
CREATE OPERATOR @ (
	LEFTARG = hstore,
	RIGHTARG = hstore,
	PROCEDURE = hs_contains,
	COMMUTATOR = '~',
	RESTRICT = contsel,
	JOIN = contjoinsel
);

CREATE OPERATOR ~ (
	LEFTARG = hstore,
	RIGHTARG = hstore,
	PROCEDURE = hs_contained,
	COMMUTATOR = '@',
	RESTRICT = contsel,
	JOIN = contjoinsel
);

CREATE FUNCTION tconvert(text,text)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_from_text'
LANGUAGE C IMMUTABLE; -- not STRICT; needs to allow (key,NULL)

CREATE FUNCTION hstore(text,text)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_from_text'
LANGUAGE C IMMUTABLE; -- not STRICT; needs to allow (key,NULL)

CREATE FUNCTION hstore(text,bool)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_from_bool'
LANGUAGE C IMMUTABLE; -- not STRICT; needs to allow (key,NULL)

CREATE FUNCTION hstore(text,numeric)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_from_numeric'
LANGUAGE C IMMUTABLE; -- not STRICT; needs to allow (key,NULL)

CREATE FUNCTION hstore(text,hstore)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_from_th'
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

CREATE FUNCTION hstore(text[],text[])
RETURNS hstore
AS 'MODULE_PATHNAME', 'hstore_from_arrays'
LANGUAGE C IMMUTABLE; -- not STRICT; allows (keys,null)

CREATE FUNCTION hstore(text[])
RETURNS hstore
AS 'MODULE_PATHNAME', 'hstore_from_array'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (text[] AS hstore)
  WITH FUNCTION hstore(text[]);

CREATE FUNCTION hstore_to_json(hstore)
RETURNS json
AS 'MODULE_PATHNAME', 'hstore_to_json'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (hstore AS json)
  WITH FUNCTION hstore_to_json(hstore);

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

CREATE FUNCTION hstore_to_json_loose(hstore)
RETURNS json
AS 'MODULE_PATHNAME', 'hstore_to_json_loose'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION hstore(record)
RETURNS hstore
AS 'MODULE_PATHNAME', 'hstore_from_record'
LANGUAGE C IMMUTABLE; -- not STRICT; allows (null::recordtype)

CREATE FUNCTION hstore_to_array(hstore)
RETURNS text[]
AS 'MODULE_PATHNAME','hstore_to_array'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR %% (
       RIGHTARG = hstore,
       PROCEDURE = hstore_to_array
);

CREATE FUNCTION hstore_to_matrix(hstore)
RETURNS text[]
AS 'MODULE_PATHNAME','hstore_to_matrix'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR %# (
       RIGHTARG = hstore,
       PROCEDURE = hstore_to_matrix
);

CREATE FUNCTION akeys(hstore)
RETURNS text[]
AS 'MODULE_PATHNAME','hstore_akeys'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION avals(hstore)
RETURNS text[]
AS 'MODULE_PATHNAME','hstore_avals'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION skeys(hstore)
RETURNS setof text
AS 'MODULE_PATHNAME','hstore_skeys'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION svals(hstore)
RETURNS setof text
AS 'MODULE_PATHNAME','hstore_svals'
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

CREATE FUNCTION each(IN hs hstore,
    OUT key text,
    OUT value text)
RETURNS SETOF record
AS 'MODULE_PATHNAME','hstore_each'
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

CREATE FUNCTION populate_record(anyelement,hstore)
RETURNS anyelement
AS 'MODULE_PATHNAME', 'hstore_populate_record'
LANGUAGE C IMMUTABLE; -- not STRICT; allows (null::rectype,hstore)

CREATE OPERATOR #= (
	LEFTARG = anyelement,
	RIGHTARG = hstore,
	PROCEDURE = populate_record
);

CREATE FUNCTION json_to_hstore(json)
RETURNS hstore
AS 'MODULE_PATHNAME','json_to_hstore'
LANGUAGE C STRICT IMMUTABLE;

CREATE CAST (json AS hstore)
WITH FUNCTION json_to_hstore(json);

CREATE FUNCTION array_to_hstore(anyarray)
RETURNS hstore
AS 'MODULE_PATHNAME','array_to_hstore'
LANGUAGE C STRICT IMMUTABLE;

-- btree support

CREATE FUNCTION hstore_eq(hstore,hstore)
RETURNS boolean
AS 'MODULE_PATHNAME','hstore_eq'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hstore_ne(hstore,hstore)
RETURNS boolean
AS 'MODULE_PATHNAME','hstore_ne'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hstore_gt(hstore,hstore)
RETURNS boolean
AS 'MODULE_PATHNAME','hstore_gt'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hstore_ge(hstore,hstore)
RETURNS boolean
AS 'MODULE_PATHNAME','hstore_ge'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hstore_lt(hstore,hstore)
RETURNS boolean
AS 'MODULE_PATHNAME','hstore_lt'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hstore_le(hstore,hstore)
RETURNS boolean
AS 'MODULE_PATHNAME','hstore_le'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION hstore_cmp(hstore,hstore)
RETURNS integer
AS 'MODULE_PATHNAME','hstore_cmp'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR = (
       LEFTARG = hstore,
       RIGHTARG = hstore,
       PROCEDURE = hstore_eq,
       COMMUTATOR = =,
       NEGATOR = <>,
       RESTRICT = eqsel,
       JOIN = eqjoinsel,
       MERGES,
       HASHES
);
CREATE OPERATOR <> (
       LEFTARG = hstore,
       RIGHTARG = hstore,
       PROCEDURE = hstore_ne,
       COMMUTATOR = <>,
       NEGATOR = =,
       RESTRICT = neqsel,
       JOIN = neqjoinsel
);

-- the comparison operators have funky names (and are undocumented)
-- in an attempt to discourage anyone from actually using them. they
-- only exist to support the btree opclass

CREATE OPERATOR #<# (
       LEFTARG = hstore,
       RIGHTARG = hstore,
       PROCEDURE = hstore_lt,
       COMMUTATOR = #>#,
       NEGATOR = #>=#,
       RESTRICT = scalarltsel,
       JOIN = scalarltjoinsel
);
CREATE OPERATOR #<=# (
       LEFTARG = hstore,
       RIGHTARG = hstore,
       PROCEDURE = hstore_le,
       COMMUTATOR = #>=#,
       NEGATOR = #>#,
       RESTRICT = scalarltsel,
       JOIN = scalarltjoinsel
);
CREATE OPERATOR #># (
       LEFTARG = hstore,
       RIGHTARG = hstore,
       PROCEDURE = hstore_gt,
       COMMUTATOR = #<#,
       NEGATOR = #<=#,
       RESTRICT = scalargtsel,
       JOIN = scalargtjoinsel
);
CREATE OPERATOR #>=# (
       LEFTARG = hstore,
       RIGHTARG = hstore,
       PROCEDURE = hstore_ge,
       COMMUTATOR = #<=#,
       NEGATOR = #<#,
       RESTRICT = scalargtsel,
       JOIN = scalargtjoinsel
);

CREATE OPERATOR CLASS btree_hstore_ops
DEFAULT FOR TYPE hstore USING btree
AS
	OPERATOR	1	#<# ,
	OPERATOR	2	#<=# ,
	OPERATOR	3	= ,
	OPERATOR	4	#>=# ,
	OPERATOR	5	#># ,
	FUNCTION	1	hstore_cmp(hstore,hstore);

-- hash support

CREATE FUNCTION hstore_hash(hstore)
RETURNS integer
AS 'MODULE_PATHNAME','hstore_hash'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR CLASS hash_hstore_ops
DEFAULT FOR TYPE hstore USING hash
AS
	OPERATOR	1	= ,
	FUNCTION	1	hstore_hash(hstore);

-- GiST support

CREATE TYPE ghstore;

CREATE FUNCTION ghstore_in(cstring)
RETURNS ghstore
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION ghstore_out(ghstore)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE TYPE ghstore (
        INTERNALLENGTH = -1,
        INPUT = ghstore_in,
        OUTPUT = ghstore_out
);

CREATE FUNCTION ghstore_compress(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ghstore_decompress(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ghstore_penalty(internal,internal,internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ghstore_picksplit(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ghstore_union(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ghstore_same(internal, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ghstore_consistent(internal,internal,int,oid,internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR CLASS gist_hstore_ops
DEFAULT FOR TYPE hstore USING gist
AS
	OPERATOR        7       @> ,
	OPERATOR        9       ?(hstore,text) ,
	OPERATOR        10      ?|(hstore,text[]) ,
	OPERATOR        11      ?&(hstore,text[]) ,
        --OPERATOR        8       <@ ,
        OPERATOR        13      @ ,
        --OPERATOR        14      ~ ,
        FUNCTION        1       ghstore_consistent (internal, internal, int, oid, internal),
        FUNCTION        2       ghstore_union (internal, internal),
        FUNCTION        3       ghstore_compress (internal),
        FUNCTION        4       ghstore_decompress (internal),
        FUNCTION        5       ghstore_penalty (internal, internal, internal),
        FUNCTION        6       ghstore_picksplit (internal, internal),
        FUNCTION        7       ghstore_same (internal, internal, internal),
        STORAGE         ghstore;

-- GIN support: default opclass

CREATE FUNCTION gin_extract_hstore(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gin_extract_hstore_query(internal, internal, int2, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gin_consistent_hstore(internal, int2, internal, int4, internal, internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR CLASS gin_hstore_ops
DEFAULT FOR TYPE hstore USING gin
AS
	OPERATOR        7       @>,
	OPERATOR        9       ?(hstore,text),
	OPERATOR        10      ?|(hstore,text[]),
	OPERATOR        11      ?&(hstore,text[]),
	FUNCTION        1       bttextcmp(text,text),
	FUNCTION        2       gin_extract_hstore(internal, internal),
	FUNCTION        3       gin_extract_hstore_query(internal, internal, int2, internal, internal),
	FUNCTION        4       gin_consistent_hstore(internal, int2, internal, int4, internal, internal),
	STORAGE         text;

-- GIN support: hash based opclass

CREATE FUNCTION gin_extract_hstore_hash(internal, internal)
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

-- output

CREATE FUNCTION hstore_print(hstore, 
							 pretty_print bool DEFAULT false,
							 array_curly_braces bool DEFAULT false,
							 root_hash_decorated bool DEFAULT false,
							 json bool DEFAULT false,
							 loose bool DEFAULT false)
RETURNS text
AS 'MODULE_PATHNAME', 'hstore_print'
LANGUAGE C IMMUTABLE STRICT;



