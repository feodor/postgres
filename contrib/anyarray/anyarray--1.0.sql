/* contrib/anyarray/anyarray--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION anyarray" to load this file. \quit

--
-- Create the user-defined type for the 1-D integer arrays (_int4)
--

CREATE FUNCTION anyset(anyelement)
	RETURNS anyarray
	AS 'MODULE_PATHNAME', 'aa_set'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION icount(anyarray)
	RETURNS int4
	AS 'MODULE_PATHNAME', 'aa_icount'
	LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR # (
	RIGHTARG = anyarray,
	PROCEDURE = icount
);

CREATE FUNCTION sort(anyarray)
	RETURNS anyarray
	AS 'MODULE_PATHNAME', 'aa_sort'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION sort(anyarray, text)
	RETURNS anyarray
	AS 'MODULE_PATHNAME', 'aa_sort'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION sort_asc(anyarray)
	RETURNS anyarray
	AS 'MODULE_PATHNAME', 'aa_sort_asc'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION sort_desc(anyarray)
	RETURNS anyarray
	AS 'MODULE_PATHNAME', 'aa_sort_desc'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION uniq(anyarray)
	RETURNS anyarray
	AS 'MODULE_PATHNAME', 'aa_uniq'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION uniq_d(anyarray)
	RETURNS anyarray
	AS 'MODULE_PATHNAME', 'aa_uniqd'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION idx(anyarray, anyelement)
	RETURNS int4
	AS 'MODULE_PATHNAME', 'aa_idx'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION subarray(anyarray, int4)
	RETURNS anyarray
	AS 'MODULE_PATHNAME', 'aa_subarray'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION subarray(anyarray, int4, int4)
	RETURNS anyarray
	AS 'MODULE_PATHNAME', 'aa_subarray'
	LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR + (
	LEFTARG = anyarray,
	RIGHTARG = anyelement,
	PROCEDURE = array_append
);

CREATE OPERATOR + (
	LEFTARG = anyarray,
	RIGHTARG = anyarray,
	PROCEDURE = array_cat
);

CREATE OPERATOR - (
	LEFTARG = anyarray,
	RIGHTARG = anyelement,
	PROCEDURE = array_remove
);

CREATE FUNCTION subtract_array(anyarray, anyarray)
	RETURNS anyarray
	AS 'MODULE_PATHNAME', 'aa_subtract_array'
	LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR - (
	LEFTARG = anyarray,
	RIGHTARG = anyarray,
	PROCEDURE = subtract_array
);

CREATE FUNCTION union_elem(anyarray, anyelement)
	RETURNS anyarray
	AS 'MODULE_PATHNAME', 'aa_union_elem'
	LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR | (
	LEFTARG = anyarray,
	RIGHTARG = anyelement,
	PROCEDURE = union_elem
);

CREATE FUNCTION union_array(anyarray, anyarray)
	RETURNS anyarray
	AS 'MODULE_PATHNAME', 'aa_union_array'
	LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR | (
	LEFTARG = anyarray,
	RIGHTARG = anyarray,
	PROCEDURE = union_array
);

CREATE FUNCTION intersect_array(anyarray, anyarray)
	RETURNS anyarray
	AS 'MODULE_PATHNAME', 'aa_intersect_array'
	LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR & (
	LEFTARG = anyarray,
	RIGHTARG = anyarray,
	PROCEDURE = intersect_array
);

CREATE FUNCTION similarity(anyarray, anyarray)
	RETURNS float4
	AS 'MODULE_PATHNAME', 'aa_similarity'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION similarity_op(anyarray, anyarray)
	RETURNS bool
	AS 'MODULE_PATHNAME', 'aa_similarity_op'
	LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR % (
	LEFTARG = anyarray,
	RIGHTARG = anyarray,
	PROCEDURE = similarity_op,
	COMMUTATOR = '%',
	RESTRICT = contsel,
	JOIN = contjoinsel
);

--GiST interface

CREATE FUNCTION ganyarrayin(cstring)
	RETURNS ganyarray
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION ganyarrayout(ganyarray)
	RETURNS cstring
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE TYPE ganyarray (
	INTERNALLENGTH = -1,
	INPUT = ganyarrayin,
	OUTPUT = ganyarrayout
);

CREATE FUNCTION ganyarray_consistent(internal,internal,int,oid,internal)
	RETURNS bool
	AS 'MODULE_PATHNAME'
	LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ganyarray_compress(internal)
	RETURNS internal
	AS 'MODULE_PATHNAME'
	LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ganyarray_decompress(internal)
	RETURNS internal
	AS 'MODULE_PATHNAME'
	LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ganyarray_penalty(internal,internal,internal)
	RETURNS internal
	AS 'MODULE_PATHNAME'
	LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ganyarray_picksplit(internal, internal)
	RETURNS internal
	AS 'MODULE_PATHNAME'
	LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ganyarray_union(bytea, internal)
	RETURNS _int4
	AS 'MODULE_PATHNAME'
	LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ganyarray_same(ganyarray, ganyarray, internal)
	RETURNS internal
	AS 'MODULE_PATHNAME'
	LANGUAGE C IMMUTABLE STRICT;


--create the operator classes for gist

CREATE OPERATOR CLASS _int2_aa_ops
FOR TYPE _int2 USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _int4_aa_ops
FOR TYPE _int4 USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _int8_aa_ops
FOR TYPE _int8 USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _float4_aa_ops
FOR TYPE _float4 USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _float8_aa_ops
FOR TYPE _float8 USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _oid_aa_ops
FOR TYPE _oid USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _timestamp_aa_ops
FOR TYPE _timestamp USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _timestamptz_aa_ops
FOR TYPE _timestamptz USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _time_aa_ops
FOR TYPE _time USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _timetz_aa_ops
FOR TYPE _timetz USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _date_aa_ops
FOR TYPE _date USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _interval_aa_ops
FOR TYPE _interval USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _macaddr_aa_ops
FOR TYPE _macaddr USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _inet_aa_ops
FOR TYPE _inet USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _cidr_aa_ops
FOR TYPE _cidr USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _text_aa_ops
FOR TYPE _text USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _varchar_aa_ops
FOR TYPE _varchar USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _char_aa_ops
FOR TYPE _char USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _bytea_aa_ops
FOR TYPE _bytea USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

CREATE OPERATOR CLASS _numeric_aa_ops
FOR TYPE _numeric USING gist
AS
	OPERATOR	3	&&	(anyarray, anyarray),
	OPERATOR	6	=	(anyarray, anyarray),
	OPERATOR	7	@>	(anyarray, anyarray),
	OPERATOR	8	<@	(anyarray, anyarray),
	OPERATOR	16	%	(anyarray, anyarray),
	FUNCTION	1	ganyarray_consistent(internal,internal,int,oid,internal),
	FUNCTION	2	ganyarray_union (bytea, internal),
	FUNCTION	3	ganyarray_compress (internal),
	FUNCTION	4	ganyarray_decompress (internal),
	FUNCTION	5	ganyarray_penalty (internal, internal, internal),
	FUNCTION	6	ganyarray_picksplit (internal, internal),
	FUNCTION	7	ganyarray_same (ganyarray, ganyarray, internal),
STORAGE         ganyarray;

