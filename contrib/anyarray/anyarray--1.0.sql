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

