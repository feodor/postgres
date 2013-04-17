/* contrib/hstore/hstore--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION hstore UPDATE TO '1.2'" to load this file. \quit

CREATE FUNCTION fetchval_hstore(hstore,text)
RETURNS hstore
AS 'MODULE_PATHNAME','hstore_fetchval_hstore'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR %> (
	LEFTARG = hstore,
	RIGHTARG = text,
	PROCEDURE = fetchval_hstore
);

