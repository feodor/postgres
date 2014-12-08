CREATE EXTENSION anyarray;

--sanity check
SELECT 
	opc.opcname, 
	t.typname, 
	opc.opcdefault  
FROM 
	pg_opclass opc, 
	pg_am am, 
	pg_type t, 
	pg_type k 
WHERE 
	opc.opcmethod = am.oid AND 
	am.amname='gist' AND 
	opc.opcintype = t.oid AND 
	opc.opckeytype = k.oid AND 
	k.typname='ganyarray'
ORDER BY opc.opcname;

SELECT 
	opc.opcname, 
	t.typname, 
	opc.opcdefault  
FROM 
	pg_opclass opc, 
	pg_am am, 
	pg_type t
WHERE 
	opc.opcmethod = am.oid AND 
	am.amname='gin' AND 
	opc.opcintype = t.oid AND 
	opc.opcname ~ '_aa_ops$'
ORDER BY opc.opcname;

SELECT 
    trim( leading '_'  from t.typname ) || '[]' AS "Array Type",
    gin.opcname AS "GIN operator class",
    gist.opcname AS "GiST operator class"
FROM
    (
        SELECT *
        FROM
            pg_catalog.pg_opclass,
            pg_catalog.pg_am
        WHERE
            pg_opclass.opcmethod = pg_am.oid AND
            pg_am.amname = 'gin' AND
            pg_opclass.opcname ~ '_aa_ops$'
    ) AS gin
    FULL OUTER JOIN
        (
            SELECT *
            FROM
                pg_catalog.pg_opclass,
                pg_catalog.pg_am
            WHERE
                pg_opclass.opcmethod = pg_am.oid AND
                pg_am.amname = 'gist' AND
                pg_opclass.opcname ~ '_aa_ops$'
        ) AS gist
        ON (
            gist.opcname = gin.opcname AND 
            gist.opcintype = gin.opcintype 
        ),
    pg_catalog.pg_type t
WHERE
    t.oid = COALESCE(gist.opcintype, gin.opcintype) AND
    t.typarray = 0
ORDER BY
    "Array Type" ASC 
;

--testing function
CREATE OR REPLACE FUNCTION epoch2timestamp(int8)
RETURNS timestamp AS $$
    SELECT ('1970-01-01 00:00:00'::timestamp +   ( ($1*3600*24 + $1) ::text || ' seconds' )::interval)::timestamp;
	$$ LANGUAGE SQL RETURNS NULL ON NULL INPUT IMMUTABLE;

CREATE OR REPLACE FUNCTION to_tsp_array(_int8)
RETURNS _timestamp AS $$
	SELECT ARRAY( 
		SELECT 
			epoch2timestamp( $1[n] )
		FROM
			generate_series( 1, array_upper( $1, 1) - array_lower( $1, 1 ) + 1 ) AS n
	);
	$$ LANGUAGE SQL RETURNS NULL ON NULL INPUT IMMUTABLE;

CREATE OR REPLACE FUNCTION to_macp_array(_int8)
RETURNS _macaddr AS $$
	SELECT ARRAY( 
		SELECT 
			('01:01:01:01:01:' || to_hex($1[n] % 256))::macaddr	
		FROM
			generate_series( 1, array_upper( $1, 1) - array_lower( $1, 1 ) + 1 ) AS n
	);
	$$ LANGUAGE SQL RETURNS NULL ON NULL INPUT IMMUTABLE;

CREATE OR REPLACE FUNCTION to_inetp_array(_int8)
RETURNS _inet AS $$
	SELECT ARRAY( 
		SELECT 
			('192.168.1.' || ($1[n] % 256))::inet	
		FROM
			generate_series( 1, array_upper( $1, 1) - array_lower( $1, 1 ) + 1 ) AS n
	);
	$$ LANGUAGE SQL RETURNS NULL ON NULL INPUT IMMUTABLE;

CREATE OR REPLACE FUNCTION array_to_col(anyarray)
RETURNS SETOF anyelement AS
$$
    SELECT $1[n] FROM generate_series( 1, array_upper( $1, 1) - array_lower( $1, 1 ) + 1 ) AS n;
$$ LANGUAGE SQL RETURNS NULL ON NULL INPUT IMMUTABLE;

CREATE OR REPLACE FUNCTION max(int8, int8)
RETURNS int8 AS
$$
	SELECT CASE WHEN $1 > $2 THEN $1 ELSE $2 END;
$$ LANGUAGE SQL RETURNS NULL ON NULL INPUT IMMUTABLE;
