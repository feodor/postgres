SELECT
    t,
	ARRAY(
		SELECT
			v::varchar(1)::"char"
		FROM
			generate_series(max(0, t - 10),  t) as v
	) AS v
	INTO test_char
FROM
	generate_series(1, 200) as t;


SET anyarray.similarity_type=cosine;
SELECT  t, similarity(v, '{1,9,8,7,6,5,4,3,2,1}') AS s FROM test_char 
	WHERE v % '{1,9,8,7,6,5,4,3,2,1}' ORDER BY s DESC, t;
SELECT  t, similarity(v, '{5,49,8,7,6,5,4,3,2,1}') AS s FROM test_char 
	WHERE v % '{5,49,8,7,6,5,4,3,2,1}' ORDER BY s DESC, t;

SET anyarray.similarity_type=jaccard;
SELECT  t, similarity(v, '{1,9,8,7,6,5,4,3,2,1}') AS s FROM test_char 
	WHERE v % '{1,9,8,7,6,5,4,3,2,1}' ORDER BY s DESC, t;
SELECT  t, similarity(v, '{5,49,8,7,6,5,4,3,2,1}') AS s FROM test_char 
	WHERE v % '{5,49,8,7,6,5,4,3,2,1}' ORDER BY s DESC, t;

SELECT t, v FROM test_char WHERE v && '{4,5}' ORDER BY t;
SELECT t, v FROM test_char WHERE v @> '{4,5}' ORDER BY t;
SELECT t, v FROM test_char WHERE v <@ '{4,5}' ORDER BY t;
SELECT t, v FROM test_char WHERE v =  '{0,1,2,3,4,5,6,7,8,9,1}' ORDER BY t;
SET anyarray.similarity_type=cosine;
SELECT t, v FROM test_char WHERE v %  '{0,1,2,3,4,5,6,7,8,9,1}' ORDER BY t;
SET anyarray.similarity_type=jaccard;
SELECT t, v FROM test_char WHERE v %  '{0,1,2,3,4,5,6,7,8,9,1}' ORDER BY t;
SET anyarray.similarity_type=overlap;
SET anyarray.similarity_threshold = 3;
SELECT t, v FROM test_char WHERE v %  '{0,1,2,3,4,5,6,7,8,9,1}' ORDER BY t;
RESET anyarray.similarity_threshold;

CREATE INDEX idx_test_char ON test_char USING gist (v _char_aa_ops);

SET enable_seqscan=off;

EXPLAIN (COSTS OFF) SELECT t, v FROM test_char WHERE v && '{4,5}' ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_char WHERE v @> '{4,5}' ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_char WHERE v <@ '{4,5}' ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_char WHERE v =  '{0,1,2,3,4,5,6,7,8,9,1}' ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_char WHERE v %  '{0,1,2,3,4,5,6,7,8,9,1}' ORDER BY t;

SELECT t, v FROM test_char WHERE v && '{4,5}' ORDER BY t;
SELECT t, v FROM test_char WHERE v @> '{4,5}' ORDER BY t;
SELECT t, v FROM test_char WHERE v <@ '{4,5}' ORDER BY t;
SELECT t, v FROM test_char WHERE v =  '{0,1,2,3,4,5,6,7,8,9,1}' ORDER BY t;
SET anyarray.similarity_type=cosine;
SELECT t, v FROM test_char WHERE v %  '{0,1,2,3,4,5,6,7,8,9,1}' ORDER BY t;
SET anyarray.similarity_type=jaccard;
SELECT t, v FROM test_char WHERE v %  '{0,1,2,3,4,5,6,7,8,9,1}' ORDER BY t;
SET anyarray.similarity_type=overlap;
SET anyarray.similarity_threshold = 3;
SELECT t, v FROM test_char WHERE v %  '{0,1,2,3,4,5,6,7,8,9,1}' ORDER BY t;
RESET anyarray.similarity_threshold;

DROP INDEX idx_test_char;
CREATE INDEX idx_test_char ON test_char USING gin (v _char_aa_ops);

SET enable_seqscan=off;

EXPLAIN (COSTS OFF) SELECT t, v FROM test_char WHERE v && '{4,5}' ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_char WHERE v @> '{4,5}' ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_char WHERE v <@ '{4,5}' ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_char WHERE v =  '{0,1,2,3,4,5,6,7,8,9,1}' ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_char WHERE v %  '{0,1,2,3,4,5,6,7,8,9,1}' ORDER BY t;

SELECT t, v FROM test_char WHERE v && '{4,5}' ORDER BY t;
SELECT t, v FROM test_char WHERE v @> '{4,5}' ORDER BY t;
SELECT t, v FROM test_char WHERE v <@ '{5,6}' ORDER BY t;
SELECT t, v FROM test_char WHERE v =  '{0,1,2,3,4,5,6,7,8,9,1}' ORDER BY t;
SET anyarray.similarity_type=cosine;
SELECT t, v FROM test_char WHERE v %  '{0,1,2,3,4,5,6,7,8,9,1}' ORDER BY t;
SET anyarray.similarity_type=jaccard;
SELECT t, v FROM test_char WHERE v %  '{0,1,2,3,4,5,6,7,8,9,1}' ORDER BY t;
SET anyarray.similarity_type=overlap;
SET anyarray.similarity_threshold = 3;
SELECT t, v FROM test_char WHERE v %  '{0,1,2,3,4,5,6,7,8,9,1}' ORDER BY t;
RESET anyarray.similarity_threshold;

