SELECT
    t,
	ARRAY(
		SELECT
			epoch2timestamp(v)::time
		FROM
			generate_series(max(0, t - 10),  t) as v
	) AS v
	INTO test_time
FROM
	generate_series(1, 200) as t;


SET anyarray.similarity_type=cosine;
SELECT  t, similarity(v, to_tsp_array('{10,9,8,7,6,5,4,3,2,1}')::time[]) AS s FROM test_time 
	WHERE v % to_tsp_array('{10,9,8,7,6,5,4,3,2,1}')::time[] ORDER BY s DESC, t;
SELECT  t, similarity(v, to_tsp_array('{50,49,8,7,6,5,4,3,2,1}')::time[]) AS s FROM test_time 
	WHERE v % to_tsp_array('{50,49,8,7,6,5,4,3,2,1}')::time[] ORDER BY s DESC, t;

SET anyarray.similarity_type=jaccard;
SELECT  t, similarity(v, to_tsp_array('{10,9,8,7,6,5,4,3,2,1}')::time[]) AS s FROM test_time 
	WHERE v % to_tsp_array('{10,9,8,7,6,5,4,3,2,1}')::time[] ORDER BY s DESC, t;
SELECT  t, similarity(v, to_tsp_array('{50,49,8,7,6,5,4,3,2,1}')::time[]) AS s FROM test_time 
	WHERE v % to_tsp_array('{50,49,8,7,6,5,4,3,2,1}')::time[] ORDER BY s DESC, t;

SELECT t, v FROM test_time WHERE v && to_tsp_array('{43,50}')::time[] ORDER BY t;
SELECT t, v FROM test_time WHERE v @> to_tsp_array('{43,50}')::time[] ORDER BY t;
SELECT t, v FROM test_time WHERE v <@ to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
SELECT t, v FROM test_time WHERE v =  to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
SET anyarray.similarity_type=cosine;
SELECT t, v FROM test_time WHERE v %  to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
SET anyarray.similarity_type=jaccard;
SELECT t, v FROM test_time WHERE v %  to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
SET anyarray.similarity_type=overlap;
SET anyarray.similarity_threshold = 3;
SELECT t, v FROM test_time WHERE v %  to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
RESET anyarray.similarity_threshold;

CREATE INDEX idx_test_time ON test_time USING gist (v _time_aa_ops);

SET enable_seqscan=off;

EXPLAIN (COSTS OFF) SELECT t, v FROM test_time WHERE v && to_tsp_array('{43,50}')::time[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_time WHERE v @> to_tsp_array('{43,50}')::time[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_time WHERE v <@ to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_time WHERE v =  to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_time WHERE v %  to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;

SELECT t, v FROM test_time WHERE v && to_tsp_array('{43,50}')::time[] ORDER BY t;
SELECT t, v FROM test_time WHERE v @> to_tsp_array('{43,50}')::time[] ORDER BY t;
SELECT t, v FROM test_time WHERE v <@ to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
SELECT t, v FROM test_time WHERE v =  to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
SET anyarray.similarity_type=cosine;
SELECT t, v FROM test_time WHERE v %  to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
SET anyarray.similarity_type=jaccard;
SELECT t, v FROM test_time WHERE v %  to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
SET anyarray.similarity_type=overlap;
SET anyarray.similarity_threshold = 3;
SELECT t, v FROM test_time WHERE v %  to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
RESET anyarray.similarity_threshold;

DROP INDEX idx_test_time;
CREATE INDEX idx_test_time ON test_time USING gin (v _time_aa_ops);

SET enable_seqscan=off;

EXPLAIN (COSTS OFF) SELECT t, v FROM test_time WHERE v && to_tsp_array('{43,50}')::time[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_time WHERE v @> to_tsp_array('{43,50}')::time[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_time WHERE v <@ to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_time WHERE v =  to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_time WHERE v %  to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;

SELECT t, v FROM test_time WHERE v && to_tsp_array('{43,50}')::time[] ORDER BY t;
SELECT t, v FROM test_time WHERE v @> to_tsp_array('{43,50}')::time[] ORDER BY t;
SELECT t, v FROM test_time WHERE v <@ to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
SELECT t, v FROM test_time WHERE v =  to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
SET anyarray.similarity_type=cosine;
SELECT t, v FROM test_time WHERE v %  to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
SET anyarray.similarity_type=jaccard;
SELECT t, v FROM test_time WHERE v %  to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
SET anyarray.similarity_type=overlap;
SET anyarray.similarity_threshold = 3;
SELECT t, v FROM test_time WHERE v %  to_tsp_array('{0,1,2,3,4,5,6,7,8,9,10}')::time[] ORDER BY t;
RESET anyarray.similarity_threshold;

