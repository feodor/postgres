SELECT
    t,
	ARRAY(
		SELECT
			('192.168.1.' || (v % 256))::inet
		FROM
			generate_series(max(0, t - 10),  t) as v
	) AS v
	INTO test_inet
FROM
	generate_series(1, 200) as t;


SET anyarray.similarity_type=cosine;
SELECT  t, similarity(v, to_inetp_array('{10,9,8,7,6,5,4,3,2,1}')::inet[]) AS s FROM test_inet 
	WHERE v % to_inetp_array('{10,9,8,7,6,5,4,3,2,1}')::inet[] ORDER BY s DESC, t;
SELECT  t, similarity(v, to_inetp_array('{50,49,8,7,6,5,4,3,2,1}')::inet[]) AS s FROM test_inet 
	WHERE v % to_inetp_array('{50,49,8,7,6,5,4,3,2,1}')::inet[] ORDER BY s DESC, t;

SET anyarray.similarity_type=jaccard;
SELECT  t, similarity(v, to_inetp_array('{10,9,8,7,6,5,4,3,2,1}')::inet[]) AS s FROM test_inet 
	WHERE v % to_inetp_array('{10,9,8,7,6,5,4,3,2,1}')::inet[] ORDER BY s DESC, t;
SELECT  t, similarity(v, to_inetp_array('{50,49,8,7,6,5,4,3,2,1}')::inet[]) AS s FROM test_inet 
	WHERE v % to_inetp_array('{50,49,8,7,6,5,4,3,2,1}')::inet[] ORDER BY s DESC, t;

SELECT t, v FROM test_inet WHERE v && to_inetp_array('{43,50}')::inet[] ORDER BY t;
SELECT t, v FROM test_inet WHERE v @> to_inetp_array('{43,50}')::inet[] ORDER BY t;
SELECT t, v FROM test_inet WHERE v <@ to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
SELECT t, v FROM test_inet WHERE v =  to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
SET anyarray.similarity_type=cosine;
SELECT t, v FROM test_inet WHERE v %  to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
SET anyarray.similarity_type=jaccard;
SELECT t, v FROM test_inet WHERE v %  to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
SET anyarray.similarity_type=overlap;
SET anyarray.similarity_threshold = 3;
SELECT t, v FROM test_inet WHERE v %  to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
RESET anyarray.similarity_threshold;

CREATE INDEX idx_test_inet ON test_inet USING gist (v _inet_aa_ops);

SET enable_seqscan=off;

EXPLAIN (COSTS OFF) SELECT t, v FROM test_inet WHERE v && to_inetp_array('{43,50}')::inet[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_inet WHERE v @> to_inetp_array('{43,50}')::inet[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_inet WHERE v <@ to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_inet WHERE v =  to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_inet WHERE v %  to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;

SELECT t, v FROM test_inet WHERE v && to_inetp_array('{43,50}')::inet[] ORDER BY t;
SELECT t, v FROM test_inet WHERE v @> to_inetp_array('{43,50}')::inet[] ORDER BY t;
SELECT t, v FROM test_inet WHERE v <@ to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
SELECT t, v FROM test_inet WHERE v =  to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
SET anyarray.similarity_type=cosine;
SELECT t, v FROM test_inet WHERE v %  to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
SET anyarray.similarity_type=jaccard;
SELECT t, v FROM test_inet WHERE v %  to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
SET anyarray.similarity_type=overlap;
SET anyarray.similarity_threshold = 3;
SELECT t, v FROM test_inet WHERE v %  to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
RESET anyarray.similarity_threshold;

DROP INDEX idx_test_inet;
CREATE INDEX idx_test_inet ON test_inet USING gin (v _inet_aa_ops);

SET enable_seqscan=off;

EXPLAIN (COSTS OFF) SELECT t, v FROM test_inet WHERE v && to_inetp_array('{43,50}')::inet[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_inet WHERE v @> to_inetp_array('{43,50}')::inet[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_inet WHERE v <@ to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_inet WHERE v =  to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_inet WHERE v %  to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;

SELECT t, v FROM test_inet WHERE v && to_inetp_array('{43,50}')::inet[] ORDER BY t;
SELECT t, v FROM test_inet WHERE v @> to_inetp_array('{43,50}')::inet[] ORDER BY t;
SELECT t, v FROM test_inet WHERE v <@ to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
SELECT t, v FROM test_inet WHERE v =  to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
SET anyarray.similarity_type=cosine;
SELECT t, v FROM test_inet WHERE v %  to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
SET anyarray.similarity_type=jaccard;
SELECT t, v FROM test_inet WHERE v %  to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
SET anyarray.similarity_type=overlap;
SET anyarray.similarity_threshold = 3;
SELECT t, v FROM test_inet WHERE v %  to_inetp_array('{0,1,2,3,4,5,6,7,8,9,10}')::inet[] ORDER BY t;
RESET anyarray.similarity_threshold;

