SELECT
    t,
	ARRAY(
		SELECT
			v::bit(10)::varbit
		FROM
			generate_series(max(0, t - 10),  t) as v
	) AS v
	INTO test_varbit
FROM
	generate_series(1, 200) as t;


SET anyarray.similarity_type=cosine;
SELECT  t, similarity(v, '{10,9,8,7,6,5,4,3,2,1}'::int4[]::bit(10)[]::varbit[]) AS s FROM test_varbit 
	WHERE v % '{10,9,8,7,6,5,4,3,2,1}'::int4[]::bit(10)[]::varbit[] ORDER BY s DESC, t;
SELECT  t, similarity(v, '{50,49,8,7,6,5,4,3,2,1}'::int4[]::bit(10)[]::varbit[]) AS s FROM test_varbit 
	WHERE v % '{50,49,8,7,6,5,4,3,2,1}'::int4[]::bit(10)[]::varbit[] ORDER BY s DESC, t;

SET anyarray.similarity_type=jaccard;
SELECT  t, similarity(v, '{10,9,8,7,6,5,4,3,2,1}'::int4[]::bit(10)[]::varbit[]) AS s FROM test_varbit 
	WHERE v % '{10,9,8,7,6,5,4,3,2,1}'::int4[]::bit(10)[]::varbit[] ORDER BY s DESC, t;
SELECT  t, similarity(v, '{50,49,8,7,6,5,4,3,2,1}'::int4[]::bit(10)[]::varbit[]) AS s FROM test_varbit 
	WHERE v % '{50,49,8,7,6,5,4,3,2,1}'::int4[]::bit(10)[]::varbit[] ORDER BY s DESC, t;

SELECT t, v FROM test_varbit WHERE v && '{43,50}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
SELECT t, v FROM test_varbit WHERE v @> '{43,50}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
SELECT t, v FROM test_varbit WHERE v <@ '{0,1,2,3,4,5,6,7,8,9,10}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
SELECT t, v FROM test_varbit WHERE v =  '{0,1,2,3,4,5,6,7,8,9,10}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
SET anyarray.similarity_type=cosine;
SELECT t, v FROM test_varbit WHERE v %  '{0,1,2,3,4,5,6,7,8,9,10}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
SET anyarray.similarity_type=jaccard;
SELECT t, v FROM test_varbit WHERE v %  '{0,1,2,3,4,5,6,7,8,9,10}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
SET anyarray.similarity_type=overlap;
SET anyarray.similarity_threshold = 3;
SELECT t, v FROM test_varbit WHERE v %  '{0,1,2,3,4,5,6,7,8,9,10}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
RESET anyarray.similarity_threshold;

CREATE INDEX idx_test_varbit ON test_varbit USING gin (v _varbit_aa_ops);

SET enable_seqscan=off;

EXPLAIN (COSTS OFF) SELECT t, v FROM test_varbit WHERE v && '{43,50}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_varbit WHERE v @> '{43,50}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_varbit WHERE v <@ '{0,1,2,3,4,5,6,7,8,9,10}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_varbit WHERE v =  '{0,1,2,3,4,5,6,7,8,9,10}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
EXPLAIN (COSTS OFF) SELECT t, v FROM test_varbit WHERE v %  '{0,1,2,3,4,5,6,7,8,9,10}'::int4[]::bit(10)[]::varbit[] ORDER BY t;

SELECT t, v FROM test_varbit WHERE v && '{43,50}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
SELECT t, v FROM test_varbit WHERE v @> '{43,50}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
SELECT t, v FROM test_varbit WHERE v <@ '{0,1,2,3,4,5,6,7,8,9,10}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
SELECT t, v FROM test_varbit WHERE v =  '{0,1,2,3,4,5,6,7,8,9,10}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
SET anyarray.similarity_type=cosine;
SELECT t, v FROM test_varbit WHERE v %  '{0,1,2,3,4,5,6,7,8,9,10}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
SET anyarray.similarity_type=jaccard;
SELECT t, v FROM test_varbit WHERE v %  '{0,1,2,3,4,5,6,7,8,9,10}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
SET anyarray.similarity_type=overlap;
SET anyarray.similarity_threshold = 3;
SELECT t, v FROM test_varbit WHERE v %  '{0,1,2,3,4,5,6,7,8,9,10}'::int4[]::bit(10)[]::varbit[] ORDER BY t;
RESET anyarray.similarity_threshold;

