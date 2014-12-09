SELECT anyset(1234);
SELECT icount('{1234234,0234234}'::int[]);
SELECT sort('{1234234,-30,0234234}'::int[]);
SELECT sort('{1234234,-30,0234234}'::int[],'asc');
SELECT sort('{1234234,-30,0234234}'::int[],'desc');
SELECT sort_asc('{1234234,-30,0234234}'::int[]);
SELECT sort_desc('{1234234,-30,0234234}'::int[]);
SELECT uniq('{1234234,-30,-30,0234234,-30}'::int[]);
SELECT uniq(sort_asc('{1234234,-30,-30,0234234,-30}'::int[]));
SELECT uniq_d('{1234234,-30,-30,0234234,-30,0234234}'::int[]);
SELECT uniq_d(sort_asc('{1234234,-30,-30,0234234,-30,0234234}'::int[]));
SELECT idx('{1234234,-30,-30,0234234,-30}'::int[],-30);
SELECT subarray('{1234234,-30,-30,0234234,-30}'::int[],2,3);
SELECT subarray('{1234234,-30,-30,0234234,-30}'::int[],-1,1);
SELECT subarray('{1234234,-30,-30,0234234,-30}'::int[],0,-1);

SELECT anyset('1234'::text);
SELECT icount('{1234234,0234234}'::text[]);
SELECT sort('{1234234,-30,0234234}'::text[]);
SELECT sort('{1234234,-30,0234234}'::text[],'asc');
SELECT sort('{1234234,-30,0234234}'::text[],'desc');
SELECT sort_asc('{1234234,-30,0234234}'::text[]);
SELECT sort_desc('{1234234,-30,0234234}'::text[]);
SELECT uniq('{1234234,-30,-30,0234234,-30}'::text[]);
SELECT uniq(sort_asc('{1234234,-30,-30,0234234,-30}'::text[]));
SELECT uniq_d('{1234234,-30,-30,0234234,-30,0234234}'::text[]);
SELECT uniq_d(sort_asc('{1234234,-30,-30,0234234,-30,0234234}'::text[]));
SELECT idx('{1234234,-30,-30,0234234,-30}'::text[], '-30');
SELECT subarray('{1234234,-30,-30,0234234,-30}'::text[],2,3);
SELECT subarray('{1234234,-30,-30,0234234,-30}'::text[],-1,1);
SELECT subarray('{1234234,-30,-30,0234234,-30}'::text[],0,-1);

SELECT #'{1234234,0234234}'::int[];
SELECT '{123,623,445}'::int[] + 1245;
SELECT '{123,623,445}'::int[] + 445;
SELECT '{123,623,445}'::int[] + '{1245,87,445}';
SELECT '{123,623,445}'::int[] - 623;
SELECT '{123,623,445}'::int[] - '{1623,623}'::int[];
SELECT '{123,623,445}'::int[] | '{1623,623}'::int[];
SELECT '{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
		21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,
		39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,
		57,58,59,60,61,62,63,64,65}'::int[] | '{1623,623, 32, 60}'::int[];
SELECT '{123,623,445}'::int[] & '{1623,623}';
SELECT '{-1,3,1}'::int[] & '{1,2}';
SELECT '{1,3,1}'::int[] & '{1,2}';
SELECT '{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
		21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,
		39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,
		57,58,59,60,61,62,63,64,65}'::int[] & '{1623,623, 32, 60}'::int[];

SELECT #'{1234234,0234234}'::text[];
SELECT '{123,623,445}'::text[] + '1245'::text;
SELECT '{123,623,445}'::text[] + '445'::text;
SELECT '{123,623,445}'::text[] + '{1245,87,445}';
SELECT '{123,623,445}'::text[] - '623'::text;
SELECT '{123,623,445}'::text[] - '{1623,623}'::text[];
SELECT '{123,623,445}'::text[] | '{1623,623}'::text[];
SELECT '{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
		21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,
		39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,
		57,58,59,60,61,62,63,64,65}'::text[] | '{1623,623, 32, 60}'::text[];
SELECT '{123,623,445}'::text[] & '{1623,623}';
SELECT '{-1,3,1}'::text[] & '{1,2}';
SELECT '{1,3,1}'::text[] & '{1,2}';
SELECT '{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
		21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,
		39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,
		57,58,59,60,61,62,63,64,65}'::text[] & '{1623,623, 32, 60}'::text[];

SET anyarray.similarity_type = 'cosine';

SELECT similarity('{3,2}'::text[], '{3,2,1}');
SELECT distance('{3,2}'::text[], '{3,2,1}');
SELECT '{3,2}'::text[] <-> '{3,2,1}';

SELECT similarity('{3,2}'::int4[], '{3,2,1}');
SELECT distance('{3,2}'::int4[], '{3,2,1}');
SELECT '{3,2}'::int4[] <-> '{3,2,1}';

SELECT similarity('{3,2}'::int[], '{3,2,1}');
SELECT similarity('{2,1}'::int[], '{3,2,1}');
SELECT similarity('{}'::int[], '{3,2,1}');
SELECT similarity('{12,10}'::int[], '{3,2,1}');
SELECT similarity('{123}'::int[], '{}');
SELECT similarity('{1,4,6}'::int[], '{1,4,6}');
SELECT similarity('{1,4,6}'::int[], '{6,4,1}');
SELECT similarity('{1,4,6}'::int[], '{5,4,6}'); 
SELECT similarity('{1,4,6}'::int[], '{5,4,6}');
SELECT similarity('{1,2}'::int[], '{2,2,2,2,2,1}');
SELECT similarity('{1,2}'::int[], '{1,2,2,2,2,2}');
SELECT similarity('{}'::int[], '{3}');
SELECT similarity('{3}'::int[], '{3}');
SELECT similarity('{2}'::int[], '{3}');

SET anyarray.similarity_type = 'overlap';

SELECT similarity('{3,2}'::int[], '{3,2,1}');
SELECT distance('{3,2}'::text[], '{3,2,1}');

SET anyarray.similarity_type = 'jaccard';

SELECT similarity('{3,2}'::int[], '{3,2,1}');
SELECT distance('{3,2}'::text[], '{3,2,1}');

SET anyarray.similarity_threshold = 0.6;

SELECT '{3,2}'::int[] % '{3,2,1}' AS "true";

SET anyarray.similarity_threshold = 0.7;

SELECT '{3,2}'::int[] % '{3,2,1}' AS "false";


