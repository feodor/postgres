CREATE EXTENSION anyarray;

SELECT anyset(1234);
SELECT icount('{1234234,0234234}'::int[]);
SELECT sort('{1234234,-30,0234234}'::int[]);
SELECT sort('{1234234,-30,0234234}'::int[],'asc');
SELECT sort('{1234234,-30,0234234}'::int[],'desc');
SELECT sort_asc('{1234234,-30,0234234}'::int[]);
SELECT sort_desc('{1234234,-30,0234234}'::int[]);
SELECT uniq('{1234234,-30,-30,0234234,-30}'::int[]);
SELECT uniq(sort_asc('{1234234,-30,-30,0234234,-30}'::int[]));
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
SELECT idx('{1234234,-30,-30,0234234,-30}'::text[], '-30');
SELECT subarray('{1234234,-30,-30,0234234,-30}'::text[],2,3);
SELECT subarray('{1234234,-30,-30,0234234,-30}'::text[],-1,1);
SELECT subarray('{1234234,-30,-30,0234234,-30}'::text[],0,-1);

SELECT #'{1234234,0234234}'::int[];
SELECT '{123,623,445}'::int[] + 1245;
SELECT '{123,623,445}'::int[] + 445;
SELECT '{123,623,445}'::int[] + '{1245,87,445}';
SELECT '{123,623,445}'::int[] - 623;

SELECT #'{1234234,0234234}'::text[];
SELECT '{123,623,445}'::text[] + '1245'::text;
SELECT '{123,623,445}'::text[] + '445'::text;
SELECT '{123,623,445}'::text[] + '{1245,87,445}';
SELECT '{123,623,445}'::text[] - '623'::text;

