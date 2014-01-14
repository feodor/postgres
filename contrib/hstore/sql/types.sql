SELECT '"foo"=>true'::hstore;
SELECT 'foo=>true'::hstore;
SELECT '"true"=>true'::hstore;
SELECT 'true=>true'::hstore;
SELECT '"t"=>true'::hstore;
SELECT 't=>true'::hstore;
SELECT '"false"=>true'::hstore;
SELECT 'false=>true'::hstore;
SELECT '"f"=>true'::hstore;
SELECT 'f=>true'::hstore;

SELECT '"foo"=>false'::hstore;
SELECT 'foo=>false'::hstore;
SELECT '"false"=>false'::hstore;
SELECT 'false=>false'::hstore;
SELECT '"t"=>false'::hstore;
SELECT 't=>false'::hstore;
SELECT '"false"=>false'::hstore;
SELECT 'false=>false'::hstore;
SELECT '"f"=>false'::hstore;
SELECT 'f=>false'::hstore;

SELECT '"1"=>x'::hstore;
SELECT '1=>x'::hstore;
SELECT 'foo=>1'::hstore;
SELECT 'foo=>1.'::hstore;
SELECT 'foo=>1.0'::hstore;
SELECT 'foo=>1.01'::hstore;
SELECT 'foo=>1.01e'::hstore;
SELECT 'foo=>1.01e1'::hstore;
SELECT 'foo=>1.01e+1'::hstore;
SELECT 'foo=>1.01e-1'::hstore;
SELECT 'foo=>.1'::hstore;
SELECT 'foo=>.1e'::hstore;
SELECT 'foo=>.1e1'::hstore;
SELECT 'foo=>.1e+1'::hstore;
SELECT 'foo=>.1e-1'::hstore;
SELECT 'foo=>0.1e-1'::hstore;
SELECT 'foo=>00.1e-1'::hstore;

SELECT 'foo=>+1'::hstore;
SELECT 'foo=>+1.'::hstore;
SELECT 'foo=>+1.0'::hstore;
SELECT 'foo=>+1.01'::hstore;
SELECT 'foo=>+1.01e'::hstore;
SELECT 'foo=>+1.01e1'::hstore;
SELECT 'foo=>+1.01e+1'::hstore;
SELECT 'foo=>+1.01e-1'::hstore;
SELECT 'foo=>+.1'::hstore;
SELECT 'foo=>+.1e'::hstore;
SELECT 'foo=>+.1e1'::hstore;
SELECT 'foo=>+.1e+1'::hstore;
SELECT 'foo=>+.1e-1'::hstore;

SELECT 'foo=>-1'::hstore;
SELECT 'foo=>-1.'::hstore;
SELECT 'foo=>-1.0'::hstore;
SELECT 'foo=>-1.01'::hstore;
SELECT 'foo=>-1.01e'::hstore;
SELECT 'foo=>-1.01e1'::hstore;
SELECT 'foo=>-1.01e+1'::hstore;
SELECT 'foo=>-1.01e-1'::hstore;
SELECT 'foo=>-.1'::hstore;
SELECT 'foo=>-.1e'::hstore;
SELECT 'foo=>-.1e1'::hstore;
SELECT 'foo=>-.1e+1'::hstore;
SELECT 'foo=>-.1e-1'::hstore;

SELECT 'foo=>1e2000'::hstore;

SELECT 'foo=>1e12, bar=>x'::hstore ^> 'foo';
SELECT 'foo=>1e12, bar=>x'::hstore ^> 'bar';
SELECT 'foo=>1e12, bar=>x'::hstore ^> 0;
SELECT 'foo=>1e12, bar=>x'::hstore ^> 1;

SELECT '[foo, 1e12, bar, x]'::hstore ^> 'foo';
SELECT '[foo, 1e12, bar, x]'::hstore ^> 'bar';
SELECT '[foo, 1e12, bar, x]'::hstore ^> 0;
SELECT '[foo, 1e12, bar, x]'::hstore ^> 1;

SELECT 'foo=>{x, 1e-12}'::hstore #^> '{foo, 0}';
SELECT 'foo=>{x, 1e-12}'::hstore #^> '{foo, 1}';

SELECT 'foo=>t, bar=>x'::hstore ?> 'foo';
SELECT 'foo=>t, bar=>x'::hstore ?> 'bar';
SELECT 'foo=>t, bar=>x'::hstore ?> 0;
SELECT 'foo=>t, bar=>x'::hstore ?> 1;

SELECT '[foo, t, bar, x]'::hstore ?> 'foo';
SELECT '[foo, t, bar, x]'::hstore ?> 'bar';
SELECT '[foo, t, bar, x]'::hstore ?> 0;
SELECT '[foo, t, bar, x]'::hstore ?> 1;

SELECT 'foo=>{x, t}'::hstore #?> '{foo, 0}';
SELECT 'foo=>{x, t}'::hstore #?> '{foo, 1}';

SELECT 'foo=>f, bar=>x'::hstore ?> 'foo';
SELECT 'foo=>f, bar=>x'::hstore ?> 'bar';
SELECT 'foo=>f, bar=>x'::hstore ?> 0;
SELECT 'foo=>f, bar=>x'::hstore ?> 1;

SELECT '[foo, f, bar, x]'::hstore ?> 'foo';
SELECT '[foo, f, bar, x]'::hstore ?> 'bar';
SELECT '[foo, f, bar, x]'::hstore ?> 0;
SELECT '[foo, f, bar, x]'::hstore ?> 1;

SELECT 'foo=>{x, f}'::hstore #?> '{foo, 0}';
SELECT 'foo=>{x, f}'::hstore #?> '{foo, 1}';


SELECT hstore_typeof('a=>b') AS hash;
SELECT hstore_typeof('{a=>b}') AS hash;
SELECT hstore_typeof('{a, b}') AS array;
SELECT hstore_typeof('{{a=>b}}') AS array;
SELECT hstore_typeof('[a, b]') AS array;
SELECT hstore_typeof('') AS "NULL";
SELECT hstore_typeof('NULL') AS "null";
SELECT hstore_typeof('1.0') AS numeric;
SELECT hstore_typeof('t') AS bool;
SELECT hstore_typeof('f') AS bool;

SELECT hstore('xxx', 't'::bool);
SELECT hstore('xxx', 'f'::bool);

SELECT hstore('xxx', 3.14);
SELECT hstore('xxx', 3.14::numeric);
SELECT hstore('xxx', '3.14'::numeric);

SELECT hstore(NULL);
SELECT hstore('NULL');

SELECT hstore('t'::bool) AS "true", hstore('f'::bool) AS "false";

SELECT hstore(3.14), hstore(3.14::numeric), hstore('3.14'::numeric);

SELECT hstore('xxx', 'foo=>t, bar=>3.14, zzz=>xxx'::hstore);

SELECT array_to_hstore('{{1,1,4},{23,3,5}}'::int2[]);
SELECT array_to_hstore('{{1,1,4},{23,3,5}}'::int4[]);
SELECT array_to_hstore('{{1,1,4},{23,3,5}}'::int8[]);
SELECT array_to_hstore('{{1,1,4},{23,3,5}}'::float4[]);
SELECT array_to_hstore('{{1,1,4},{23,3,5}}'::float8[]);
SELECT array_to_hstore('{{1,1,f},{f,t,NULL}}'::bool[]);
SELECT array_to_hstore('{{1,1,4},{23,3,5}}'::text[]);
SELECT array_to_hstore('{{1,1,4},{23,3,5}}'::varchar[]);

SELECT array_to_hstore('{{{1,11},{1,1},{4,41}},{{23,231},{3,31},{5,51}}}'::int4[]);
SELECT hstore('array', array_to_hstore('{{{1,11},{1,1},{4,41}},{{23,231},{3,31},{5,51}}}'::int4[]));

SELECT 'a=>"00012333", b=>"12233", c=>00012333, d=>12233'::hstore;
SELECT hstore_to_json('a=>"00012333", b=>"12233", c=>00012333, d=>12233'::hstore);
SELECT hstore_to_json_loose('a=>"00012333", b=>"12233", c=>00012333, d=>12233'::hstore);
