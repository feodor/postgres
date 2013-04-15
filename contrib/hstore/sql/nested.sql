
select 'ff => {a=>12, b=>16}'::hstore;

select 'ff => {a=>12, b=>16}, qq=> 123'::hstore;

select 'aa => {a,aaa}, qq=>{ a=>12, b=>16 , c=> { c1, c2}, d=>{d1=>d1, d2=>d2, d1=>d3} }'::hstore;

select '"aa"=>{a,aaa}, "qq"=>{"a"=>"12", "b"=>"16", "c"=>{c1,c2}, "d"=>{"d1"=>"d1", "d2"=>"d2"}}'::hstore;

select '"aa"=>{a,aaa}, "qq"=>{"a"=>"12", "b"=>"16", "c"=>{c1,c2,{c3},{c4=>4}}, "d"=>{"d1"=>"d1", "d2"=>"d2"}}'::hstore;

select 'ff => {a,aaa}'::hstore;

--test optional outer braces
SELECT	'a=>1'::hstore;
SELECT	'{a=>1}'::hstore;
SELECT	'a,b'::hstore;
SELECT	'{a,b}'::hstore;
SELECT	'a,{b}'::hstore;
SELECT	'{a,{b}}'::hstore;
SELECT	'{a},b'::hstore;
SELECT	'{{a},b}'::hstore;
SELECT	'a,{b},{c}'::hstore;
SELECT	'{a,{b},{c}}'::hstore;
SELECT	'{a},{b},c'::hstore;
SELECT	'{{a},{b},c}'::hstore;
SELECT	'{a},b,{c}'::hstore;
SELECT	'{{a},b,{c}}'::hstore;
SELECT	'a,{b=>1}'::hstore;
SELECT	'{a,{b=>1}}'::hstore;
SELECT	'{a},{b=>1}'::hstore;
SELECT	'{{a},{b=>1}}'::hstore;
SELECT	'{a},{b=>1},{c}'::hstore;
SELECT	'{{a},{b=>1},{c}}'::hstore;
SELECT	'a'::hstore;
SELECT	'{a}'::hstore;
SELECT	''::hstore;
SELECT	'{}'::hstore;

--nested json

SELECT	hstore_to_json('a=>1');
SELECT	hstore_to_json('{a=>1}');
SELECT	hstore_to_json('a,b');
SELECT	hstore_to_json('{a,b}');
SELECT	hstore_to_json('a,{b}');
SELECT	hstore_to_json('{a,{b}}');
SELECT	hstore_to_json('{a},b');
SELECT	hstore_to_json('{{a},b}');
SELECT	hstore_to_json('a,{b},{c}');
SELECT	hstore_to_json('{a,{b},{c}}');
SELECT	hstore_to_json('{a},{b},c');
SELECT	hstore_to_json('{{a},{b},c}');
SELECT	hstore_to_json('{a},b,{c}');
SELECT	hstore_to_json('{{a},b,{c}}');
SELECT	hstore_to_json('a,{b=>1}');
SELECT	hstore_to_json('{a,{b=>1}}');
SELECT	hstore_to_json('{a},{b=>1}');
SELECT	hstore_to_json('{{a},{b=>1}}');
SELECT	hstore_to_json('{a},{b=>1},{c}');
SELECT	hstore_to_json('{{a},{b=>1},{c}}');
SELECT	hstore_to_json('a');
SELECT	hstore_to_json('{a}');
SELECT	hstore_to_json('');
SELECT	hstore_to_json('{}');

select hstore_to_json('"aa"=>{a,aaa}, "qq"=>{"a"=>"12", "b"=>"16", "c"=>{c1,c2,{c3},{c4=>4}}, "d"=>{"d1"=>"d1", "d2"=>"d2"}}'::hstore);

--

SELECT 'ff => {a=>12, b=>16}, qq=> 123, x=>{1,2}, Y=>NULL'::hstore -> 'ff', 
	   'ff => {a=>12, b=>16}, qq=> 123, x=>{1,2}, Y=>NULL'::hstore -> 'qq', 
	   ('ff => {a=>12, b=>16}, qq=> 123, x=>{1,2}, Y=>NULL'::hstore -> 'Y') IS NULL AS t, 
	   'ff => {a=>12, b=>16}, qq=> 123, x=>{1,2}, Y=>NULL'::hstore -> 'x'; 

SELECT '[ a, b, c, d]'::hstore -> 'a';
--

CREATE TABLE testtype (i int, h hstore, a int[]);
INSERT INTO testtype VALUES (1, 'a=>1', '{1,2,3}');

SELECT populate_record(v, 'i=>2'::hstore) FROM testtype v;
SELECT populate_record(v, 'i=>2, a=>{7,8,9}'::hstore) FROM testtype v;
SELECT populate_record(v, 'i=>2, h=>{b=>3}, a=>{7,8,9}'::hstore) FROM testtype v;

--complex delete

select 'b=>{a,c}'::hstore - 'a'::text;
select 'b=>{a,c}, a=>1'::hstore - 'a'::text;
select 'b=>{a,c}, a=>[2,3]'::hstore - 'a'::text;
select 'b=>{a,c}, a=>[2,3]'::hstore - 'a'::text;
select '[2,3,a]'::hstore - 'a'::text;
select '[a,2,3,a]'::hstore - 'a'::text;
select '[a,a]'::hstore - 'a'::text;
select '[a]'::hstore - 'a'::text;
select 'a=>1'::hstore - 'a'::text;
select ''::hstore - 'a'::text;

select 'a, 1 , b,2, c,3'::hstore - ARRAY['d','b'];

select 'a=>{1,2}, v=>23, b=>c'::hstore - 'v'::hstore;
select 'a=>{1,2}, v=>23, b=>c'::hstore - 'v=>23'::hstore;
select 'a=>{1,2}, v=>23, b=>c'::hstore - 'v=>{1,2}'::hstore;
select 'a=>{1,2}, v=>23, b=>c'::hstore - 'a=>{1,2}'::hstore;
select 'a, {1,2}, v, 23, b, c'::hstore - 'v'::hstore;
select 'a, {1,2}, v, 23, b, c'::hstore - 'v=>23'::hstore;
select 'a, {1,2}, v, 23, b, c'::hstore - 'v,23'::hstore;
select 'a, {1,2}, v, 23, b, c'::hstore - 'v,{1,2}'::hstore;

--joining

select 'aa=>1 , b=>2, cq=>3'::hstore || 'cq,l, b,g, fg,f, 1,2'::hstore;
select 'aa,1 , b,2, cq,3'::hstore || 'cq,l, b,g, fg,f, 1,2'::hstore;

--slice
select slice_array(hstore 'aa=>1, b=>2, c=>3', ARRAY['g','h','i']);
select slice_array(hstore 'aa,1, b,2, c,3', ARRAY['g','h','i']);
select slice_array(hstore 'aa=>1, b=>2, c=>3', ARRAY['b','c']);
select slice_array(hstore 'aa,1, b,2, c,3', ARRAY['b','c']);
select slice_array(hstore 'aa=>1, b=>{2=>1}, c=>{1,2}', ARRAY['b','c']);


--decoration

SET hstore.array_square_brackets=false;
SET hstore.root_array_decorated=false;
SET hstore.root_hash_decorated=false;
SELECT 'a=>1, b=>{c=>3}, d=>[4,[5]]'::hstore AS h, 'a, {b=>c}, [c, d, e]'::hstore AS a;

SET hstore.array_square_brackets=false;
SET hstore.root_array_decorated=false;
SET hstore.root_hash_decorated=true;
SELECT 'a=>1, b=>{c=>3}, d=>[4,[5]]'::hstore AS h, 'a, {b=>c}, [c, d, e]'::hstore AS a;

SET hstore.array_square_brackets=false;
SET hstore.root_array_decorated=true;
SET hstore.root_hash_decorated=false;
SELECT 'a=>1, b=>{c=>3}, d=>[4,[5]]'::hstore AS h, 'a, {b=>c}, [c, d, e]'::hstore AS a;

SET hstore.array_square_brackets=false;
SET hstore.root_array_decorated=true;
SET hstore.root_hash_decorated=true;
SELECT 'a=>1, b=>{c=>3}, d=>[4,[5]]'::hstore AS h, 'a, {b=>c}, [c, d, e]'::hstore AS a;

SET hstore.array_square_brackets=true;
SET hstore.root_array_decorated=false;
SET hstore.root_hash_decorated=false;
SELECT 'a=>1, b=>{c=>3}, d=>[4,[5]]'::hstore AS h, 'a, {b=>c}, [c, d, e]'::hstore AS a;

SET hstore.array_square_brackets=true;
SET hstore.root_array_decorated=false;
SET hstore.root_hash_decorated=true;
SELECT 'a=>1, b=>{c=>3}, d=>[4,[5]]'::hstore AS h, 'a, {b=>c}, [c, d, e]'::hstore AS a;

SET hstore.array_square_brackets=true;
SET hstore.root_array_decorated=true;
SET hstore.root_hash_decorated=false;
SELECT 'a=>1, b=>{c=>3}, d=>[4,[5]]'::hstore AS h, 'a, {b=>c}, [c, d, e]'::hstore AS a;

SET hstore.array_square_brackets=true;
SET hstore.root_array_decorated=true;
SET hstore.root_hash_decorated=true;
SELECT 'a=>1, b=>{c=>3}, d=>[4,[5]]'::hstore AS h, 'a, {b=>c}, [c, d, e]'::hstore AS a;

RESET hstore.array_square_brackets;
RESET hstore.root_array_decorated;
RESET hstore.root_hash_decorated;
