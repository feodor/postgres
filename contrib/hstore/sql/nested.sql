
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

CREATE TABLE testtype (i int, h hstore, a int[]);
INSERT INTO testtype VALUES (1, 'a=>1', '{1,2,3}');

SELECT populate_record(v, 'i=>2'::hstore) FROM testtype v;
SELECT populate_record(v, 'i=>2, a=>{7,8,9}'::hstore) FROM testtype v;
SELECT populate_record(v, 'i=>2, h=>{b=>3}, a=>{7,8,9}'::hstore) FROM testtype v;

