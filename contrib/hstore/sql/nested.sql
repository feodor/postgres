
SELECT 'ff => {a=>12, b=>16}'::hstore;

SELECT 'ff => {a=>12, b=>16}, qq=> 123'::hstore;

SELECT 'aa => {a,aaa}, qq=>{ a=>12, b=>16 , c=> { c1, c2}, d=>{d1=>d1, d2=>d2, d1=>d3} }'::hstore;

SELECT '"aa"=>{a,aaa}, "qq"=>{"a"=>"12", "b"=>"16", "c"=>{c1,c2}, "d"=>{"d1"=>"d1", "d2"=>"d2"}}'::hstore;

SELECT '"aa"=>{a,aaa}, "qq"=>{"a"=>"12", "b"=>"16", "c"=>{c1,c2,{c3},{c4=>4}}, "d"=>{"d1"=>"d1", "d2"=>"d2"}}'::hstore;

SELECT 'ff => {a,aaa}'::hstore;


select 'null'::hstore;
select '{null}'::hstore;
select ''::hstore;
select '{}'::hstore;

--test optional outer braces
SELECT	'a=>1'::hstore;
SELECT	'{a=>1}'::hstore;
SELECT	'{a,b}'::hstore;
SELECT	'{a,{b}}'::hstore;
SELECT	'{{a},b}'::hstore;
SELECT	'{a,{b},{c}}'::hstore;
SELECT	'{{a},{b},c}'::hstore;
SELECT	'{{a},b,{c}}'::hstore;
SELECT	'{a,{b=>1}}'::hstore;
SELECT	'{{a},{b=>1}}'::hstore;
SELECT	'a'::hstore;
SELECT	'{a}'::hstore;
SELECT	''::hstore;
SELECT	'{}'::hstore;

--nested json

SELECT	hstore_to_json('a=>1');
SELECT	hstore_to_json('{a=>1}');
SELECT	hstore_to_json('{a,b}');
SELECT	hstore_to_json('{a,{b}}');
SELECT	hstore_to_json('{{a},b}');
SELECT	hstore_to_json('{a,{b},{c}}');
SELECT	hstore_to_json('{{a},{b},c}');
SELECT	hstore_to_json('{{a},b,{c}}');
SELECT	hstore_to_json('{a,{b=>1}}');
SELECT	hstore_to_json('{{a},{b=>1}}');
SELECT	hstore_to_json('{{a},{b=>1},{c}}');
SELECT	hstore_to_json('a');
SELECT	hstore_to_json('{a}');
SELECT	hstore_to_json('');
SELECT	hstore_to_json('{}');

SELECT hstore_to_json('"aa"=>{a,aaa}, "qq"=>{"a"=>"12", "b"=>"16", "c"=>{c1,c2,{c3},{c4=>4}}, "d"=>{"d1"=>"d1", "d2"=>"d2"}}'::hstore);

--

SELECT 'ff => {a=>12, b=>16}, qq=> 123, x=>{1,2}, Y=>NULL'::hstore -> 'ff', 
	   'ff => {a=>12, b=>16}, qq=> 123, x=>{1,2}, Y=>NULL'::hstore -> 'qq', 
	   ('ff => {a=>12, b=>16}, qq=> 123, x=>{1,2}, Y=>NULL'::hstore -> 'Y') IS NULL AS t, 
	   'ff => {a=>12, b=>16}, qq=> 123, x=>{1,2}, Y=>NULL'::hstore -> 'x'; 

SELECT '[ a, b, c, d]'::hstore -> 'a';
--

CREATE TABLE testtype (i int, h hstore, a int[], j json);
INSERT INTO testtype VALUES (1, 'a=>1', '{1,2,3}', '{"x": 2}');

SELECT populate_record(v, 'i=>2'::hstore) FROM testtype v;
SELECT populate_record(v, 'i=>2, a=>{7,8,9}'::hstore) FROM testtype v;
SELECT populate_record(v, 'i=>2, h=>{b=>3}, a=>{7,8,9}'::hstore) FROM testtype v;
SELECT populate_record(v, 'i=>2, h=>{b=>3}, a=>{7,8,9}, j=>{a=>{1,2,3}}'::hstore) FROM testtype v;

--complex delete

SELECT 'b=>{a,c}'::hstore - 'a'::text;
SELECT 'b=>{a,c}, a=>1'::hstore - 'a'::text;
SELECT 'b=>{a,c}, a=>[2,3]'::hstore - 'a'::text;
SELECT 'b=>{a,c}, a=>[2,3]'::hstore - 'a'::text;
SELECT '[2,3,a]'::hstore - 'a'::text;
SELECT '[a,2,3,a]'::hstore - 'a'::text;
SELECT '[a,a]'::hstore - 'a'::text;
SELECT '[a]'::hstore - 'a'::text;
SELECT 'a=>1'::hstore - 'a'::text;
SELECT ''::hstore - 'a'::text;

SELECT '{a, 1 , b,2, c,3}'::hstore - ARRAY['d','b'];

SELECT '{a=>{1,2}, v=>23, b=>c}'::hstore - 'v'::hstore;
SELECT '{a=>{1,2}, v=>23, b=>c}'::hstore - 'v=>23'::hstore;
SELECT '{a=>{1,2}, v=>23, b=>c}'::hstore - 'v=>{1,2}'::hstore;
SELECT '{a=>{1,2}, v=>23, b=>c}'::hstore - 'a=>{1,2}'::hstore;
SELECT '{a, {1,2}, v, 23, b, c}'::hstore - 'v'::hstore;
SELECT '{a, {1,2}, v, 23, b, c}'::hstore - 'v=>23'::hstore;
SELECT '{a, {1,2}, v, 23, b, c}'::hstore - '[v,23]'::hstore;
SELECT '{a, {1,2}, v, 23, b, c}'::hstore - '[v,{1,2}]'::hstore;

--joining

SELECT 'aa=>1 , b=>2, cq=>3'::hstore || '{cq,l, b,g, fg,f, 1,2}'::hstore;
SELECT '{aa,1 , b,2, cq,3}'::hstore || '{cq,l, b,g, fg,f, 1,2}'::hstore;
SELECT  'x'::hstore || 'a=>"1"':: hstore;

--slice
SELECT slice_array(hstore 'aa=>1, b=>2, c=>3', ARRAY['g','h','i']);
SELECT slice_array(hstore '{aa,1, b,2, c,3}', ARRAY['g','h','i']);
SELECT slice_array(hstore 'aa=>1, b=>2, c=>3', ARRAY['b','c']);
SELECT slice_array(hstore '{aa,1, b,2, c,3}', ARRAY['b','c']);
SELECT slice_array(hstore 'aa=>1, b=>{2=>1}, c=>{1,2}', ARRAY['b','c']);

SELECT slice(hstore '{aa=>1, b=>2, c=>3}', ARRAY['g','h','i']);
SELECT slice(hstore '{aa,1, b,2, c,3}', ARRAY['g','h','i']);
SELECT slice(hstore '{aa=>1, b=>2, c=>3}', ARRAY['b','c']);
SELECT slice(hstore '{aa,1, b,2, c,3}', ARRAY['b','c']);
SELECT slice(hstore '{aa=>1, b=>{2=>1}, c=>{1,2}}', ARRAY['b','c']);

--to array
SELECT %% 'aa=>1, cq=>l, b=>{a,n}, fg=>NULL';
SELECT %% '{aa,1, cq,l, b,g, fg,NULL}';
SELECT hstore_to_matrix( 'aa=>1, cq=>l, b=>{a,n}, fg=>NULL');
SELECT hstore_to_matrix( '{aa,1, cq,l, b,g, fg,NULL}');


--contains
SELECT 'a=>b'::hstore @> 'a=>b, c=>b';
SELECT 'a=>b, c=>b'::hstore @> 'a=>b';
SELECT 'a=>{1,2}, c=>b'::hstore @> 'a=>{1,2}';
SELECT 'a=>{2,1}, c=>b'::hstore @> 'a=>{1,2}';
SELECT 'a=>{1=>2}, c=>b'::hstore @> 'a=>{1,2}';
SELECT 'a=>{2=>1}, c=>b'::hstore @> 'a=>{1,2}';
SELECT 'a=>{1=>2}, c=>b'::hstore @> 'a=>{1=>2}';
SELECT 'a=>{2=>1}, c=>b'::hstore @> 'a=>{1=>2}';
SELECT '{a,b}'::hstore @> '{a,b, c,b}';
SELECT '{a,b, c,b}'::hstore @> '{a,b}';
SELECT '{a,b, c,{1,2}}'::hstore @> '{a,{1,2}}';
SELECT '{a,b, c,{1,2}}'::hstore @> '{b,{1,2}}';

SELECT 'a=>{1,2}, c=>b'::hstore @> 'a=>{1}';
SELECT 'a=>{1,2}, c=>b'::hstore @> 'a=>{2}';
SELECT 'a=>{1,2}, c=>b'::hstore @> 'a=>{3}';
SELECT 'a=>{1,2,{c=>3, x=>4}}, c=>b'::hstore @> 'a=>{{c=>3}}';
SELECT 'a=>{1,2,{c=>3, x=>4}}, c=>b'::hstore @> 'a=>{{x=>4}}';
SELECT 'a=>{1,2,{c=>3, x=>4}}, c=>b'::hstore @> 'a=>{{x=>4},3}';
SELECT 'a=>{1,2,{c=>3, x=>4}}, c=>b'::hstore @> 'a=>{{x=>4},1}';

-- %>

SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore %> 'n';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore %> 'a';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore %> 'b';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore %> 'c';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore %> 'd';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore %> 'd' %> '1';

SELECT '[1,2,3,{a,b}]'::hstore %> '1';
SELECT '["1",2,3,{a,b}]'::hstore %> '1';

SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore %> 5;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore %> 4;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore %> 3;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore %> 2;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore %> 1;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore %> 0;

SELECT '[a,b, c,{1,2}, NULL]'::hstore %> 5;
SELECT '[a,b, c,{1,2}, NULL]'::hstore %> 4;
SELECT '[a,b, c,{1,2}, NULL]'::hstore %> 3;
SELECT '[a,b, c,{1,2}, NULL]'::hstore %> 2;
SELECT '[a,b, c,{1,2}, NULL]'::hstore %> 1;
SELECT '[a,b, c,{1,2}, NULL]'::hstore %> 0;

-- ->
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore -> 5;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore -> 4;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore -> 3;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore -> 2;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore -> 1;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore -> 0;

SELECT '[a,b, c,{1,2}, NULL]'::hstore -> 5;
SELECT '[a,b, c,{1,2}, NULL]'::hstore -> 4;
SELECT '[a,b, c,{1,2}, NULL]'::hstore -> 3;
SELECT '[a,b, c,{1,2}, NULL]'::hstore -> 2;
SELECT '[a,b, c,{1,2}, NULL]'::hstore -> 1;
SELECT '[a,b, c,{1,2}, NULL]'::hstore -> 0;

SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore -> -6;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore -> -5;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore -> -4;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore -> -3;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore -> -2;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore -> -1;

SELECT '[a,b, c,{1,2}, NULL]'::hstore -> -6;
SELECT '[a,b, c,{1,2}, NULL]'::hstore -> -5;
SELECT '[a,b, c,{1,2}, NULL]'::hstore -> -4;
SELECT '[a,b, c,{1,2}, NULL]'::hstore -> -3;
SELECT '[a,b, c,{1,2}, NULL]'::hstore -> -2;
SELECT '[a,b, c,{1,2}, NULL]'::hstore -> -1;

-- #>

SELECT 'a=>b, c=>{1,2,3}'::hstore #> '{0}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #> '{a}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #> '{c}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #> '{c, 0}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #> '{c, 1}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #> '{c, 2}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #> '{c, 3}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #> '{c, -1}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #> '{c, -2}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #> '{c, -3}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #> '{c, -4}';

SELECT '[0, 1, 2, {3,4}, {5=>five}]'::hstore #> '{0}';
SELECT '[0, 1, 2, {3,4}, {5=>five}]'::hstore #> '{3}';
SELECT '[0, 1, 2, {3,4}, {5=>five}]'::hstore #> '{4}';
SELECT '[0, 1, 2, {3,4}, {5=>five}]'::hstore #> '{4,5}';

-- #%>

SELECT 'a=>b, c=>{1,2,3}'::hstore #%> '{0}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #%> '{a}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #%> '{c}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #%> '{c, 0}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #%> '{c, 1}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #%> '{c, 2}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #%> '{c, 3}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #%> '{c, -1}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #%> '{c, -2}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #%> '{c, -3}';
SELECT 'a=>b, c=>{1,2,3}'::hstore #%> '{c, -4}';

SELECT '[0, 1, 2, {3,4}, {5=>five}]'::hstore #%> '{0}';
SELECT '[0, 1, 2, {3,4}, {5=>five}]'::hstore #%> '{3}';
SELECT '[0, 1, 2, {3,4}, {5=>five}]'::hstore #%> '{4}';
SELECT '[0, 1, 2, {3,4}, {5=>five}]'::hstore #%> '{4,5}';

-- ?

SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore ? 5;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore ? 4;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore ? 3;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore ? 2;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore ? 1;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore ? 0;

SELECT '[a,b, c,{1,2}, NULL]'::hstore ? 5;
SELECT '[a,b, c,{1,2}, NULL]'::hstore ? 4;
SELECT '[a,b, c,{1,2}, NULL]'::hstore ? 3;
SELECT '[a,b, c,{1,2}, NULL]'::hstore ? 2;
SELECT '[a,b, c,{1,2}, NULL]'::hstore ? 1;
SELECT '[a,b, c,{1,2}, NULL]'::hstore ? 0;

SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore ? -6;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore ? -5;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore ? -4;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore ? -3;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore ? -2;
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore ? -1;

SELECT '[a,b, c,{1,2}, NULL]'::hstore ? -6;
SELECT '[a,b, c,{1,2}, NULL]'::hstore ? -5;
SELECT '[a,b, c,{1,2}, NULL]'::hstore ? -4;
SELECT '[a,b, c,{1,2}, NULL]'::hstore ? -3;
SELECT '[a,b, c,{1,2}, NULL]'::hstore ? -2;
SELECT '[a,b, c,{1,2}, NULL]'::hstore ? -1;

SELECT 'a=>b, c=>{1,2,3}'::hstore #? '{0}'::text[];
SELECT 'a=>b, c=>{1,2,3}'::hstore #? '{a}'::text[];
SELECT 'a=>b, c=>{1,2,3}'::hstore #? '{c}'::text[];
SELECT 'a=>b, c=>{1,2,3}'::hstore #? '{b}'::text[];
SELECT 'a=>b, c=>{1,2,3}'::hstore #? '{c, 0}'::text[];
SELECT 'a=>b, c=>{1,2,3}'::hstore #? '{c, 1}'::text[];
SELECT 'a=>b, c=>{1,2,3}'::hstore #? '{c, 2}'::text[];
SELECT 'a=>b, c=>{1,2,3}'::hstore #? '{c, 3}'::text[];
SELECT 'a=>b, c=>{1,2,3}'::hstore #? '{c, -1}'::text[];
SELECT 'a=>b, c=>{1,2,3}'::hstore #? '{c, -2}'::text[];
SELECT 'a=>b, c=>{1,2,3}'::hstore #? '{c, -3}'::text[];
SELECT 'a=>b, c=>{1,2,3}'::hstore #? '{c, -4}'::text[];
SELECT 'a=>b, c=>{1,2,3}'::hstore #? '{c, -5}'::text[];

SELECT '[0, 1, 2, {3,4}, {5=>five}]'::hstore #? '{0}'::text[];
SELECT '[0, 1, 2, {3,4}, {5=>five}]'::hstore #? '{3}'::text[];
SELECT '[0, 1, 2, {3,4}, {5=>five}]'::hstore #? '{4}'::text[];
SELECT '[0, 1, 2, {3,4}, {5=>five}]'::hstore #? '{4,5}'::text[];

--deep delete

SELECT 'a=>1'::hstore #- '{x}';
SELECT 'a=>1'::hstore #- '{a}';
SELECT 'a=>1'::hstore #- '{NULL}';
SELECT 'a=>1, b=>2, c=>3'::hstore #- '{x}';
SELECT 'a=>1, b=>2, c=>3'::hstore #- '{a}';
SELECT 'a=>1, b=>2, c=>3'::hstore #- '{b}';
SELECT 'a=>1, b=>2, c=>3'::hstore #- '{c}';
SELECT 'a=>1'::hstore #- '{x,1}';
SELECT 'a=>1'::hstore #- '{a,1}';
SELECT 'a=>1'::hstore #- '{NULL,1}';
SELECT 'a=>1, b=>2, c=>3'::hstore #- '{x,1}';
SELECT 'a=>1, b=>2, c=>3'::hstore #- '{a,1}';
SELECT 'a=>1, b=>2, c=>3'::hstore #- '{b,1}';
SELECT 'a=>1, b=>2, c=>3'::hstore #- '{c,1}';

SELECT '[a]'::hstore #- '{2}';
SELECT '[a]'::hstore #- '{1}';
SELECT '[a]'::hstore #- '{0}';
SELECT '[a]'::hstore #- '{-1}';
SELECT '[a]'::hstore #- '{-2}';

SELECT '[a,b,c]'::hstore #- '{3}';
SELECT '[a,b,c]'::hstore #- '{2}';
SELECT '[a,b,c]'::hstore #- '{1}';
SELECT '[a,b,c]'::hstore #- '{0}';
SELECT '[a,b,c]'::hstore #- '{-1}';
SELECT '[a,b,c]'::hstore #- '{-2}';
SELECT '[a,b,c]'::hstore #- '{-3}';
SELECT '[a,b,c]'::hstore #- '{-4}';

SELECT '[a,b,c]'::hstore #- '{0,0}';

SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore #- '{x}';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore #- '{a}';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore #- '{b}';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore #- '{c}';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore #- '{d}';

SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore #- '{b, 0}';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore #- '{b, -1}';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore #- '{b, -1}' #- '{b, -1}';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore #- '{c, 1}';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore #- '{c, 2}';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore #- '{d, 1, -2}';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore #- '{d, 1, 1}';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore #- '{d, 1, 0}';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore #- '{d, 1, 0}' #- '{d, 1, 0}';
SELECT 'n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore #- '{d, 1, 0}' #- '{d, 1, 0}' #- '{d, 1, 0}';

-- delete(int)

SELECT '[a,b,c]'::hstore - 3;
SELECT '[a,b,c]'::hstore - 2;
SELECT '[a,b,c]'::hstore - 1;
SELECT '[a,b,c]'::hstore - 0;
SELECT '[a,b,c]'::hstore - -1;
SELECT '[a,b,c]'::hstore - -2;
SELECT '[a,b,c]'::hstore - -3;
SELECT '[a,b,c]'::hstore - -4;

SELECT 'a=>1, b=>2, c=>3'::hstore - 3;
SELECT 'a=>1, b=>2, c=>3'::hstore - 2;
SELECT 'a=>1, b=>2, c=>3'::hstore - 1;
SELECT 'a=>1, b=>2, c=>3'::hstore - 0;
SELECT 'a=>1, b=>2, c=>3'::hstore - -1;
SELECT 'a=>1, b=>2, c=>3'::hstore - -2;
SELECT 'a=>1, b=>2, c=>3'::hstore - -3;
SELECT 'a=>1, b=>2, c=>3'::hstore - -4;

--replace

SELECT replace('n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore, '{n}', '{1,2,3}');
SELECT replace('n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore, '{b,-1}', '{1,2,3}');
SELECT replace('n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore, '{d,1,0}', '{1,2,3}');
SELECT replace('n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore, '{d,NULL,0}', '{1,2,3}');

--deep concat

SELECT concat_path('n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore, '{}', 'n=>not_null');
SELECT concat_path('n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore, '{n}', 'n=>not_null');
SELECT concat_path('n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore, '{n}', 'not_null');
SELECT concat_path('n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore, '{n}', '{not_null}');
SELECT concat_path('n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore, '{}', 'b=>{3,4}');
SELECT concat_path('n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore, '{b}', '{3,4}');
SELECT concat_path('n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore, '{d,1}', '{4,5}');
SELECT concat_path('n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore, '{d,1}', '4=>5');
SELECT concat_path('n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore, '{d}', '2=>{4,5}');
SELECT concat_path('n=>NULL, a=>1, b=>{1,2}, c=>{1=>2}, d=>{1=>{2,3}}'::hstore, '{NULL,1}', '4=>5');
SELECT concat_path('x'::hstore, '{}'::text[], 'a=>"1"':: hstore);

--cast 

SELECT ('{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::text)::hstore AS err;
SELECT ('{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::json)::hstore AS ok;

--hvals

SELECT q->'tags' FROM hvals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore) AS q;

SELECT q FROM hvals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{}') AS q;
SELECT q FROM hvals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{-3}') AS q;
SELECT q FROM hvals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{-2}') AS q;
SELECT q FROM hvals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{-1}') AS q;
SELECT q FROM hvals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{0}') AS q;
SELECT q FROM hvals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{1}') AS q;
SELECT q FROM hvals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{2}') AS q;
SELECT q FROM hvals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{NULL}') AS q;
SELECT q FROM hvals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{-3,tags}') AS q;
SELECT q FROM hvals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{-2,tags}') AS q;
SELECT q FROM hvals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{-1,tags}') AS q;
SELECT q FROM hvals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{0,tags}') AS q;
SELECT q FROM hvals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{1,tags}') AS q;
SELECT q FROM hvals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{2,tags}') AS q;
SELECT q FROM hvals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{NULL,tags}') AS q;

SELECT q FROM hvals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{}') AS q;
SELECT q FROM hvals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{1}') AS q;
SELECT q FROM hvals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{a}') AS q;
SELECT q FROM hvals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{b}') AS q;
SELECT q FROM hvals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{c}') AS q;
SELECT q FROM hvals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{NULL}') AS q;
SELECT q FROM hvals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{a,c}') AS q;
SELECT q FROM hvals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{NULL,c}') AS q;
SELECT q FROM hvals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{b,NULL}') AS q;
SELECT q FROM hvals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{NULL,1}') AS q;
SELECT q FROM hvals('a=>{b=>c, c=>b, 1=>first}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{NULL,1}') AS q;

--svals path

SELECT q FROM svals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{}') AS q;
SELECT q FROM svals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{-3}') AS q;
SELECT q FROM svals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{-2}') AS q;
SELECT q FROM svals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{-1}') AS q;
SELECT q FROM svals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{0}') AS q;
SELECT q FROM svals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{1}') AS q;
SELECT q FROM svals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{2}') AS q;
SELECT q FROM svals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{NULL}') AS q;
SELECT q FROM svals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{-3,tags}') AS q;
SELECT q FROM svals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{-2,tags}') AS q;
SELECT q FROM svals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{-1,tags}') AS q;
SELECT q FROM svals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{0,tags}') AS q;
SELECT q FROM svals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{1,tags}') AS q;
SELECT q FROM svals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{2,tags}') AS q;
SELECT q FROM svals('{{tags=>1, sh=>2}, {tags=>3, sh=>4}}'::hstore, '{NULL,tags}') AS q;

SELECT q FROM svals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{}') AS q;
SELECT q FROM svals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{1}') AS q;
SELECT q FROM svals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{a}') AS q;
SELECT q FROM svals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{b}') AS q;
SELECT q FROM svals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{c}') AS q;
SELECT q FROM svals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{NULL}') AS q;
SELECT q FROM svals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{a,c}') AS q;
SELECT q FROM svals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{NULL,c}') AS q;
SELECT q FROM svals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{b,NULL}') AS q;
SELECT q FROM svals('a=>{b=>c, c=>b}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{NULL,1}') AS q;
SELECT q FROM svals('a=>{b=>c, c=>b, 1=>first}, b=>{1,2}, c=>cc, 1=>first'::hstore, '{NULL,1}') AS q;

--each

SELECT * FROM each('a=>b, c=>cc'::hstore) AS q;
SELECT * FROM each('[a, b, c, cc]'::hstore) AS q;
SELECT * FROM each('a=>{b=>c, c=>b, 1=>first}, b=>{1,2}, c=>cc, 1=>first, n=>null'::hstore) AS q;

SELECT * FROM each_hstore('a=>b, c=>cc'::hstore) AS q;
SELECT * FROM each_hstore('[a, b, c, cc]'::hstore) AS q;
SELECT * FROM each_hstore('a=>{b=>c, c=>b, 1=>first}, b=>{1,2}, c=>cc, 1=>first, n=>null'::hstore) AS q;

--decoration

SELECT 'a=>1, b=>{c=>3}, d=>[4,[5]]'::hstore AS h, '[a, {b=>c}, [c, d, e]]'::hstore AS a;

SET hstore.pretty_print = true;
SELECT 'a=>1, b=>{c=>3}, d=>[4,[5]], e=>[1,2,3,4], f=>g, g=>j'::hstore AS h, 
	   '[a, {b=>c, c=>d}, [c, d, e, [1,2], h, {f=>g, g=>f}]]'::hstore AS a;
RESET hstore.pretty_print;

SELECT hstore_print('a=>t, f=>t, t=>"f", arr=>[1,2,3,"3",x], 123=>string'::hstore);
SELECT hstore_print('a=>t, f=>t, t=>"f", arr=>[1,2,3,"3",x], 123=>string'::hstore, loose := true );
SELECT hstore_print('a=>t, f=>t, t=>"f", arr=>[1,2,3,"3",x], 123=>string'::hstore, root_hash_decorated := true );
SELECT hstore_print('a=>t, f=>t, t=>"f", arr=>[1,2,3,"3",x], 123=>string'::hstore, array_curly_braces := true );
SELECT hstore_print('a=>t, f=>t, t=>"f", arr=>[1,2,3,"3",x], 123=>string'::hstore, json := true );

SELECT hstore_print('a=>t, f=>t, t=>"f", arr=>[1,2,3,"3",x], 123=>string'::hstore, json := true, loose := true );
SELECT hstore_print('a=>t, f=>t, t=>"f", arr=>[1,2,3,"3",x], 123=>string'::hstore, json := true, root_hash_decorated := true );
SELECT hstore_print('a=>t, f=>t, t=>"f", arr=>[1,2,3,"3",x], 123=>string'::hstore, json := true, array_curly_braces := true );

SELECT hstore_print('a=>t, f=>t, t=>"f", arr=>[1,2,3,"3",x], 123=>string'::hstore, loose := true, root_hash_decorated := true );
SELECT hstore_print('a=>t, f=>t, t=>"f", arr=>[1,2,3,"3",x], 123=>string'::hstore, loose := true, array_curly_braces := true );

SELECT hstore_print('a=>t, f=>t, t=>"f", arr=>[1,2,3,"3",x], 123=>string'::hstore, root_hash_decorated := true, array_curly_braces := true );
SELECT hstore_print('a=>t, f=>t, t=>"f", arr=>[1,2,3,"3",x], 123=>string'::hstore, root_hash_decorated := true, array_curly_braces := true, loose := true);
