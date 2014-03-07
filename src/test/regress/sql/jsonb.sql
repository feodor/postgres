-- Strings.
SELECT '""'::jsonb;				-- OK.
SELECT $$''$$::jsonb;			-- ERROR, single quotes are not allowed
SELECT '"abc"'::jsonb;			-- OK
SELECT '"abc'::jsonb;			-- ERROR, quotes not closed
SELECT '"abc
def"'::jsonb;					-- ERROR, unescaped newline in string constant
SELECT '"\n\"\\"'::jsonb;		-- OK, legal escapes
SELECT '"\v"'::jsonb;			-- ERROR, not a valid JSON escape
SELECT '"\u"'::jsonb;			-- ERROR, incomplete escape
SELECT '"\u00"'::jsonb;			-- ERROR, incomplete escape
SELECT '"\u000g"'::jsonb;		-- ERROR, g is not a hex digit
SELECT '"\u0000"'::jsonb;		-- OK, legal escape
-- use octet_length here so we don't get an odd unicode char in the
-- output
SELECT octet_length('"\uaBcD"'::jsonb::text); -- OK, uppercase and lower case both OK

-- Numbers.
SELECT '1'::jsonb;				-- OK
SELECT '0'::jsonb;				-- OK
SELECT '01'::jsonb;				-- ERROR, not valid according to JSON spec
SELECT '0.1'::jsonb;				-- OK
SELECT '9223372036854775808'::jsonb;	-- OK, even though it's too large for int8
SELECT '1e100'::jsonb;			-- OK
SELECT '1.3e100'::jsonb;			-- OK
SELECT '1f2'::jsonb;				-- ERROR
SELECT '0.x1'::jsonb;			-- ERROR
SELECT '1.3ex100'::jsonb;		-- ERROR

-- Arrays.
SELECT '[]'::jsonb;				-- OK
SELECT '[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]'::jsonb;  -- OK
SELECT '[1,2]'::jsonb;			-- OK
SELECT '[1,2,]'::jsonb;			-- ERROR, trailing comma
SELECT '[1,2'::jsonb;			-- ERROR, no closing bracket
SELECT '[1,[2]'::jsonb;			-- ERROR, no closing bracket

-- Objects.
SELECT '{}'::jsonb;				-- OK
SELECT '{"abc"}'::jsonb;			-- ERROR, no value
SELECT '{"abc":1}'::jsonb;		-- OK
SELECT '{1:"abc"}'::jsonb;		-- ERROR, keys must be strings
SELECT '{"abc",1}'::jsonb;		-- ERROR, wrong separator
SELECT '{"abc"=1}'::jsonb;		-- ERROR, totally wrong separator
SELECT '{"abc"::1}'::jsonb;		-- ERROR, another wrong separator
SELECT '{"abc":1,"def":2,"ghi":[3,4],"hij":{"klm":5,"nop":[6]}}'::jsonb; -- OK
SELECT '{"abc":1:2}'::jsonb;		-- ERROR, colon in wrong spot
SELECT '{"abc":1,3}'::jsonb;		-- ERROR, no value

-- Miscellaneous stuff.
SELECT 'true'::jsonb;			-- OK
SELECT 'false'::jsonb;			-- OK
SELECT 'null'::jsonb;			-- OK
SELECT ' true '::jsonb;			-- OK, even with extra whitespace
SELECT 'true false'::jsonb;		-- ERROR, too many values
SELECT 'true, false'::jsonb;		-- ERROR, too many values
SELECT 'truf'::jsonb;			-- ERROR, not a keyword
SELECT 'trues'::jsonb;			-- ERROR, not a keyword
SELECT ''::jsonb;				-- ERROR, no value
SELECT '    '::jsonb;			-- ERROR, no value

-- make sure jsonb is passed through json generators without being escaped
select array_to_json(ARRAY [jsonb '{"a":1}', jsonb '{"b":[2,3]}']);

-- jsonb extraction functions

CREATE TEMP TABLE test_jsonb (
       json_type text,
       test_json jsonb
);

INSERT INTO test_jsonb VALUES
('scalar','"a scalar"'),
('array','["zero", "one","two",null,"four","five"]'),
('object','{"field1":"val1","field2":"val2","field3":null}');

SELECT test_json -> 'x' FROM test_jsonb WHERE json_type = 'scalar';
SELECT test_json -> 'x' FROM test_jsonb WHERE json_type = 'array';
SELECT test_json -> 'x' FROM test_jsonb WHERE json_type = 'object';
SELECT test_json -> 'field2' FROM test_jsonb WHERE json_type = 'object';

SELECT test_json ->> 'field2' FROM test_jsonb WHERE json_type = 'scalar';
SELECT test_json ->> 'field2' FROM test_jsonb WHERE json_type = 'array';
SELECT test_json ->> 'field2' FROM test_jsonb WHERE json_type = 'object';

SELECT test_json -> 2 FROM test_jsonb WHERE json_type = 'scalar';
SELECT test_json -> 2 FROM test_jsonb WHERE json_type = 'array';
SELECT test_json -> 9 FROM test_jsonb WHERE json_type = 'array';
SELECT test_json -> 2 FROM test_jsonb WHERE json_type = 'object';

SELECT test_json ->> 2 FROM test_jsonb WHERE json_type = 'scalar';
SELECT test_json ->> 2 FROM test_jsonb WHERE json_type = 'array';
SELECT test_json ->> 2 FROM test_jsonb WHERE json_type = 'object';

SELECT jsonb_object_keys(test_json) FROM test_jsonb WHERE json_type = 'scalar';
SELECT jsonb_object_keys(test_json) FROM test_jsonb WHERE json_type = 'array';
SELECT jsonb_object_keys(test_json) FROM test_jsonb WHERE json_type = 'object';

-- nulls

select (test_json->'field3') is null as expect_false from test_jsonb where json_type = 'object';
select (test_json->>'field3') is null as expect_true from test_jsonb where json_type = 'object';
select (test_json->3) is null as expect_false from test_jsonb where json_type = 'array';
select (test_json->>3) is null as expect_true from test_jsonb where json_type = 'array';

-- equality and inequality
select '{"x":"y"}'::jsonb = '{"x":"y"}'::jsonb;
select '{"x":"y"}'::jsonb = '{"x":"z"}'::jsonb;

select '{"x":"y"}'::jsonb <> '{"x":"y"}'::jsonb;
select '{"x":"y"}'::jsonb <> '{"x":"z"}'::jsonb;

-- containment
select jsonb_contains('{"a":"b", "b":1, "c":null}', '{"a":"b"}');
select jsonb_contains('{"a":"b", "b":1, "c":null}', '{"a":"b", "c":null}');
select jsonb_contains('{"a":"b", "b":1, "c":null}', '{"a":"b", "g":null}');
select jsonb_contains('{"a":"b", "b":1, "c":null}', '{"g":null}');
select jsonb_contains('{"a":"b", "b":1, "c":null}', '{"a":"c"}');
select jsonb_contains('{"a":"b", "b":1, "c":null}', '{"a":"b"}');
select jsonb_contains('{"a":"b", "b":1, "c":null}', '{"a":"b", "c":"q"}');
select '{"a":"b", "b":1, "c":null}'::jsonb @> '{"a":"b"}';
select '{"a":"b", "b":1, "c":null}'::jsonb @> '{"a":"b", "c":null}';
select '{"a":"b", "b":1, "c":null}'::jsonb @> '{"a":"b", "g":null}';
select '{"a":"b", "b":1, "c":null}'::jsonb @> '{"g":null}';
select '{"a":"b", "b":1, "c":null}'::jsonb @> '{"a":"c"}';
select '{"a":"b", "b":1, "c":null}'::jsonb @> '{"a":"b"}';
select '{"a":"b", "b":1, "c":null}'::jsonb @> '{"a":"b", "c":"q"}';

select jsonb_contained('{"a":"b"}', '{"a":"b", "b":1, "c":null}');
select jsonb_contained('{"a":"b", "c":null}', '{"a":"b", "b":1, "c":null}');
select jsonb_contained('{"a":"b", "g":null}', '{"a":"b", "b":1, "c":null}');
select jsonb_contained('{"g":null}', '{"a":"b", "b":1, "c":null}');
select jsonb_contained('{"a":"c"}', '{"a":"b", "b":1, "c":null}');
select jsonb_contained('{"a":"b"}', '{"a":"b", "b":1, "c":null}');
select jsonb_contained('{"a":"b", "c":"q"}', '{"a":"b", "b":1, "c":null}');
select '{"a":"b"}'::jsonb <@ '{"a":"b", "b":1, "c":null}';
select '{"a":"b", "c":null}'::jsonb <@ '{"a":"b", "b":1, "c":null}';
select '{"a":"b", "g":null}'::jsonb <@ '{"a":"b", "b":1, "c":null}';
select '{"g":null}'::jsonb <@ '{"a":"b", "b":1, "c":null}';
select '{"a":"c"}'::jsonb <@ '{"a":"b", "b":1, "c":null}';
select '{"a":"b"}'::jsonb <@ '{"a":"b", "b":1, "c":null}';
select '{"a":"b", "c":"q"}'::jsonb <@ '{"a":"b", "b":1, "c":null}';

-- array length

SELECT jsonb_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]');
SELECT jsonb_array_length('[]');
SELECT jsonb_array_length('{"f1":1,"f2":[5,6]}');
SELECT jsonb_array_length('4');

-- each

select jsonb_each('{"f1":[1,2,3],"f2":{"f3":1},"f4":null}');
select * from jsonb_each('{"f1":[1,2,3],"f2":{"f3":1},"f4":null,"f5":99,"f6":"stringy"}') q;

select jsonb_each_text('{"f1":[1,2,3],"f2":{"f3":1},"f4":null,"f5":"null"}');
select * from jsonb_each_text('{"f1":[1,2,3],"f2":{"f3":1},"f4":null,"f5":99,"f6":"stringy"}') q;

-- exists

select jsonb_exists('{"a":null, "b":"qq"}', 'a');
select jsonb_exists('{"a":null, "b":"qq"}', 'b');
select jsonb_exists('{"a":null, "b":"qq"}', 'c');
select jsonb_exists('{"a":"null", "b":"qq"}', 'a');
select jsonb '{"a":null, "b":"qq"}' ? 'a';
select jsonb '{"a":null, "b":"qq"}' ? 'b';
select jsonb '{"a":null, "b":"qq"}' ? 'c';
select jsonb '{"a":"null", "b":"qq"}' ? 'a';

select jsonb_exists_any('{"a":null, "b":"qq"}', ARRAY['a','b']);
select jsonb_exists_any('{"a":null, "b":"qq"}', ARRAY['b','a']);
select jsonb_exists_any('{"a":null, "b":"qq"}', ARRAY['c','a']);
select jsonb_exists_any('{"a":null, "b":"qq"}', ARRAY['c','d']);
select jsonb_exists_any('{"a":null, "b":"qq"}', '{}'::text[]);
select jsonb '{"a":null, "b":"qq"}' ?| ARRAY['a','b'];
select jsonb '{"a":null, "b":"qq"}' ?| ARRAY['b','a'];
select jsonb '{"a":null, "b":"qq"}' ?| ARRAY['c','a'];
select jsonb '{"a":null, "b":"qq"}' ?| ARRAY['c','d'];
select jsonb '{"a":null, "b":"qq"}' ?| '{}'::text[];

select jsonb_exists_all('{"a":null, "b":"qq"}', ARRAY['a','b']);
select jsonb_exists_all('{"a":null, "b":"qq"}', ARRAY['b','a']);
select jsonb_exists_all('{"a":null, "b":"qq"}', ARRAY['c','a']);
select jsonb_exists_all('{"a":null, "b":"qq"}', ARRAY['c','d']);
select jsonb_exists_all('{"a":null, "b":"qq"}', '{}'::text[]);
select jsonb '{"a":null, "b":"qq"}' ?& ARRAY['a','b'];
select jsonb '{"a":null, "b":"qq"}' ?& ARRAY['b','a'];
select jsonb '{"a":null, "b":"qq"}' ?& ARRAY['c','a'];
select jsonb '{"a":null, "b":"qq"}' ?& ARRAY['c','d'];
select jsonb '{"a":null, "b":"qq"}' ?& '{}'::text[];

-- typeof
SELECT jsonb_typeof('{}') AS object;
SELECT jsonb_typeof('{"a":"b"}') AS object;
SELECT jsonb_typeof('[]') AS array;
SELECT jsonb_typeof('["a", 1]') AS array;
SELECT jsonb_typeof('null') AS "null";
SELECT jsonb_typeof('1.0') AS number;
SELECT jsonb_typeof('true') AS boolean;
SELECT jsonb_typeof('false') AS boolean;
SELECT jsonb_typeof('"hello"') AS string;
SELECT jsonb_typeof('"true"') AS string;
SELECT jsonb_typeof('"1.0"') AS string;


-- extract_path, extract_path_as_text

select jsonb_extract_path('{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}','f4','f6');
select jsonb_extract_path('{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}','f2');
select jsonb_extract_path('{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}','f2',0::text);
select jsonb_extract_path('{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}','f2',1::text);
select jsonb_extract_path_text('{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}','f4','f6');
select jsonb_extract_path_text('{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}','f2');
select jsonb_extract_path_text('{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}','f2',0::text);
select jsonb_extract_path_text('{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}','f2',1::text);

-- extract_path nulls

select jsonb_extract_path('{"f2":{"f3":1},"f4":{"f5":null,"f6":"stringy"}}','f4','f5') is null as expect_false;
select jsonb_extract_path_text('{"f2":{"f3":1},"f4":{"f5":null,"f6":"stringy"}}','f4','f5') is null as expect_true;
select jsonb_extract_path('{"f2":{"f3":1},"f4":[0,1,2,null]}','f4','3') is null as expect_false;
select jsonb_extract_path_text('{"f2":{"f3":1},"f4":[0,1,2,null]}','f4','3') is null as expect_true;

-- extract_path operators

select '{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::jsonb#>array['f4','f6'];
select '{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::jsonb#>array['f2'];
select '{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}'::jsonb#>array['f2','0'];
select '{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}'::jsonb#>array['f2','1'];
select '{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::jsonb#>>array['f4','f6'];
select '{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::jsonb#>>array['f2'];
select '{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}'::jsonb#>>array['f2','0'];
select '{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}'::jsonb#>>array['f2','1'];

-- same using array literals
select '{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::jsonb#>'{f4,f6}';
select '{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::jsonb#>'{f2}';
select '{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}'::jsonb#>'{f2,0}';
select '{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}'::jsonb#>'{f2,1}';
select '{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::jsonb#>>'{f4,f6}';
select '{"f2":{"f3":1},"f4":{"f5":99,"f6":"stringy"}}'::jsonb#>>'{f2}';
select '{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}'::jsonb#>>'{f2,0}';
select '{"f2":["f3",1],"f4":{"f5":99,"f6":"stringy"}}'::jsonb#>>'{f2,1}';

-- same on jsonb scalars (expecting errors)
select '42'::jsonb#>array['f2'];
select '42'::jsonb#>array['0'];
select '42'::jsonb#>>array['f2'];
select '42'::jsonb#>>array['0'];

-- array_elements

select jsonb_array_elements('[1,true,[1,[2,3]],null,{"f1":1,"f2":[7,8,9]},false]');
select * from jsonb_array_elements('[1,true,[1,[2,3]],null,{"f1":1,"f2":[7,8,9]},false]') q;
select jsonb_array_elements_text('[1,true,[1,[2,3]],null,{"f1":1,"f2":[7,8,9]},false,"stringy"]');
select * from jsonb_array_elements_text('[1,true,[1,[2,3]],null,{"f1":1,"f2":[7,8,9]},false,"stringy"]') q;


-- populate_record
create type jbpop as (a text, b int, c timestamp);

select * from jsonb_populate_record(null::jbpop,'{"a":"blurfl","x":43.2}') q;
select * from jsonb_populate_record(row('x',3,'2012-12-31 15:30:56')::jbpop,'{"a":"blurfl","x":43.2}') q;

select * from jsonb_populate_record(null::jbpop,'{"a":"blurfl","x":43.2}', true) q;
select * from jsonb_populate_record(row('x',3,'2012-12-31 15:30:56')::jbpop,'{"a":"blurfl","x":43.2}', true) q;

select * from jsonb_populate_record(null::jbpop,'{"a":[100,200,false],"x":43.2}', true) q;
select * from jsonb_populate_record(row('x',3,'2012-12-31 15:30:56')::jbpop,'{"a":[100,200,false],"x":43.2}', true) q;
select * from jsonb_populate_record(row('x',3,'2012-12-31 15:30:56')::jbpop,'{"c":[100,200,false],"x":43.2}', true) q;

-- populate_recordset

select * from jsonb_populate_recordset(null::jbpop,'[{"a":"blurfl","x":43.2},{"b":3,"c":"2012-01-20 10:42:53"}]',false) q;
select * from jsonb_populate_recordset(row('def',99,null)::jbpop,'[{"a":"blurfl","x":43.2},{"b":3,"c":"2012-01-20 10:42:53"}]',false) q;
select * from jsonb_populate_recordset(null::jbpop,'[{"a":"blurfl","x":43.2},{"b":3,"c":"2012-01-20 10:42:53"}]',true) q;
select * from jsonb_populate_recordset(row('def',99,null)::jbpop,'[{"a":"blurfl","x":43.2},{"b":3,"c":"2012-01-20 10:42:53"}]',true) q;
select * from jsonb_populate_recordset(row('def',99,null)::jbpop,'[{"a":[100,200,300],"x":43.2},{"a":{"z":true},"b":3,"c":"2012-01-20 10:42:53"}]',true) q;
select * from jsonb_populate_recordset(row('def',99,null)::jbpop,'[{"c":[100,200,300],"x":43.2},{"a":{"z":true},"b":3,"c":"2012-01-20 10:42:53"}]',true) q;

-- using the default use_json_as_text argument

select * from jsonb_populate_recordset(null::jbpop,'[{"a":"blurfl","x":43.2},{"b":3,"c":"2012-01-20 10:42:53"}]') q;
select * from jsonb_populate_recordset(row('def',99,null)::jbpop,'[{"a":"blurfl","x":43.2},{"b":3,"c":"2012-01-20 10:42:53"}]') q;
select * from jsonb_populate_recordset(row('def',99,null)::jbpop,'[{"a":[100,200,300],"x":43.2},{"a":{"z":true},"b":3,"c":"2012-01-20 10:42:53"}]') q;
select * from jsonb_populate_recordset(row('def',99,null)::jbpop,'[{"c":[100,200,300],"x":43.2},{"a":{"z":true},"b":3,"c":"2012-01-20 10:42:53"}]') q;


-- handling of unicode surrogate pairs

select octet_length((jsonb '{ "a":  "\ud83d\ude04\ud83d\udc36" }' -> 'a')::text)  as correct_in_utf8;
select jsonb '{ "a":  "\ud83d\ud83d" }' -> 'a'; -- 2 high surrogates in a row
select jsonb '{ "a":  "\ude04\ud83d" }' -> 'a'; -- surrogates in wrong order
select jsonb '{ "a":  "\ud83dX" }' -> 'a'; -- orphan high surrogate
select jsonb '{ "a":  "\ude04X" }' -> 'a'; -- orphan low surrogate

--handling of simple unicode escapes

select jsonb '{ "a":  "the Copyright \u00a9 sign" }' ->> 'a' as correct_in_utf8;
select jsonb '{ "a":  "dollar \u0024 character" }' ->> 'a' as correct_everywhere;
select jsonb '{ "a":  "null \u0000 escape" }' ->> 'a' as not_unescaped;

--jsonb_typeof() function
select value, jsonb_typeof(value)
  from (values (jsonb '123.4'),
               (jsonb '-1'),
               (jsonb '"foo"'),
               (jsonb 'true'),
               (jsonb 'false'),
               (jsonb 'null'),
               (jsonb '[1, 2, 3]'),
               (jsonb '[]'),
               (jsonb '{"x":"foo", "y":123}'),
               (jsonb '{}'),
               (NULL::jsonb))
      as data(value);

-- indexing

CREATE TABLE testjsonb (j jsonb);
\copy testjsonb from 'data/jsonb.data'

select count(*) from testjsonb where j @> '{"wait":null}';
select count(*) from testjsonb where j @> '{"wait":"CC"}';
select count(*) from testjsonb where j @> '{"wait":"CC", "public":true}';
select count(*) from testjsonb where j @> '{"age":25}';
select count(*) from testjsonb where j @> '{"age":25.0}';
select count(*) from testjsonb where j ? 'public';
select count(*) from testjsonb where j ?| ARRAY['public','disabled'];
select count(*) from testjsonb where j ?& ARRAY['public','disabled'];

create index jidx on testjsonb using gist(j);
set enable_seqscan=off;

select count(*) from testjsonb where j @> '{"wait":null}';
select count(*) from testjsonb where j @> '{"wait":"CC"}';
select count(*) from testjsonb where j @> '{"wait":"CC", "public":true}';
select count(*) from testjsonb where j @> '{"age":25}';
select count(*) from testjsonb where j @> '{"age":25.0}';
select count(*) from testjsonb where j ? 'public';
select count(*) from testjsonb where j ?| ARRAY['public','disabled'];
select count(*) from testjsonb where j ?& ARRAY['public','disabled'];

RESET enable_seqscan;

drop index jidx;
create index jidx on testjsonb using gin (j);
set enable_seqscan=off;

select count(*) from testjsonb where j @> '{"wait":null}';
select count(*) from testjsonb where j @> '{"wait":"CC"}';
select count(*) from testjsonb where j @> '{"wait":"CC", "public":true}';
select count(*) from testjsonb where j @> '{"age":25}';
select count(*) from testjsonb where j @> '{"age":25.0}';
select count(*) from testjsonb where j ? 'public';
select count(*) from testjsonb where j ?| ARRAY['public','disabled'];
select count(*) from testjsonb where j ?& ARRAY['public','disabled'];


RESET enable_seqscan;

select count(*) from (select (jsonb_each(j)).key from testjsonb) as wow;
select key, count(*) from (select (jsonb_each(j)).key from testjsonb) as wow group by key order by count desc, key;

-- sort/hash
select count(distinct j) from testjsonb;
set enable_hashagg = false;
select count(*) from (select j from (select * from testjsonb union all select * from testjsonb) js group by j) js2;
set enable_hashagg = true;
set enable_sort = false;
select count(*) from (select j from (select * from testjsonb union all select * from testjsonb) js group by j) js2;
select distinct * from (values (jsonb '{}' || ''),('{}')) v(j);
set enable_sort = true;

RESET enable_hashagg;
RESET enable_sort;

-- btree
drop index jidx;
create index jidx on testjsonb using btree (j);
set enable_seqscan=off;

select count(*) from testjsonb where j > '{"p":1}';
select count(*) from testjsonb where j = '{"pos":98, "line":371, "node":"CBA", "indexed":true}';

--gin hash
drop index jidx;
create index jidx on testjsonb using gin (j jsonb_hash_ops);
set enable_seqscan=off;

select count(*) from testjsonb where j @> '{"wait":null}';
select count(*) from testjsonb where j @> '{"wait":"CC"}';
select count(*) from testjsonb where j @> '{"wait":"CC", "public":true}';
select count(*) from testjsonb where j @> '{"age":25}';
select count(*) from testjsonb where j @> '{"age":25.0}';

RESET enable_seqscan;
drop index jidx;

-- nested tests

SELECT '{"ff":{"a":12,"b":16}}'::jsonb;
SELECT '{"ff":{"a":12,"b":16},"qq":123}'::jsonb;
SELECT '{"aa":["a","aaa"],"qq":{"a":12,"b":16,"c":["c1","c2"],"d":{"d1":"d1","d2":"d2","d1":"d3"}}}'::jsonb;
SELECT '{"aa":["a","aaa"],"qq":{"a":"12","b":"16","c":["c1","c2"],"d":{"d1":"d1","d2":"d2"}}}'::jsonb;
SELECT '{"aa":["a","aaa"],"qq":{"a":"12","b":"16","c":["c1","c2",["c3"],{"c4":4}],"d":{"d1":"d1","d2":"d2"}}}'::jsonb;
SELECT '{"ff":["a","aaa"]}'::jsonb;

SELECT
  '{"ff":{"a":12,"b":16},"qq":123,"x":[1,2],"Y":null}'::jsonb -> 'ff',
  '{"ff":{"a":12,"b":16},"qq":123,"x":[1,2],"Y":null}'::jsonb -> 'qq',
  ('{"ff":{"a":12,"b":16},"qq":123,"x":[1,2],"Y":null}'::jsonb -> 'Y') IS NULL AS f,
  ('{"ff":{"a":12,"b":16},"qq":123,"x":[1,2],"Y":null}'::jsonb ->> 'Y') IS NULL AS t,
   '{"ff":{"a":12,"b":16},"qq":123,"x":[1,2],"Y":null}'::jsonb -> 'x';

-- nested containment
SELECT '{"a":[1,2],"c":"b"}'::jsonb @> '{"a":[1,2]}';
SELECT '{"a":[2,1],"c":"b"}'::jsonb @> '{"a":[1,2]}';
SELECT '{"a":{"1":2},"c":"b"}'::jsonb @> '{"a":[1,2]}';
SELECT '{"a":{"2":1},"c":"b"}'::jsonb @> '{"a":[1,2]}';
SELECT '{"a":{"1":2},"c":"b"}'::jsonb @> '{"a":{"1":2}}';
SELECT '{"a":{"2":1},"c":"b"}'::jsonb @> '{"a":{"1":2}}';
SELECT '["a","b"]'::jsonb @> '["a","b","c","b"]';
SELECT '["a","b","c","b"]'::jsonb @> '["a","b"]';
SELECT '["a","b","c",[1,2]]'::jsonb @> '["a",[1,2]]';
SELECT '["a","b","c",[1,2]]'::jsonb @> '["b",[1,2]]';

SELECT '{"a":[1,2],"c":"b"}'::jsonb @> '{"a":[1]}';
SELECT '{"a":[1,2],"c":"b"}'::jsonb @> '{"a":[2]}';
SELECT '{"a":[1,2],"c":"b"}'::jsonb @> '{"a":[3]}';

SELECT '{"a":[1,2,{"c":3,"x":4}],"c":"b"}'::jsonb @> '{"a":[{"c":3}]}';
SELECT '{"a":[1,2,{"c":3,"x":4}],"c":"b"}'::jsonb @> '{"a":[{"x":4}]}';
SELECT '{"a":[1,2,{"c":3,"x":4}],"c":"b"}'::jsonb @> '{"a":[{"x":4},3]}';
SELECT '{"a":[1,2,{"c":3,"x":4}],"c":"b"}'::jsonb @> '{"a":[{"x":4},1]}';
