select 'ff => {a,aaa}'::hstore;

select 'ff => {a=>12, b=>16}'::hstore;

select 'ff => {a=>12, b=>16}, qq=> 123'::hstore;

select 'aa => {a,aaa}, qq=>{ a=>12, b=>16 , c=> { c1, c2}, d=>{d1=>d1, d2=>d2, d1=>d3} }'::hstore;

select '"aa"=>{a,aaa}, "qq"=>{"a"=>"12", "b"=>"16", "c"=>{c1,c2}, "d"=>{"d1"=>"d1", "d2"=>"d2"}}'::hstore;

CREATE TABLE testtype (i int, h hstore, a int[]);
INSERT INTO testtype VALUES (1, 'a=>1', '{1,2,3}');

SELECT populate_record(v, 'i=>2'::hstore) FROM testtype v;
SELECT populate_record(v, 'i=>2, a=>{7,8,9}'::hstore) FROM testtype v;
SELECT populate_record(v, 'i=>2, h=>{b=>3}, a=>{7,8,9}'::hstore) FROM testtype v;

