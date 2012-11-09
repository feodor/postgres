select 'ff => {a,aaa}'::hstore;

select 'ff => {a=>12, b=>16}'::hstore;

select 'ff => {a=>12, b=>16}, qq=> 123'::hstore;

select 'aa => {a,aaa}, qq=>{ a=>12, b=>16 , c=> { c1, c2}, d=>{d1=>d1, d2=>d2, d1=>d3} }'::hstore;

