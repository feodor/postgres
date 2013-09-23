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


