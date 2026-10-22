 F SELECT @@innodb_hnsw_ef_search
Error Unknown system variable 'innodb_hnsw_ef_search'
 F SELECT DISTANCE(TO_VECTOR('[1]'),TO_VECTOR('[1]'),'EUCLIDEAN')
Error FUNCTION test.DISTANCE does not exist
 S SELECT status_value FROM performance_schema.keyring_component_status WHERE       status_key='component_status' rows:0
 F select @@innodb_temp_tablespace_encrypt
Error Unknown system variable 'innodb_temp_tablespace_encrypt'
seed=>42
tbs-count=>1
undo-tbs-count=>3
undo-tbs-sql=>1
jlddl=>0
no-ddl=>0
only-cl-ddl=>0
only-cl-sql=>0
no-encryption=>1
no-column-compression=>1
no-table-compression=>1
no-tbs=>1
no-blob=>0
no-virtual=>0
tables=>3
indexes=>7
columns=>7
index-columns=>10
no-auto-inc=>0
no-desc-index=>0
only-temp-tables=>0
only-partition-tables=>0
records=>10
seconds=>8
alter-table-encrypt=>0
alt-discard-tbs=>1
alter-table-engine=>1
alter-table-compress=>0
modify-column=>10
reload-keyring=>0
pk-prob=>50
set-variable=>0
rotate-master-key=>0
rotate-encryption-key=>0
rotate-gcache-key=>0
alter-redo-log=>0
rotate-redo-log-key=>0
alt-tbs-enc=>0
alt-tbs-rename=>0
alt-db-enc=>0
no-select=>0
no-insert=>0
no-update=>0
no-delete=>0
select-all-rows=>8
select-single-row=>800
insert-row=>600
update-with-cond=>200
delete-all-rows=>1
delete-with-cond=>200
log-all-queries=>1
log-query-duration=>0
log-failed-queries=>1
log-query-statistics=>0
log-query-numbers=>0
log-client-output=>0
log-succeeded-queries=>0
test-connection=>0
help=>0
pquery=>0
no-shuffle=>0
port=>3306
queries-per-thread=>80
threads=>3
verbose=>0
trx-prob-k=>1
trx-size=>10
commit-prob=>95
savepoint-prob-k=>10
check=>5
check-preload=>0
add-drop-partition=>3
max-partitions=>25
step=>2
grammar-sql=>10
drop-column=>1
add-column=>1
drop-index=>1
add-index=>1
rename-column=>1
rename-index=>1
optimize=>3
analyze=>1
truncate=>1
recreate-table=>1
exact-initial-records=>0
prepare=>0
no-temp-tables=>0
no-partition-tables=>0
no-fk-tables=>0
fk-prob=>50
partition-prob=>10
temporary-prob=>10
no-vector=>0
vector-prob=>0
vector-max-dim=>0
select-vector-ann=>0
set-hnsw-ef-search=>0
add-drop-vector-index=>0
 S USE test rows:0
Initial seed 44
CURRENT SEED IS 85136
 value of rand_int(100) 54
 S REPLACE INTO tt_3  ( ipkey ,tN200 ,iN10 ,iN23 ) VALUES( 599, 'qNNGgyLLEHdjScbZ3Vcz6ggrqtTHW0bqxa0wrfceYSvELXbm45fYcViypbkFtDQGf6DdRCZZKcUeALYdkyrF562hcnIKq5p6cBrPDMChAIqFpy676HHh7VOAkQkWLCt7x1QkWeAOoV6MepOJ7J2wt35laRMMSSZ0Cq4atbNpY5XlUoH4xffKKgnIR7jq8EPwKhSBZFkJ5yfjQQDC1ueicsT8usXdYcWevcQuJw6ej491rQnm6HtxaH4n9olQHAfbZuGPvrcXM6POkwdJYaU9fTzZSPb3M4Fj5rhY0eoNLWxpqFHOU47o0EESv4LAnEEWyzMUWqvNGknu1HHTh1izqyCwRKhpPSfJ7J41BI6Zmag1UvuMfHy90XF7u4MhYaV5oCnJLB2sqUOCH3IelaO6UVekZ8FDZQ13j3zIFo8XpnmIhuBHhIhuE96FLPmWu9LMMqSS8rVmPKzk709WmzEKzQnwDzfLK31rsaVg3gqogjysdFU9jK2UcWNxnW7lTzp', 821, 3 ) rows:1
 F SELECT * FROM tt_1_fk WHERE i1_rename = 10
Error Table 'test.tt_1_fk' doesn't exist
 S SELECT * FROM tt_1 WHERE iN184_rename = 511 rows:0
 S SELECT * FROM tt_3 WHERE iN10 = 671 rows:0
 S INSERT INTO tt_2  ( i0_rename ,mt1_rename ,i2_rename ,i3 ,iN184 ,tN240_rename ,vN112 ,tN236 ) VALUES( 215, 'xhMWLl68HfAknDh4ndzXaQ94Y1Vy8EQiCQpnselTCJX1l5xLdM6JBgUi6uVrShJ0OrnXxhtKPDBWsVq5HFiL3zMnFB89gO', 9, 1, 1, false, 'GzuvZtLG5', true ) rows:1
 S SELECT * FROM tt_1 WHERE cN11 = 'PcTxx8CIGOAIHXOPQtK' rows:0
 F REPLACE INTO tt_1_fk  ( ifk_col ,i1_rename ) VALUES( 10, 7 )
Error Table 'test.tt_1_fk' doesn't exist
 S SELECT * FROM tt_1 WHERE cN11 = 'h6PPIhG23' rows:0
