# MySQL_Tools

1. delete_cold_data : 用於刪除過期的資料 ,也可以選擇刪除前先備份到同庫的his表中
-- 使用自動模式刪除 ,並在刪除前 ,先備份到users_login_log_his表中
insert into dba_delete_data(db_name ,tb_name ,tb_type ,time_condition ,time_condition_operator ,keep_day,other_condition ,delete_per ,sleep_second)
values('users' ,'users_login_log' ,1 ,'log_dt' ,'auto' ,62 ,'' ,1000 ,0.1) ;

-- cold / his
insert into dba_cold_data(db_name ,source_table ,target_table ,time_condition ,migrate_day_ago ,batch_per ,sleep_second)
values('users' ,'users_login_log' ,'users_login_log_his' ,'gmt_created' ,365 ,10000 ,0.1) ;

-- 使用自定義的刪除條件方式
insert into dba_delete_data(db_name ,tb_name ,tb_type ,time_condition ,time_condition_operator ,time_condition_format ,keep_day,other_condition ,delete_per ,sleep_second)
values('transaction' ,'transaction_record' ,1 ,'record_id' ,' in ' ,'(select record_id from transaction_record_finished where created_date<= date_sub(current_date() ,interval 63 day) )' ,7 ,'' ,1000 ,0.1) ;

2. qps_monitor : 