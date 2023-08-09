-- ---------------------------------------------------------------
-- threshold setting
-- yellow : avg + ((max - avg) * (avg / max))
-- red : avg + ((max - (avg/1.1)) * (avg / max))
-- ---------------------------------------------------------------


drop table if exists dba_monitor_qps_summary ;
create table dba_monitor_qps_summary(
 id bigint unsigned auto_increment primary key
 ,dt timestamp not null default current_timestamp
 ,com_select bigint unsigned not null default 0 
 ,com_insert bigint unsigned not null default 0 
 ,com_update bigint unsigned not null default 0 
 ,com_delete bigint unsigned not null default 0 
 ,com_insert_select bigint unsigned not null default 0 
 ,com_update_multi bigint unsigned not null default 0 
 ,com_delete_multi bigint unsigned not null default 0 
 ,com_commit bigint unsigned not null default 0
 ,wait_select bigint unsigned not null default 0 
 ,wait_insert bigint unsigned not null default 0 
 ,wait_update bigint unsigned not null default 0 
 ,wait_delete bigint unsigned not null default 0 
 ,wait_insert_select bigint unsigned not null default 0 
 ,wait_update_multi bigint unsigned not null default 0 
 ,wait_delete_multi bigint unsigned not null default 0
 ,wait_commit bigint unsigned not null default 0
 ,index idx_01(dt)
) ;


-- 將dba_monitor_qps_summary的資料進行相減 ,得出每秒鐘的相關數值
drop view if exists v_dba_monitor_qps_summary ;
create view v_dba_monitor_qps_summary
as
select x.dt 
,((x.com_select - y.com_select) + (x.com_insert - y.com_insert) + (x.com_update - y.com_update) + (x.com_delete - y.com_delete)
 + (x.com_insert_select - y.com_insert_select) + (x.com_update_multi - y.com_update_multi)
 + (x.com_delete_multi - y.com_delete_multi))
 / (unix_timestamp(x.dt) - unix_timestamp(y.dt))
 as qps_throughput
,((x.com_insert - y.com_insert) + (x.com_update - y.com_update) + (x.com_delete - y.com_delete)
 + (x.com_insert_select - y.com_insert_select) + (x.com_update_multi - y.com_update_multi)
 + (x.com_delete_multi - y.com_delete_multi))
 / (unix_timestamp(x.dt) - unix_timestamp(y.dt))
as tps_throughput
,(((x.wait_select - y.wait_select) + (x.wait_insert - y.wait_insert) + (x.wait_delete - y.wait_delete)
 + (x.wait_insert_select - y.wait_insert_select) + (x.wait_update_multi - y.wait_update_multi)
 + (x.wait_delete_multi - y.wait_delete_multi) ) / 1000000000000)
/ (unix_timestamp(x.dt) - unix_timestamp(y.dt))
as qps_latency
,(( + (x.wait_insert - y.wait_insert) + (x.wait_delete - y.wait_delete)
 + (x.wait_insert_select - y.wait_insert_select) + (x.wait_update_multi - y.wait_update_multi)
 + (x.wait_delete_multi - y.wait_delete_multi) ) / 1000000000000)
/ (unix_timestamp(x.dt) - unix_timestamp(y.dt))
as tps_latency
,(x.com_select - y.com_select) / (unix_timestamp(x.dt) - unix_timestamp(y.dt))
as select_throughput
,((x.com_insert - y.com_insert) + (x.com_insert_select - y.com_insert_select)) / (unix_timestamp(x.dt) - unix_timestamp(y.dt))
as insert_throughput
,((x.com_update - y.com_update) + (x.com_update_multi - y.com_update_multi)) / (unix_timestamp(x.dt) - unix_timestamp(y.dt))
as update_throughput
,((x.com_delete - y.com_delete) + (x.com_delete_multi - y.com_delete_multi)) / (unix_timestamp(x.dt) - unix_timestamp(y.dt))
as delete_throughput
,((x.com_commit - y.com_commit)) / (unix_timestamp(x.dt) - unix_timestamp(y.dt))
as commit_throughput
,(if(x.wait_select < y.wait_select ,0 ,x.wait_select - y.wait_select) / 1000000000000)
 / (unix_timestamp(x.dt) - unix_timestamp(y.dt))
as select_latency
,( (if(x.wait_insert < y.wait_insert ,0 ,x.wait_insert - y.wait_insert)
 + if(x.wait_insert_select < y.wait_insert_select ,0 ,x.wait_insert_select - y.wait_insert_select) ) / 1000000000000)
 / (unix_timestamp(x.dt) - unix_timestamp(y.dt))
as insert_latency
,( (if(x.wait_update < y.wait_update ,0 ,x.wait_update - y.wait_update)
 + if(x.wait_update_multi < y.wait_update_multi ,0 ,x.wait_update_multi - y.wait_update_multi) ) / 1000000000000)
 / (unix_timestamp(x.dt) - unix_timestamp(y.dt))
as update_latency
,( (if(x.wait_delete < y.wait_delete ,0 ,x.wait_delete - y.wait_delete)
 + if(x.wait_delete_multi < y.wait_delete_multi ,0 ,x.wait_delete_multi - y.wait_delete_multi) ) / 1000000000000)
 / (unix_timestamp(x.dt) - unix_timestamp(y.dt))
as delete_latency
,( if(x.wait_commit < y.wait_commit ,0 ,x.wait_commit - y.wait_commit ) / 1000000000000)
 / (unix_timestamp(x.dt) - unix_timestamp(y.dt))
as commit_latency
from dba_monitor_qps_summary x join dba_monitor_qps_summary y
 on x.id -1 = y.id  ;

-- --------------------------------------------------------------
-- 每秒搜集一次各種command執行的次數以及latency
-- --------------------------------------------------------------
drop event if exists event_dba_monitor_qps_summary ;
delimiter $$
create event event_dba_monitor_qps_summary
 on schedule every 1 second starts '2022-01-01'
do
begin
    insert into dba_monitor_qps_summary(
        dt ,com_select ,com_insert ,com_update ,com_delete ,com_insert_select ,com_update_multi ,com_delete_multi ,com_commit
        ,wait_select ,wait_insert ,wait_update ,wait_delete ,wait_insert_select ,wait_update_multi ,wait_delete_multi ,wait_commit
        )
    select now() as dt
    ,max(case when event_name = 'select' then count_star else 0 end) as com_select
    ,max(case when event_name = 'insert' then count_star else 0 end) as com_insert
    ,max(case when event_name = 'update' then count_star else 0 end) as com_update
    ,max(case when event_name = 'delete' then count_star else 0 end) as com_delete
    ,max(case when event_name = 'insert_select' then count_star else 0 end) as com_insert_select
    ,max(case when event_name = 'update_multi' then count_star else 0 end) as com_update_multi
    ,max(case when event_name = 'delete_multi' then count_star else 0 end) as com_delete_multi
    ,max(case when event_name = 'commit' then count_star else 0 end) as com_commit
    ,max(case when event_name = 'select' then sum_timer_wait else 0 end) as wait_select
    ,max(case when event_name = 'insert' then sum_timer_wait else 0 end) as wait_insert
    ,max(case when event_name = 'update' then sum_timer_wait else 0 end) as wait_update
    ,max(case when event_name = 'delete' then sum_timer_wait else 0 end) as wait_delete
    ,max(case when event_name = 'insert_select' then sum_timer_wait else 0 end) as wait_insert_select
    ,max(case when event_name = 'update_multi' then sum_timer_wait else 0 end) as wait_update_multi
    ,max(case when event_name = 'delete_multi' then sum_timer_wait else 0 end) as wait_delete_multi
    ,max(case when event_name = 'commit' then sum_timer_wait else 0 end) as wait_commit
    from (
        select replace(event_name,'statement/sql/','') as event_name ,count_star ,sum_timer_wait ,sum_lock_time
        from performance_schema.events_statements_summary_global_by_event_name
        where event_name in ( 
        'statement/sql/select'
        ,'statement/sql/insert'
        ,'statement/sql/update'
        ,'statement/sql/delete'
        ,'statement/sql/insert_select'
        ,'statement/sql/update_multi'
        ,'statement/sql/delete_multi'
        ,'statement/sql/commit'
        )
    )t
    group by now() ;
end ;
$$
delimiter ;

-- --------------------------------------------------------------
-- 每天統計一次每分鐘以及7天的latency數值
-- --------------------------------------------------------------
drop table if exists dba_monitor_qps_summary_minute ;
create table dba_monitor_qps_summary_minute(
 id bigint unsigned auto_increment primary key
 ,dt timestamp not null default current_timestamp
 ,min_select_latency decimal(18 ,6) not null default 0
 ,avg_select_latency decimal(18 ,6) not null default 0
 ,max_select_latency decimal(18 ,6) not null default 0
 ,min_insert_latency decimal(18 ,6) not null default 0
 ,avg_insert_latency decimal(18 ,6) not null default 0
 ,max_insert_latency decimal(18 ,6) not null default 0
 ,min_update_latency decimal(18 ,6) not null default 0
 ,avg_update_latency decimal(18 ,6) not null default 0
 ,max_update_latency decimal(18 ,6) not null default 0
 ,min_delete_latency decimal(18 ,6) not null default 0
 ,avg_delete_latency decimal(18 ,6) not null default 0
 ,max_delete_latency decimal(18 ,6) not null default 0
 ,min_commit_latency decimal(18 ,6) not null default 0
 ,avg_commit_latency decimal(18 ,6) not null default 0
 ,max_commit_latency decimal(18 ,6) not null default 0
 ,unique index uk_01(dt)
) ;

drop table if exists dba_monitor_qps_summary_week ;
create table dba_monitor_qps_summary_week(
 id bigint unsigned auto_increment primary key
 ,summary_week tinyint unsigned not null
 ,summary_time time not null
 ,min_select_latency decimal(18 ,6) not null default 0
 ,avg_select_latency decimal(18 ,6) not null default 0
 ,max_select_latency decimal(18 ,6) not null default 0
 ,min_insert_latency decimal(18 ,6) not null default 0
 ,avg_insert_latency decimal(18 ,6) not null default 0
 ,max_insert_latency decimal(18 ,6) not null default 0
 ,min_update_latency decimal(18 ,6) not null default 0
 ,avg_update_latency decimal(18 ,6) not null default 0
 ,max_update_latency decimal(18 ,6) not null default 0
 ,min_delete_latency decimal(18 ,6) not null default 0
 ,avg_delete_latency decimal(18 ,6) not null default 0
 ,max_delete_latency decimal(18 ,6) not null default 0
 ,min_commit_latency decimal(18 ,6) not null default 0
 ,avg_commit_latency decimal(18 ,6) not null default 0
 ,max_commit_latency decimal(18 ,6) not null default 0
 ,mdt timestamp not null default current_timestamp on update current_timestamp
 ,unique index uk_01(summary_week ,summary_time)
) ;

-- --------------------------------------------------------------
-- 每天統計一次每分鐘以及7天的latency數值
-- --------------------------------------------------------------
drop event if exists event_dba_monitor_qps_daily ;
delimiter $$
create event event_dba_monitor_qps_daily
 on schedule every 1 day starts '2022-01-01 00:00:02'
do
begin
    replace into dba_monitor_qps_summary_minute(
      dt
     ,min_select_latency ,avg_select_latency ,max_select_latency
     ,min_insert_latency ,avg_insert_latency ,max_insert_latency
     ,min_update_latency ,avg_update_latency ,max_update_latency
     ,min_delete_latency ,avg_delete_latency ,max_delete_latency
     ,min_commit_latency ,avg_commit_latency ,max_commit_latency
    )
    select date_format(dt ,'%y-%m-%d %H:%i:00') as dt
     ,min(select_latency) as min_select_latency ,avg(select_latency) as avg_select_latency ,max(select_latency) as max_select_latency
     ,min(insert_latency) as min_insert_latency ,avg(insert_latency) as avg_insert_latency ,max(insert_latency) as max_insert_latency
     ,min(update_latency) as min_update_latency ,avg(update_latency) as avg_update_latency ,max(update_latency) as max_update_latency
     ,min(delete_latency) as min_delete_latency ,avg(delete_latency) as avg_delete_latency ,max(delete_latency) as max_delete_latency
     ,min(commit_latency) as min_commit_latency ,avg(commit_latency) as avg_commit_latency ,max(commit_latency) as max_commit_latency
    from v_dba_monitor_qps_summary
    where dt >= date_add(current_date() ,interval -1 day)
     and dt < date_add(current_date() ,interval 0 day)
    group by date_format(dt ,'%y-%m-%d %H:%i:00') ;

    -- 一週的資料統計成一天
    insert into dba_monitor_qps_summary_week(
      summary_week ,summary_time
     ,min_select_latency ,avg_select_latency ,max_select_latency
     ,min_insert_latency ,avg_insert_latency ,max_insert_latency
     ,min_update_latency ,avg_update_latency ,max_update_latency
     ,min_delete_latency ,avg_delete_latency ,max_delete_latency
     ,min_commit_latency ,avg_commit_latency ,max_commit_latency
     )
    select dayofweek(date_add(now() ,interval -1 day)) ,date_format(dt ,'%H:%i:00') as dt
     ,min(select_latency) as min_select_latency ,avg(select_latency) as avg_select_latency ,max(select_latency) as max_select_latency
     ,min(insert_latency) as min_insert_latency ,avg(insert_latency) as avg_insert_latency ,max(insert_latency) as max_insert_latency
     ,min(update_latency) as min_update_latency ,avg(update_latency) as avg_update_latency ,max(update_latency) as max_update_latency
     ,min(delete_latency) as min_delete_latency ,avg(delete_latency) as avg_delete_latency ,max(delete_latency) as max_delete_latency
     ,min(commit_latency) as min_commit_latency ,avg(commit_latency) as avg_commit_latency ,max(commit_latency) as max_commit_latency
    from v_dba_monitor_qps_summary
    where dt >= date_add(current_date() ,interval -7 day)
     and dt < date_add(current_date() ,interval 0 day)
    group by dayofweek(date_add(now() ,interval -1 day)) ,date_format(dt ,'%H:%i:00')
    on duplicate key update
      min_select_latency = min_select_latency
     ,avg_select_latency = avg_select_latency
     ,max_select_latency = max_select_latency
     ,min_insert_latency = min_insert_latency
     ,avg_insert_latency = avg_insert_latency
     ,max_insert_latency = max_insert_latency
     ,min_update_latency = min_update_latency
     ,avg_update_latency = avg_update_latency
     ,max_update_latency = max_update_latency
     ,min_delete_latency = min_delete_latency
     ,avg_delete_latency = avg_delete_latency
     ,max_delete_latency = max_delete_latency
     ,min_commit_latency = min_commit_latency
     ,avg_commit_latency = avg_commit_latency
     ,max_commit_latency = max_commit_latency
     ;

    -- 七天各時間的內容統計成一筆
    insert into dba_monitor_qps_summary_week(
      summary_week ,summary_time
     ,min_select_latency ,avg_select_latency ,max_select_latency
     ,min_insert_latency ,avg_insert_latency ,max_insert_latency
     ,min_update_latency ,avg_update_latency ,max_update_latency
     ,min_delete_latency ,avg_delete_latency ,max_delete_latency
     ,min_commit_latency ,avg_commit_latency ,max_commit_latency
     )
    select '0' ,summary_time
     ,min(min_select_latency) as min_select_latency ,avg(avg_select_latency) as avg_select_latency ,max(max_select_latency) as max_select_latency
     ,min(min_insert_latency) as min_insert_latency ,avg(avg_insert_latency) as avg_insert_latency ,max(max_insert_latency) as max_insert_latency
     ,min(min_update_latency) as min_update_latency ,avg(avg_update_latency) as avg_update_latency ,max(max_update_latency) as max_update_latency
     ,min(min_delete_latency) as min_delete_latency ,avg(avg_delete_latency) as avg_delete_latency ,max(max_delete_latency) as max_delete_latency
     ,min(min_commit_latency) as min_commit_latency ,avg(avg_commit_latency) as avg_commit_latency ,max(max_commit_latency) as max_commit_latency
    from dba_monitor_qps_summary_week
    where summary_week >= 1
    group by '0' ,summary_time
    on duplicate key update
      min_select_latency = min_select_latency
     ,avg_select_latency = avg_select_latency
     ,max_select_latency = max_select_latency
     ,min_insert_latency = min_insert_latency
     ,avg_insert_latency = avg_insert_latency
     ,max_insert_latency = max_insert_latency
     ,min_update_latency = min_update_latency
     ,avg_update_latency = avg_update_latency
     ,max_update_latency = max_update_latency
     ,min_delete_latency = min_delete_latency
     ,avg_delete_latency = avg_delete_latency
     ,max_delete_latency = max_delete_latency
     ,min_commit_latency = min_commit_latency
     ,avg_commit_latency = avg_commit_latency
     ,max_commit_latency = max_commit_latency
     ;

end ;
$$
delimiter ;

-- ------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------
--
-- ------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------

drop table if exists sequence;
create table sequence (
 seq_name varchar(100) not null
,current_value int not null
,increment int not null default 1
,primary key (seq_name)
) engine = innodb ;

drop function if exists currval;
delimiter $$
create function currval (v_seq_name varchar(50))
 returns integer
begin
    declare v_value int default 0 ;

    select current_value into v_value
    from sequence
    where seq_name = v_seq_name;

    return v_value;
end
$$
delimiter ;

drop function if exists nextval ;
delimiter $$
create function nextval (v_seq_name varchar(50))
 returns integer
begin
    update sequence
    set current_value = current_value + increment
    where seq_name = v_seq_name ;

    return currval(v_seq_name) ;
end
$$
delimiter ;

drop function if exists setval ;
delimiter $$
create function setval (v_seq_name varchar(50), v_value integer)
 returns integer
begin
    update sequence
    set current_value = v_value
    where seq_name = v_seq_name ;

    return currval(v_seq_name) ;
end
$$
delimiter ;
-- ---------
-- row data
-- ---------
drop table if exists dba_monitor_status ;
create table dba_monitor_status(
 id bigint unsigned auto_increment primary key
,log_xid bigint unsigned not null
,log_yid bigint unsigned not null
,dt timestamp not null default current_timestamp
,server_name varchar(100)
,global_status json
,unique index uk_01(server_name ,log_xid)
,index idx_01(server_name ,log_yid)
) ;

insert into sequence(seq_name) values('test_db');

drop event if exists event_dba_monitor_status ;
delimiter $$
create event event_dba_monitor_status
 on schedule every 1 second starts '2022-01-01'
do
begin
    declare v_server_name varchar(100) default 'test_db' ;
    insert ignore into dba_monitor_status(log_xid ,log_yid ,dt ,server_name ,global_status)
    select @i := nextval(v_server_name) ,@i - 1 ,now() ,v_server_name ,cast(concat('{',group_concat(concat('"',variable_name ,'":"',variable_value ,'"')) ,'}') as json)
    from performance_schema.global_status
    where variable_name not like 'rsa%' ;
end
$$
delimiter ;

-- ---------
-- setting target
-- ---------
create table dba_monitor_target(
id int unsigned auto_increment primary key
,target_name varchar(50)
) ;

insert into dba_monitor_target(target_name)
values('Bytes_sent') ,('Bytes_received') ,('Questions') ;

-- ---------
-- extract target
-- ---------
drop table if exists dba_monitor_target_log ;
create table dba_monitor_target_log(
id bigint unsigned auto_increment primary key
,server_name varchar(100) not null
,dt timestamp not null default current_timestamp
,target_name varchar(100) not null
,target_value bigint unsigned not null default 0
,index idx_01(server_name ,target_name ,dt)
) ;

drop table if exists dba_monitor_server_log ;
create table dba_monitor_server_log(
 server_name varchar(100) not null primary key
,current_value bigint unsigned not null default 0
) ;

insert into dba_monitor_server_log(server_name ,current_value)
values('test_db' ,( select max(log_xid) from dba_monitor_status where server_name = 'test_db' ) ) ;

drop event if exists event_dba_monitor_target_log ;
delimiter $$
create event event_dba_monitor_target_log
 on schedule every 1 second starts '2022-01-01'
do
begin
    declare v_server_name varchar(100) default 'test_db' ;
    declare v_monitor_id bigint default 0;

    select current_value into v_monitor_id
    from dba_monitor_server_log
    where server_name = v_server_name ;

    insert ignore into dba_monitor_target_log( server_name ,dt ,target_name ,target_value )
    select
      x.server_name
     ,x.dt
     ,z.target_name
     ,if(
        cast(json_extract(x.global_status ,concat('$.',z.target_name)) as unsigned)
        < cast(json_extract(y.global_status ,concat('$.',z.target_name)) as unsigned)
        ,0
        ,cast(json_extract(x.global_status ,concat('$.',z.target_name)) as unsigned)
        - cast(json_extract(y.global_status ,concat('$.',z.target_name)) as unsigned)
     ) / (unix_timestamp(x.dt) - unix_timestamp(y.dt)) as target_value
    from dba_monitor_status x join dba_monitor_status y
     on x.log_yid = y.log_xid  and x.server_name = y.server_name ,
        (
        select @i := @i + 1 as idx ,target_name
        from dba_monitor_target ,(select @i := 0)t
        order by id asc
        ) z
    where x.log_xid = v_monitor_id and x.server_name = v_server_name ;

    update dba_monitor_server_log set current_value = current_value + 1 where server_name = v_server_name ;
end
$$
delimiter ;


-- -------------------

drop table if exists dba_monitor_status_summary_minute ;
create table dba_monitor_status_summary_minute(
 id bigint unsigned auto_increment primary key
 ,dt timestamp not null default current_timestamp
 ,server_name varchar(100)
 ,target_name varchar(100)
 ,min_value decimal(60 ,2) not null default 0
 ,avg_value decimal(60 ,2) not null default 0
 ,max_value decimal(60 ,2) not null default 0
 ,unique index uk_01(server_name ,target_name ,dt)
) ;


drop table if exists dba_monitor_status_summary_week ;
create table dba_monitor_status_summary_week(
 id bigint unsigned auto_increment primary key
 ,summary_week tinyint unsigned not null
 ,summary_time time not null
 ,server_name varchar(100)
 ,target_name varchar(100)
 ,min_value decimal(60 ,2) not null default 0
 ,avg_value decimal(60 ,2) not null default 0
 ,max_value decimal(60 ,2) not null default 0
 ,yellow_value decimal(60 ,2) as (avg_value + (max_value-avg_value)*(avg_value/max_value) )
 ,red_value decimal(60 ,2) as (avg_value + (max_value-(avg_value/1.1))*(avg_value/max_value) )
 ,mdt timestamp default current_timestamp on update current_timestamp
 ,unique index uk_01(server_name ,target_name ,summary_time ,summary_week)
) ;

drop procedure if exists sp_dba_monitor_status_summary ;
delimiter $$
create procedure sp_dba_monitor_status_summary(v_server_name varchar(100) ,v_target_name varchar(100) )
begin
    replace into dba_monitor_status_summary_minute(
      dt ,server_name ,target_name
     ,min_value ,avg_value ,max_value
    )
    select date_format(dt ,'%y-%m-%d %H:%i:00') as dt ,v_server_name ,v_target_name
     ,min(target_value) as min_value ,avg(target_value) as avg_value ,max(target_value) as max_value
    from dba_monitor_target_log
    where dt >= date_add(current_date() ,interval -1 day)
     and dt < date_add(current_date() ,interval 0 day)
     and server_name = v_server_name
     and target_name = v_target_name
    group by date_format(dt ,'%y-%m-%d %H:%i:00') ;

    -- 一週的資料統計成一天
    insert into dba_monitor_status_summary_week(
      summary_week ,summary_time ,server_name ,target_name ,min_value ,avg_value ,max_value
     )
    select dayofweek(date_add(now() ,interval -1 day)) ,date_format(dt ,'%H:%i:00') as dt ,v_server_name ,v_target_name
     ,min(target_value) as min_value ,avg(target_value) as avg_value ,max(target_value) as max_value
    from dba_monitor_target_log
    where dt >= date_add(current_date() ,interval -7 day)
     and dt < date_add(current_date() ,interval 0 day)
     and server_name = v_server_name
     and target_name = v_target_name
    group by dayofweek(date_add(now() ,interval -1 day)) ,date_format(dt ,'%H:%i:00') ,server_name ,target_name
    on duplicate key update
      min_value = min_value
     ,avg_value = avg_value
     ,max_value = max_value
     ;

    -- 七天各時間的內容統計成一筆
    insert into dba_monitor_status_summary_week(
      summary_week ,summary_time ,server_name ,target_name ,min_value ,avg_value ,max_value
     )
    select '0' ,summary_time ,v_server_name ,v_target_name
     ,min(min_value) as min_value ,avg(avg_value) as avg_value ,max(max_value) as max_value
    from dba_monitor_status_summary_week
    where summary_week >= 1
     and server_name = v_server_name
     and target_name = v_target_name
    group by '0' ,summary_time ,server_name ,target_name
    on duplicate key update
      min_value = min_value
     ,avg_value = avg_value
     ,max_value = max_value
     ;
end ;
$$
delimiter ;

-- -----------------------------------------------------------------
drop table if exists dba_monitor_match_log ;
create table dba_monitor_match_log(
id bigint unsigned auto_increment primary key
,server_name varchar(10) not null
,target_name varchar(10) not null
,alert_type tinyint unsigned not null default 1 comment '1:yellow 2:red'
,alert_value decimal(60,2) not null
,cdt timestamp not null default current_timestamp
,index idx_01(server_name ,target_name ,cdt ,alert_type)
) ;

drop table if exists dba_monitor_notification_log ;
create table dba_monitor_notification_log(
id bigint unsigned auto_increment primary key
,server_name varchar(10) not null
,target_name varchar(10) not null
,alert_type tinyint unsigned not null default 1 comment '1:yellow 2:red'
,alert_value decimal(60,2) not null
,cdt timestamp not null default current_timestamp
,index idx_01(server_name ,target_name ,cdt ,alert_type)
) ;



