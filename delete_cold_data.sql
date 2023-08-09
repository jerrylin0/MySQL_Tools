
-- drop table if exists dba_delete_data ;
create table dba_delete_data(
  id int unsigned not null auto_increment primary key
 ,db_name varchar(30) not null default '' comment '庫名'
 ,tb_name varchar(50) not null default '' comment '表名'
 ,tb_type tinyint not null default 1 comment '1:一般資料表 2:實體切割子資料表 3:partition tb'
 ,time_condition varchar(50) not null default '' comment '时间欄位名稱'
 ,time_condition_operator varchar(50) not null default 'auto' comment '时间欄位比較式'
 ,time_condition_format varchar(100) not null default '' comment 'date_format'
 ,keep_day smallint unsigned not null default 70 comment '保留天数'
 ,other_condition varchar(1000) not null default '' comment '其他刪除條件'
 ,delete_per smallint unsigned not null default 10000 comment '批次删除列数'
 ,sleep_second decimal(5, 2) unsigned not null default 1 comment '每次删除后sleep秒数'
 ,state tinyint not null default 1 comment '1:排程會挑選刪除 非1:排程不會挑選刪除'
 ,work_date datetime not null default '1970-01-01' comment '工作結束時間'
 ,gmt_created timestamp not null default current_timestamp
 ,gmt_modified timestamp not null default current_timestamp on update current_timestamp
) engine=innodb charset=utf8 comment '排程刪除資料 設定表';

-- drop table if exists dba_clean_data_log ;
create table dba_clean_data_log(
  id bigint unsigned not null auto_increment primary key
 ,db_name varchar(30) not null default '' comment '庫名'
 ,tb_name varchar(50) not null default '' comment '表名'
 ,operate_type tinyint unsigned not null default 1 comment '1:表示刪除 2:表示搬移'
 ,operate_rows bigint unsigned not null default 0 comment '删除/搬移列数'
 ,consume_time int unsigned not null default 0 comment '耗時'
 ,operate_date timestamp not null default current_timestamp
 ,gmt_created timestamp not null default current_timestamp
 ,operate_sql text
) engine=innodb charset=utf8 ;

-- drop table if exists dba_cold_data ;
create table dba_cold_data(
  id int unsigned not null auto_increment primary key
 ,db_name varchar(30) not null default '' comment '庫名'
 ,source_table varchar(50) not null default '' comment '表名'
 ,target_table varchar(50) not null default '' comment '表名'
 ,time_condition varchar(50) not null default '' comment '时间欄位名稱'
 ,migrate_day_ago smallint unsigned not null default 1 comment '搬移幾天以前的資料'
 ,other_condition varchar(1000) not null default '' comment '其他搬移條件'
 ,batch_per smallint unsigned not null default 10000 comment '批次搬移列数'
 ,sleep_second decimal(5, 2) unsigned not null default 1 comment '每次搬移后sleep秒数'
 ,state tinyint not null default 1 comment '1:排程會挑選 非1:排程不會挑選'
 ,gmt_created timestamp not null default current_timestamp
 ,gmt_modified timestamp not null default current_timestamp on update current_timestamp
) engine=innodb charset=utf8 comment '排程搬移資料到his 設定表' ;



-- trigger
drop trigger if exists trg_insert_dba_delete_data ;
delimiter $$
create trigger trg_insert_dba_delete_data
before insert on dba_delete_data for each row
begin
declare msg varchar(2000) default '' ;

    select count(1) into @v_tb_cnt
    from dba_delete_data
    where db_name = new.db_name
     and tb_name = new.tb_name ;

    if @v_tb_cnt > 0 then
        set msg = concat(new.tb_name ,' has existed in schedule table !') ;
        signal sqlstate '45000' set message_text = msg ;
    end if ;

    if new.tb_type = 1 then
        if new.time_condition_format <> '' then
            set @v_cnt = 0 ;
        else
            select count(1) into @v_cnt
            from information_schema.statistics
            where table_schema = new.db_name
             and table_name = new.tb_name
             and column_name = new.time_condition
             and seq_in_index = 1 ;
        end if ;

    elseif new.tb_type = 2 then
        if new.time_condition_format = '' then
            select count(1) into @v_cnt
            from information_schema.statistics
            where table_schema = new.db_name
             and table_name = new.tb_name
             and column_name = new.time_condition
             and seq_in_index = 1 ;
        elseif new.time_condition_format <> '' and new.time_condition = 'month' then
            select count(1) into @v_cnt
            from information_schema.statistics
            where table_schema = new.db_name
             and table_name = new.tb_name
             and column_name = new.time_condition
             and seq_in_index = 1 ;
        else
            set @v_cnt = 0 ;
        end if ;

    end if ;

    if new.tb_type = 1 and @v_cnt = 0 then
        set msg = concat('Please check ' ,new.tb_name ,' table structure and indexes ') ;
        signal sqlstate '45000' set message_text = msg ;
    elseif new.tb_type = 2 and @v_cnt = 0 then
        set msg = concat('Please check ' ,new.tb_name ,' SAMPLE table structure and indexes') ;
        signal sqlstate '45000' set message_text = msg ;
    end if ;

end
$$
delimiter ;

--
drop trigger if exists trg_update_dba_delete_data ;
delimiter $$
create trigger trg_update_dba_delete_data
before update on dba_delete_data for each row
begin
declare msg varchar(2000) default '' ;
    if old.work_date = new.work_date then
        set msg = concat("Can't update schedule table ,please using delete then insert the new schedule record ! ") ;
        signal sqlstate '45000' set message_text = msg ;
     end if ;
end
$$
delimiter ;

-- procedure
drop procedure if exists sp_dba_delete_data ;
delimiter $$
create procedure sp_dba_delete_data()
begin
declare affect_rows int unsigned default 0 ;
declare total_delete_rows bigint unsigned default 0 ;
declare c_db_name varchar(30) ;
declare c_tb_name varchar(50) ;
declare c_tb_type tinyint ;
declare c_time_condition varchar(100) ;
declare c_time_condition_operator varchar(100) ;
declare c_time_condition_format varchar(200) ;
declare c_other_condition varchar(10000) ;
declare c_keep_day smallint ;
declare c_delete_per smallint ;
declare c_sleep_second decimal(5 ,2) ;
declare done1 tinyint default 0 ;

declare cur_delete_list cursor for
    select db_name ,tb_name ,tb_type ,time_condition ,time_condition_operator ,time_condition_format ,keep_day ,delete_per ,sleep_second ,other_condition
    from dba_delete_data
    where state = 1 ;
declare continue handler for not found set done1 = true ;

open cur_delete_list ;
delete_main_loop:loop
    fetch next from cur_delete_list into c_db_name ,c_tb_name ,c_tb_type ,c_time_condition ,c_time_condition_operator ,c_time_condition_format ,c_keep_day ,c_delete_per ,c_sleep_second ,c_other_condition ;
    if done1 then
        leave delete_main_loop ;
    end if ;

    -- 一般資料表刪除流程
    if c_tb_type = 1 then
        set @v_chk_dt = date(date_sub(now() ,interval c_keep_day day));
        set @sql_cmd = concat("select date(min(",c_time_condition ,")) into @v_min_dt from " ,c_db_name ,'.' ,c_tb_name) ;
        prepare stmt from @sql_cmd ;
        execute stmt ;
        deallocate prepare stmt ;


        while_loop1:while @v_min_dt <= @v_chk_dt do
            set affect_rows = 0 ;
            set total_delete_rows = 0 ;
            set @sdt = unix_timestamp(now()) ;

            -- 遷移資料到同庫的his表中
            select ifnull(id,0) into @v_cold_id
            from (
                select max(id) as id
                from dba_cold_data
                where db_name = c_db_name
                 and source_table = c_tb_name
                 )t;

            if c_time_condition_operator = 'auto' then
                select case data_type when 'date' then 1 when 'datetime' then 2 when 'timestamp' then 3 end
                 into @v_data_type
                from information_schema.columns
                where table_schema = c_db_name
                 and table_name = c_tb_name
                 and column_name = c_time_condition ;


                set @v_sdt = @v_min_dt;
                set @v_edt = date_add(@v_min_dt ,interval 1 day);

                if @v_data_type = 1 then
                    set @v_delete_condition_operator = concat(c_time_condition ," = '" ,@v_sdt ,"' ") ;
                elseif @v_data_type = 2 then
                    set @v_delete_condition_operator = concat(c_time_condition ," >= '" ,@v_sdt ,"' and " ,c_time_condition ," < '" ,@v_edt ,"'") ;
                elseif @v_data_type = 3 then
                    set @v_delete_condition_operator = concat(c_time_condition ," >= '" ,@v_sdt ,"' and " ,c_time_condition ," < '" ,@v_edt ,"'") ;
                end if ;

                if @v_cold_id > 0 then
                    call sp_dba_cold_data(@v_cold_id ,@v_sdt ,@v_delete_condition_operator ,1) ;
                end if ;
            else
                set @v_time_condition_operator = c_time_condition_operator ;
                if @v_cold_id > 0 then
                    call sp_dba_cold_data(@v_cold_id ,@v_chk_dt ,@v_time_condition_operator ,2) ;
                end if ;
            end if ;

            if c_time_condition_operator <> 'auto' then
                set @sql_cmd = concat('delete from ',c_db_name ,'.' ,c_tb_name
                    ,' where ' ,c_time_condition ,@v_time_condition_operator ,"'",@v_chk_dt ,"'"
                    ,if(c_other_condition = '' ,'' ,concat(' and ' ,c_other_condition) )
                    ,' limit ' ,c_delete_per ,' ;') ;
            else
                set @sql_cmd = concat('delete from ',c_db_name ,'.' ,c_tb_name
                    ,' where ' ,@v_delete_condition_operator
                    ,if(c_other_condition = '' ,'' ,concat(' and ' ,c_other_condition) )
                    ,' limit ' ,c_delete_per ,' ;') ;
            end if ;

            prepare stmt from @sql_cmd ;
            repeat
                execute stmt ;
                select row_count() into affect_rows ;
                set total_delete_rows = total_delete_rows + affect_rows ;
                -- select sleep(c_sleep_second) ;
            until affect_rows < c_delete_per
            end repeat ;
            deallocate prepare stmt;

            set @edt = unix_timestamp(now()) ;


            insert into dba_clean_data_log(db_name ,tb_name ,operate_type ,operate_rows ,operate_date ,operate_sql ,consume_time)
             values(c_db_name ,c_tb_name ,1 ,total_delete_rows ,if(c_time_condition_operator = 'auto' ,@v_min_dt ,@v_chk_dt) ,@sql_cmd ,@edt - @sdt) ;
            select sleep(c_sleep_second) ;

            set @sql_cmd = concat("select date(min(",c_time_condition ,")) into @v_min_dt from " ,c_db_name ,'.' ,c_tb_name ,if(c_other_condition = '' ,'' ,concat(' where ' ,c_other_condition) ) ) ;
            prepare stmt from @sql_cmd ;
            execute stmt ;
            deallocate prepare stmt ;

            if c_time_condition_operator <> 'auto' then
                leave while_loop1 ;
            end if ;
        end while ;
        update dba_delete_data set work_date = now() where db_name = c_db_name and tb_name = c_tb_name ;


    -- 實體切割子資料表刪除流程
    elseif c_tb_type = 2 then
        set affect_rows = 0 ;
        set total_delete_rows = 0 ;

        select substring(c_tb_name ,1 ,length(c_tb_name)-length(substring_index(c_tb_name,'_',-1))) into @v_parent_table_name ;
        drop temporary table if exists tmp_delete_tb_list ;
        set @sql_cmd = concat("create temporary table tmp_delete_tb_list as
            select concat(table_schema ,'.' ,table_name) as tb_name
            from information_schema.tables
            where table_schema = '" ,c_db_name ,"'and table_name regexp '" ,@v_parent_table_name ,"[a-zA-Z0-9]*$'") ;
        prepare stmt from @sql_cmd ;
        execute stmt ;
        select row_count() into affect_rows ;
        deallocate prepare stmt ;

        if affect_rows > 0 then
            begin
                declare c2_tb_name varchar(200) ;
                declare done2 tinyint default 0 ;
                declare cur_delete_list2 cursor for
                    select tb_name
                    from tmp_delete_tb_list ;
                declare continue handler for not found set done2 = true ;
                open cur_delete_list2 ;
                tenant_loop:loop
                    fetch next from cur_delete_list2 into c2_tb_name ;
                    if done2 then
                        leave tenant_loop ;
                    end if ;

                    if c_time_condition_format = '' then
                        set @v_chk_dt = date(date_sub(now() ,interval c_keep_day day));
                        set @sql_cmd = concat("select date(min(",c_time_condition ,")) into @v_min_dt from " ,c2_tb_name) ;
                        prepare stmt from @sql_cmd ;
                        execute stmt ;
                        deallocate prepare stmt ;
                    else
                        set @v_chk_dt = date_format(date(date_sub(now() ,interval c_keep_day day)) ,c_time_condition_format);
                        set @v_min_dt = @v_chk_dt ;
                    end if ;

                    while_loop2:while @v_min_dt <= @v_chk_dt do
                        set total_delete_rows = 0 ;
                        set @sdt = unix_timestamp(now()) ;
                        if c_time_condition_operator = 'auto' then
                            select case data_type when 'date' then 1 when 'datetime' then 2 when 'timestamp' then 3 when 'tinyint' then 4 end
                             into @v_data_type
                            from information_schema.columns
                            where table_schema = c_db_name
                             and table_name = substring_index(c2_tb_name,'.',-1)
                             and column_name = c_time_condition ;

                            set @v_sdt = @v_min_dt;
                            set @v_edt = date_add(@v_min_dt ,interval 1 day);

                            if @v_data_type = 1 then
                                set @v_delete_condition_operator = concat(c_time_condition ," = '" ,@v_sdt ,"' ") ;
                            elseif @v_data_type = 2 then
                                set @v_delete_condition_operator = concat(c_time_condition ," >= '" ,@v_sdt ,"' and " ,c_time_condition ," < '" ,@v_edt ,"'") ;
                            elseif @v_data_type = 3 then
                                set @v_delete_condition_operator = concat(c_time_condition ," >= '" ,@v_sdt ,"' and " ,c_time_condition ," < '" ,@v_edt ,"'") ;
                            elseif @v_data_type = 4 then
                                set @v_delete_condition_operator = concat(c_time_condition ," = " ,@v_sdt ," ") ;
                            end if ;

                        else
                            set @v_time_condition_operator = c_time_condition_operator ;
                        end if ;

                        if c_time_condition_operator <> 'auto' then
                            set @sql_cmd = concat('delete from ',c2_tb_name
                                ,' where ' ,c_time_condition ,@v_time_condition_operator ,"'" ,@v_chk_dt ,"'"
                                ,if(c_other_condition = '' ,'' ,concat(' and ' ,c_other_condition) )
                                ,' limit ' ,c_delete_per ,' ;') ;
                        else
                            set @sql_cmd = concat('delete from ',c2_tb_name
                                ,' where ' ,@v_delete_condition_operator
                                ,if(c_other_condition = '' ,'' ,concat(' and ' ,c_other_condition) )
                                ,' limit ' ,c_delete_per ,' ;') ;
                        end if ;
                        prepare stmt from @sql_cmd ;

                        repeat
                            execute stmt ;
                            select row_count() into affect_rows ;
                            set total_delete_rows = total_delete_rows + affect_rows ;
                            -- select sleep(c_sleep_second) ;
                        until affect_rows < c_delete_per
                        end repeat ;
                        deallocate prepare stmt;
                        set @edt = unix_timestamp(now()) ;

                        insert into dba_clean_data_log(db_name ,tb_name ,operate_type ,operate_rows ,operate_date ,operate_sql ,consume_time)
                         values(c_db_name ,substring_index(c2_tb_name ,'.' ,-1) ,1 ,total_delete_rows ,if(c_time_condition_operator = 'auto' ,@v_min_dt ,@v_chk_dt) ,@sql_cmd ,@edt - @sdt ) ;
                        select sleep(c_sleep_second) ;

                        set @sql_cmd = concat("select date(min(",c_time_condition ,")) into @v_min_dt from " ,c2_tb_name ,if(c_other_condition = '' ,'' ,concat(' where ' ,c_other_condition) ) ) ;
                        prepare stmt from @sql_cmd ;
                        execute stmt ;
                        deallocate prepare stmt ;
                        if c_time_condition_operator <> 'auto' then
                            leave while_loop2 ;
                        end if ;
                    end while ;
                end loop ;
                update dba_delete_data set work_date = now() where db_name = c_db_name and tb_name = c_tb_name ;
                close cur_delete_list2 ;
            end ;
        end if ;
    end if ;
end loop ;
close cur_delete_list ;
end ;
$$
delimiter ;


-- procedure
drop procedure if exists sp_dba_cold_data ;
delimiter $$
create procedure sp_dba_cold_data( v_id int ,v_dt date ,v_condition varchar(2000) ,v_type int )
begin
declare affect_rows int unsigned default 0 ;
declare total_delete_rows bigint unsigned default 0 ;
declare c_db_name varchar(30) ;
declare c_source_table varchar(50) ;
declare c_target_table varchar(50) ;
declare c_time_condition varchar(100) ;
declare c_other_condition varchar(100) ;
declare c_migrate_day_ago smallint ;
declare c_batch_per smallint ;
declare c_sleep_second decimal(5 ,2) ;
declare done1 tinyint default 0 ;

declare cur_cold_list cursor for
    select db_name ,source_table ,target_table ,time_condition ,migrate_day_ago ,batch_per ,sleep_second ,other_condition
    from dba_cold_data
    where state = 1
     and id = v_id;
declare continue handler for not found set done1 = true ;

-- can't overflow 4294967295
-- https://bugs.mysql.com/bug.php?id=102344
-- bug fix in 8.0.26
set session group_concat_max_len = 4294967295 ;

open cur_cold_list ;
cold_main_loop:loop

    fetch next from cur_cold_list into c_db_name ,c_source_table ,c_target_table ,c_time_condition ,c_migrate_day_ago ,c_batch_per ,c_sleep_second ,c_other_condition ;
    if done1 then
        leave cold_main_loop ;
    end if ;

    drop temporary table if exists tmp_delete_list ;
    create temporary table tmp_delete_list(
     id bigint unsigned auto_increment primary key
     ,list_id bigint unsigned not null default 0
     ) ;


    if v_type = 1 then
        set @sql_cmd = concat("insert into tmp_delete_list(list_id) select id from ",c_source_table
            ," where " ,v_condition );
    elseif v_type = 2 then
        set @sql_cmd = concat("insert into tmp_delete_list(list_id) select id from ",c_source_table
            ," where " ,c_time_condition ,v_condition ," '" ,v_dt ,"'");
    end if ;
    prepare stmt from @sql_cmd ;
    execute stmt ;
    deallocate prepare stmt;

    /*
    set @sql_cmd = concat("select ifnull(min(id),0) ,ifnull(max(id),0) into @v_min_id ,@v_max_id from ",c_source_table
        ," where " ,c_time_condition ," < date_sub(current_date() ,interval ",c_migrate_day_ago  ," day)");
    prepare stmt from @sql_cmd ;
    execute stmt ;
    deallocate prepare stmt;
    */

    select ifnull(min(id),0) ,ifnull(max(id),0) into @v_min_id ,@v_max_id
    from tmp_delete_list ;

    set @v_operate_rows = 0 ;
    set @sdt = unix_timestamp(now()) ;
    if (@v_min_id <> @v_max_id) or (@v_min_id = @v_max_id and @v_min_id <> 0) then
        select group_concat(concat('`',column_name,'`')) into @v_column_lists
        from information_schema.columns
        where table_schema = c_db_name
         and table_name = c_source_table ;

        while @v_min_id <= @v_max_id do
                set @sql_cmd1 = "select group_concat(list_id) into @v_id_list from tmp_delete_list where id >= ? and id < ?" ;
                prepare stmt1 from @sql_cmd1 ;
                set @v_offset = @v_min_id+c_batch_per ;
                execute stmt1 using @v_min_id ,@v_offset ;

                set @sql_cmd2 = concat("insert ignore into " ,c_target_table ,"(" ,@v_column_lists ,") select " ,@v_column_lists ," from " ,c_source_table ," where id in ( " ,@v_id_list ," )") ;
                prepare stmt2 from @sql_cmd2 ;
                execute stmt2 ;

                set @v_operate_rows = @v_operate_rows + row_count() ;
                set @v_min_id = @v_min_id + c_batch_per ;

                select sleep(c_sleep_second) ;
        end while ;
        deallocate prepare stmt1;
        deallocate prepare stmt2;

    end if ;
    set @edt = unix_timestamp(now()) ;
    insert into dba_clean_data_log(db_name ,tb_name ,operate_type ,operate_rows ,operate_date ,operate_sql ,consume_time )
     values(c_db_name ,c_source_table ,2 ,@v_operate_rows ,v_dt ,concat(@sql_cmd ,'\n' ,@sql_cmd2) ,@edt - @sdt ) ;


end loop ;
close cur_cold_list ;
end ;
$$
delimiter ;


-- event
drop event if exists event_dba_delete_data;
delimiter $$
create event event_dba_delete_data
 on schedule every 1 day starts '2022-06-05 04:00:00'
 comment 'dba delete old data schedule'
do
begin
    call sp_dba_delete_data() ;
end ;
$$
delimiter ;


