-- -----------------
-- pre-requirement
create table if not exists dba_chk_partition_tab_col_log(
 id int unsigned auto_increment primary key
 ,ddl_cmd text ,log_dt datetime default current_timestamp(0)
) ;

create table if not exists clean_tables (
  id int(10) unsigned not null auto_increment,
  db varchar(30) not null,
  table_name varchar(500) not null default '' ,
  count_per int(10) unsigned not null default '10000',
  state tinyint(3) unsigned not null comment '0-delete ,1-drop',
  gmt_created timestamp(3) not null default current_timestamp(3),
  gmt_modified timestamp(3) not null default current_timestamp(3) on update current_timestamp(3),
  primary key (id),
  unique key uk_db_tablename (db,table_name)
) engine=innodb default charset=utf8 ;

-- -----------
-- 從clean_tables找出有哪些實體分割表需要檢查欄位是否有缺少或型態不一致
-- 假設樣板表名order_info_test ,會找出有哪些order_info_xxx表上所有欄位是否有缺少或型態不一致

drop procedure if exists sp_dba_chk_partition_tab_col ;
delimiter $$
create procedure sp_dba_chk_partition_tab_col(db_name varchar(50))
begin
	declare done  boolean default false;
	declare tb_name varchar(50) default '' ;
	declare sql_cmd3 text ;

	declare cur1 cursor for select table_name from clean_tables where state = 1 ;
	declare continue handler for not found set done = 1 ;
	create temporary table if not exists tmp_tb1(table_name varchar(100)) ;
	-- create table if not exists tmp_tb1(table_name varchar(100)) ;

	open cur1 ;
	loop_1 : loop
		fetch cur1 into tb_name;
		if done then leave loop_1 ; end if ;
		delete from tmp_tb1 ;
		set @sql_cmd = concat("insert into tmp_tb1 
			select table_name
			from (
			select table_name ,count(1) as cnt
			from information_schema.columns
			where table_schema = '",db_name ,"'
			 and table_name in (
				select table_name
				from information_schema.tables
				where table_schema = '" ,db_name ,"'
				 and table_name regexp '^" , tb_name ,"[0-9a-z]*$'
				 and table_name <> '" ,tb_name ,"test'
			)
			group by table_name
			)t
			where cnt < (select count(1) from information_schema.columns where table_name = '" ,tb_name ,"test'
			 and table_schema ='" ,db_name ,"')
			") ;
		prepare stmt1 from @sql_cmd ;
		execute stmt1 ;
		deallocate prepare stmt1 ;


		select count(1) into @cnt from tmp_tb1 ;
		if @cnt > 0 then
		begin
			declare done2 boolean default false;
			declare tb_name2 varchar(50) default '' ;
			declare cur2 cursor for select table_name from tmp_tb1 ;
			declare continue handler for not found set done2 = 1 ;
			create temporary table if not exists tmp_tb2(ddl_cmd text) ;
			delete from tmp_tb2 ;
			-- create table if not exists tmp_tb2(ddl_cmd text) ;
			open cur2 ;
			loop_2 : loop
				fetch cur2 into tb_name2 ;
				if done2 then 
					set done2 = false ;
					leave loop_2 ;
				end if ;
				

				set @sql_cmd2 = concat("insert into tmp_tb2
					select concat('alter table ", tb_name2 ,"',group_concat(concat(if(z.column_type is null ,' add ' ,concat(' change ' ,x.column_name ,' ')) ,x.column_name ,' ' ,x.column_type ,x.default_value ,' after ' ,y.column_name) order by if(z.column_name is null ,1 ,2))
					)
					from
					(
					select column_name ,ordinal_position ,column_type 
					,case when is_nullable = 'NO' and column_default is not null then concat(' not null default ''' ,column_default ,''' ' )
					  when is_nullable = 'NO' and column_default is null then ' not null '
					  when is_nullable = 'YES' and column_default is not null then concat(' default ''' ,column_default ,''' ' )
					  else '' end as default_value
					from information_schema.columns
					where table_schema = '" ,db_name ,"'
					 and table_name = '" ,tb_name ,"test'
					) x join 
					(
					select column_name ,ordinal_position 
					from information_schema.columns
					where table_schema = '" ,db_name ,"'
					 and table_name = '" ,tb_name ,"test'
					) y
					 on x.ordinal_position = y.ordinal_position + 1  left join
					(
					select column_name ,column_type
					from information_schema.columns
					where table_schema = '" ,db_name ,"'
					 and table_name = '" ,tb_name2 ,"'
					) z
					  on x.column_name = z.column_name
					where z.column_name is null or x.column_type <> z.column_type"
					) ;

				prepare stmt2 from @sql_cmd2 ;
				execute stmt2 ;
				deallocate prepare stmt2 ;
			end loop;
			close cur2;
		end ;
		end if ;

		
		if @cnt > 0 then
			select count(1) into @cnt2 from tmp_tb2 ;
			if @cnt2 > 0 then
			begin
				declare done3 boolean default false;
				declare cur3 cursor for select ddl_cmd from tmp_tb2 ;
				declare continue handler for not found set done3 = 1 ;

				open cur3 ;
				loop_3 : loop
					fetch from cur3 into sql_cmd3 ;
					if done3 then
						set done3 = false ;
						leave loop_3 ; 
					end if ;
					set @sql_cmd3 = sql_cmd3 ;
					prepare stmt3 from @sql_cmd3 ;
					execute stmt3 ;
					deallocate prepare stmt3 ;
					insert into dba_chk_partition_tab_col_log(ddl_cmd) select @sql_cmd3 ;
				end loop;
				close cur3;
			end ;
			end if ;
		end if ;
	end loop;
	close cur1;
end;
$$
delimiter ;

-- -----------------------------------------------------
-- 從clean_tables找出有哪些實體分割表需要檢查索引是否有缺少
-- 假設樣板表名order_info_test ,會找出有哪些order_info_xxx表上所有缺少的索引

drop procedure if exists sp_dba_chk_partition_tab_idx ;
delimiter $$
create procedure sp_dba_chk_partition_tab_idx(db_name varchar(50))
begin
	declare done  boolean default false;
	declare tb_name varchar(50) default '' ;
	declare sql_cmd3 text ;

	declare cur1 cursor for select table_name from clean_tables where state = 1 ;
	declare continue handler for not found set done = 1 ;
	create temporary table if not exists tmp_tb1(table_name varchar(100)) ;
	-- create table if not exists tmp_tb1(table_name varchar(100)) ;

	open cur1 ;
	loop_1 : loop
		fetch cur1 into tb_name;
		if done then leave loop_1 ; end if ;
		delete from tmp_tb1 ;
		set @sql_cmd = concat("insert into tmp_tb1 
				select table_name
				from information_schema.tables
				where table_schema = '" ,db_name ,"'
				 and table_name regexp '^" , tb_name ,"[0-9a-z]*$'
				 and table_name <> '" ,tb_name ,"test'
			") ;
		prepare stmt1 from @sql_cmd ;
		execute stmt1 ;
		deallocate prepare stmt1 ;


		select count(1) into @cnt from tmp_tb1 ;
		if @cnt > 0 then
		begin
			declare done2 boolean default false;
			declare tb_name2 varchar(50) default '' ;
			declare cur2 cursor for select table_name from tmp_tb1 ;
			declare continue handler for not found set done2 = 1 ;
			create temporary table if not exists tmp_tb2(ddl_cmd text) ;
			delete from tmp_tb2 ;
			-- create table if not exists tmp_tb2(ddl_cmd text) ;
			open cur2 ;
			loop_2 : loop
				fetch cur2 into tb_name2 ;
				if done2 then 
					set done2 = false ;
					leave loop_2 ;
				end if ;
				
				set @sql_cmd2  = concat ("insert into tmp_tb2 select concat(' alter table ",tb_name2, "' ,group_concat(ddl_str) )
				from (
				select concat(' add index ' ,x.index_name ,'(' ,group_concat(x.column_name  order by x.seq_in_index) ,')') as ddl_str 
				from
				(
				select index_name ,seq_in_index ,column_name ,concat('_' ,replace(column_name,'_','')) as index_name2
				from information_schema.statistics 
				where table_schema = '" ,db_name ,"'
				 and table_name = '" ,tb_name ,"test'
				) x left join
				(
				select index_name ,column_name
				from information_schema.statistics 
				where table_schema = '" ,db_name ,"'
				 and table_name = '" ,tb_name2 ,"'
				) y
				 on x.index_name = y.index_name and x.column_name = y.column_name
				where y.index_name is null
				 and and y.table_schema = '",db_name ,"'
				group by x.index_name
				) z") ;

				prepare stmt2 from @sql_cmd2 ;
				execute stmt2 ;
				deallocate prepare stmt2 ;
			end loop;
			close cur2 ;
		end ;
		end if ;

		if @cnt > 0 then
			select count(1) into @cnt2 from tmp_tb2 where ddl_cmd is not null ;
			if @cnt2 > 0 then
			begin
				declare done3 boolean default false;
				declare cur3 cursor for select ddl_cmd from tmp_tb2 where ddl_cmd is not null ;
				declare continue handler for not found set done3 = 1 ;

				open cur3 ;
				loop_3 : loop
					fetch from cur3 into sql_cmd3 ;
					if done3 then
						set done3 = false ;
						leave loop_3 ; 
					end if ;
					set @sql_cmd3 = sql_cmd3 ;
					insert into dba_chk_partition_tab_col_log(ddl_cmd) select @sql_cmd3 ;
					prepare stmt3 from @sql_cmd3 ;
					execute stmt3 ;
					deallocate prepare stmt3 ;
					
				end loop;
				close cur3;
			end ;
			end if ;
		end if ;
	end loop;
	close cur1;
end ;
$$

delimiter ;



-- ----------------------------------------------------------
-- ----------------------------------------------------------
-- ----------------------------------------------------------
set @db_name = 'db_name';
set @tb_name = 'template_table_' ;
set @tb_name2 = 'template_table_beCompared' ;
set @tb_name2 = 'template_table_test' ;
select table_name
from (
select table_name ,count(1) as cnt
from information_schema.columns
where table_schema = @db_name
 and table_name in (
	select table_name
	from information_schema.tables
	where table_schema = @db_name
	 and table_name regexp '^template_table_[0-9a-z]*$'
	 and table_name <> @tb_name3
)
group by table_name
)t
where cnt < (select count(1) from information_schema.columns where table_name = @tb_name2 ) ;


-- ----------------------------------------------------------
-- 上面得到的表名稱 ,拿到下面這邊一個一個產生 alter 語句

set @sql_cmd2 = concat("
select concat('alter table ", @tb_name2 ,"'
	,group_concat(concat(if(z.column_type is null ,' add ' ,concat(' change ' ,x.column_name ,' ')) ,x.column_name ,' ' ,x.column_type ,x.default_value ,' after ' ,y.column_name) order by if(z.column_name is null ,1 ,2))
)from
(
select column_name ,ordinal_position ,column_type 
,case when is_nullable = 'NO' and column_default is not null then concat(' not null default ''' ,column_default ,''' ' )
  when is_nullable = 'NO' and column_default is null then ' not null '
  when is_nullable = 'YES' and column_default is not null then concat(' default ''' ,column_default ,''' ' )
  else '' end as default_value
from information_schema.columns
where table_schema = '" ,@db_name ,"'
 and table_name = '" ,@tb_name ,"test'
) x join 
(
select column_name ,ordinal_position 
from information_schema.columns
where table_schema = '" ,@db_name ,"'
 and table_name = '" ,@tb_name ,"test'
) y
 on x.ordinal_position = y.ordinal_position + 1  left join
(
select column_name ,column_type 
from information_schema.columns
where table_schema = '" ,@db_name ,"'
 and table_name = '" ,@tb_name2 ,"'
) z
  on x.column_name = z.column_name
where z.column_name is null or x.column_type <> z.column_type" );

