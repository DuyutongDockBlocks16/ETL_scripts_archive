# !/usr/bin/env python
# -*- coding: utf-8 -*-

from template.base_sql_task import *


#
# 目前支持：RUNNER_SPARK_SQL和RUNNER_HIVE
#
sql_runner=RUNNER_HIVE


def get_customized_items():
    """
     if you need some special values in your sql, please define and calculate then here
     to refer it as {YOUR_VAR} in your sql
    """
    today = Time.today()
    TX_PRE_60_DATE = Time.date_sub(date=today, itv=60)
    TX_PRE_365_DATE = Time.date_sub(date=today, itv=365)
    TX_MONTHFIRSTDATE = Time.date_sub(date=sql_task.get_tx_date(), itv=int(str(sql_task.get_tx_date())[8:10]))
    return locals()


sql_map={

     # ATTENTION:   ！！！！ sql_01  因为系统按字典顺序进行排序，小于10 的一定要写成0加编号，否则会顺序混乱，数据出问题，切记，切记！！！！
     
     "sql_01": """
    use dmf_tmp;
    drop table if exists dmf_tmp.tmp_jcw_dmfbi_profits_finance_rpt_sum_s_d_dyt_mapping;
    """,
     
    "sql_02": """
    use dmf_tmp;
    create table if not exists dmf_tmp.tmp_jcw_dmfbi_profits_finance_rpt_sum_s_d_dyt_mapping
    as 
    
    select 
    code,
    l3_code,
    l3_name,
    l4_code,
    l4_name 
    from dmf_bi.dmfbi_profits_dim_dept_mgmt_s_d
    where dt = '{TX_DATE}'
    group by code,l3_code,l3_name,l4_code,l4_name
    
    """,
    
    "sql_03": """
    use dmf_tmp;
    drop table if exists dmf_tmp.tmp_jcw_dmfbi_profits_finance_rpt_sum_s_d_dyt_detail_001;
    """,  
    
    "sql_04": """
    use dmf_tmp;
    create table if not exists dmf_tmp.tmp_jcw_dmfbi_profits_finance_rpt_sum_s_d_dyt_detail_001
    as 
    
    SELECT -------------------三方
            main.da_te  				as dim_day,
            main.index_code				as sub_code,
            main.bizline_code			as line_code,
            concat(main.health_type_new, '业务') as health_flag,
            main.bizdept             	as rpt_incm_prod_cd,
            main.inds_mark              as inds_mark,
            main.product_2              as project_lvl2,
            main.index_value			as index_value
            
            FROM
            (
            SELECT * FROM dmf_bi.dmfbi_profits_pay_pay_sum_pay_week_s_d 
            WHERE dt = '{TX_DATE}'
            and da_te>= '2021-01-01'
            )main
            

            union all

    SELECT ---------------------聚合
            main.biz_day				as dim_day,
            main.manage_subject_code	as sub_code,
            main.manage_bizline_code	as line_code,
            concat(main.is_health, '业务') as health_flag,
            main.bizdept             	as rpt_incm_prod_cd,
            ''                          as inds_mark,
            main.project_lvl2           as project_lvl2,
            main.index_value			as index_value

            FROM
            (
            SELECT * FROM dmf_bi.dmfbi_profits_pay_inds_merg_week_pay_s_d 
            WHERE dt = '{TX_DATE}'
            and biz_day>='2021-01-01'
            )main
            

            union all

    SELECT ---------------------企业支付
            main.stats_dt				as dim_day,
            main.index_code         	as sub_code,
            main.bizline_code	        as line_code,
            main.is_health              as health_flag,
            main.bizdept             	as rpt_incm_prod_cd,
            main.inds_mark              as inds_mark,
            main.project_lvl2           as project_lvl2,
            main.index_value			as index_value
            
            FROM
            (
            SELECT * FROM dmf_bi.dmfbi_profits_pay_qyzf_corp_week_rpt_s_d 
            WHERE dt = '{TX_DATE}'
            and stats_dt>='2021-01-01'
            )main
    
    """,
    
    "sql_05": """
    use dmf_tmp;
    drop table if exists dmf_tmp.tmp_jcw_dmfbi_profits_finance_rpt_sum_s_d_dyt_merge_001;
    """, 
    
    "sql_06": """
    use dmf_tmp;
    create table if not exists dmf_tmp.tmp_jcw_dmfbi_profits_finance_rpt_sum_s_d_dyt_merge_001
    as 
    
    select 
        dim_day,
        sub_code,
        line_code,
        health_flag,
        rpt_incm_prod_cd,
        inds_mark,
        project_lvl2,
        sum(index_value) as index_value
    from dmf_tmp.tmp_jcw_dmfbi_profits_finance_rpt_sum_s_d_dyt_detail_001
    group by 
        dim_day,
        sub_code,
        line_code,
        health_flag,
        rpt_incm_prod_cd,
        inds_mark,
        project_lvl2
    
    """,
    
    "sql_07": """
    use dmf_tmp;
    drop table if exists dmf_tmp.tmp_jcw_dmfbi_profits_finance_rpt_sum_s_d_dyt_merge_002;
    """, 
    
    "sql_08": """
    use dmf_tmp;
    create table if not exists dmf_tmp.tmp_jcw_dmfbi_profits_finance_rpt_sum_s_d_dyt_merge_002
    as 
    
    select 
        dim_day,
        sub_code,
        line_code as remark, ---原有业务线编码放在备注，用产品二级映射出新的业务线编码，这个逻辑来源于张惠
        health_flag,
        rpt_incm_prod_cd,
        inds_mark,
        case 
        when project_lvl2 = '京东收银产品' then 'mgt-ywx-000175'
        when project_lvl2 = '京东支付产品' then 'mgt-ywx-000176'
        when project_lvl2 = '企业支付产品' then 'mgt-ywx-000178'
        when project_lvl2 = '网银通道产品' then 'mgt-ywx-000177'
        when project_lvl2 = '银行营销产品' then 'mgt-ywx-000179' ---匹配关系from张惠
        else '业务线编码异常' end as man_line_code,
        index_value
    from dmf_tmp.tmp_jcw_dmfbi_profits_finance_rpt_sum_s_d_dyt_merge_001

    
    """,
    

     
    "sql_09": """
    use dmf_tmp;
    drop table if exists dmf_tmp.tmp_jcw_dmfbi_profits_finance_rpt_sum_s_d_dyt_merge_003;
    """,
    
    "sql_10": """
    use dmf_tmp;
    create table if not exists dmf_tmp.tmp_jcw_dmfbi_profits_finance_rpt_sum_s_d_dyt_merge_003
    as 
    
    SELECT 
            from_unixtime(unix_timestamp(), 'yyyy-MM-dd hh:mm:ss') as etl_dt,
            main.dim_day  				as dim_day,
            manaline.c1dep_code         as c1_dpt_code,
            manaline.c1dep_name         as c1_dpt_name,
            mdmsub.sub_type_code        as sub_type_code,
            mdmsub.sub_type_name        as sub_type_name,
            main.sub_code				as sub_code,
            mdmsub.sub_name        		as sub_name,
            mdmsub.sub_name_kanban      as sub_name_kanban,
            manaline.prd_code         	as dept_code,
            manaline.prd_name           as dept_name,
            manaline.prd_fst_code       as prd_fst_code,
            manaline.prd_fst_name       as prd_fst_name,
            manaline.prd_scd_code       as prd_scd_code,
            manaline.prd_scd_name       as prd_scd_name,
            ''					        as prd_thd_code,
            ''					        as prd_thd_name,
            main.man_line_code			as man_line_code,
            manaline.name               as man_line_name,
            remark                      as remark1,
            main.health_flag            as health_flag,
            mgmtdept.l3_code            as c1_ind_code,
            mgmtdept.l3_name            as c1_ind_name,
            mgmtdept.l4_code            as ind_fst_code,
            mgmtdept.l4_name            as ind_fst_name,
            ''            as ind_scd_code,
            ''            as ind_scd_name,
            ''            as ind_thd_code,
            ''            as ind_thd_name,
            manaline.product            as prd_dept_group,
            case
            when main.inds_mark = '' or main.inds_mark is null then manaline.is_supply
            else main.inds_mark end     as chanl_supply_flag,
            manaline.is_technology_business as tech_busi_flag,
            '服务业务'                  as asset_service,
            main.rpt_incm_prod_cd       as rpt_incm_prod_cd,
            manaline.prd_name           as rpt_incm_prod_nm,
            mgmtdept.l3_code            as rpt_incm_indus_cd,
            mgmtdept.l3_name            as rpt_incm_indus_nm,
            case
            when main.inds_mark = '' or main.inds_mark is null then manaline.is_supply
            else main.inds_mark end     as rpt_incm_chanl,
            'act'					    as data_type,
            main.index_value			as index_value,
            'system'					as create_user,
            from_unixtime(unix_timestamp(), 'yyyy-MM-dd hh:mm:ss') as create_time,
            'system'					as modify_user,
            from_unixtime(unix_timestamp(), 'yyyy-MM-dd hh:mm:ss') as modify_time 

            FROM
            (
                select
                *
                from dmf_tmp.tmp_jcw_dmfbi_profits_finance_rpt_sum_s_d_dyt_merge_002
            )main
            left join (SELECT * from dmf_bi.dmfbi_profits_fi_manage_line_code_dim_s_d WHERE dt = '{TX_DATE}') manaline 
            on main.man_line_code      = manaline.code    --产品维表（可用管理业务线关联）
            left join (SELECT * from dmf_tmp.tmp_jcw_dmfbi_profits_finance_rpt_sum_s_d_dyt_mapping )          mgmtdept  
            on main.rpt_incm_prod_cd   = mgmtdept.code	  --行业维表   
            left join (SELECT * from dmf_dim.dmfdim_dim_profits_mdm_mag_sub_s_d 	  WHERE dt = '{TX_DATE}') mdmsub   
            on main.sub_code           = mdmsub.sub_code--管理科目维表

    """,
    
   
    "sql_11": """
    
    alter table dmf_bi.dmfbi_profits_finance_rpt_sum_s_d drop partition (dt='{TX_DATE}',sys_src_code='zf');
    insert into table dmf_bi.dmfbi_profits_finance_rpt_sum_s_d partition(dt='{TX_DATE}',sys_src_code='zf') 
    
    select
        *
    from dmf_tmp.tmp_jcw_dmfbi_profits_finance_rpt_sum_s_d_dyt_merge_003
        
    """,
    
}


# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)