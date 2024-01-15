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
    return locals()


sql_map={

     # ATTENTION:   ！！！！ sql_01  因为系统按字典顺序进行排序，小于10 的一定要写成0加编号，否则会顺序混乱，数据出问题，切记，切记！！！！
    
    "sql_01": """
    use dmf_tmp;
    drop table if exists dmf_tmp.pay_offline_detail_001_dyt;
    """,
 
    "sql_02": """
    use dmf_tmp;
    create table if not exists dmf_tmp.pay_offline_detail_001_dyt
    as 
    select
    unequip.biz_day,
    coalesce(unequip.gmv,0.0)-coalesce(equip.gmv,0.0)-coalesce(offline.gmv,0.0) as gmv,
    unequip.gmv as bz1,
    equip.gmv as bz2,
    offline.gmv as bz3,
    coalesce(unequip.maoli,0.0)-coalesce(equip.maoli,0.0)-coalesce(offline.maoli,0.0) as maoli
    from
    (
    select---支付未剔除硬件设备业绩周报
    day as biz_day,
    sum(gmv+0.0)+0.0 as gmv,
    sum(shoruu+0.0)+0.0 as maoli
    from
    dmf_bi.dmfbi_profits_fi_pay_un_equip_week_a_d
    group by 1) unequip
    left join
    (
    select--支付聚合支付设备周报
    biz_day,
    sum(gmv+0.0)+0.0 as gmv,
    sum(maoli+0.0)+0.0 as maoli
    from
    dmf_bi.dmfbi_profits_equip_inds_week_a_d
    group by 1) equip on unequip.biz_day=equip.biz_day
    left join 
    (
    select --线下生态
    day as biz_day,
    sum(ord_amount+0.0)+0.0 as gmv,
    sum(income+0.0)+0.0 as maoli
    from
    dmf_bi.dmfbi_profits_offline_pay_week_a_d
    where hangye = '生态'
    group by 1) offline on unequip.biz_day=offline.biz_day
    """,
    
    "sql_03": """
    use dmf_tmp;
    drop table if exists dmf_tmp.pay_offline_detail_002_dyt;
    """,
    
    "sql_04": """
    use dmf_tmp;
    create table if not exists dmf_tmp.pay_offline_detail_002_dyt
    as 
     select
     biz_day,
     biz_type,
     '交易额' as index_name,  ---取设备业绩周报，gmv指标的数据
     gmv as index_value,
     sum(gmv) over(partition by yearmonth,biz_type order by biz_day ) as mtd_index_value,
     sum(gmv) over(partition by year,biz_type order by biz_day ) as ytd_index_value
     from 
     (
     select
     biz_type,
     biz_day,
     substring(biz_day,1,7) yearmonth,
     substring(biz_day,1,4) year,
     sum(gmv) as gmv
     from
     dmf_bi.dmfbi_profits_equip_inds_week_a_d
     where biz_day is not null
     group by 
     biz_type,
     biz_day
     ) a
     
     union all 
     
     select
     biz_day,
     biz_type,
     '不含税收入' as index_name,  ---取设备业绩周报，GAAP收入的数据
     maoli as index_value,
     sum(maoli) over(partition by yearmonth,biz_type order by biz_day ) as mtd_index_value,
     sum(maoli) over(partition by year,biz_type order by biz_day ) as ytd_index_value
     from 
     (
     select
     biz_type,
     biz_day,
     substring(biz_day,1,7) yearmonth,
     substring(biz_day,1,4) year,
     sum(maoli)/1.06 as maoli
     from
     dmf_bi.dmfbi_profits_equip_inds_week_a_d
     where biz_day is not null
     group by 
     biz_type,
     biz_day
     ) a
     
     union all 
     
     select
     biz_day,
     biz_type,
     '不含税收入（还原出表）' as index_name,  ---取设备业绩周报，GAAP收入的数据
     maoli as index_value,
     sum(maoli) over(partition by yearmonth,biz_type order by biz_day ) as mtd_index_value,
     sum(maoli) over(partition by year,biz_type order by biz_day ) as ytd_index_value
     from 
     (
     select
     biz_type,
     biz_day,
     substring(biz_day,1,7) yearmonth,
     substring(biz_day,1,4) year,
     sum(maoli)/1.06 as maoli
     from
     dmf_bi.dmfbi_profits_equip_inds_week_a_d
     where biz_day is not null
     group by 
     biz_type,
     biz_day
     ) a
     
     union all
     
     select
     biz_day,
     biz_type,
     '含税收入' as index_name,  ---取设备业绩周报，含税收入的数据
     maoli as index_value,
     sum(maoli) over(partition by yearmonth,biz_type order by biz_day ) as mtd_index_value,
     sum(maoli) over(partition by year,biz_type order by biz_day ) as ytd_index_value
     from 
     (
     select
     biz_type,
     biz_day,
     substring(biz_day,1,7) yearmonth,
     substring(biz_day,1,4) year,
     sum(maoli) as maoli
     from
     dmf_bi.dmfbi_profits_equip_inds_week_a_d
     where biz_day is not null
     group by 
     biz_type,
     biz_day
     ) a
     
     union all
     
     select
     biz_day,
     '生态聚合' as biz_type,
     '交易额' as index_name,  ---取生态业绩周报，生态行业，gmv指标的数据
     gmv as index_value,
     sum(gmv) over(partition by yearmonth order by biz_day ) as mtd_index_value,
     sum(gmv) over(partition by year order by biz_day ) as ytd_index_value
     from
     (
     select 
     day as biz_day,
     substring(day,1,7) yearmonth,
     substring(day,1,4) year,
     sum(ord_amount+0.0) as gmv
     from
     dmf_bi.dmfbi_profits_offline_pay_week_a_d
     where hangye = '生态'
     group by 1
     ) a
     
     union all
     
     select
     biz_day,
     '生态聚合' as biz_type,
     '含税收入' as index_name,  ---取生态业绩周报，生态行业，含税收入指标的数据
     maoli as index_value,
     sum(maoli) over(partition by yearmonth order by biz_day ) as mtd_index_value,
     sum(maoli) over(partition by year order by biz_day ) as ytd_index_value
     from
     (
     select 
     day as biz_day,
     substring(day,1,7) yearmonth,
     substring(day,1,4) year,
     sum(income+0.0) as maoli
     from
     dmf_bi.dmfbi_profits_offline_pay_week_a_d
     where hangye = '生态'
     group by 1
     ) a
     
     union all
     
     select
     biz_day,
     '生态聚合' as biz_type,
     '不含税收入' as index_name,  ---取生态业绩周报，生态行业，GAAP收入指标的数据
     maoli as index_value,
     sum(maoli) over(partition by yearmonth order by biz_day ) as mtd_index_value,
     sum(maoli) over(partition by year order by biz_day ) as ytd_index_value
     from
     (
     select 
     day as biz_day,
     substring(day,1,7) yearmonth,
     substring(day,1,4) year,
     sum(income+0.0)/1.06 as maoli --GAAP收入需要除1.06
     from
     dmf_bi.dmfbi_profits_offline_pay_week_a_d
     where hangye = '生态'
     group by 1
     ) a
     
     union all
     
     select
     biz_day,
     '生态聚合' as biz_type,
     '不含税收入（还原出表）' as index_name,  ---取生态业绩周报，生态行业，GAAP收入指标的数据
     maoli as index_value,
     sum(maoli) over(partition by yearmonth order by biz_day ) as mtd_index_value,
     sum(maoli) over(partition by year order by biz_day ) as ytd_index_value
     from
     (
     select 
     day as biz_day,
     substring(day,1,7) yearmonth,
     substring(day,1,4) year,
     sum(income+0.0)/1.06 as maoli --GAAP收入需要除1.06
     from
     dmf_bi.dmfbi_profits_offline_pay_week_a_d
     where hangye = '生态'
     group by 1
     ) a
     
     union all
     
     select
     biz_day,
     '线下聚合' as biz_type,
     '交易额' as index_name,  ---取线下聚合表（上面加工的），gmv指标的数据
     gmv as index_value,
     sum(gmv) over(partition by yearmonth order by biz_day ) as mtd_index_value,
     sum(gmv) over(partition by year order by biz_day ) as ytd_index_value
     from
     (
     select 
     biz_day as biz_day,
     substring(biz_day,1,7) yearmonth,
     substring(biz_day,1,4) year,
     sum(gmv+0.0) as gmv
     from
     dmf_tmp.pay_offline_detail_001_dyt
     group by 1
     ) a
     
     union all
     
     select
     biz_day,
     '线下聚合' as biz_type,
     '含税收入' as index_name,  ---取线下聚合表（上面加工的），不含税收入指标的数据
     maoli as index_value,
     sum(maoli) over(partition by yearmonth order by biz_day ) as mtd_index_value,
     sum(maoli) over(partition by year order by biz_day ) as ytd_index_value
     from
     (
     select 
     biz_day as biz_day,
     substring(biz_day,1,7) yearmonth,
     substring(biz_day,1,4) year,
     sum(maoli+0.0) as maoli
     from
     dmf_tmp.pay_offline_detail_001_dyt
     group by 1
     ) a
     
     union all
     
     select
     biz_day,
     '线下聚合' as biz_type,
     '不含税收入' as index_name,  ---取线下聚合表（上面加工的），GAAP收入指标的数据
     maoli as index_value,
     sum(maoli) over(partition by yearmonth order by biz_day ) as mtd_index_value,
     sum(maoli) over(partition by year order by biz_day ) as ytd_index_value
     from
     (
     select 
     biz_day as biz_day,
     substring(biz_day,1,7) yearmonth,
     substring(biz_day,1,4) year,
     sum(maoli+0.0)/1.06 as maoli --GAAP收入需要除1.06
     from
     dmf_tmp.pay_offline_detail_001_dyt
     group by 1
     ) a
     
     union all
     
     select
     biz_day,
     '线下聚合' as biz_type,
     '不含税收入（还原出表）' as index_name,  ---取线下聚合表（上面加工的），GAAP收入指标的数据
     maoli as index_value,
     sum(maoli) over(partition by yearmonth order by biz_day ) as mtd_index_value,
     sum(maoli) over(partition by year order by biz_day ) as ytd_index_value
     from
     (
     select 
     biz_day as biz_day,
     substring(biz_day,1,7) yearmonth,
     substring(biz_day,1,4) year,
     sum(maoli+0.0)/1.06 as maoli --GAAP收入需要除1.06
     from
     dmf_tmp.pay_offline_detail_001_dyt
     group by 1
     ) a
    
    """,
    
    
    "sql_05": """
    use dmf_tmp;
    drop table if exists dmf_tmp.pay_offline_detail_003_dyt;
    """,
    
    "sql_06": """
    use dmf_tmp;
    create table if not exists dmf_tmp.pay_offline_detail_003_dyt
    as 
    select ---这一步对数据做打标，打管理口径维度（科目、业务线）
    a.biz_day,
    a.biz_type,
    a.index_name,
    a.index_value,
    a.mtd_index_value,
    a.ytd_index_value,
    case when a.biz_type in ('phenixPay','pos','xintonglu','JDSM','selfhelp') then 'YWX.992.08.13'
         when a.biz_type in ('生态聚合','线下聚合') then 'YWX.992.50.52'
         else '' end as manage_bizline_code,
    case when a.index_name in ('交易额') then 'JDF.02'
         when a.index_name in ('含税收入') then 'JDF.08'
         when a.index_name in ('不含税收入') then 'JDF.09'
         when a.index_name in ('不含税收入（还原出表）') then 'JDF.32'
         else '' end as manage_subject_code,
    case when a.biz_type in ('phenixPay') then '京东收银产品'---直接平铺着写 方便之后更改
         when a.biz_type in ('pos') then '京东收银产品'
         when a.biz_type in ('xintonglu') then '京东收银产品' 
         when a.biz_type in ('JDSM') then '京东收银产品' 
         when a.biz_type in ('selfhelp') then '京东收银产品'
         when a.biz_type in ('生态聚合') then '京东收银产品'
         when a.biz_type in ('线下聚合') then '京东收银产品'
         else '' end as project_lvl2,
    case when a.biz_type in ('phenixPay') then '智能收银产品'---直接平铺着写 方便之后更改
         when a.biz_type in ('pos') then '智能收银产品'
         when a.biz_type in ('xintonglu') then '智能收银产品' 
         when a.biz_type in ('JDSM') then '智能收银产品' 
         when a.biz_type in ('selfhelp') then '智能收银产品'
         when a.biz_type in ('生态聚合') then '聚合收银产品'
         when a.biz_type in ('线下聚合') then '聚合收银产品'
         else '' end as project_lvl3,
    case when a.biz_type in ('phenixPay') then '保险事业部'---直接平铺着写 方便之后更改
         when a.biz_type in ('pos') then '金融科技群-京东生态'
         when a.biz_type in ('xintonglu') then '金融科技群-京东生态' 
         when a.biz_type in ('JDSM') then '金融科技群-京东生态' 
         when a.biz_type in ('selfhelp') then '保险事业部'
         when a.biz_type in ('生态聚合') then '金融科技群-京东生态'
         when a.biz_type in ('线下聚合') then '保险事业部'
         else '' end as hangye,
    case when a.biz_type in ('phenixPay') then '健康'---直接平铺着写 方便之后更改
         when a.biz_type in ('pos') then '健康'
         when a.biz_type in ('xintonglu') then '健康' 
         when a.biz_type in ('JDSM') then '健康' 
         when a.biz_type in ('selfhelp') then '健康'
         when a.biz_type in ('生态聚合') then '健康'
         when a.biz_type in ('线下聚合') then '健康'
         else '' end as is_health   
    from dmf_tmp.pay_offline_detail_002_dyt a
    
    """,
    
    "sql_07": """
    use dmf_tmp;
    drop table if exists dmf_tmp.pay_offline_detail_004_dyt;
    """,
    
    "sql_08": """
    use dmf_tmp;
    create table if not exists dmf_tmp.pay_offline_detail_004_dyt
    as 
    
    select 
	aaa.biz_day,
    aaa.biz_type,
    aaa.index_name,
    aaa.index_value,
    aaa.mtd_index_value,
    aaa.ytd_index_value,
    aaa.manage_bizline_code,
    aaa.manage_subject_code,
    aaa.project_lvl2,
    aaa.project_lvl3,
    aaa.hangye,
    aaa.is_health,
    biz_dept.bizdept
	from dmf_tmp.pay_offline_detail_003_dyt aaa
    left join dmf_add.dmfadd_add_pay_thd_pty_map_bizdept_a_d biz_dept on aaa.hangye=biz_dept.hangye  
    
    
    """,
    
    
    
    
    "sql_09": """
    alter table dmf_bi.dmfbi_profits_pay_inds_merg_week_pay_s_d drop partition (dt='{TX_DATE}');
    insert into table dmf_bi.dmfbi_profits_pay_inds_merg_week_pay_s_d  partition(dt='{TX_DATE}')
    select 
	biz_day,
    biz_type,
    index_name,
    index_value,
    mtd_index_value,
    ytd_index_value,
    manage_bizline_code,
    manage_subject_code,
    project_lvl2,
    project_lvl3,
    hangye,
    is_health,
    bizdept
	from 
    dmf_tmp.pay_offline_detail_004_dyt
    """,
    
}


# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)