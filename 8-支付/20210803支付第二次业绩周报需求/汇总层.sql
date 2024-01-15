 select
 biz_day,
 biz_type,
 'GMV' as index_name,  ---取设备业绩周报，gmv指标的数据
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
 'GAAP收入' as index_name,  ---取设备业绩周报，GAAP收入的数据
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
 '不含税收入' as index_name,  ---取设备业绩周报，不含税收入的数据
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
 'GMV' as index_name,  ---取生态业绩周报，生态行业，gmv指标的数据
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
 '不含税收入' as index_name,  ---取生态业绩周报，生态行业，不含税收入指标的数据
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
 'GAAP收入' as index_name,  ---取生态业绩周报，生态行业，GAAP收入指标的数据
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
 'GMV' as index_name,  ---取线下聚合表（上面加工的），gmv指标的数据
 gmv as index_value,
 sum(gmv) over(partition by yearmonth order by biz_day ) as mtd_index_value,
 sum(gmv) over(partition by year order by biz_day ) as ytd_index_value
 from
 (
 select 
 day as biz_day,
 substring(day,1,7) yearmonth,
 substring(day,1,4) year,
 sum(gmv+0.0) as gmv
 from
 dmf_tmp.pay_offline_sum_001_dyt
 group by 1
 ) a
 
 union all
 
 select
 biz_day,
 '生态聚合' as biz_type,
 '不含税收入' as index_name,  ---取线下聚合表（上面加工的），不含税收入指标的数据
 maoli as index_value,
 sum(maoli) over(partition by yearmonth order by biz_day ) as mtd_index_value,
 sum(maoli) over(partition by year order by biz_day ) as ytd_index_value
 from
 (
 select 
 day as biz_day,
 substring(day,1,7) yearmonth,
 substring(day,1,4) year,
 sum(maoli+0.0) as maoli
 from
 dmf_tmp.pay_offline_sum_001_dyt
 group by 1
 ) a
 
 union all
 
 select
 biz_day,
 '生态聚合' as biz_type,
 'GAAP收入' as index_name,  ---取线下聚合表（上面加工的），GAAP收入指标的数据
 maoli as index_value,
 sum(maoli) over(partition by yearmonth order by biz_day ) as mtd_index_value,
 sum(maoli) over(partition by year order by biz_day ) as ytd_index_value
 from
 (
 select 
 day as biz_day,
 substring(day,1,7) yearmonth,
 substring(day,1,4) year,
 sum(maoli+0.0)/1.06 as maoli --GAAP收入需要除1.06
 from
 dmf_tmp.pay_offline_sum_001_dyt
 group by 1
 ) a