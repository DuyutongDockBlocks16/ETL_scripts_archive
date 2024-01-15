use dmc_dev;
drop table if exists dmc_dev.jh_merchant_info_w1_zh;
create table dmc_dev.jh_merchant_info_w1_zh as

SELECT
date_add(to_date(day),(6-datediff(to_date(day),'2016-01-01')%7)) as weekday,
day,
cate,
merchant_no1,
sum(gmv) as total_gmv,
sum(shouxufei) as total_shouxufei,
sum(orders) as orders_num,
sum(active_orders_day) as is_huoyue
from 
(
	select day,case
	when company='哆啦宝' then '哆啦宝'
	when sys_type='哆啦宝' then '哆啦宝-京东'
	when sys_type='乐惠' then '乐惠'
	else '区域' end as cate,
	case when sys_type='哆啦宝' then shop_no else merchant_no end as merchant_no1,
	sum(ord_amount) as gmv,
	sum(COALESCE(fee-cost,0)) as shouxufei,
	sum(case when ord_amount>=1 then 1 else 0 end)as active_orders_day,
	count(distinct order_id1) as orders
	FROM(
		select day,company,sys_type,shop_no,merchant_no,ord_amount,fee,cost,order_id1
		from dmc_bc.dmcbc_bc_sd_trade_s_d
		where dt=default.sysdate(-1)
		and day>='2021-01-01'
		and region not in ('其他')
		and refund_flag=0
		and merchant_second_no not in ( '110225410008','110225410016')
		group by day,company,sys_type,shop_no,merchant_no,ord_amount,fee,cost,order_id1) a 
		group BY day,
case
	when company='哆啦宝' then '哆啦宝'
	when sys_type='哆啦宝' then '哆啦宝-京东'
	when sys_type='乐惠' then '乐惠'
	else '区域' end,
case 
	when sys_type='哆啦宝' then shop_no 
	else merchant_no end 
)t
group BY
date_add(to_date(day),(6-datediff(to_date(day),'2016-01-01')%7)),
day,
cate,
merchant_no1;


select day,weekday,cate,sum(total_gmv) as gmv,sum(total_shouxufei) as shoruu, sum(orders_num) as num 
from dmc_dev.jh_merchant_info_w1_zh
where day between '2021-03-01' and "2021-03-31"
group by weekday,cate,day

