drop table dmf_tmp.pay_offline_sum_001_dyt; 
CREATE TABLE dmf_tmp.pay_offline_sum_001_dyt AS
select
unequip.biz_day,
unequip.gmv-equip.gmv-offline.gmv+0.0 as gmv,
unequip.maoli-equip.maoli-offline.maoli+0.0 as maoli
from
(
select---支付未剔除硬件设备业绩周报
day as biz_day,
sum(gmv+0.0) as gmv,
sum(shoruu+0.0) as maoli
from
dmf_bi.dmfbi_profits_fi_pay_un_equip_week_a_d
group by 1) unequip
left join
(
select--支付聚合支付设备周报
biz_day,
sum(gmv+0.0) as gmv,
sum(maoli+0.0) as maoli
from
dmf_bi.dmfbi_profits_equip_inds_week_a_d
group by 1) equip on unequip.biz_day=equip.biz_day
left join 
(
select --线下生态
day as biz_day,
sum(ord_amount+0.0) as gmv,
sum(income+0.0) as maoli
from
dmf_bi.dmfbi_profits_offline_pay_week_a_d
where hangye = '生态'
group by 1) offline on unequip.biz_day=offline.biz_day