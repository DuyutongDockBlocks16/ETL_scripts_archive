----------------代码开始
use dmc_dev;
drop table dmc_dev.dmcbc_bc_pay_pay_rpt_i_d_1_new_zh;
create table dmc_dev.dmcbc_bc_pay_pay_rpt_i_d_1_new_zh as
select
  dt as da_te,
  trade_name,
  product_id,
  mer_id,
  pay_tools,
  sum(order_amt) as order_amt,
  sum(trade_amt) as trade_amt,
  sum(mer_fee) as mer_fee,
  sum(bank_fee) as bank_fee,
  sum(trade_num) as trade_num,
  mer_big_type,
  mer_sma_type,
  prod_type
from
  dmf_bc.dmfbc_oar_bc_pay_sis_fin_rpt_i_d
where
  dt >= '2019-01-01'
  and dept_nm = '支付业务部'
group by
  dt,
  mer_id,
  trade_name,
  product_id,
  pay_tools,
  mer_big_type,
  mer_sma_type,
  prod_type 



----建 表 导 入 行 业 商 户 号 
use dmc_dev;
drop table if exists dmc_dev.mer_hangye_new_zh;
create table dmc_dev.mer_hangye_new_zh(mer_id string, hangye string, health_type string) row format delimited fields terminated by '\t' stored as textfile;

终 端 ： LOAD DATA LOCAL INPATH '/soft/data/商户号行业维表 - 新.txt' INTO TABLE dmc_dev.mer_hangye_new_zh;


-----关 联 行 业 
use dmc_dev;
drop table dmc_dev.dmcbc_bc_pay_pay_rpt_i_d_2_new_zh;
create table dmc_dev.dmcbc_bc_pay_pay_rpt_i_d_2_new_zh as
select
a.*,
case when a.prod_type='线上-通道业务-POP代付有收入' then '京东生态' else b.hangye end as hangye,
b.health_type
from
dmc_dev.dmcbc_bc_pay_pay_rpt_i_d_1_new_zh a
left join dmc_dev.mer_hangye_new_zh b on a.mer_id = b.mer_id

----建 表 导 入 关 联 表 1 
use dmc_dev;
drop table if exists dmc_dev.guanlianbia_new_zh;
create table dmc_dev.guanlianbia_new_zh(glzd string, produc_1 string, produc_2 string, health_type string) 
row format delimited fields terminated by '\t' stored as textfile;

终 端 ： LOAD DATA LOCAL INPATH '/soft/data/产品维表 - 新.txt' INTO TABLE dmc_dev.guanlianbia_new_zh;


----建 表 导 入 关 联 表 2 
use dmc_dev;
drop table if exists dmc_dev.guanlianbia2_new_zh;
create table dmc_dev.guanlianbia2_new_zh(glzd string, hangye string) row format delimited fields terminated by '\t' stored as textfile;

终 端 ： LOAD DATA LOCAL INPATH '/soft/data/行业关联表 - 新.txt' INTO TABLE dmc_dev.guanlianbia2_new_zh;




---------------添 加 产 品 和 行 业 
use dmc_dev;
drop table dmc_dev.dmcbc_bc_pay_pay_rpt_i_d_3_new_zh;
create table dmc_dev.dmcbc_bc_pay_pay_rpt_i_d_3_new_zh as
select
a.*,
case when a.mer_id='110247176001' then '京东收银产品' else b.produc_1 end as product_2,----------------会员码产品取个单独标识
case when a.mer_id='110247176001' then '会员码产品' else b.produc_2 end as product_3,---------------同上
case
when a.hangye is not null then a.hangye
else c.hangye
end as hangyenew,
case
when a.health_type is not null then a.health_type
else b.health_type
end as health_type_new
from
(
select
*,
concat(
nvl(trade_name, ''),
nvl(product_id, ''),
nvl(pay_tools, ''),
nvl(mer_big_type, ''),
nvl(mer_sma_type, ''),
nvl(prod_type, '')
) as glzd
from dmc_dev.dmcbc_bc_pay_pay_rpt_i_d_2_new_zh
) a
left join dmc_dev.guanlianbia_new_zh b on a.glzd = b.glzd
left join dmc_dev.guanlianbia2_new_zh c on a.glzd = c.glzd 

----------添加倒数日期
use dmc_dev;
drop table dmc_dev.dmcbc_bc_pay_pay_rpt_i_d_4_new_zh;
create table dmc_dev.dmcbc_bc_pay_pay_rpt_i_d_4_new_zh as
select
case
when da_te between '2021-04-08' and '2021-04-14' then '21W15'
when da_te between '2020-04-07' and '2020-04-13' then '20W15'
else '其他'
end as timetype,
trade_name,
product_id,
pay_tools,
sum(order_amt) as order_amt,
sum(trade_amt) as trade_amt,
sum(mer_fee) as mer_fee,
sum(bank_fee) as bank_fee,
sum(trade_num) as trade_num,
mer_big_type,
mer_sma_type,
prod_type,
product_2,
product_3,
hangyenew,
health_type_new,
row_number () over () as rn
from dmc_dev.dmcbc_bc_pay_pay_rpt_i_d_3_new_zh
where
da_te >= '2019-09-20'
group by
case
when da_te between '2021-04-08' and '2021-04-14' then '21W15'
when da_te between '2020-04-07' and '2020-04-13' then '20W15'
else '其他'
end,
trade_name,
product_id,
pay_tools,
mer_big_type,
mer_sma_type,
prod_type,
product_2,
product_3,
hangyenew,
health_type_new


-----------导出明细（下面的可以选中了一起跑）
select * from dmc_dev.dmcbc_bc_pay_pay_rpt_i_d_4_new_zh
where rn <= 1000;

select * from dmc_dev.dmcbc_bc_pay_pay_rpt_i_d_4_new_zh
where rn >= 1001;

select * from dmc_dev.dmcbc_bc_pay_pay_rpt_i_d_4_new_zh
where rn >= 2001;

select * from dmc_dev.dmcbc_bc_pay_pay_rpt_i_d_4_new_zh
where rn >= 3001;

select * from dmc_dev.dmcbc_bc_pay_pay_rpt_i_d_4_new_zh
where rn >= 4001;

select * from dmc_dev.dmcbc_bc_pay_pay_rpt_i_d_4_new_zh
where rn >= 5001;
