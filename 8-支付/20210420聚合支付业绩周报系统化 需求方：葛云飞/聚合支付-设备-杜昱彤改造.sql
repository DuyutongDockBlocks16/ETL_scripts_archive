-----第一段：区分自助收银、POS、pad、双屏机
use dmf_tmp;
drop table dmf_tmp.dmcbc_bc_znyjjf_zh;
create table dmf_tmp.dmcbc_bc_znyjjf_zh as
select 
	type
	,merchant_no---------------------------------自助收银
	from 
	(
	select 
	select
	biz_type as type---业务类型
	,pay_mht_num as merchant_no-----商户号
	,device_id as deviceid---设备id
	from
	idm.idm_f02_pay_syyj_ordr_dtl_i_d
	where biz_type='selfhelp'
	and dt<='{TX_DATE}'
	and ordr_status='SUCCESS'
	and sys_src = 'inn'
	and cooperate_model <> 'test'
	group by 1,2,3
	)
	
	
union ALL--------------------------------------------------------------pos，六里桥pad

	select biz_type as type---业务类型
	,pay_mht_num as merchant_no-----商户号
	from idm.idm_f02_pay_syyj_ordr_dtl_i_d  
	where biz_type in('pos','phenixPay')----pos，phenixpay 六里桥pad
	and dt<='{TX_DATE}' 
	and ordr_status='SUCCESS'
	and sys_src = 'inn'
	group by 1,2
	
	
	
UNION ALL-------------------------------------------------------------双屏


	select * 
	from
	(select case when prod_type='双屏收银机及配件' then 'xintonglu' ----双屏_新通路渠道
            	when agent_num='111936383' and (device like'JDD%'or device like 'JX%' or device like'ZQ%') then 'JDSM'----双屏_京东数码渠道
       	end as type---业务类型
	,mht_num as merchant_no---商户号
	from
	(select *
	,get_json_object(regexp_replace(Get_json_object(xpn1,'\$.ext_map_ordr'), '\\[|\\]', ''),'\$.deviceInfo') as device---设备号
	,row_number() over(partition by pay_num order by ordr_modify_time desc) as rn
	from idm.idm_f02_pay_sfjh_tx_dtl_i_d
	where dt<='{TX_DATE}' 
	and ordr_type in ('PAY') and ordr_status='SUCCESS') a
	where rn=1 and is_yn=1
	group by 1,2) b
	where type is not null

------第二段，同一类型内商户号去重
	use dmf_tmp;
	drop table dmf_tmp.dmcbc_bc_znyjordjf;
	create table dmf_tmp.dmcbc_bc_znyjordjf as
	select *
	from
	(select type,merchant_no
	,row_number() over(partition by merchant_no order by type) as rn
	from dmf_tmp.dmcbc_bc_znyjjf_zh) a
	where rn=1

-----第三段：匹配各商户号的交易情况
	use dmf_tmp;
	drop table dmf_tmp.dmcbc_bc_znyjmerjf;
	create table dmf_tmp.dmcbc_bc_znyjmerjf as
	select a.*,b.ords,b.GMV,b.maoli,b.day
	from 
	dmf_tmp.dmcbc_bc_znyjordjf a
	left join
	(select day,merchant_no
	,count(distinct order_id1) as ords
	,sum(ord_amount) as GMV
	,sum(case when fee is not null then income
          when region='华北-代理' and sys_type='合利宝' and fee is null then ord_amount*0.00280476412824299
          when region='大客户-商超拓展' and sys_type='合利宝' and fee is null then ord_amount*9/10000
     else 0 end) as maoli
	from
	(
		select day,merchant_no,order_id1,ord_amount,fee,income,region,sys_type
		from dmc_bc.dmcbc_bc_sd_trade_s_d
		where dt='{TX_DATE}' 
		and refund_flag=0
		and merchant_second_no not in ('110225410008','110225410016','111434658002','111317672003','110225410010','111097131013') --去掉京邦达
		group by 1,2,3,4,5,6,7,8) c
		group by 1,2) b
		on a.merchant_no=b.merchant_no

------第四段：统计各产品的订单、交易额、毛利
	select 
	type,
	day,
	sum(ords) as ord----聚合订单数
	,sum(GMV) as GMV---聚合交易额
	,sum(maoli) as maoli---收入(含税)
	from dmf_tmp.dmcbc_bc_znyjmerjf
	group by 1,2
