# !/usr/bin/env python
# -*- coding: utf-8 -*-

from template.base_sql_task import *
import datetime
import math
########################################################################################################################
#  Creater        :liqi867
#  Creation Time  :20210219
#  Description    :
#                   
#  Modify By      :
#  Modify Time    :
#  Modify Content :
#  Script Version :1.0.0
#######################################################################################################################


#
# 目前支持：RUNNER_SPARK_SQL和RUNNER_HIVE,RUNNER_STINGER
#
sql_runner = RUNNER_STINGER
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)


#将字符串转换成datetime类型
def strtodatetime(datestr, format):
  return datetime.datetime.strptime(datestr, format)
 
 
#时间转换成字符串,格式为2015-02-02
def datetostr(date):
  return str(date)[0:10]
 
 
#时间转换成字符串,格式为2015-02-02
def datetostr_secod(date):
  return str(date)[0:19]

def datediff():
  format = "%Y-%m-%d"
  bd = strtodatetime(sql_task.get_tx_date(), format)
  ed = strtodatetime('2012-01-06', format)
  diffday = bd - ed
  return diffday.days
  
 
def week_start_days():
    format = "%Y-%m-%d"
    f_day=math.fmod(datediff(),7)
    day_cnt=abs(int(f_day))
    now=strtodatetime(sql_task.get_tx_date(), format)
    delta=datetime.timedelta(days=day_cnt)
    week_start_day=now - delta
    return week_start_day.strftime('%Y-%m-%d')

def get_customized_items():
    """
     if you need some special values in your sql, please define and calculate then here
     to refer it as {YOUR_VAR} in your sql
    """
    today = Time.today()
    TX_PRE_60_DATE = Time.date_sub(date=today, itv=60)
    TX_PRE_365_DATE = Time.date_sub(date=today, itv=365)
    WEEK_DAY = week_start_days()
    return locals()


sql_map = {

    # ATTENTION:   ！！！！ sql_01  因为系统按字典顺序进行排序，小于10 的一定要写成0加编号，否则会顺序混乱，数据出问题，切记，切记！！！！
    
    # 1 2 3 店铺 商户 设备
    # 4 5     水滴inn（按v033去重）、erp
    # 6 7     erp_all（关联inn打标）、sfjh_all（关联inn、erp打标）
    # 8       inn_all
    # 9 10     insert、结算回刷insert
    
    
    # 店铺明细
    "sql_01": """
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_shop;
    create table if not exists idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_shop as
    select
    tshop.shop_num
    ,tshop.shop_nm  -- 店铺名称
    ,tshop.shop_inds_code1  -- 店铺一级行业编码
    ,tshop.shop_inds_nm1  -- 店铺一级行业名称
    ,tshop.shop_inds_code2  -- 店铺二级行业编码
    ,tshop.shop_inds_nm2  -- 店铺二级行业名称
    ,tshop.agent_num  -- 代理商编号
    ,tagent.agent_full_nm  -- 代理商全称
    from
    (
        select
        shop_num
        ,shop_nm
        ,inds_code1 as shop_inds_code1
        ,inds_nm1 as shop_inds_nm1
        ,inds_code2 as shop_inds_code2
        ,inds_nm2 as shop_inds_nm2
        ,nvl(agent_num,'') as agent_num
        from idm.idm_c01_pay_sfjh_shop_info_s_d
        where dt = '{TX_DATE}'
    )tshop
    left join
    (
        select
        agent_num
        ,agent_full_name as agent_full_nm
        from idm.idm_c01_pay_sfjh_agent_info_s_d
        where dt = '{TX_DATE}'
    )tagent
    on tshop.agent_num=tagent.agent_num
    ;
    """,
    
    # 商户明细
    "sql_02": """
    -- 商户
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_mht;
    create table if not exists idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_mht as
    select
    mht_num
    ,mht_full_nm
    ,mht_province
    ,mht_city
    ,mht_district
    ,inds_num as mht_inds_code
    ,inds_nm as mht_inds_nm
    ,agent_num 
    ,agent_full_nm
    from idm.idm_c01_pay_sfjh_mht_info_s_d
    where dt = '{TX_DATE}'
    ;
    """,
    
    # 设备
    "sql_03": """
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_device;
    create table if not exists idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_device as
    select * from 
    (
        select device_no,cooperate_model,bind_status
        ,row_number() over(partition by device_no order by modified_date desc) as rn
        from odm.odm_pay_jhzf_n_self_device_info_i_d
        where dt <= '{TX_DATE}'
    )t
    where rn=1
    ;
    """,
    
    # inn
    "sql_04": """
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_inn;
    create table if not exists idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_inn as
    select * from
    (
  
    
        select v045 as trade_no
        ,v033 as ordr_num
        ,v042 as out_ordr_num
        ,v032 as reqst_ordr_num
        ,'' as bank_ordr_num
        ,v031 as pay_mht_num    ---这个字段用了
        ,get_json_object(v072,'$.merchantNo') as biz_mht_num
        ,v020 as shop_num
        ,v035 as shop_nm
        ,v036 as device_id     ---这个字段用了
        ,v013 as device_type
        ,v038 as device_model
        ,v043 as mem_id
        ,v017 as mem_phone
        ,get_json_object(v072,'$.openId') as open_id
        ,v040 as user_pin
        ,get_json_object(v072,'$.authCode') as auth_code
        ,v039 as cart_id
        ,v009 as region
        ,v054 as belg_org
        ,v021 as biz_type   ----这个字段用了
        ,v016 as biz_src
        ,get_json_object(v072,'$.baiTiaoType') as bt_type
        ,if(v015='UNIONPAY','UNIPAY',V015) as pay_prod
        ,v014 as pay_way
        ,v019 as pay_chnl
        ,v004 as sku_cnt
        ,nvl(d001,0) as ordr_amt
        ,nvl(v041,0) as recvbl_amt
        ,nvl(d002,0) as actl_pay_amt
        ,nvl(d004,0) as actl_refd_amt
        ,nvl(d003,0) as mht_coup_amt
        ,nvl(get_json_object(v072,'$.discountAmount'),0) as plat_coup_amt
        ,'PAY' as ordr_type
        ,case when v001=3 and v002=2 then 'REFUND'
            when v002=0 then 'INIT'
            when v002=1 then 'ING'
            when v002=2 then 'SUCCESS'  --用的是这个枚举
            when v002=3 then 'FAIL'
            else v002
            end as ordr_status ---这个字段用了
        ,case when v003=0 then 'INIT'
            when v003=1 then 'PART'
            when v003=2 then 'FULL'
            when v003=3 then 'MORE'
            else v003
            end as refd_type
        ,created_date as create_time
        ,modified_date as modify_time
        ,t002 as cmplt_time
        ,v072 as xpn1
        ,row_number() over(partition by v033 order by modified_date desc) as rn --  平台订单号
        from odm.ODM_INN_DSP_BIG_00_000_I_D
        where dt='{TX_DATE}'
        and v021 in ('selfhelp','pos','phenixPay')
        --and v002='2' and v001='1' ---支付成功且未发生退款
    )t
    where rn=1
    ;
    """,
    
    # erp
    "sql_05": """
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_erp;
    create table if not exists idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_erp as
    select
        trade_req_no as trade_no
        ,order_no as ordr_num
        ,'' as orgnl_ordr_num
        ,order_no as out_ordr_num
        ,merchant_order_no as reqst_ordr_num
        ,out_trans_order_id as bank_ordr_num
        ,pay_merchant_no as pay_mht_num
        ,merchant_no as biz_mht_num
        ,shop_id as shop_num
        ,shop_name as shop_nm
        ,device_id as device_id
        ,'' as device_type
        ,'' as device_model
        ,'' as mem_id
        ,'' as mem_phone
        ,out_user_id as open_id
        ,'' as user_pin
        ,auth_code as auth_code
        ,'' as cart_id
        ,'' as region
        ,'' as belg_org
        ,if(business_id='selfhelp','selfhelp-erp',business_id) as biz_type
        ,'' as biz_src
        ,'' as bt_type
        ,pay_channe as pay_prod
        ,pay_type as pay_way
        ,'' as pay_chnl
        ,'' as sku_cnt
        ,pay_amount as ordr_amt
        ,discount_amount as plat_coup_amt
        ,if(trade_type<>'REF','PAY','REFUND') as ordr_type
        ,case when trade_type='TRADE' and status='FINI' then 'SUCCESS'
            when trade_type='REVOKE' and status='FINI' then 'CANCEL'
            when status='PROCESS' then 'ING'
            else status
            end as ordr_status
        ,'' as refd_type
        ,created_time as create_time
        ,modified_time as modify_time
        ,fini_time as cmplt_time
        ,ext_map as xpn1
        --,row_number() over(partition by order_no order by modified_time desc) as rn  -- 订单号
    from odm.odm_pay_syyj_zzsy_self_trade_table_i_d
    where dt='{TX_DATE}'
    ;
    """,
    
    
    # erp中台交易明细
    "sql_06": """
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_erp_all;
    create table if not exists idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_erp_all as
    select
    trade_no
    ,ordr_num
    ,orgnl_ordr_num
    ,out_ordr_num
    ,reqst_ordr_num
    ,bank_ordr_num
    ,pay_mht_num
    ,biz_mht_num
    ,mht_full_nm
    ,mht_province
    ,mht_city
    ,mht_district
    ,mht_inds_code
    ,mht_inds_nm
    ,shop_num
    ,shop_nm
    ,shop_inds_code1
    ,shop_inds_nm1
    ,shop_inds_code2
    ,shop_inds_nm2
    ,agent_num
    ,agent_full_nm
    ,device_id
    ,device_type
    ,device_model
    ,cooperate_model
    ,bind_status
    ,mem_id
    ,mem_phone
    ,open_id
    ,user_pin
    ,auth_code
    ,cart_id
    ,region
    ,belg_org
    ,biz_type
    ,biz_src
    ,bt_type
    ,pay_prod
    ,pay_way
    ,scan_type
    ,pay_chnl
    ,sku_cnt
    ,ordr_amt
    ,refund_amt
    ,recvbl_amt
    ,actl_pay_amt
    ,actl_refd_amt
    ,mht_coup_amt
    ,plat_coup_amt
    ,sett_amt
    ,fee_amt
    ,ordr_type
    ,ordr_status
    ,refd_type
    ,create_time
    ,modify_time
    ,cmplt_time
    ,xpn1
    ,is_yn
    from
    (
        select
        trade_no
        ,terp.ordr_num
        ,orgnl_ordr_num
        ,out_ordr_num
        ,reqst_ordr_num
        ,bank_ordr_num
        ,pay_mht_num
        ,biz_mht_num
        ,tmht.mht_full_nm
        ,tmht.mht_province
        ,tmht.mht_city
        ,tmht.mht_district
        ,tmht.mht_inds_code
        ,tmht.mht_inds_nm
        -- 店铺
        ,terp.shop_num
        ,if(nvl(terp.shop_nm,'')<>'',terp.shop_nm,tshop.shop_nm) as shop_nm
        ,tshop.shop_inds_code1
        ,tshop.shop_inds_nm1
        ,tshop.shop_inds_code2
        ,tshop.shop_inds_nm2
        -- 代理商
        ,if(nvl(tmht.agent_num,'')<>'',tmht.agent_num,tshop.agent_num) as agent_num
        ,if(nvl(tmht.agent_full_nm,'')<>'',tmht.agent_full_nm,tshop.agent_full_nm) as agent_full_nm
        ,device_id
        ,device_type
        ,device_model
        ,tdevice.cooperate_model
        ,tdevice.bind_status
        ,mem_id
        ,mem_phone
        ,open_id
        ,user_pin
        ,auth_code
        ,cart_id
        ,region
        ,belg_org
        ,biz_type
        ,biz_src
        ,bt_type
        ,pay_prod
        ,pay_way
        ,'' as scan_type
        ,pay_chnl
        ,sku_cnt
        ,cast(ordr_amt as decimal(38,12)) as ordr_amt
        ,cast('' as decimal(38,12)) as refund_amt
        ,cast('' as decimal(38,12)) as recvbl_amt
        ,cast('' as decimal(38,12)) as actl_pay_amt
        ,cast(''  as decimal(38,12)) as actl_refd_amt
        ,cast('' as decimal(38,12)) as mht_coup_amt
        ,cast(plat_coup_amt as decimal(38,12)) as plat_coup_amt 
        ,cast('' as decimal(38,12)) as sett_amt
        ,cast('' as decimal(38,12)) as fee_amt        
        ,ordr_type
        ,ordr_status
        ,refd_type
        ,create_time
        ,modify_time
        ,cmplt_time
        ,xpn1
        ,if(nvl(trade_no_inn,'')<>'',0,1) as is_yn
        ,row_number() over(partition by terp.ordr_num order by if(nvl(trade_no_inn,'')<>'',0,1) asc,modify_time desc) as rn  -- 订单号
        -- erp中台有2次请求成功的订单（理论上只有一笔有效，对应同一笔商户订单号，有2个trade_req_no）给到水滴系统只有一笔，此时保留水滴系统该笔订单。
        from
        (
            select * from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_erp
            --where rn=1
        )terp
        left join
        (
            select
            mht_num
            ,mht_full_nm
            ,mht_province
            ,mht_city
            ,mht_district
            ,mht_inds_code
            ,mht_inds_nm
            ,agent_num
            ,agent_full_nm
            from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_mht
        )tmht
        on nvl(terp.pay_mht_num,'')=tmht.mht_num
        left join
        (
            select
            shop_num
            ,shop_nm
            ,shop_inds_code1
            ,shop_inds_nm1
            ,shop_inds_code2
            ,shop_inds_nm2
            ,agent_num
            ,agent_full_nm
            from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_shop
        )tshop
        on nvl(terp.shop_num,'')=tshop.shop_num
        left join
        (
            select device_no,cooperate_model,bind_status
            from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_device
        )tdevice
        on nvl(terp.device_id,'')=tdevice.device_no
        left join
        (
            select trade_no as trade_no_inn from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_inn
            where trim(nvl(trade_no,''))<>'' group by trade_no
        )tinn
        on nvl(terp.trade_no,'')=tinn.trade_no_inn  -- inn.v045=erp.trade_req_no
    )t
    where rn=1
    ;
    """,
    
    
    # 四方明细
    "sql_07": """
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_sfjh_all;
    create table if not exists idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_sfjh_all as
    select
    trade_no
    ,tsf.ordr_num
    ,orgnl_ordr_num
    ,out_ordr_num
    ,reqst_ordr_num
    ,bank_ordr_num
    ,pay_mht_num
    ,biz_mht_num
    ,mht_full_nm
    ,mht_province
    ,mht_city
    ,mht_district
    ,mht_inds_code
    ,mht_inds_nm
    ,shop_num
    ,shop_nm
    ,shop_inds_code1
    ,shop_inds_nm1
    ,shop_inds_code2
    ,shop_inds_nm2
    ,agent_num
    ,agent_full_nm
    ,device_id
    ,device_type
    ,device_model
    ,cooperate_model
    ,nvl(bind_status,if(nvl(mach_bind_time,'')<>'',1,0)) as bind_status
    ,mem_id
    ,mem_phone
    ,open_id
    ,user_pin
    ,auth_code
    ,cart_id
    ,region
    ,belg_org
    ,biz_type
    ,biz_src
    ,bt_type
    ,pay_prod
    ,pay_way
    ,scan_type
    ,pay_chnl
    ,sku_cnt
    ,cast(ordr_amt as decimal(38,12)) as ordr_amt
    ,cast(refund_amt as decimal(38,12)) as refund_amt
    ,cast('' as decimal(38,12)) as recvbl_amt
    ,cast(actl_pay_amt as decimal(38,12)) as actl_pay_amt
    ,cast(actl_refd_amt as decimal(38,12)) as actl_refd_amt 
    ,cast('' as decimal(38,12)) as mht_coup_amt
    ,cast(plat_coup_amt as decimal(38,12)) as plat_coup_amt 
    ,cast(sett_amt as decimal(38,12)) as sett_amt
    ,cast(fee_amt as decimal(38,12)) as fee_amt    
    ,ordr_type
    ,ordr_status
    ,refd_type
    ,create_time
    ,modify_time
    ,cmplt_time
    ,xpn1
    ,if(nvl(trade_no_inn,'')<>'' or nvl(auth_code_erp,'')<>'',0,1) as is_yn  -- 水滴能关联上（以水滴为准）或者erp反接聚合的（erp都过水滴）都标记无效。
    from
    (
        select * from
        (
            select
            reqst_num as trade_no  -- 与水滴v045关联
            ,ordr_num as ordr_num
            ,orgnl_ordr_num
            ,ordr_num as out_ordr_num
            ,reqst_num as reqst_ordr_num
            ,bank_ordr_num as bank_ordr_num
            ,mht_num as pay_mht_num
            ,'' as biz_mht_num
            ,mht_full_nm
            ,mht_province
            ,mht_city
            ,mht_district
            ,mht_inds_code
            ,mht_inds_nm
            ,if(nvl(get_json_object(get_json_object(xpn1,'$.mht_ret_info_ordr'),'$.shopid'),'')<>'',get_json_object(get_json_object(xpn1,'$.mht_ret_info_ordr'),'$.shopid'),shop_num) as shop_num
            ,shop_nm as shop_nm
            ,shop_inds_code1
            ,shop_inds_nm1
            ,shop_inds_code2
            ,shop_inds_nm2
            ,agent_num
            ,agent_full_nm
            --,get_json_object(get_json_object(xpn1,'$.mht_ret_info_ordr'),'$.sn') as device_id
            ,if(trim(nvl(get_json_object(get_json_object(xpn1,'\$.ext_map_ordr'),'\$.deviceInfo'),''))<>''
                ,get_json_object(get_json_object(xpn1,'\$.ext_map_ordr'),'\$.deviceInfo')
                ,get_json_object(Get_json_object(xpn1,'\$.mht_ret_info_ordr'),'\$.sn')
                ) as device_id  -- 20210616新增主扫设备 JDSM
            ,mach_type as device_type
            ,'' as device_model
            ,mem_num as mem_id
            ,'' as mem_phone
            ,open_id as open_id
            ,user_pin as user_pin
            ,auth_code as auth_code
            ,'' as cart_id
            ,'' as region
            ,'' as belg_org
            ,case when prod_type in ('双屏收银机及配件-新通路渠道') then 'xintonglu'
                when prod_type in ('双屏收银机及配件-京东数码渠道') then 'JDSM'
                when prod_type in ('自助收银机') then 'selfhelp'
                when prod_type in ('智能POS及配件') then 'pos'
                when prod_type in ('青鸾app') then 'phenixPay'
                end as biz_type
            ,'' as biz_src
            ,bt_pay_way as bt_type
            ,pay_way_code as pay_prod
            ,orgnl_pay_way_code as pay_way
            ,chnl_code as pay_chnl
            ,scan_type
            ,'' as sku_cnt
            ,'' as refd_type
            ,get_json_object(xpn1,'$.ordr_amt_ordr') as ordr_amt
            ,ordr_create_time as create_time
            ,ordr_modify_time as modify_time
            ,ordr_cmplt_time as cmplt_time
            ,xpn1 as xpn1
            ,mach_bind_time
            ,row_number() over(partition by ordr_num order by ordr_modify_time,pay_modify_time desc) as rn
            from idm.idm_f02_pay_sfjh_tx_dtl_i_d
            where dt='{TX_DATE}' and is_yn=1
            -- and (to_date(ordr_create_time)='{TX_DATE}' or to_date(ordr_modify_time)='{TX_DATE}')
            and (nvl(pay_num,'')='' or get_json_object(xpn1,'$.pay_way_ordr')=orgnl_pay_way_code)
            and prod_type in ('双屏收银机及配件-新通路渠道','自助收银机','智能POS及配件','青鸾app','双屏收银机及配件-京东数码渠道')
        )t
        where rn=1
    )tsf
    left join
    (
        select ordr_num
        ,SUM(if(ordr_type='PAY',actl_tx_amt,0)) as actl_pay_amt
        ,SUM(if(ordr_type='REFUND',actl_tx_amt,0)) as actl_refd_amt
        ,SUM(if(ordr_type='PAY',coupon_amt,0)) as plat_coup_amt
        ,SUM(if(ordr_type='PAY',sett_amt,0)-if(ordr_type='REFUND',sett_amt,0)) as sett_amt
        ,SUM(if(ordr_type='PAY',fee_amt,0)-if(ordr_type='REFUND',fee_amt,0)) as fee_amt
        from idm.idm_f02_pay_sfjh_tx_dtl_i_d
        where dt='{TX_DATE}' and is_yn=1
        --and (to_date(ordr_create_time)='{TX_DATE}' or to_date(ordr_modify_time)='{TX_DATE}')
        and prod_type in ('双屏收银机及配件-新通路渠道','自助收银机','智能POS及配件','青鸾app','双屏收银机及配件-京东数码渠道')
        and ordr_status='SUCCESS'
        group by ordr_num
    )tamt
    on nvl(tsf.ordr_num,'')=tamt.ordr_num
    left join
    (
        select ordr_num as ordr_num_ordr
        ,ordr_type
        ,if(nvl(orgnl_ordr_num,'')<>'','REFUND',ordr_status) as ordr_status 
        ,refund_amt
        from
        (
            select ordr_num,ordr_type,ordr_status,refund_amt
            from idm.idm_f02_pay_sfjh_ordr_dtl_i_d
            where dt='{TX_DATE}' and is_yn=1--and mht_src = 'xintonglu'
        )t1
        left join
        (
            select orgnl_ordr_num
            from idm.idm_f02_pay_sfjh_ordr_dtl_i_d
            where dt='{TX_DATE}'  and is_yn=1 --and mht_src = 'xintonglu' 
            and ordr_type='REFUND' and ordr_status='SUCCESS' -- 退款成功的给原单打标
            and (orgnl_ordr_num is not null or orgnl_ordr_num<>'')
            group by orgnl_ordr_num
        )t2
        on t1.ordr_num=t2.orgnl_ordr_num
    )tordr
    on nvl(tsf.ordr_num,'')=tordr.ordr_num_ordr
    left join
    (
        select device_no,cooperate_model,bind_status 
        from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_device
    )tdevice
    on nvl(tsf.device_id,'')=tdevice.device_no
    left join
    (
        -- select out_ordr_num as ordr_num_inn from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_inn
        select trade_no as trade_no_inn from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_inn
        where trim(nvl(trade_no,''))<>'' group by trade_no
    )tinn
    -- on tsf.ordr_num=tinn.ordr_num_inn
    on tsf.trade_no=tinn.trade_no_inn  -- 20210615改为用交易号关联，inn.v045=sf.reqst_ordr_num
    left join
    (
        select auth_code as auth_code_erp from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_erp
        where trim(nvl(auth_code,''))<>'' group by auth_code
    )terp
    on nvl(tsf.auth_code,'')=terp.auth_code_erp  -- erp反接聚合的要标记无效，erp中台到厂商再到聚合
    where to_date(create_time)='{TX_DATE}' or to_date(modify_time)='{TX_DATE}'
    ;
    """,
    
    
    # 水滴明细
    "sql_08": """
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_inn_all;
    create table if not exists idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_inn_all as
    select
    trade_no
    ,tinn.ordr_num
    ,nvl(tsf.orgnl_ordr_num,terp.orgnl_ordr_num) as orgnl_ordr_num
    ,out_ordr_num
    ,reqst_ordr_num
    ,bank_ordr_num
    ,pay_mht_num
    ,biz_mht_num
    ,tmht.mht_full_nm
    ,tmht.mht_province
    ,tmht.mht_city
    ,tmht.mht_district
    ,tmht.mht_inds_code
    ,tmht.mht_inds_nm
    -- 店铺
    ,tinn.shop_num
    ,if(nvl(tinn.shop_nm,'')<>'',tinn.shop_nm,tshop.shop_nm) as shop_nm
    ,tshop.shop_inds_code1
    ,tshop.shop_inds_nm1
    ,tshop.shop_inds_code2
    ,tshop.shop_inds_nm2
    -- 代理商
    ,if(nvl(tmht.agent_num,'')<>'',tmht.agent_num,tshop.agent_num) as agent_num
    ,if(nvl(tmht.agent_full_nm,'')<>'',tmht.agent_full_nm,tshop.agent_full_nm) as agent_full_nm
    ,device_id
    ,device_type
    ,device_model
    ,cooperate_model
    ,nvl(nvl(tdevice.bind_status,nvl(tsf.bind_status,terp.bind_status)),0) as bind_status
    ,mem_id
    ,mem_phone
    ,open_id
    ,user_pin
    ,if(trim(nvl(tinn.auth_code,''))<>'',tinn.auth_code,if(trim(nvl(tsf.auth_code,''))<>'',tsf.auth_code,terp.auth_code)) as auth_code
    ,cart_id
    ,region
    ,belg_org
    ,biz_type
    ,biz_src
    ,bt_type
    ,pay_prod
    ,pay_way
    ,nvl(tsf.scan_type,terp.scan_type) as scan_type
    ,pay_chnl
    ,sku_cnt
    ,cast(ordr_amt as decimal(38,12)) as ordr_amt
    ,cast(nvl(tsf.refund_amt,terp.refund_amt) as decimal(38,12)) as refund_amt
    ,cast(recvbl_amt as decimal(38,12)) as recvbl_amt
    ,cast(actl_pay_amt as decimal(38,12)) as actl_pay_amt
    ,cast(actl_refd_amt as decimal(38,12)) as actl_refd_amt
    ,cast(mht_coup_amt as decimal(38,12)) as mht_coup_amt
    ,cast(plat_coup_amt as decimal(38,12)) as plat_coup_amt
    ,cast(nvl(tsf.sett_amt,terp.sett_amt) as decimal(38,12)) as sett_amt
    ,cast(nvl(tsf.fee_amt,terp.sett_amt) as decimal(38,12)) as fee_amt
    ,ordr_type
    ,ordr_status
    ,refd_type
    ,create_time
    ,modify_time
    ,cmplt_time
    ,xpn1
    ,1 as is_yn
    from
    (
        select * from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_inn
    )tinn
    left join
    (
        select
        trade_no as trade_no_sf
        ,ordr_num
        ,orgnl_ordr_num
        ,scan_type
        ,refund_amt
        ,sett_amt
        ,fee_amt
        ,bind_status,auth_code
        from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_sfjh_all
    )tsf
    -- on tinn.out_ordr_num=tsf.ordr_num
    on nvl(tinn.trade_no,'')=tsf.trade_no_sf
    left join
    (
        select
        trade_no_inn
        ,a.auth_code
        ,b.orgnl_ordr_num
        ,b.scan_type
        ,b.refund_amt
        ,b.sett_amt
        ,b.fee_amt
        ,b.bind_status
        from
        (
            select trade_no as trade_no_inn,auth_code
            from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_erp
        )a
        -- 关联erp反接聚合数据
        left join
        (
            select
            trade_no as trade_no_sf
            ,ordr_num
            ,orgnl_ordr_num
            ,scan_type
            ,refund_amt
            ,sett_amt
            ,fee_amt
            ,bind_status,auth_code
            from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_sfjh_all
            where biz_type='selfhelp'
        )b
        on nvl(a.auth_code,'')=nvl(b.auth_code,'')
    )terp
    on nvl(tinn.trade_no,'')=terp.trade_no_inn
    left join
    (
        select
        mht_num
        ,mht_full_nm
        ,mht_province
        ,mht_city
        ,mht_district
        ,mht_inds_code
        ,mht_inds_nm
        ,agent_num
        ,agent_full_nm
        from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_mht
    )tmht
    on nvl(tinn.pay_mht_num,'')=tmht.mht_num
    left join
    (
        select
        shop_num
        ,shop_nm
        ,shop_inds_code1
        ,shop_inds_nm1
        ,shop_inds_code2
        ,shop_inds_nm2
        ,agent_num
        ,agent_full_nm
        from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_shop
    )tshop
    on nvl(tinn.shop_num,'')=tshop.shop_num
    left join
    (
        select 
        device_no
        ,cooperate_model
        ,bind_status
        from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_device
    )tdevice
    on nvl(tinn.device_id,'')=tdevice.device_no
    ;
    """,

    
    "sql_09": """
    use idm;
    insert overwrite table idm.idm_f02_pay_syyj_ordr_dtl_i_d partition (dt='{TX_DATE}',sys_src)
    select
         current_timestamp() as etl_dt       --ETL日期
        ,*
    from
    (
        select *,'sfjh' as sys_src from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_sfjh_all
        union all
        select *,'inn' as sys_src from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_inn_all
        union all
        select *,'erp' as sys_src from idm_tmp.idm_f02_pay_syyj_ordr_dtl_i_d_erp_all
    )t
    ;
    """,
    
    "sql_10": """
    use idm;
    insert overwrite table idm.idm_f02_pay_syyj_ordr_dtl_i_d partition (dt,sys_src)
    -- 回刷结算4天
    select
    current_timestamp() as etl_dt
    ,trade_no
    ,t1.ordr_num
    ,orgnl_ordr_num
    ,out_ordr_num
    ,reqst_ordr_num
    ,bank_ordr_num
    ,pay_mht_num
    ,biz_mht_num
    ,mht_full_nm
    ,mht_province
    ,mht_city
    ,mht_district
    ,mht_inds_code
    ,mht_inds_nm
    ,shop_num
    ,shop_nm
    ,shop_inds_code1
    ,shop_inds_nm1
    ,shop_inds_code2
    ,shop_inds_nm2
    ,agent_num
    ,agent_full_nm
    ,device_id
    ,device_type
    ,device_model
    ,cooperate_model
    ,bind_status
    ,mem_id
    ,mem_phone
    ,open_id
    ,user_pin
    ,auth_code
    ,cart_id
    ,region
    ,belg_org
    ,biz_type
    ,biz_src
    ,bt_type
    ,pay_prod
    ,pay_way
    ,scan_type
    ,pay_chnl
    ,sku_cnt
    ,ordr_amt
    ,refund_amt
    ,recvbl_amt
    ,actl_pay_amt
    ,actl_refd_amt
    ,mht_coup_amt
    ,plat_coup_amt
    ,tsett.sett_amt
    ,tsett.fee_amt
    ,ordr_type
    ,ordr_status
    ,refd_type
    ,create_time
    ,modify_time
    ,cmplt_time
    ,xpn1
    ,is_yn
    ,t1.dt
    ,t1.sys_src
    from
    (
        select * from idm.idm_f02_pay_syyj_ordr_dtl_i_d
        where dt between date_sub('{TX_DATE}',4) and date_sub('{TX_DATE}',1)
        and sys_src in ('sfjh','inn')
    )t1
    left join
    (
        select dt,ordr_num
        ,SUM(if(ordr_type='PAY',sett_amt,0)-if(ordr_type='REFUND',sett_amt,0)) as sett_amt
        ,SUM(if(ordr_type='PAY',fee_amt,0)-if(ordr_type='REFUND',fee_amt,0)) as fee_amt
        from idm.idm_f02_pay_sfjh_tx_dtl_i_d 
        where dt between date_sub('{TX_DATE}',4) and date_sub('{TX_DATE}',1) and is_yn=1
        and prod_type in ('双屏收银机及配件-新通路渠道','自助收银机','智能POS及配件','青鸾app','双屏收银机及配件-京东数码渠道')
        and ordr_status='SUCCESS'
        group by dt,ordr_num
    )tsett
    on t1.dt=tsett.dt and t1.out_ordr_num=tsett.ordr_num
    ;
    """,
}

"""
并行sql_keys配置
同层之间的SQL并行, 如下，第一层为parallel_keys[0], 即: 'sql_01', 'sql_02'. 以此类推
并行数=min(层级sql数, 默认最大并行数)
只有当前面层次sql全部执行完毕才会执行后面层次的sql
"""
parallel_keys = [
    ['sql_01','sql_02','sql_03'],
    ['sql_04','sql_05'],
    ['sql_06','sql_07'],
    ['sql_08'],
    ['sql_09','sql_10']
]

# 以下部分无需改动，除非作业有特殊要求
sql_task.set_customized_items(get_customized_items())
# 第二个参数为并行sql_keys配置
return_code = sql_task.execute_sqls_parallel(sql_map, parallel_keys)
exit(return_code)