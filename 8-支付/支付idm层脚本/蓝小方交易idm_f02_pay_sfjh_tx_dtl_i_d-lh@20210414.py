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
    
    # 乐惠订单表payments2 取其他信息
    "sql_01": """
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_lh_ordr2;
    create table if not exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_lh_ordr2 as 
    -- 乐惠订单表payments2 取其他信息
    select
     ordr_num
     ,ordr_src
     ,channel
     ,auth_code
     ,chnl_code
     ,tordr2.business_account_id
     ,if(nvl(tchnl.parent_merchant_id,'')<>'',tchnl.parent_merchant_id,tordr2.parent_merchant_id) as mht_num_dlb  -- 哆啦宝侧的商户编号，优先取在哆啦宝重新入驻的
     from
     (
         select ordr_num,ordr_src,channel,auth_code,chnl_code,business_account_id,parent_merchant_id
         from
         (
             select
             order_no as ordr_num
             ,source as ordr_src
             ,channel
             ,get_json_object(extra, '$.auth_code') as auth_code
             ,get_json_object(channel, '$.name') as chnl_code
             ,get_json_object(channel, '$.business_account_id') as business_account_id
             ,if(get_json_object(channel, '$.name') in ('lanxiaofang','lanxiaofang_dlb')
                     ,get_json_object(channel, '$.parent_merchant_id')
                     ,'') as parent_merchant_id
             ,row_number() over(partition by order_no order by updated_at) as rn
             from odm.odm_pay_lh_payments2_i_d
             where dt between date_sub('{TX_DATE}',30) and  '{TX_DATE}'
         )t
         where rn=1
     )tordr2
     left join
     (
         -- 乐惠渠道表取乐惠和哆啦宝商户关系
         select business_account_id,parent_merchant_id from
         (
             select business_account_id,parent_merchant_id,updated_at
                 ,row_number() over(partition by business_account_id order by updated_at desc) as rn2
             from
             (
                 select business_account_id,parent_merchant_id,updated_at,status
                 ,row_number() over(partition by business_account_id,parent_merchant_id order by updated_at desc) as rn1
                 from odm.odm_pay_lh_business_account_channels_i_d -- 商户支付渠道路由
                 where dt <= '{TX_DATE}'
                 and ordering_channel in ('lanxiaofang','lanxiaofang_dlb')
             )t1
             where rn1=1 and status=1  -- 先按2个字段排重更精确 防止若有效parent_merchant_id的时间靠前 直接按照business_account_id排重后取错
         )t2
         where rn2=1  -- 防止排重后 仍有1对多的情况
     )tchnl
     on tordr2.business_account_id=tchnl.business_account_id
    ;
    """,
    
    # 取wuid与pin与openid关系
    "sql_02": """
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_lh_pin;
    create table if not exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_lh_pin as 
    -- 取wuid与pin与openid关系
    select wuid,tw.openid as open_id,pin
    from
    (
     select wuid,openid from
     (
         select wuid,openid
         ,row_number() over(partition by wuid order by updated_at desc) rn
         from odm.odm_pay_lh_wechat_users_i_d
         where dt<='{TX_DATE}'
     )t
     where rn=1
    )tw
    left join
    (
     select open_id,pin from
     (
         select open_id,pin,is_yn
         ,row_number() over(partition by open_id order by modify_time desc ) as rn
         from idm.idm_c01_pay_sfjh_pin_openid_s_d
         where dt='{TX_DATE}' --and plat_src<>'lh'
     )t
     where rn=1 and is_yn=1
    )tp
    on tw.openid=tp.open_id
    ;
    """,
    
    # 取乐惠店铺销售、省市、行业等信息
    "sql_03": """
    -- 取乐惠店铺省市区
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_lh_shop_area;
    create table idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_lh_shop_area as
    select code,name,parent_code from
    (
        select code,name,parent_code
        ,row_number() over(partition by code order by id desc) rn
        from odm.odm_pay_lh_area_codes_s_d
        where dt='{TX_DATE}'
    )t
    where rn=1;

    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_lh_shop;
    create table idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_lh_shop as
    -- 店铺
    select
    tshop.meid as shop_num
    ,tshop.name as shop_nm
    ,shop_province
    ,shop_city
    ,tshop.category_id as shop_inds_code2
    ,tinds.shop_inds_nm2
    ,tinds.shop_inds_nm1
    ,tshop.maid as sale_num
    from
    (
        select meid,maid,name,city,category_id from
        (
            select meid,maid,name,city,category_id
            ,row_number() over(partition by meid order by updated_at desc) as rn 
            from odm.odm_pay_lh_merchants_i_d
            where dt<='{TX_DATE}'
        )t
        where rn=1
    )tshop
    left join
    (
        -- 城市省份，主键code 及店铺表的city
        select c.code,c.name as shop_city,p.name as shop_province
        from (select code,name,parent_code from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_lh_shop_area)c
        left join (select code,name from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_lh_shop_area)p
        on c.parent_code=p.code
    )tarea
    on tshop.city=tarea.code
    left join
    (
        select code as shop_inds_code2
        ,name as shop_inds_nm2
        ,category as shop_inds_nm1
        from odm.odm_pay_lh_categories_s_d
        where dt='{TX_DATE}'
    )tinds
    on tshop.category_id=tinds.shop_inds_code2
    ;
    """,
    
    # 乐惠交易信息，合并正逆单
    "sql_04": """
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_lh_tx;
    create table if not exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_lh_tx as 
    select
    -- 正单
    tordr.order_no as ordr_num
    ,tordr.order_no as pay_num
    ,'' as orgnl_ordr_num
    ,vendor_order_no as reqst_num
    ,wx_order_no as bank_ordr_num
    ,tordr2.business_account_id as mht_num
    ,meid as shop_num
    ,regexp_extract(vendor_terminal_id,'^device:([0-9]*)') as mach_num
    ,wuid as wuid   -- 用于关联用户openid
    ,tordr2.auth_code as auth_code
    ,bank_type as bank_type
    ,trade_type as orgnl_pay_way_code
    ,case when trade_type like '%alipay.jsp%' then '支付宝主扫支付'
        when trade_type like '%alipay.mic%' then '支付宝被扫支付'
        when trade_type like '%jdpay.jsp%' then '京东主扫支付'
        when trade_type like '%jdpay.mic%' then '京东被扫支付'
        when trade_type like '%unionpay.jsp%' then '银联在线主扫支付'
        when trade_type like '%unionpay.mic%' then '银联在线被扫支付'
        when trade_type like '%weixin.jsp%' then '微信主扫支付'
        when trade_type like '%weixin.mic%' then '微信被扫支付'
        when trade_type like '%weixin.mpp%' then '微信小程序支付'
        when trade_type like '%deposit.jsp%' then '储值卡主扫支付'
        when trade_type like '%deposit.mic%' then '储值卡被扫支付'
        when trade_type like '%deposit.mpp%' then '储值卡小程序支付'
        else '其他'
        end as orgnl_pay_way_nm
    ,case when trade_type like '%alipay%' then 'ALIPAY'
        when trade_type like '%jdpay%' then 'JDPAY'
        when trade_type like '%union%' then 'UNIPAY'
        when trade_type like '%weixin%' then 'WX'
        when trade_type like '%deposit%' then 'DEPOSIT'
        else 'OTHER'
        end as pay_way_code
    ,case when trade_type like '%alipay%' then '支付宝支付'
        when trade_type like '%jdpay%' then '京东支付'
        when trade_type like '%union%' then '银联在线支付'
        when trade_type like '%weixin%' then '微信支付'
        when trade_type like '%deposit%' then '储值卡支付'
        else '其他'
        end as pay_way_nm
    ,case when trade_type LIKE '%jsp%'  then '主扫' 
        when trade_type LIKE '%mic%' then '被扫'  
        when trade_type like '%mppay%' then '主扫' -- 小程序支付
        end as scan_type
    ,tordr2.chnl_code as chnl_code
    ,case when bank_type='BAITIAO' and app_id like 'wx%' then '白条前置' 
        when bank_type='BAITIAO' then '白条支付'
        end as bt_pay_way
    ,app_id as app_id
    ,tordr2.ordr_src as ordr_src
    ,'PAY' as ordr_type
    ,case when status in ('closed') then 'CLOSE'
        --when status in ('ing') then ''
        when status in ('new') then 'INIT'
        else upper(status)  -- success cancel fail
        end as ordr_status  -- 转换
    ,'PAY' as pay_type
    ,case when status in ('closed') then 'CLOSE'
        --when status in ('ing') then ''
        when status in ('new') then 'INIT'
        else upper(status)  -- success cancel fail
        end as pay_status  -- 转换
    ,if(nvl(trefd.order_no,'')<>'','1','0') as refund_flag
    ,cast(amount as decimal(38,12))/100 as tx_amt
    ,cast(real_amount as decimal(38,12))/100 as actl_tx_amt
    ,cast(coupon_off as decimal(38,12))/100 as coupon_amt
    ,cast(0 as decimal(38,12))/100 as sett_amt
    ,cast(commission as decimal(38,12))/100 as fee_amt
    ,cast(commission_rate as decimal(38,12))/100 as fee_rate
    ,created_at as ordr_create_time
    ,updated_at as ordr_modify_time
    ,finished_at as ordr_cmplt_time
    ,created_at as pay_create_time
    ,updated_at as pay_modify_time
    ,finished_at as pay_cmplt_time
    ,if(nvl(tdlb.request_num,'')<>'',0,1) as is_yn
    ,concat('{{'
            ,'"channel":',nvl(tordr2.channel,'""'),','
            ,'"mht_num_dlb":"',nvl(tordr2.mht_num_dlb,''),'",'
            ,'"paid":"',nvl(paid,''),'",'
            ,'"vendor_notify_url":"',nvl(vendor_notify_url,''),'",'
            ,'"merchant_id":"',nvl(merchant_id,''),'",'
            ,'"qrid":"',nvl(qrid,''),'",'
            ,'"wuid":"',nvl(wuid,''),'",'
            ,'"first_off":"',nvl(first_off,''),'",'
            ,'"used_coupon_id":"',nvl(used_coupon_id,''),'",'
            ,'"wx_coupon_fee":"',nvl(wx_coupon_fee,''),'",'
            ,'"wx_coupon_count":"',nvl(wx_coupon_count,''),'",'
            ,'"merchant_coupon_fee":"',nvl(merchant_coupon_fee,''),'",'
            ,'"activity_id":"',nvl(activity_id,''),'",'
            ,'"client_ip":"',nvl(client_ip,''),'",'
            ,'"gateway":"',nvl(gateway,''),'",'
            ,'"fail_reason":"',nvl(fail_reason,''),'",'
            ,'"wx_prepay_id":"',nvl(wx_prepay_id,''),'",'
            ,'"vendor_client_id":"',nvl(vendor_client_id,''),'",'
            ,'"vendor_terminal_id":"',nvl(vendor_terminal_id,''),'",'
            ,'"bid":"',nvl(bid,''),'",'
            ,'"client_network":"',nvl(client_network,''),'",'
            ,'"pay_code":"',nvl(pay_code,'')
            ,'"}}') as xpn1
    from
    (
        select * from odm.odm_pay_lh_payments_i_d 
        where dt = '{TX_DATE}'
        and ( to_date(created_at)= '{TX_DATE}' or to_date(updated_at)= '{TX_DATE}')
    )tordr
    left join
    (
        select ordr_num,ordr_src,channel,auth_code,chnl_code,business_account_id,mht_num_dlb
        from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_lh_ordr2
    )tordr2
    on tordr.order_no=tordr2.ordr_num
    left join
    (
        select request_num from odm.odm_pay_dora_order_info_0_i_d
        where dt='{TX_DATE}'
        group by request_num
    )tdlb
    on tordr.order_no=tdlb.request_num
    left join
    (
        select order_no from odm.odm_pay_lh_refunds_i_d
        where dt='{TX_DATE}' and status='success'
        group by order_no
    )trefd
    on tordr.order_no=trefd.order_no
    union all
    select
    -- 退单
    refund_no as ordr_num
    ,refund_no as pay_num
    ,trefd.order_no as orgnl_ordr_num
    ,tordr.vendor_order_no as reqst_num  -- vendor_order_no
    ,wx_order_no as bank_ordr_num
    ,tordr2.business_account_id as mht_num
    ,meid as shop_num
    ,regexp_extract(tordr.vendor_terminal_id,'^device:([0-9]*)') as mach_num
    ,wuid as wuid   -- 用于关联用户openid
    ,tordr2.auth_code as auth_code
    ,tordr.bank_type as bank_type -- bank_type
    ,tordr.trade_type as orgnl_pay_way_code
    ,case when tordr.trade_type like '%alipay.jsp%' then '支付宝主扫支付'
        when tordr.trade_type like '%alipay.mic%' then '支付宝被扫支付'
        when tordr.trade_type like '%jdpay.jsp%' then '京东主扫支付'
        when tordr.trade_type like '%jdpay.mic%' then '京东被扫支付'
        when tordr.trade_type like '%unionpay.jsp%' then '银联在线主扫支付'
        when tordr.trade_type like '%unionpay.mic%' then '银联在线被扫支付'
        when tordr.trade_type like '%weixin.jsp%' then '微信主扫支付'
        when tordr.trade_type like '%weixin.mic%' then '微信被扫支付'
        when tordr.trade_type like '%weixin.mpp%' then '微信小程序支付'
        when tordr.trade_type like '%deposit.jsp%' then '储值卡主扫支付'
        when tordr.trade_type like '%deposit.mic%' then '储值卡被扫支付'
        when tordr.trade_type like '%deposit.mpp%' then '储值卡小程序支付'
        else '其他'
        end as orgnl_pay_way_nm
    ,case when tordr.trade_type like '%alipay%' then 'ALIPAY'
        when tordr.trade_type like '%jdpay%' then 'JDPAY'
        when tordr.trade_type like '%union%' then 'UNIPAY'
        when tordr.trade_type like '%weixin%' then 'WX'
        when tordr.trade_type like '%deposit%' then 'DEPOSIT'
        else 'OTHER'
        end as pay_way_code
    ,case when tordr.trade_type like '%alipay%' then '支付宝支付'
        when tordr.trade_type like '%jdpay%' then '京东支付'
        when tordr.trade_type like '%union%' then '银联在线支付'
        when tordr.trade_type like '%weixin%' then '微信支付'
        when tordr.trade_type like '%deposit%' then '储值卡支付'
        else '其他'
        end as pay_way_nm
    ,case when tordr.trade_type LIKE '%jsp%'  then '主扫' 
        when tordr.trade_type LIKE '%mic%' then '被扫'  
        when tordr.trade_type like '%mppay%' then '主扫' -- 小程序支付
        end as scan_type
    ,tordr2.chnl_code as chnl_code
    ,case when bank_type='BAITIAO' and app_id like 'wx%' then '白条前置' 
        when bank_type='BAITIAO' then '白条支付'
        end as bt_pay_way
    ,app_id as app_id
    ,tordr2.ordr_src as ordr_src
    ,'REFUND' as ordr_type
    ,case when status in ('closed') then 'CLOSE'
        --when status in ('ing') then ''
        when status in ('new') then 'INIT'
        else upper(status)  -- success cancel fail
        end as ordr_status  -- 转换
    ,'REFUND' as pay_type
    ,case when status in ('closed') then 'CLOSE'
        --when status in ('ing') then ''
        when status in ('new') then 'INIT'
        else upper(status)  -- success cancel fail
        end as pay_status  -- 转换
    ,'1' as refund_flag
    ,cast(amount as decimal(38,12))/100 as tx_amt
    ,cast(user_amount as decimal(38,12))/100 as actl_tx_amt
    ,cast(tordr.coupon_off as decimal(38,12))/100 as coupon_amt  --coupon_off
    ,cast(0 as decimal(38,12))/100 as sett_amt
    ,cast(commission as decimal(38,12))/100 as fee_amt
    ,cast(commission_rate as decimal(38,12))/100 as fee_rate
    ,created_at as ordr_create_time
    ,updated_at as ordr_modify_time
    ,if(status not in ('new','ing'),updated_at,'') as ordr_cmplt_time
    ,created_at as pay_create_time
    ,updated_at as pay_modify_time
    ,if(status not in ('new','ing'),updated_at,'') as pay_cmplt_time
    ,if(nvl(tdlb.request_num,'')<>'',0,1) as is_yn
    ,concat('{{'
            ,'"channel":',nvl(tordr2.channel,'""'),','
            ,'"mht_num_dlb":"',nvl(tordr2.mht_num_dlb,''),'",'
            ,'"app_id":"',nvl(app_id,''),'",'
            ,'"rid":"',nvl(rid,''),'",'
            ,'"paid":"',nvl(paid,''),'",'
            ,'"bid":"',nvl(bid,''),'",'
            ,'"wuid":"',nvl(wuid,''),'",'
            ,'"action_wuid":"',nvl(action_wuid,''),'",'
            ,'"merchant_amount":"',nvl(merchant_amount,''),'",'
            ,'"fail_reason":"',nvl(fail_reason,''),'",'
            ,'"refuse_reason":"',nvl(refuse_reason,''),'",'
            ,'"mode":"',nvl(mode,'')
            ,'"}}') as xpn1
    from
    (
        select * from odm.odm_pay_lh_refunds_i_d 
        where dt = '{TX_DATE}'
        and ( to_date(created_at)= '{TX_DATE}' or to_date(updated_at)= '{TX_DATE}')
    )trefd
    left join
    (
        select ordr_num,ordr_src,channel,auth_code,chnl_code,business_account_id,mht_num_dlb
        from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_lh_ordr2
    )tordr2
    on trefd.order_no=tordr2.ordr_num
    left join
    (
        select request_num from odm.odm_pay_dora_order_info_0_i_d
        where dt='{TX_DATE}'
        group by request_num
    )tdlb
    on trefd.order_no=tdlb.request_num
    left join
    (
        select * from
        (
            select order_no
            ,vendor_order_no
            ,vendor_terminal_id
            ,bank_type
            ,trade_type
            ,coupon_off
            ,row_number() over (partition by order_no order by updated_at desc) as rn
            from odm.odm_pay_lh_payments_i_d
            where dt between date_sub('{TX_DATE}',7) and '{TX_DATE}'
        )t
        where rn=1
    )tordr
    on trefd.order_no=tordr.order_no
    ;
    """,
    
    # 关联取其他信息
    "sql_05": """
    use idm;
    insert overwrite table idm.idm_f02_pay_sfjh_tx_dtl_i_d partition (dt='{TX_DATE}',plat_src='LH')
    select
        current_timestamp() as etl_dt       --ETL日期
        ,tx.ordr_num
        ,tx.pay_num
        ,tx.orgnl_ordr_num
        ,tx.reqst_num
        ,'' as refund_reqst_num
        ,'' as ordr_batch_num
        ,'' as pay_batch_num
        ,'' as pay_orgnl_batch_num
        ,tx.bank_ordr_num
        ,tx.mht_num
        ,'' as chnl_mht_num
        ,'' as mht_full_nm
        ,tshop.shop_province as mht_province
        ,tshop.shop_city as mht_city
        ,'' as mht_district
        ,tshop.shop_inds_code2 as mht_inds_code
        ,tshop.shop_inds_nm2 as mht_inds_nm
        ,'' as mht_type_code
        ,'' as mht_type_nm
        ,'lhrh' as mht_sys_src
        ,tx.shop_num
        ,tshop.shop_nm as shop_nm
        ,'' as shop_inds_code1
        ,tshop.shop_inds_nm1 as shop_inds_nm1
        ,tshop.shop_inds_code2 as shop_inds_code2
        ,tshop.shop_inds_nm2 as shop_inds_nm2
        ,'' as agent_num
        ,'' as agent_full_nm
        ,tshop.sale_num as mht_sale_num
        ,'' as mht_sale_nm
        ,'' as mht_sale_dept_num
        ,'' as agent_sale_num
        ,'' as agent_sale_nm
        ,tx.mach_num
        ,'' as mach_serl_num
        ,'' as mach_type
        ,'' as mach_type_code
        ,'' as mach_type_nm
        ,'' as mem_num
        ,tpin.open_id as open_id
        ,tpin.pin as user_pin
        ,tx.auth_code
        ,tx.bank_type
        ,'' as card_type
        ,'四方聚合收银台产品' as prod_type
        ,'' as prod_code
        ,'' as prod_nm
        ,'T_GEN' as trade_type_code
        ,'收单交易' as trade_type_nm
        ,'' as pay_src_code
        ,'' as pay_src_nm
        ,tx.orgnl_pay_way_code
        ,tx.orgnl_pay_way_nm
        ,tx.pay_way_code
        ,tx.pay_way_nm
        ,tx.scan_type
        ,tchnl.chnl_code as chnl_code
        ,tchnl.chnl_nm as chnl_nm
        ,'前置交易' as chnl_tx_type
        ,'' as prst_sys_type
        ,tx.bt_pay_way
        ,'' as wx_pay_way
        ,'' as alpy_hb_stg_cnt
        ,tx.app_id
        ,'' as biz_type
        ,'' as mht_src
        ,tx.ordr_src
        ,tx.ordr_type
        ,tx.ordr_status
        ,'' as orgnl_pay_type
        ,tx.pay_type
        ,tx.pay_status
        ,tx.refund_flag
        ,tx.tx_amt
        ,tx.actl_tx_amt
        ,tx.coupon_amt
        ,'' as can_accounted
        ,'' as is_accounted
        ,'' as sett_type
        ,'' as fee_type
        ,tx.sett_amt
        ,if(tx.pay_type='REFUND' and nvl(tx.fee_amt,'')<>'',0-abs(tx.fee_amt),tx.fee_amt) as fee_amt
        ,tx.fee_rate
        ,tx.ordr_create_time
        ,tx.ordr_modify_time
        ,tx.ordr_cmplt_time
        ,tx.pay_create_time
        ,tx.pay_modify_time
        ,tx.pay_cmplt_time
        ,'' as mht_sett_time
        ,'' as shop_sett_time
        ,'' as mach_bind_time
        ,tx.is_yn
        ,tx.xpn1
    from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_lh_tx tx
    left join 
    (
        select shop_num
        ,shop_nm
        ,shop_province
        ,shop_city
        ,shop_inds_code2
        ,shop_inds_nm2
        ,shop_inds_nm1
        ,sale_num
        from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_lh_shop
    )tshop
    on tx.shop_num=tshop.shop_num
    left join
    (
        select wuid,open_id,pin
        from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_lh_pin 
    )tpin
    on tx.wuid=tpin.wuid
    left join
    (
        select src_dim_emrt
        ,dim_emrt as chnl_code
        ,dim_emrt_desc as chnl_nm
        from dim.dim_c02_pay_sfjh_rel_map_a_d
        where src_sys = '乐惠' and dim_type='channel'
    )tchnl
    on tx.chnl_code=tchnl.src_dim_emrt
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
    ['sql_04'],
    ['sql_05']
]

# 以下部分无需改动，除非作业有特殊要求
sql_task.set_customized_items(get_customized_items())
# 第二个参数为并行sql_keys配置
return_code = sql_task.execute_sqls_parallel(sql_map, parallel_keys)
exit(return_code)