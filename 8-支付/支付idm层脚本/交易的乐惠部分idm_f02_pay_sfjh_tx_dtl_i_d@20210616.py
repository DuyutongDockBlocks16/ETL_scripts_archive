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
    
    # 维度枚举
    "sql_01": """
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_dim;
    create table if not exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_dim as
    select
    case when src_code in ('MHT_TYPE') then 'mht_type'
        when src_code in ('PAY_BUSINESS_THREE','PRODUCT_TYPE') then 'prod'
        when src_code in ('TRAD_TYPE') then 'trade_type'
        when src_code in ('PAY_SOURCE') then 'pay_src'
        when src_code in ('PAY_WAY','PI_TYPE') then 'pay_way'
        when src_code in ('CHANNEL_TYPE') then 'chnl'
        end as dim
    ,src_value as dim_code
    ,src_value_desc as dim_nm
    from dim.dim_c02_pay_all_emrt_desc_s_d
    where dt = date_sub('{TX_DATE}',1)
    and src_sys in ('四方聚合','哆啦宝','自研聚合')
    and src_code in ('MHT_TYPE','PAY_BUSINESS_THREE','PRODUCT_TYPE','TRAD_TYPE','PAY_SOURCE','PAY_WAY','PI_TYPE','CHANNEL_TYPE')
    group by case when src_code in ('MHT_TYPE') then 'mht_type'
        when src_code in ('PAY_BUSINESS_THREE','PRODUCT_TYPE') then 'prod'
        when src_code in ('TRAD_TYPE') then 'trade_type'
        when src_code in ('PAY_SOURCE') then 'pay_src'
        when src_code in ('PAY_WAY','PI_TYPE') then 'pay_way'
        when src_code in ('CHANNEL_TYPE') then 'chnl'
        end,src_value,src_value_desc
    ;
    """,
    
    # 结算明细
    "sql_02": """
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_sett;
    create table if not exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_sett as
    -- 哆啦宝
    select
    'DLB' as plat_src
    ,pay_num as pay_num
    ,concat_ws(',',collect_list(if(business_type in ('SALES','SHARING','REFUND'),business_type,null))) as sett_type
    ,concat_ws(',',collect_list(if(business_type in ('SALES_FEE','SHARING_FEE','REFUND_FEE'),business_type,null))) as fee_type
    ,sum(case when business_type in ('SALES','SHARING') then amount
            when business_type in ('REFUND') then (0-amount)
            end) as sett_amt
    ,sum(case when business_type in ('SALES_FEE','SHARING_FEE') then amount
            when business_type in ('REFUND_FEE') then (0-amount)
            end) as fee_amt
    from
    (
        select chg_num,pay_num,biz_type as business_type,sett_amt as amount
        ,row_number() over(partition by chg_num order by modify_time desc) as rn
        from idm.idm_f02_pay_sfjh_dora_acct_chg_dtl_i_d --odm.odm_pay_jhzf_dora_account_change_0_i_d
        where dt BETWEEN date_sub('{TX_DATE}',34) AND '{TX_DATE}'
    )t
    where rn=1
    group by pay_num
    union all
    -- 自研
    select
    'JD' as plat_src
    ,trade_no as pay_num
    ,'' as sett_type
    ,'' as fee_type
    ,set_amt/100 as sett_amt
    ,mer_fee/100 as fee_amt
    from
    (
        select trade_no,request_no,set_amt,mer_fee
        ,row_number()over(partition by trade_no order by modified_date desc) rn
        from odm.odm_pay_jhzf_orifile_set_detail_20190101_00_s_d 
        where dt=date_sub('{TX_DATE}',2) -- 再往前一日 
        and to_date(pay_time) BETWEEN date_sub('{TX_DATE}',34) AND '{TX_DATE}'
        and trade_status not in('ACSU', '撤销')
        and order_amt > 0
    )t
    where rn=1
    ;
    """,
    
    # 店铺明细
    "sql_03": """
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_shop;
    create table if not exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_shop as
    select
    tshop.shop_num
    ,tshop.shop_nm  -- 店铺名称
    ,tshop.shop_inds_code1  -- 店铺一级行业编码
    ,tshop.shop_inds_nm1  -- 店铺一级行业名称
    ,tshop.shop_inds_code2  -- 店铺二级行业编码
    ,tshop.shop_inds_nm2  -- 店铺二级行业名称
    ,tshop.agent_num  -- 代理商编号
    ,tagent.agent_full_nm  -- 代理商全称
    ,tagent.agent_sale_num  -- 代理商销售编号
    ,tagent.agent_sale_nm  -- 代理商销售名称
    ,tshop.shop_sett_time  -- 店铺入驻时间
    ,tshop.mht_sys_src
    from
    (
        select
        shop_num
        ,shop_nm
        ,inds_code1 as shop_inds_code1
        ,inds_nm1 as shop_inds_nm1
        ,inds_code2 as shop_inds_code2
        ,inds_nm2 as shop_inds_nm2
        ,trim(nvl(agent_num,'')) as agent_num  -- 防止关联时有null值
        ,shop_sett_time
        ,mht_sys_src
        from idm.idm_c01_pay_sfjh_shop_info_s_d
        where dt = '{TX_DATE}'
    )tshop
    left join
    (
        select
        agent_num
        ,agent_full_name as agent_full_nm
        ,agent_sellr_num as agent_sale_num
        ,agent_sellr_nm as agent_sale_nm
        from idm.idm_c01_pay_sfjh_agent_info_s_d
        where dt = '{TX_DATE}'
    )tagent
    on tshop.agent_num=tagent.agent_num
    ;
    """,
    
    # 商户明细
    "sql_04": """
    -- 商户
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_mht;
    create table if not exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_mht as
    select
    tmht.mht_num
    ,tmht.mht_full_nm  -- 商户全称
    ,tmht.mht_province  -- 商户省份
    ,tmht.mht_city  -- 商户城市
    ,tmht.mht_district  -- 商户区域
    ,tmht.mht_inds_code  -- 商户行业编码
    ,tmht.mht_inds_nm  -- 商户行业名称
    ,tmht.mht_type_code  -- 商户类型编码
    ,tmhttyp.dim_nm as mht_type_nm
    ,tmht.mht_sale_num  -- 商户销售编码，关联字段
    ,tmht.mht_sale_nm  -- 商户销售名称
    ,tmht.mht_sale_dept_num  -- 商户销售部门编码
    ,tmht.agent_num   -- 关联代理商
    ,tagent.agent_full_nm
    ,tagent.agent_sale_num
    ,tagent.agent_sale_nm
    ,tmht.mht_sett_time  -- 商户入驻时间
    ,tmht.mht_sys_src  -- 商户来源
    from
    (
        select
        mht_num
        ,mht_full_nm
        ,mht_province
        ,mht_city
        ,mht_district
        ,inds_num as mht_inds_code
        ,inds_nm as mht_inds_nm
        ,nvl(mht_type,'') as mht_type_code
        ,xiaoer_num as mht_sale_num
        ,xiaoer_nm as mht_sale_nm
        ,xiaoer_dept_num as mht_sale_dept_num
        ,nvl(agent_num,'') as agent_num   -- 防止关联时有null值
        ,fst_report_cmplt_time as mht_sett_time
        ,if(sys_src in ('ppms','lhrh'),sys_src,'dlb') as mht_sys_src
        from idm.idm_c01_pay_sfjh_mht_info_s_d
        where dt = '{TX_DATE}'
    )tmht
    left join
    (
        select
        agent_num
        ,agent_full_name as agent_full_nm
        ,agent_sellr_num as agent_sale_num
        ,agent_sellr_nm as agent_sale_nm
        from idm.idm_c01_pay_sfjh_agent_info_s_d
        where dt = '{TX_DATE}'
    )tagent
    on tmht.agent_num=tagent.agent_num
    left join
    (
        select dim,dim_code,dim_nm
        from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_dim
        where dim='mht_type'
    )tmhttyp
    on tmht.mht_type_code=tmhttyp.dim_code
    ;
    """,
    
    # 订单明细
    "sql_05": """
    -- 订单
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_ordr;
    create table if not exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_ordr as
    select
    tordr.ordr_num  -- 订单编号
    ,tordr.orgnl_ordr_num  -- 原订单编号
    ,tordr.reqst_num  -- 商户请求号
    ,tordr.ordr_batch_num  -- 订单批次编号
    ,tordr.mht_num  -- 商户编号
    ,tordr.shop_num  -- 店铺编号
    ,tordr.mht_sale_num  -- 商户销售编号
    ,tordr.mach_num  -- 机具编号
    ,tordr.mem_num  -- 会员编号
    ,tordr.trade_type_code  -- 交易类型编码
    ,ttradtyp.dim_nm as trade_type_nm
    ,tordr.pay_way_ordr  -- 支付方式编码
    ,tordr.biz_type  -- 业务类型
    ,tordr.mht_src -- 商户来源
    ,tordr.ordr_src  -- 订单来源
    ,tordr.ordr_type  -- 订单类型
    ,tordr.ordr_status  -- 订单状态
    ,tordr.refund_flag  -- 退款标记  -- 哆啦宝
    ,tordr.ordr_amt  -- 订单金额
    --,tordr.fee_rate  -- 费率
    ,tordr.ordr_create_time  -- 订单创建时间
    ,tordr.ordr_modify_time  -- 订单修改时间
    ,tordr.ordr_cmplt_time  -- 订单完成时间
    ,tordr.is_yn  -- 是否有效
    ,tordr.refund_time  -- 扩展字段
    ,tordr.xpn1  -- 扩展字段
    ,tordr.mht_ret_info  -- 扩展字段
    --,nvl(get_json_object(xpn1,'$.biz_trade_no'),'') as biz_trade_no
    ,tordr.plat_src
    ,case when tordr.mht_src = 'xintonglu' then '双屏收银机及配件-新通路渠道'
        when tyj.v021 = 'selfhelp' then '自助收银机'
        when tyj.v021 = 'pos' then '智能POS及配件'
        when tyj.v021 = 'phenixPay' then '青鸾app'
        else null
        end as prod_type,device
    from
    (
        select * from
        (
            select
            ordr_num
            ,orgnl_ordr_num
            ,nvl(reqst_ordr_num,'') as reqst_num
            ,batch_num as ordr_batch_num
            ,mht_num
            ,shop_num
            ,user_num as mht_sale_num
            ,mach_num
            ,mem_num
            ,case when plat_src='DLB' then 'T_GEN'
                when plat_src='JD' then nvl(get_json_object(xpn1,'$.trade_type'),'')
                end as trade_type_code
            ,pay_way as pay_way_ordr
            ,biz_type
            ,mht_src
            ,ordr_src
            ,ordr_type
            ,ordr_status
            ,if(trim(nvL(refund_time,''))<>'' or refund_amt>0,'1','0') as refund_flag
            ,ordr_amt
            ,create_time as ordr_create_time
            ,modify_time as ordr_modify_time
            ,cmplt_time as ordr_cmplt_time
            ,is_yn
            ,xpn1
            ,refund_time
            ,mht_ret_info  -- 扩展字段
            ,plat_src
            ,row_number() over(partition by ordr_num order by modify_time desc) as rn
            --,get_json_object(get_json_object(xpn1,'\$.ext_map'),'\$.deviceInfo') as bs_device --设备号  20210310
            --,get_json_object(Get_json_object(xpn1,'\$.mht_ret_info'),'\$.sn') as zs_device---主扫设备号 20210616
            ,if(trim(nvl(get_json_object(get_json_object(xpn1,'\$.ext_map'),'\$.deviceInfo'),''))<>''
                ,get_json_object(get_json_object(xpn1,'\$.ext_map'),'\$.deviceInfo')
                ,get_json_object(Get_json_object(xpn1,'\$.mht_ret_info'),'\$.sn')
                ) as device  -- 20210616 增加主扫 合并主被扫设备号 用来判断双屏 京东数码渠道
            from idm.idm_f02_pay_sfjh_ordr_dtl_i_d
            where dt between date_sub('{TX_DATE}',30) and '{TX_DATE}'
            and sys_src in ('DLB','JD')
        )t
        where rn=1
    )tordr
    left join
    (
        select dim,dim_code,dim_nm
        from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_dim
        where dim='trade_type'
    )ttradtyp
    on tordr.trade_type_code=ttradtyp.dim_code
    left join
    (
        select v021,v045
        from odm.ODM_INN_DSP_BIG_00_000_I_D
        where dt='{TX_DATE}'
        and v021 in ('selfhelp','pos','phenixPay')
        and trim(nvl(v045,''))<>''
        group by v021,v045
    )tyj
    on tordr.reqst_num=tyj.v045  -- 20210616调整
    --on tordr.ordr_num=tyj.v042
    ;
    """,
    
    # 支付单明细
    "sql_06": """
    -- 支付单
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_pay;
    create table if not exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_pay as
    select
    tpay.pay_num  -- 支付单编号
    ,tpay.ordr_num  -- 订单编号 ,用于关联
    ,tpay.orgnl_ordr_num  -- 原订单编号
    ,tpay.refund_reqst_num  -- 商户退款请求号
    ,tpay.pay_batch_num  -- 支付单批次编号
    ,tpay.pay_orgnl_batch_num  -- 支付单原批次编号
    ,tpay.bank_ordr_num  -- 银行订单号
    ,tpay.mht_num  -- 商户编号 ,用于关联
    ,tpay.chnl_mht_num  -- 渠道商户号
    ,tpay.shop_num  -- 店铺编号 ,用于关联
    ,tpay.mach_num  -- 机具编号 ,用于关联
    ,tpay.mem_num  -- 会员编号
    ,tpay.open_id  -- 用户openid
    ,tpay.user_pin  -- 用户pin
    ,tpay.auth_code  -- 授权码
    ,tpay.app_id  -- appid
    ,tpay.bank_type  -- 银行类型
    ,tpay.card_type  -- 卡类型
    ,tpay.prod_code  -- 产品编码
    ,tprod.dim_nm as prod_nm
    ,tpay.pay_src_code  -- 支付来源编码
    ,tpaysrc.dim_nm as pay_src_nm
    ,tpay.pay_way_pay  -- 原支付方式编码
    --,tpay.scan_type  -- 主被扫
    ,tpay.chnl_code  -- 渠道编码
    ,tchnl.dim_nm as chnl_nm
    ,tpay.bt_pay_way  -- 白条支付方式
    ,tpay.wx_pay_way  -- 微信支付方式
    ,tpay.alpy_hb_stg_cnt  -- 支付宝花呗分期数
    ,tpay.prst_sys_type  -- 前置系统类型
    ,tpay.orgnl_pay_type  -- 原支付类型
    ,tpay.pay_type  -- 支付类型
    ,tpay.pay_status  -- 支付状态
    ,tpay.pay_amt  -- 交易金额  支付单金额
    ,tpay.actl_tx_amt  -- 实际交易金额
    ,tpay.coupon_amt  -- 优惠券金额
    ,tpay.pay_create_time  -- 支付单创建时间
    ,tpay.pay_modify_time  -- 支付单修改时间
    ,tpay.pay_cmplt_time  -- 支付单完成时间
    ,tpay.is_yn  -- 是否有效
    ,tpay.xpn1  -- 扩展字段
    ,tpay.can_accounted
    ,tpay.is_accounted
    ,tpay.info_detail
    ,tpay.actl_pay_amt
    ,tpay.plat_src
    from
    (
        select * from
        (
            select
            pay_num
            ,ordr_num
            ,orgnl_ordr_num
            ,refund_reqst_num
            ,batch_num as pay_batch_num
            ,orgnl_batch_num as pay_orgnl_batch_num
            ,bank_ordr_num
            ,mht_num
            ,chnl_mht_num
            ,shop_num
            ,mach_num
            ,mem_num
            ,open_id
            ,user_pin
            ,auth_code
            ,chnl_mht_app_id as app_id
            ,bank_type
            ,card_type
            ,nvl(prod_num,'') as prod_code
            ,nvl(pay_src,'') as pay_src_code
            ,nvl(pay_way,'') as pay_way_pay
            ,nvl(get_json_object(xpn1,'$.channel_type'),'') as chnl_code
            ,case 
                -- 哆啦宝
                when plat_src='DLB' and pay_way in ('SHHFYXN_JIOU_h5') then '白条直连'
                when plat_src='DLB' and bank_type = 'JDPAY' and card_type = '1' then '白条支付'
                when plat_src='DLB' and pay_src='WX' and bank_type = 'JDPAY' then '白条前置'
                -- 自研
                when plat_src='JD' and pay_way in ('JIOU') then '白条直连'
                when plat_src='JD' and get_json_object(get_json_object(xpn1,'\$.ext_map'),'\$.preProduct') like '%BAITIAO_ALL%' then '白条前置'
                end as bt_pay_way
            ,wx_pay_way
            ,alpy_hb_stg_cnt
            ,prst_sys_type
            ,orgnl_pay_type
            ,pay_type
            ,pay_status
            ,pay_amt
            ,case when plat_src='DLB' and pay_type='PAY' then actl_pay_amt
                when plat_src='DLB' and pay_type='REFUND' then actl_refund_amt
                when plat_src='JD' then actl_pay_amt
                end as actl_tx_amt
            ,coupon_amt
            ,create_time as pay_create_time
            ,modify_time as pay_modify_time
            ,cmplt_time as pay_cmplt_time
            ,is_yn
            ,xpn1
            ,can_accounted
            ,is_accounted
            ,info_detail
            ,actl_pay_amt
            ,plat_src
            ,row_number() over(partition by pay_num order by modify_time desc) as rn
            from idm.idm_f02_pay_sfjh_pay_dtl_i_d
            where dt between date_sub('{TX_DATE}',30) and '{TX_DATE}'
            and sys_src in ('DLB','JD')
        )t
        where rn=1
    )tpay
    left join
    (
        select dim,dim_code,dim_nm
        from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_dim
        where dim='prod'
    )tprod
    on tpay.prod_code=tprod.dim_code
    left join
    (
        select dim,dim_code,dim_nm
        from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_dim
        where dim='pay_src'
    )tpaysrc
    on tpay.pay_src_code=tpaysrc.dim_code
    left join
    (
        select dim,dim_code,dim_nm
        from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_dim
        where dim='chnl'
    )tchnl
    on tpay.chnl_code=tchnl.dim_code
    ;
    """,
    
    # 关联,订单支付单
    "sql_07": """
    -- 关联,订单支付单
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_ordr_pay;
    create table if not exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_ordr_pay as
    select
    -- 单号
    tordr.ordr_num  -- 订单编号 -- trade_no
    ,tpay.pay_num  -- 支付单编号
    ,tpay.orgnl_ordr_num  -- 原订单编号
    ,tordr.reqst_num  -- 商户请求号 -- out_trade_no
    ,tpay.refund_reqst_num  -- 商户退款请求号
    ,tordr.ordr_batch_num  -- 订单批次编号 -- trade_pay_request_no
    ,tpay.pay_batch_num  -- 支付单批次编号 -- trade_pay_request_no
    ,tpay.pay_orgnl_batch_num  -- 支付单原批次编号  -- original_trade_pay_request_no
    ,tpay.bank_ordr_num  -- 银行订单号 -- 可用于关联核销订单号，自研没有
    -- 商户
    ,if(nvl(tpay.mht_num,'')<>'',tpay.mht_num,nvl(tordr.mht_num,'')) as mht_num -- 商户编号
    ,tpay.chnl_mht_num  -- 渠道商户号
    -- 店铺
    ,if(nvl(tpay.shop_num,'')<>'',tpay.shop_num,nvl(tordr.shop_num,'')) as shop_num
    -- 销售
    ,tordr.mht_sale_num  -- 商户销售编号
    -- 机具
    ,if(nvl(tpay.mach_num,'')<>'',tpay.mach_num,nvl(tordr.mach_num,'')) as mach_num  -- 机具编号
    -- 用户
    ,if(nvl(tpay.mem_num,'')<>'',tpay.mem_num,nvl(tordr.mem_num,'')) as mem_num  -- 会员编号
    ,tpay.open_id  -- 用户openid
    ,tpay.user_pin  -- 用户pin
    ,tpay.auth_code  -- 授权码
    ,tpay.bank_type  -- 银行类型
    ,tpay.card_type  -- 卡类型
    -- 支付维度
    ,tordr.prod_type
    ,tpay.prod_code  -- 产品编码
    ,tpay.prod_nm  -- 产品名称
    ,tordr.trade_type_code  -- 交易类型编码
    ,tordr.trade_type_nm
    ,tpay.pay_src_code  -- 支付来源编码
    ,tpay.pay_src_nm
    -- payway
    ,if(nvl(tpay.pay_way_pay,'')<>'',tpay.pay_way_pay,nvl(tordr.pay_way_ordr,'')) as orgnl_pay_way_code
    --,tpay.pay_way_code  -- 支付方式编码
    --,tpay.scan_type  -- 主被扫
    ,tpay.chnl_code  -- 渠道编码
    ,tpay.chnl_nm  -- 渠道名称
    ,tpay.bt_pay_way  -- 白条支付方式
    -- 支付维度
    ,tpay.wx_pay_way  -- 微信支付方式
    ,tpay.alpy_hb_stg_cnt  -- 支付宝花呗分期数
    ,tpay.prst_sys_type  -- 前置系统类型
    ,tpay.app_id  -- appid
    ,tordr.biz_type  -- 业务类型
    ,tordr.mht_src  -- 商户来源
    ,tordr.ordr_src  -- 订单来源
    ,case when tpay.pay_type in ('PAY','REFUND') then tpay.pay_type  -- 哆啦宝关联后存在 ordr_type='PAY' pay_type='REFUND'的情况
        else tordr.ordr_type
        end as ordr_type  -- 订单类型
    ,case when tordr.ordr_status not in ('SUCCESS','REFUND') then tordr.ordr_status
        when tordr.ordr_status in ('SUCCESS','REFUND') and tpay.pay_status = 'SUCCESS' then 'SUCCESS'
        else tpay.pay_status
        end as ordr_status -- 订单状态
    ,tpay.orgnl_pay_type  -- 原支付类型
    ,tpay.pay_type  -- 支付类型
    ,tpay.pay_status  -- 支付状态
    ,tordr.refund_flag  -- 退款标记
    -- 金额
    ,if(nvl(tpay.pay_num,'')<>'',pay_amt,ordr_amt) as tx_amt  -- 交易金额
    ,tpay.actl_tx_amt  -- 实际交易金额
    ,tpay.coupon_amt  -- 优惠券金额
    --,tordr.fee_rate  -- 费率
    -- 结算
    ,tpay.can_accounted
    ,tpay.is_accounted
    ,tordr.ordr_create_time  -- 订单创建时间
    ,tordr.ordr_modify_time  -- 订单修改时间
    ,tordr.ordr_cmplt_time  -- 订单完成时间
    ,tpay.pay_create_time  -- 支付单创建时间
    ,tpay.pay_modify_time  -- 支付单修改时间
    ,tpay.pay_cmplt_time  -- 支付单完成时间
    ,if(nvl(tordr.is_yn,'')<>'',tordr.is_yn,tpay.is_yn) as is_yn
    -- 扩展字段 订单
    ,nvl(get_json_object(tordr.xpn1,'$.type'),'') as type_ordr
    ,nvl(get_json_object(tordr.xpn1,'$.biz_trade_no'),'') as biz_trade_no_ordr
    ,nvl(get_json_object(tordr.xpn1,'$.ext_map'),'') as ext_map_ordr
    ,tordr.ordr_amt as ordr_amt_ordr
    ,tordr.pay_way_ordr
    ,tordr.refund_time as refund_time_ordr
    ,tordr.mht_ret_info as mht_ret_info_ordr
    -- 扩展字段 支付单
    ,nvl(get_json_object(tpay.xpn1,'$.ext_map'),'') as ext_map_pay
    ,tpay.info_detail as info_detail_pay
    ,tpay.actl_pay_amt as actl_pay_amt_pay
    -- 分区
    ,tordr.plat_src,device
    from
    (
        select
        ordr_num
        ,orgnl_ordr_num
        ,reqst_num
        ,ordr_batch_num
        ,mht_num
        ,shop_num
        ,mht_sale_num
        ,mach_num
        ,mem_num
        ,trade_type_code
        ,trade_type_nm
        ,pay_way_ordr
        ,biz_type
        ,mht_src
        ,ordr_src
        ,ordr_type
        ,ordr_status
        ,refund_flag
        ,ordr_amt
        ,ordr_create_time
        ,ordr_modify_time
        ,ordr_cmplt_time
        ,is_yn
        ,refund_time
        ,mht_ret_info
        ,xpn1
        ,plat_src
        ,prod_type,device
        from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_ordr
    )tordr
    left join
    (
        select
        pay_num
        ,nvl(ordr_num,'') as ordr_num
        ,orgnl_ordr_num
        ,refund_reqst_num
        ,pay_batch_num
        ,pay_orgnl_batch_num
        ,bank_ordr_num
        ,mht_num
        ,chnl_mht_num
        ,shop_num
        ,mach_num
        ,mem_num
        ,open_id
        ,user_pin
        ,auth_code
        ,app_id
        ,bank_type
        ,card_type
        ,prod_code
        ,prod_nm
        ,pay_src_code
        ,pay_src_nm
        ,pay_way_pay
        ,chnl_code
        ,chnl_nm
        ,bt_pay_way
        ,wx_pay_way
        ,alpy_hb_stg_cnt
        ,prst_sys_type
        ,orgnl_pay_type
        ,pay_type
        ,pay_status
        ,pay_amt
        ,actl_tx_amt
        ,coupon_amt
        ,can_accounted
        ,is_accounted
        ,pay_create_time
        ,pay_modify_time
        ,pay_cmplt_time
        ,is_yn
        ,xpn1
        ,info_detail
        ,actl_pay_amt
        ,plat_src
        from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_pay
    )tpay
    on tordr.ordr_num=tpay.ordr_num
    where to_date(tordr.ordr_create_time)='{TX_DATE}' or to_date(tordr.ordr_modify_time)='{TX_DATE}'
     or to_date(tpay.pay_create_time)='{TX_DATE}' or to_date(tpay.pay_modify_time)='{TX_DATE}'
    ;
    """,
    
    # 交易关联其他维度
    "sql_08": """
    use idm_tmp;
    drop table if exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_all;
    create table if not exists idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_all as
    -- 交易关联其他维度
    select
    -- 单号
    ordr_num
    ,tx.pay_num
    ,tx.orgnl_ordr_num
    ,tx.reqst_num
    ,tx.refund_reqst_num
    ,tx.ordr_batch_num
    ,tx.pay_batch_num
    ,tx.pay_orgnl_batch_num
    ,tx.bank_ordr_num
    -- 商户
    ,tx.mht_num
    ,tx.chnl_mht_num
    ,tmht.mht_full_nm
    ,tmht.mht_province
    ,tmht.mht_city
    ,tmht.mht_district
    ,tmht.mht_inds_code
    ,tmht.mht_inds_nm
    ,tmht.mht_type_code
    ,tmht.mht_type_nm
    -- mht_sys_src
    ,if(nvl(tmht.mht_sys_src,'')<>'',tmht.mht_sys_src,tshop.mht_sys_src) as mht_sys_src
    -- 店铺
    ,tx.shop_num
    ,tshop.shop_nm
    ,tshop.shop_inds_code1
    ,tshop.shop_inds_nm1
    ,tshop.shop_inds_code2
    ,tshop.shop_inds_nm2
    -- 代理商
    ,if(nvl(tmht.agent_num,'')<>'',tmht.agent_num,tshop.agent_num) as agent_num
    ,if(nvl(tmht.agent_full_nm,'')<>'',tmht.agent_full_nm,tshop.agent_full_nm) as agent_full_nm
    -- 商户销售
    ,if(nvl(tx.mht_sale_num,'')<>'',tx.mht_sale_num,tmht.mht_sale_num) as mht_sale_num
    ,tmht.mht_sale_nm
    ,tmht.mht_sale_dept_num
    -- 代理商销售
    ,if(nvl(tmht.agent_sale_num,'')<>'',tmht.agent_sale_num,tshop.agent_sale_num) as agent_sale_num
    ,if(nvl(tmht.agent_sale_nm,'')<>'',tmht.agent_sale_nm,tshop.agent_sale_nm) as agent_sale_nm
    -- 机具
    ,tx.mach_num
    ,tmach.mach_serl_num
    ,tmach.mach_type
    ,tmach.mach_type_code
    ,tmach.mach_type_nm
    -- 用户
    ,tx.mem_num
    ,tx.open_id
    ,tx.user_pin
    ,tx.auth_code
    ,tx.bank_type
    ,tx.card_type
    -- 支付维度
    ,case
        when nvl(terp.auth_code,'')<>'' then '自助收银机'  -- 20210616 新增判断erp反接聚合的大屏
        when nvl(tx.prod_type,'')<>'' then tx.prod_type
        when if(nvl(tmht.agent_num,'')<>'',tmht.agent_num,tshop.agent_num)='111936383' 
            and (device like'JDD%' or device like 'JX%' or device like'ZQ%') then '双屏收银机及配件-京东数码渠道'
        when tmht.mht_full_nm like '%北京京邦达贸易有限公司%' then 'POS收单'
        when tx.mht_num in ('110225410','113027451','111904953','111097131','111317672','111434658'
                            ,'10001115998167915649917','10001116087148672361098') then 'POS收单'
        when chnl_code in ('WYZX') then '三方聚合收银台产品'
        else '四方聚合收银台产品'
        end as prod_type
    ,tx.prod_code
    ,tx.prod_nm
    ,tx.trade_type_code
    ,tx.trade_type_nm
    ,tx.pay_src_code
    ,tx.pay_src_nm
    -- payway
    ,tx.orgnl_pay_way_code
    ,tpayway.dim_nm as orgnl_pay_way_nm
    -- payway拆分
    ,case
         when tx.plat_src<>'DLB' then tx.orgnl_pay_way_code
         when tx.orgnl_pay_way_code like '%JIOU%'then 'JIOU'  -- 白条支付
         when tx.orgnl_pay_way_code like '%KOUBEI%' then 'KOUBEI'  --'口碑支付宝支付'
         when tx.orgnl_pay_way_code like '%RED_PACKET%'
            or tx.orgnl_pay_way_code in ('COUPON','DISCOUNT') then 'COUPON'  --'营销'
         when tx.orgnl_pay_way_code like '%QQ%' then 'QQ'  -- 'QQ支付'
         when tx.orgnl_pay_way_code like '%WX%' then 'WX'   -- 微信支付
         when tx.orgnl_pay_way_code like '%XCX' then 'WX'  -- 微信支付
         when tx.orgnl_pay_way_code like '%CARD_PAY%' then 'WX'  -- 微信支付
         when tx.orgnl_pay_way_code like '%ALIPAY%' then 'ALIPAY'  -- 支付宝支付
         when tx.orgnl_pay_way_code like '%JD%' then 'JDPAY'  -- 京东支付
         when tx.orgnl_pay_way_code like '%POS%' then 'POS'  -- POP支付
         when tx.orgnl_pay_way_code like '%UNI%' then 'UNIPAY'  -- 银联在线支付
         else tx.orgnl_pay_way_code 
         end as pay_way_code
    ,case
         when tx.plat_src<>'DLB' then tpayway.dim_nm
         when tx.orgnl_pay_way_code like '%JIOU%'then '白条支付'  -- 白条支付
         when tx.orgnl_pay_way_code like '%KOUBEI%' then '口碑支付宝支付'  --'口碑支付宝支付'
         when tx.orgnl_pay_way_code like '%RED_PACKET%'
            or tx.orgnl_pay_way_code in ('COUPON','DISCOUNT') then '营销'  --'营销'
         when tx.orgnl_pay_way_code like '%QQ%' then 'QQ支付'  -- 'QQ支付'
         when tx.orgnl_pay_way_code like '%WX%' then '微信支付'   -- 微信支付
         when tx.orgnl_pay_way_code like '%XCX' then '微信支付'  -- 微信支付
         when tx.orgnl_pay_way_code like '%CARD_PAY%' then '微信支付'  -- 微信支付
         when tx.orgnl_pay_way_code like '%ALIPAY%' then '支付宝支付'  -- 支付宝支付
         when tx.orgnl_pay_way_code like '%JD%' then '京东支付'  -- 京东支付
         when tx.orgnl_pay_way_code like '%POS%' then 'POP支付'  -- POP支付
         when tx.orgnl_pay_way_code like '%UNI%' then '银联在线支付'  -- 银联在线支付
         else tpayway.dim_nm
         end as pay_way_nm
    ,case when tx.plat_src='DLB' and tx.orgnl_pay_way_code like '%SCAN%' then '被扫'
        when tx.plat_src='DLB' then '主扫' 
        when tx.plat_src='JD' and tx.prod_code='BAR' then '被扫' 
        when tx.plat_src='JD' and tx.prod_code='SIGN' then '代扣'
        else  '主扫'
        end as scan_type
    ,case when tx.plat_src='JD' then tx.chnl_code
        when tx.orgnl_pay_way_code like'%HELIPAY%' then'HELIPAY'
        when tx.orgnl_pay_way_code like'%YEEPAY%' then 'YEEPAY'
        when tx.orgnl_pay_way_code like'%XH%' then 'XINHUI'
        when tx.orgnl_pay_way_code in('WXSCANSERVER','WXSERVER','WX_SERVER_NATIVE','CARD_PAY','WX','WX_POP') then 'WXSERVER'
        when tx.orgnl_pay_way_code in('ALIPAY_SERVER','ALIPAY_SERVER_SCAN','ALIPAY','ALIPAY_SERVER_NATIVE','KOUBEI_ALIPAY','KOUBEI_ALIPAY_SCAN') then 'ALISERVER'
        else 'OTHER'
        end as chnl_code
    ,case when tx.plat_src='JD' then tx.chnl_nm
        when tx.orgnl_pay_way_code like'%HELIPAY%' then'合利宝'
        when tx.orgnl_pay_way_code like'%YEEPAY%' then '易宝'
        when tx.orgnl_pay_way_code like'%XH%' then '信汇'
        when tx.orgnl_pay_way_code in('WXSCANSERVER','WXSERVER','WX_SERVER_NATIVE','CARD_PAY','WX','WX_POP') then '微信直连'
        when tx.orgnl_pay_way_code in('ALIPAY_SERVER','ALIPAY_SERVER_SCAN','ALIPAY','ALIPAY_SERVER_NATIVE','KOUBEI_ALIPAY','KOUBEI_ALIPAY_SCAN') then '支付宝直连'
        else '其他' 
        end as chnl_nm
    -- 支付维度
    ,case when tx.plat_src='DLB' and tx.prst_sys_type in ('HELIPAY_FRONT','XH_FRONT') then '前置交易'
        when tx.plat_src='DLB' and (
                orgnl_pay_way_code like '%HELIPAY%' 
                or orgnl_pay_way_code like '%YEEPAY%' 
                or orgnl_pay_way_code like '%XH%'
            ) then '非前置交易'
        when tx.plat_src='DLB' and orgnl_pay_way_code in (
                'WXSCANSERVER','WXSERVER','WX_SERVER_NATIVE','CARD_PAY','WX','WX_POP'  -- 微信直连
                ,'ALIPAY_SERVER','ALIPAY_SERVER_SCAN','ALIPAY','ALIPAY_SERVER_NATIVE','KOUBEI_ALIPAY','KOUBEI_ALIPAY_SCAN' -- 支付宝直连
            )then '直连交易'
        -- when tx.plat_src='DLB' then '非前置交易'
        when tx.plat_src='JD' and chnl_code in ('HLB_U_F','XH_U_F') then '前置交易'
        when tx.plat_src='JD' and chnl_code in ('ZL','WYZX') then '直连交易'
        -- when tx.plat_src='JD' then '非前置交易'
        else '非前置交易'
        end as chnl_tx_type
    ,case when tx.plat_src='JD' and chnl_code = 'HLB_U_F' then 'HELIPAY_FRONT'
        when tx.plat_src='JD' and chnl_code = 'XH_U_F' then 'XH_FRONT'
        else tx.prst_sys_type
        end as prst_sys_type
    ,tx.bt_pay_way
    ,tx.wx_pay_way
    ,tx.alpy_hb_stg_cnt
    ,tx.app_id
    ,tx.biz_type
    ,tx.mht_src
    ,tx.ordr_src
    ,tx.ordr_type
    ,tx.ordr_status
    ,tx.orgnl_pay_type
    ,tx.pay_type
    ,tx.pay_status
    ,tx.refund_flag
    -- 金额
    ,tx.tx_amt
    ,tx.actl_tx_amt
    ,tx.coupon_amt
    -- 结算
    ,tx.can_accounted
    ,tx.is_accounted
    ,tsett.sett_type
    ,tsett.fee_type
    ,tsett.sett_amt
    ,if(tx.plat_src='JD' and tx.pay_type='REFUND' and nvl(tsett.fee_amt,'')<>'',0-tsett.fee_amt,tsett.fee_amt) as fee_amt  -- 老聚合，退单手续费>0，需要特殊处理。
    ,'' as fee_rate  -- dlb一个商户对应多个费率 
    -- 时间
    ,tx.ordr_create_time
    ,tx.ordr_modify_time
    ,tx.ordr_cmplt_time
    ,tx.pay_create_time
    ,tx.pay_modify_time
    ,tx.pay_cmplt_time
    -- 商户入住时间
    ,tmht.mht_sett_time
    ,tshop.shop_sett_time
    ,tmach.mach_bind_time
    -- is_yn
    ,tx.is_yn
    -- xpn1
    ,concat('{{'       
            ,'\\\\"type_ordr\\\\":\\\\"',nvl(tx.type_ordr,''),'\\\\",'
            ,'\\\\"pay_way_ordr\\\\":\\\\"',nvl(tx.pay_way_ordr,''),'\\\\",'
            ,'\\\\"ordr_amt_ordr\\\\":\\\\"',nvl(tx.ordr_amt_ordr,''),'\\\\",'
            ,'\\\\"actl_pay_amt_pay\\\\":\\\\"',nvl(tx.actl_pay_amt_pay,''),'\\\\",'
            ,'\\\\"biz_trade_no_ordr\\\\":\\\\"',nvl(tx.biz_trade_no_ordr,''),'\\\\",'
            ,'\\\\"refund_time_ordr\\\\":\\\\"',nvl(tx.refund_time_ordr,''),'\\\\",'
            ,'\\\\"mht_ret_info_ordr\\\\":',if(nvl(tx.mht_ret_info_ordr,'')<>'',tx.mht_ret_info_ordr,'\\\\"\\\\"'),',' -- json
            ,'\\\\"info_detail_pay\\\\":',if(nvl(tx.info_detail_pay,'')<>'',tx.info_detail_pay,'\\\\"\\\\"'),','  -- json
            ,'\\\\"ext_map_ordr\\\\":',if(nvl(tx.ext_map_ordr,'')<>'',tx.ext_map_ordr,'\\\\"\\\\"'),','  -- json
            ,'\\\\"ext_map_pay\\\\":',if(nvl(tx.ext_map_pay,'')<>'',tx.ext_map_pay,'\\\\"\\\\"')  -- json
            ,'}}') as xpn1
    -- 分区
    ,tx.plat_src
    from
    (
        select
        ordr_num
        ,nvl(pay_num,'') as pay_num
        ,orgnl_ordr_num
        ,reqst_num
        ,refund_reqst_num
        ,ordr_batch_num
        ,pay_batch_num
        ,pay_orgnl_batch_num
        ,bank_ordr_num
        ,nvl(mht_num,'') as mht_num
        ,chnl_mht_num
        ,nvl(shop_num,'') as shop_num
        ,mht_sale_num
        ,nvl(mach_num,'') as mach_num
        ,mem_num
        ,open_id
        ,user_pin
        ,nvl(auth_code,'') as auth_code
        ,bank_type
        ,card_type
        ,prod_type
        ,prod_code
        ,prod_nm
        ,trade_type_code
        ,trade_type_nm
        ,pay_src_code
        ,pay_src_nm
        ,nvl(orgnl_pay_way_code,'') as orgnl_pay_way_code
        ,chnl_code
        ,chnl_nm
        ,bt_pay_way
        ,wx_pay_way
        ,alpy_hb_stg_cnt
        ,prst_sys_type
        ,app_id
        ,biz_type
        ,mht_src
        ,ordr_src
        ,ordr_type
        ,ordr_status
        ,orgnl_pay_type
        ,pay_type
        ,pay_status
        ,refund_flag
        ,tx_amt
        ,actl_tx_amt
        ,coupon_amt
        ,can_accounted
        ,is_accounted
        ,ordr_create_time
        ,ordr_modify_time
        ,ordr_cmplt_time
        ,pay_create_time
        ,pay_modify_time
        ,pay_cmplt_time
        ,is_yn
        ,type_ordr
        ,biz_trade_no_ordr
        ,ext_map_ordr
        ,ordr_amt_ordr
        ,pay_way_ordr
        ,refund_time_ordr
        ,if(nvl(mht_ret_info_ordr,'')<>'' and mht_ret_info_ordr not like '{{%%',concat('\\\\"',mht_ret_info_ordr,'\\\\"'),mht_ret_info_ordr) as mht_ret_info_ordr
        ,ext_map_pay
        ,info_detail_pay
        ,actl_pay_amt_pay
        ,plat_src,device
        from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_ordr_pay
    )tx
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
        ,mht_type_code
        ,mht_type_nm
        ,mht_sale_num
        ,mht_sale_nm
        ,mht_sale_dept_num
        ,agent_num
        ,agent_full_nm
        ,agent_sale_num
        ,agent_sale_nm
        ,mht_sett_time
        ,mht_sys_src
        from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_mht
    )tmht
    on tx.mht_num=tmht.mht_num
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
        ,agent_sale_num
        ,agent_sale_nm
        ,shop_sett_time
        ,mht_sys_src
        from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_shop
    )tshop
    on tx.shop_num=tshop.shop_num
    left join
    (
        select
        mach_num
        ,out_serl_num as mach_serl_num
        ,mach_type
        ,mach_type_num as mach_type_code
        ,mach_type_nm
        ,bind_time as mach_bind_time
        from idm.idm_c01_pay_sfjh_machine_info_s_d  -- 机具
        where dt = '{TX_DATE}'
    )tmach
    on tx.mach_num=tmach.mach_num
    left join
    (
        select dim,dim_code,dim_nm
        from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_dim
        where dim='pay_way'
    )tpayway
    on tx.orgnl_pay_way_code=tpayway.dim_code
    left join
    (
        select pay_num,sett_type,fee_type,sett_amt,fee_amt
        from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_sett  -- 关联pay_num
    )tsett
    on tx.pay_num=tsett.pay_num
    left join
    (
        select auth_code
        from odm.odm_pay_syyj_zzsy_self_trade_table_i_d
        where dt='{TX_DATE}' and trim(nvl(auth_code,''))<>''
        group by auth_code
    )terp
    on terp.auth_code=tx.auth_code -- 20210616 新增判断erp反接聚合的大屏
    ;
    """,
    
    "sql_09": """
    use idm;
    insert overwrite table idm.idm_f02_pay_sfjh_tx_dtl_i_d partition (dt='{TX_DATE}',plat_src)
    select
         current_timestamp() as etl_dt       --ETL日期
        ,*
    from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_all
    ;
    """,
    
    "sql_10": """
    use idm;
    insert overwrite table idm.idm_f02_pay_sfjh_tx_dtl_i_d partition (dt,plat_src)
    -- 回刷结算4天
    select
    etl_dt
    ,ordr_num
    ,this.pay_num
    ,orgnl_ordr_num
    ,reqst_num
    ,refund_reqst_num
    ,ordr_batch_num
    ,pay_batch_num
    ,pay_orgnl_batch_num
    ,bank_ordr_num
    ,mht_num
    ,chnl_mht_num
    ,mht_full_nm
    ,mht_province
    ,mht_city
    ,mht_district
    ,mht_inds_code
    ,mht_inds_nm
    ,mht_type_code
    ,mht_type_nm
    ,mht_sys_src
    ,shop_num
    ,shop_nm
    ,shop_inds_code1
    ,shop_inds_nm1
    ,shop_inds_code2
    ,shop_inds_nm2
    ,agent_num
    ,agent_full_nm
    ,mht_sale_num
    ,mht_sale_nm
    ,mht_sale_dept_num
    ,agent_sale_num
    ,agent_sale_nm
    ,mach_num
    ,mach_serl_num
    ,mach_type
    ,mach_type_code
    ,mach_type_nm
    ,mem_num
    ,open_id
    ,user_pin
    ,auth_code
    ,bank_type
    ,card_type
    ,prod_type
    ,prod_code
    ,prod_nm
    ,trade_type_code
    ,trade_type_nm
    ,pay_src_code
    ,pay_src_nm
    ,orgnl_pay_way_code
    ,orgnl_pay_way_nm
    ,pay_way_code
    ,pay_way_nm
    ,scan_type
    ,chnl_code
    ,chnl_nm
    ,chnl_tx_type
    ,prst_sys_type
    ,bt_pay_way
    ,wx_pay_way
    ,alpy_hb_stg_cnt
    ,app_id
    ,biz_type
    ,mht_src
    ,ordr_src
    ,ordr_type
    ,ordr_status
    ,orgnl_pay_type
    ,pay_type
    ,pay_status
    ,refund_flag
    ,tx_amt
    ,actl_tx_amt
    ,coupon_amt
    ,can_accounted
    ,is_accounted
    ,tsett.sett_type
    ,tsett.fee_type
    ,tsett.sett_amt
    --,tsett.fee_amt
    ,if(plat_src='JD' and pay_type='REFUND' and nvl(tsett.fee_amt,'')<>'',0-tsett.fee_amt,tsett.fee_amt) as fee_amt  -- 老聚合，退单手续费>0，需要特殊处理。
    ,fee_rate
    ,ordr_create_time
    ,ordr_modify_time
    ,ordr_cmplt_time
    ,pay_create_time
    ,pay_modify_time
    ,pay_cmplt_time
    ,mht_sett_time
    ,shop_sett_time
    ,mach_bind_time
    ,is_yn
    ,xpn1
    ,dt
    ,plat_src
    from
    (
        select * from idm.idm_f02_pay_sfjh_tx_dtl_i_d
        where dt between date_sub('{TX_DATE}',4) and date_sub('{TX_DATE}',1)
        and plat_src in ('DLB','JD')
    )this
    left join
    (
        select pay_num,sett_type,fee_type,sett_amt,fee_amt
        from idm_tmp.idm_f02_pay_sfjh_tx_dtl_i_d_sett  -- 关联pay_num
    )tsett
    on nvl(this.pay_num,'')=tsett.pay_num
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
    ['sql_04','sql_05','sql_06'],
    ['sql_07'],
    ['sql_08','sql_10'],
    ['sql_09']
]

# 以下部分无需改动，除非作业有特殊要求
sql_task.set_customized_items(get_customized_items())
# 第二个参数为并行sql_keys配置
return_code = sql_task.execute_sqls_parallel(sql_map, parallel_keys)
exit(return_code)