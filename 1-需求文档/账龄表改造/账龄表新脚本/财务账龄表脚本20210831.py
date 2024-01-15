# !/usr/bin/env python
# -*- coding: utf-8 -*-

from template.base_sql_task import *


#
# 目前支持：RUNNER_SPARK_SQL和RUNNER_HIVE
#
#sql_runner=RUNNER_HIVE
sql_runner=RUNNER_STINGER

def get_customized_items():
    """
     if you need some special values in your sql, please define and calculate then here
     to refer it as {YOUR_VAR} in your sql
    """
    today = Time.today()
    TX_PRE_60_DATE = Time.date_sub(date=today, itv=60)
    TX_PRE_365_DATE = Time.date_sub(date=today, itv=365)
    TX_DATA_DATE = Time.date_sub(date=sql_task.get_tx_date(), itv=int(str(sql_task.get_tx_date())[8:10]))
    print("*************计算账龄数据快照日期为：************")
    print(sql_task.get_tx_date())
    print("*************计算账龄业务日期为：************")
    print(TX_DATA_DATE)
    print("*************基础数据为作业运行时T-1日的数据快照，业务数据为T-1日的上个月月末时点及之前的数据，比如2020-10-07，计算的业务数据为2020-09-30的账龄************")
    return locals()


sql_map={
     # ATTENTION:   ！！！！ sql_01  因为系统按字典顺序进行排序，小于10 的一定要写成0加编号，否则会顺序混乱，数据出问题，切记，切记！！！！
    "sql_01": """
    use dmf_tmp;
    create table if not exists dmf_tmp.ebs_bp_cwaduit_coas_acct_period_evidence_d_s--建表，表结构保持与原金蝶表结构相同，后面用这个表承载ebs与金蝶凭证明细的数据
    (
     id             bigint  comment 'id',
     vouchernum     string  comment '凭证编号',
     bookeddate     string  comment '凭证日期',
     comfid         string  comment '公司fid',
     comnum         string  comment '公司编码',
     comname        string  comment '公司名称',
     accviewgroup   string  comment '会计科目组合',
     accviewlongnum string  comment '会计科目',
     accviewnum     string  comment '科目编码',
     accviewfdc     string  comment '科目方向',
     costorgfid     string  comment '成本中心fid',
     costorgname    string  comment '成本中心',
     costorgnumber  string  comment '成本中心编码',
     dbbank         string  comment '开户行',
     bankaccount    string  comment '银行账户',
     providerfid    string  comment '供应商fid',
     providernum    string  comment '供应商编码',
     providername   string  comment '供应商',
     bizline        string  comment '业务线',
     projectname    string  comment '项目',
     invoicetype    string  comment '发票类型',
     entryseq       double  comment '行号',
     voucheredesc   string  comment '分录摘要',
     assentrydesc   string  comment '辅助账摘要',
     currency       string  comment '币种',
     jforiamt       double  comment '借方金额',
     dforiamt       double  comment '贷方金额',
     jflocalamt     double  comment '借方金额(本位币)',
     dflocalamt     double  comment '贷方金额(本位币)',
     sourcesys      int     comment '来源',
     sourcetype     int     comment '类别',
     creatorname    string  comment '创建人',
     createtime     string  comment '创建时间',
     voucherstatus  string  comment '凭证状态',
     voucherdesc    string  comment '参考信息',
     attachments    double  comment '附件张数',
     bizdate        string  comment '业务日期',
     lastupdatetime string  comment '最后修改时间',
     vouchertype    string  comment '凭证类型',
     bizlinenum     string  comment '业务线代码',
     xzname         string  comment '性质',
     accbizlinenum  string  comment '科目与业务线代码',
     gencompanyfid  string  comment '调整公司fid',
     gencompanynum  string  comment '调整公司代码',
     gencompanyname string  comment '调整公司',
     remark         string  comment '备注',
     userid         string  comment '报表创建者id'
)partitioned BY (dt  string  COMMENT '数据日期分区')
stored AS orc;
    """,
    
    "sql_02": """
    use dmf_tmp;
    alter table dmf_tmp.ebs_bp_cwaduit_coas_acct_period_evidence_d_s drop if exists partition(dt = '{TX_DATA_DATE}');
        """,
        
        
    "sql_03": """
    use dmf_tmp;
    insert into dmf_tmp.ebs_bp_cwaduit_coas_acct_period_evidence_d_s partition(dt = '{TX_DATA_DATE}')
    select
    id	,	--	id
    vouchernum	,	--	凭证编号
    bookeddate	,	--	凭证日期
    comfid	,	--	公司fid
    comnum	,	--	公司编码
    comname	,	--	公司名称
    accviewgroup	,	--	会计科目组合
    case when accviewnum like '1002%' then '银行存款'   ---这两个科目在EBS是一对多的，在映射时出现了问题，与需求方协商一致这里特殊处理为一级科目
         when accviewnum like '1012%' then '其他货币资金'
    else accviewlongnum end     as                                  
    accviewlongnum	,	--	会计科目
    case when accviewnum like '1002%' then '1002'
         when accviewnum like '1012%' then '1012'
    else accviewnum end     as    
    accviewnum	,	--	科目编码
    accviewfdc	,	--	科目方向
    costorgfid	,	--	成本中心fid
    costorgname	,	--	成本中心
    costorgnumber	,	--	成本中心编码
    dbbank	,	--	开户行
    bankaccount	,	--	银行账户
    providerfid	,	--	供应商fid
    providernum	,	--	供应商编码
    providername	,	--	供应商
    bizline	,	--	业务线
    projectname	,	--	项目
    invoicetype	,	--	发票类型
    entryseq	,	--	行号
    voucheredesc	,	--	分录摘要
    assentrydesc	,	--	辅助账摘要
    currency	,	--	币种
    jforiamt	,	--	借方金额
    dforiamt	,	--	贷方金额
    jflocalamt	,	--	借方金额(本位币)
    dflocalamt	,	--	贷方金额(本位币)
    sourcesys	,	--	来源
    sourcetype	,	--	类别
    creatorname	,	--	创建人
    createtime	,	--	创建时间
    cast(voucherstatus as string) as	voucherstatus,	--	凭证状态
    voucherdesc	,	--	参考信息
    attachments	,	--	附件张数
    bizdate	,	--	业务日期
    lastupdatetime	,	--	最后修改时间
    vouchertype	,	--	凭证类型
    bizlinenum	,	--	业务线代码
    xzname	,	--	性质
    accbizlinenum	,	--	科目与业务线代码
    gencompanyfid	,	--	调整公司fid
    gencompanynum	,	--	调整公司代码
    gencompanyname	,	--	调整公司
    remark	,	--	备注
    userid		--	报表创建者id
    from dmf_ada.dmfada_adrpt_h_data_sttag_h_age_a_d voucher --主表
    where dt='2021-05-31' --只取金蝶最后一个分区中的数据
    and bookeddate<='2021-01-01' --由于EBS中包含了2021年全年的数据，且最后一笔金蝶辅助账是2021年1月1日导入，所以此处用20210101节点将数据分割开
    
    union all
    

    select
    je_header_id                                                as id	,	--	id
    cast(doc_sequence_value as string)                          as vouchernum	,	--	凭证编号
    substr(default_effective_date,0,10)                         as bookeddate	,	--	凭证日期
    coa_co_code                                                 as comfid	,	--	公司fid,因为要用公司fid作为汇总的维度，但ebs中没有公司id字段只好选用公司编码
    coa_co_code                                                 as comnum	,	--	公司编码
    coa_co_desc                                                 as comname	,	--	公司名称
    ''                                                          as accviewgroup	,	--	会计科目组合
    case when coa_acc_code like '1002%' then '银行存款'   ---这两个科目在EBS是一对多的，在映射时出现了问题，与需求方协商一致这里特殊处理为一级科目
         when coa_acc_code like '1012%' then '其他货币资金'
    else coa_acc_desc end                                       as accviewlongnum	,	--	会计科目
    case when coa_acc_code like '1002%' then '1002'
         when coa_acc_code like '1012%' then '1012'
    else coa_acc_code end                                       as accviewnum	,	--	科目编码
    ''                                                          as accviewfdc	,	--	科目方向
    coa_dept_code                                               as costorgfid	,	--	成本中心fid
    coa_dept_desc                                               as costorgname	,	--	成本中心
    coa_dept_code                                               as costorgnumber	,	--	成本中心编码
    ''                                                          as dbbank	,	--	开户行
    coa_sac_desc                                                as bankaccount	,	--	银行账户
    coalesce(vendor_data.vendor_site_code,main.vendor_siet_code) as providerfid	,	--	供应商fid,因为要用客商fid作为汇总的维度，但ebs中没有公司id字段只好选用公司编码
    coalesce(vendor_data.vendor_site_code,main.vendor_siet_code) as providernum	,	--	供应商编码
    coalesce(vendor_data.company_name,main.vendor_name)         as providername	,	--	供应商
    case when coa_busi_desc='缺省' then ''                ----缺省的转成空字符串以适配金蝶的空字符串
    else coa_busi_desc
    end                                                         as bizline	,	--	业务线
    coa_pro_desc                                                as projectname	,	--	项目
    ''                                                          as invoicetype	,	--	发票类型
    je_line_num                                                 as entryseq	,	--	行号
    ''                                                          as voucheredesc	,	--	分录摘要
    ''                                                          as assentrydesc	,	--	辅助账摘要
    case when currency_name='人民币元' then '人民币'--ebs中人民币与港元的币种名称与金蝶不同，这里做归一化处理
         when currency_name='港元' then '港币'
         else currency_name
         end                                                    as  currency	,	--	币种
    entered_dr                                                  as jforiamt	,	--	借方金额
    entered_cr                                                  as dforiamt	,	--	贷方金额
    accounted_dr                                                as jflocalamt	,	--	借方金额(本位币)
    accounted_cr                                                as dflocalamt	,	--	贷方金额(本位币)
    cast(je_source  as int)                                     as sourcesys	,	--	来源
    cast(je_category  as int)                                   as sourcetype	,	--	类别
    CREATED_BY_ERP                                              as creatorname	,	--	创建人
    CREATION_DATE                                               as createtime	,	--	创建时间
    STATUS                                                      as voucherstatus	,	--	凭证状态
    ''                                                          as voucherdesc	,	--	参考信息
    0.0                                                         as attachments	,	--	附件张数
    default_effective_date                                      as bizdate	,	--	业务日期
    LAST_UPDATE_DATE                                            as lastupdatetime	,	--	最后修改时间
    ''                                                          as vouchertype	,	--	凭证类型
    case when COA_BUSI_CODE ='0' then ''                ----缺省的转成空字符串以适配金蝶的空字符串
    else COA_BUSI_CODE
    end                                                         as bizlinenum 	,	--	业务线代码
    ''                                                          as xzname	,	--	性质
    ''                                                          as accbizlinenum	,	--	科目与业务线代码
    ''                                                          as gencompanyfid	,	--	调整公司fid
    ''                                                          as gencompanynum	,	--	调整公司代码
    ''                                                          as gencompanyname	,	--	调整公司
    ''                                                          as remark	,	--	备注
    ''                                                          as userid		--	报表创建者id
    from odm.ODM_FI_EBS_CUX_JDTMAS_JOURNAL_DETAILS_V_I_D main --ebs凭证明细表
    left join ( 
        select vendor_site_code,company_name,ou from
        odm.odm_fi_fin_subject_merchant_s_d 
        where 1=1
        and yn_flag='1' 
        and dt = '{TX_DATE}'
        and ou is not null
        and ou not in ('0')
        and vendor_site_code is not null
        ) vendor_data on main.coa_ic_code = vendor_data.ou
    where substr(default_effective_date,0,10)>'2021-01-01'
    and period_name not like '%A'
    and substr(default_effective_date,0,10)<='{TX_DATA_DATE}'
    and STATUS != 'U'---这里去掉未过账的
    
    ;
    """,
    
    "sql_08": """
use dmf_tmp;
drop table if exists dmf_tmp.ebs_bp_cwaduit_coas_evidence;
    """,
    "sql_09": """
use dmf_tmp;
create table if not exists dmf_tmp.ebs_bp_cwaduit_coas_evidence
  as 
  select 
    e.bookedDate,
    e.comfid,
    e.accViewNum,
    e.providerFid,
    e.providernum,
    e.bizlinenum,
    coalesce(e.jfLocalAmt,0.0) as jfLocalAmt,
    coalesce(e.dfLocalAmt,0.0) as dfLocalAmt,
    case when coalesce(e.jfLocalAmt,0.0) > 0.0 and coalesce(e.dfLocalAmt,0.0) = 0.0 then coalesce(e.jfLocalAmt,0.0) 
         when coalesce(e.dfLocalAmt,0.0) < 0.0 and coalesce(e.jfLocalAmt,0.0) = 0.0 then 0.0 - coalesce(e.dfLocalAmt,0.0)
         when coalesce(e.dfLocalAmt,0.0) > 0.0 and coalesce(e.jfLocalAmt,0.0) = 0.0 then coalesce(e.dfLocalAmt,0.0)
         when coalesce(e.jfLocalAmt,0.0) < 0.0 and coalesce(e.dfLocalAmt,0.0) = 0.0 then 0.0 - coalesce(e.jfLocalAmt,0.0)
         else 0.0
    end as amt,
    case when coalesce(e.jfLocalAmt,0.0) > 0.0 and coalesce(e.dfLocalAmt,0.0) = 0.0 then '借正' 
         when coalesce(e.dfLocalAmt,0.0) < 0.0 and coalesce(e.jfLocalAmt,0.0) = 0.0 then '借正' 
         when coalesce(e.dfLocalAmt,0.0) > 0.0 and coalesce(e.jfLocalAmt,0.0) = 0.0 then '贷正' 
         when coalesce(e.jfLocalAmt,0.0) < 0.0 and coalesce(e.dfLocalAmt,0.0) = 0.0 then '贷正'
         else '其他'
    end as dxlx,
    bwb.currency_name as currency,
    case when  substr(accViewNum,1,1) in ('1') then '资产'
         when  substr(accViewNum,1,4) in ('9001') then '资产'
         when  substr(accViewNum,1,1) in ('2') then '负债'
         when  substr(accViewNum,1,1) in ('4') then '权益'
         else '其他' end as kmlx
    from (select * from dmf_tmp.ebs_bp_cwaduit_coas_acct_period_evidence_d_s where dt = '{TX_DATA_DATE}') e
    left join (
      select 
        tba.coa_co_code
        ,tba.base_currency_code
        ,tbb.currency_name
        from
        (
            select
            a.coa_co_code,a.base_currency_code
                from(
                select
                coa_co_code,base_currency_code
                ,row_number() over(partition by coa_co_code order by incremental_date desc) as rn
                from 
                odm.odm_fi_ebs_cux_jdtmas_gl_balances_v_i_d) a --从科目余额表里取出公司编码对应的公司本位币
                where rn=1
                group by coa_co_code,base_currency_code
        ) tba
        left join dmf_add.dmfadd_add_cur_nm_map_a_d tbb --用补录表把国际通用格式的币别转为中文
        on tba.base_currency_code = tbb.currency_code
      ) bwb
    on e.comfid = bwb.coa_co_code
    where (e.accViewNum <> '') 
    and (e.accViewNum <> 'nan') 
    and (e.accViewNum is not null) 
    and COALESCE(e.voucherType,'') != '上季度调整冲销' --这里得问一下，怎么判断
    and COALESCE(e.voucherStatus,'') != '2'
    and e.accViewNum != ''
    and COALESCE(e.voucherDesc,'') not like '%结转损益%' --这里得问一下，怎么判断
;
    """,
    
     "sql_10": """
use dmf_tmp;
drop table if exists dmf_tmp.ebs_bp_cwaduit_coas_evidence_class_day_sum;
    """,
    
    "sql_11": """
use dmf_tmp;

create table if not exists dmf_tmp.ebs_bp_cwaduit_coas_evidence_class_day_sum
    as
    select
    coalesce(bookedDate,'')    as bookedDate,
    coalesce(comfid,'')        as comfid,
    coalesce(accViewNum,'')    as accViewNum,
    coalesce(providerFid,'')   as providerFid,
    coalesce(providernum,'')   as providernum,
    coalesce(bizlinenum,'')    as bizlinenum,
    sum(coalesce(amt,0.0))     as amt,
    sum(case when dxlx = '借正' then coalesce(amt,0.0) else 0.0 end ) as jfamt,
    sum(case when dxlx = '贷正' then coalesce(amt,0.0) else 0.0 end ) as dfamt,
    coalesce(currency,'')      as currency,
    coalesce(kmlx,'')          as kmlx
    from dmf_tmp.ebs_bp_cwaduit_coas_evidence
    where bookedDate <= '{TX_DATA_DATE}' and  
    kmlx in ('资产','负债','权益')
    group by
    bookedDate,
    comfid,
    accViewNum,
    providerFid,
    providernum,
    bizlinenum,
    currency,
    kmlx
;
    """,
    
    "sql_12": """
use dmf_tmp;
drop table if exists dmf_tmp.ebs_bp_cwaduit_coas_age_sum_evd;
    """,
    "sql_13": """
use dmf_tmp;
create table if not exists dmf_tmp.ebs_bp_cwaduit_coas_age_sum_evd
    as
    select
     coalesce(comfid,'')       as  comfid
    ,coalesce(accViewNum,'')   as  accViewNum
    ,coalesce(providerFid,'')  as  providerFid
    ,coalesce(providernum,'')  as  providernum
    ,coalesce(bizlinenum,'')   as  bizlinenum
    ,coalesce(currency,'')     as  currency
    ,coalesce(kmlx,'')         as  kmlx
    ,amt        
    ,case when cast(s.amt as decimal(28,6)) >0 then '有余额' 
          when cast(s.amt as decimal(28,6)) <0 then '有抵消'
          else '平账' end as flag
    from (
        select 
            coalesce(comfid,'')      as comfid,
            coalesce(accViewNum,'')  as accViewNum,
            coalesce(providerFid,'') as providerFid,
            coalesce(providernum,'') as providernum,
            coalesce(bizlinenum,'')  as bizlinenum,
            sum( case when kmlx = '资产' then coalesce(jfamt,0.0) - coalesce(dfamt,0.0)
                      when kmlx = '负债' then coalesce(dfamt,0.0) - coalesce(jfamt,0.0)
                      when kmlx = '权益' then coalesce(dfamt,0.0) - coalesce(jfamt,0.0)
                      else 0.0 end
                ) as amt,
            coalesce(currency,'')    as currency,
            coalesce(kmlx,'')        as kmlx
            from dmf_tmp.ebs_bp_cwaduit_coas_evidence_class_day_sum
            where bookedDate <= '{TX_DATA_DATE}'
            group by 
            comfid,
            accViewNum,
            providerFid,
            providernum,
            bizlinenum,
            currency,
            kmlx
        ) s 
    where cast(s.amt as decimal(28,6)) != 0.0 
        ;
    """,
    
    "sql_14": """
use dmf_tmp;
drop table if exists dmf_tmp.ebs_bp_cwaduit_coas_age_base_evd;
    """,
     "sql_15": """
use dmf_tmp;
create table if not exists dmf_tmp.ebs_bp_cwaduit_coas_age_base_evd
    as
    select 
     coalesce(a.bookedDate ,'')      as bookedDate 
    ,coalesce(a.comfid     ,'')      as comfid     
    ,coalesce(a.accViewNum ,'')      as accViewNum 
    ,coalesce(a.providerFid,'')      as providerFid
    ,coalesce(a.providernum,'')      as providernum
    ,coalesce(a.bizlinenum ,'')      as bizlinenum 
    ,coalesce(a.currency   ,'')      as currency   
    ,coalesce(a.kmlx       ,'')      as kmlx       
    ,case when a.kmlx = '资产' then coalesce(a.jfamt,0.0)
          when a.kmlx = '负债' then coalesce(a.dfamt,0.0)
          when a.kmlx = '权益' then coalesce(a.dfamt,0.0)
          else 0.0 end as  amt        
    from (select * from dmf_tmp.ebs_bp_cwaduit_coas_evidence_class_day_sum ) a
    inner join (select * from dmf_tmp.ebs_bp_cwaduit_coas_age_sum_evd where flag = '有余额' )b
    on  a.comfid      = b.comfid     
    and a.accViewNum  = b.accViewNum 
    and a.providerFid = b.providerFid
    and a.providernum = b.providernum
    and a.bizlinenum  = b.bizlinenum
    and a.currency    = b.currency   
    and a.kmlx        = b.kmlx       
;
    """,
    
    "sql_16": """
use dmf_tmp;
drop table if exists dmf_tmp.ebs_bp_cwaduit_coas_age_base_evd_sum;
    """,
    "sql_17": """
use dmf_tmp;
create table if not exists dmf_tmp.ebs_bp_cwaduit_coas_age_base_evd_sum
    as
    select
     bookedDate
    ,comfid    
    ,accViewNum
    ,providerFid
    ,providernum
    ,bizlinenum
    ,currency  
    ,kmlx  
    ,sum(amt) as sum_amt
    from (
        select 
         b.bookedDate 
        ,a.comfid     
        ,a.accViewNum 
        ,a.providerFid
        ,a.providernum
        ,a.bizlinenum
        ,a.currency   
        ,a.kmlx  
        ,a.amt
        from dmf_tmp.ebs_bp_cwaduit_coas_age_base_evd a
        left join (
            select 
             bookedDate 
            ,comfid     
            ,accViewNum 
            ,providerFid
            ,providernum
            ,bizlinenum
            ,currency   
            ,kmlx  
            from dmf_tmp.ebs_bp_cwaduit_coas_age_base_evd
            group by 
             bookedDate 
            ,comfid     
            ,accViewNum 
            ,providerFid
            ,providernum
            ,bizlinenum
            ,currency   
            ,kmlx  
            ) b
        on  a.comfid       = b.comfid     
        and a.accViewNum   = b.accViewNum 
        and a.providerFid  = b.providerFid
        and a.providernum  = b.providernum
        and a.bizlinenum   = b.bizlinenum
        and a.currency     = b.currency   
        and a.kmlx         = b.kmlx        
        where a.bookeddate  <= b.bookeddate
        ) xx
    group by 
     bookedDate 
    ,comfid     
    ,accViewNum 
    ,providerFid
    ,providernum
    ,bizlinenum
    ,currency   
    ,kmlx  
    ;
    """,
    
    "sql_18": """
use dmf_tmp;
drop table if exists dmf_tmp.ebs_bp_cwaduit_coas_age_off_base_evd;
;
    """,
	"sql_19": """
use dmf_tmp;
create table if not exists dmf_tmp.ebs_bp_cwaduit_coas_age_off_base_evd
    as
    select 
       '{TX_DATA_DATE}' as bookedDate,
       comfid,
       accViewNum,
       providerFid,
       providernum,
       bizlinenum,
       currency,
       kmlx,
       sum(amt) as amt 
       from 
       (
              select 
              a.bookedDate   as bookedDate 
             ,a.comfid       as comfid     
             ,a.accViewNum   as accViewNum 
             ,a.providerFid  as providerFid
             ,a.providernum  as providernum
             ,a.bizlinenum   as bizlinenum
             ,a.currency     as currency   
             ,a.kmlx         as kmlx       
             ,case when a.kmlx = '资产' then coalesce(a.dfamt,0.0)
                   when a.kmlx = '负债' then coalesce(a.jfamt,0.0)
                   when a.kmlx = '权益' then coalesce(a.jfamt,0.0)
                   else 0.0 end as  amt       --这里也没有负数  
             from dmf_tmp.ebs_bp_cwaduit_coas_evidence_class_day_sum a
             inner join (
                select 
                comfid     
                ,accViewNum 
                ,providerFid
                ,providernum
                ,bizlinenum
                ,currency   
                ,kmlx   
                from dmf_tmp.ebs_bp_cwaduit_coas_age_sum_evd where flag = '有余额' 
                group by
                comfid     
                ,accViewNum 
                ,providerFid
                ,providernum
                ,bizlinenum
                ,currency   
                ,kmlx   
                ) b
             on  a.comfid      = b.comfid     
             and a.accViewNum  = b.accViewNum 
             and a.providerFid = b.providerFid
             and a.providernum = b.providernum
             and a.bizlinenum  = b.bizlinenum
             and a.currency    = b.currency   
             and a.kmlx        = b.kmlx   
             where a.bookedDate <= '{TX_DATA_DATE}'
        ) xx
       group by 
       comfid,
       accViewNum,
       providerFid,
       providernum,
       bizlinenum,
       currency,
       kmlx
;
    """,
    
    
    "sql_20": """
use dmf_tmp;
drop table if exists dmf_tmp.ebs_bp_cwaduit_coas_age_evd_check_off;
    """,
	"sql_21": """
use dmf_tmp;
create table if not exists dmf_tmp.ebs_bp_cwaduit_coas_age_evd_check_off
    as
    select
     a.bookedDate 
    ,a.comfid     
    ,a.accViewNum 
    ,a.providerFid
    ,a.providernum
    ,a.bizlinenum
    ,a.currency   
    ,a.kmlx    
    ,(a.sum_amt - b.amt)  as check_amt
    from dmf_tmp.ebs_bp_cwaduit_coas_age_base_evd_sum a
    left join dmf_tmp.ebs_bp_cwaduit_coas_age_off_base_evd b
    on  
        a.comfid      = b.comfid     
    and a.accViewNum  = b.accViewNum 
    and a.providerFid = b.providerFid
    and a.providernum = b.providernum
    and a.bizlinenum  = b.bizlinenum
    and a.currency    = b.currency   
    and a.kmlx        = b.kmlx    
;
    """,
    
    "sql_22": """
use dmf_tmp;
drop table if exists dmf_tmp.ebs_bp_cwaduit_coas_age_evd_check_off_first;
    """,
	"sql_23": """
use dmf_tmp;
create table if not exists dmf_tmp.ebs_bp_cwaduit_coas_age_evd_check_off_first
    as
    select 
          x.bookedDate
         ,x.comfid     
         ,x.accViewNum 
         ,x.providerFid
         ,x.providernum
         ,x.bizlinenum
         ,x.currency   
         ,x.kmlx 
         ,x.check_amt
    from dmf_tmp.ebs_bp_cwaduit_coas_age_evd_check_off x
    inner join (
          select 
          min(bookedDate) as  bookedDate
         ,comfid     
         ,accViewNum 
         ,providerFid
         ,providernum
         ,bizlinenum
         ,currency   
         ,kmlx   
         from dmf_tmp.ebs_bp_cwaduit_coas_age_evd_check_off 
         where check_amt > 0.0
         group by 
          comfid     
         ,accViewNum 
         ,providerFid
         ,providernum
         ,bizlinenum
         ,currency   
         ,kmlx  
        ) y
    on      x.bookedDate     = y.bookedDate
        and x.comfid         = y.comfid     
        and x.accViewNum     = y.accViewNum 
        and x.providerFid    = y.providerFid
        and x.providernum    = y.providernum
        and x.bizlinenum     = y.bizlinenum
        and x.currency       = y.currency   
        and x.kmlx           = y.kmlx 
;
    """,
    
    "sql_24": """
use dmf_tmp;
drop table if exists dmf_tmp.ebs_bp_cwaduit_coas_age_base_evd_tmp;
    """,
	"sql_25": """
use dmf_tmp;
create table if not exists dmf_tmp.ebs_bp_cwaduit_coas_age_base_evd_tmp
    as
    select 
     a.bookedDate
    ,a.comfid     
    ,a.accViewNum 
    ,a.providerFid
    ,a.providernum
    ,a.bizlinenum
    ,a.currency   
    ,a.kmlx  
    ,case when a.bookedDate = b.bookedDate then b.check_amt 
          when a.bookedDate > b.bookedDate then a.amt
          else 0.0 
          end as amt
from dmf_tmp.ebs_bp_cwaduit_coas_age_base_evd a 
left join dmf_tmp.ebs_bp_cwaduit_coas_age_evd_check_off_first b
    on  a.comfid      = b.comfid     
    and a.accViewNum  = b.accViewNum 
    and a.providerFid = b.providerFid
    and a.providernum = b.providernum
    and a.bizlinenum  = b.bizlinenum
    and a.currency    = b.currency   
    and a.kmlx        = b.kmlx        
where a.bookedDate >= b.bookedDate
;
    """,
    
    "sql_26": """
use dmf_tmp;
drop table if exists dmf_tmp.ebs_bp_cwaduit_coas_age_tmp;
    """,
	"sql_27": """
use dmf_tmp;
create table if not exists dmf_tmp.ebs_bp_cwaduit_coas_age_tmp
    as
    select
         a.report_date
        ,a.comfid
        ,b.comnum
        ,b.comname
        ,a.accviewnum
        ,e.accviewlongnum
        ,a.providerfid
        ,a.providernum
        ,c.providername
        ,a.bizlinenum
        ,d.bizline
        ,a.currency
        ,a.kmlx
        ,a.acc_age_total
        ,a.acc_age_0_30d
        ,a.acc_age_31_60d
        ,a.acc_age_61_90d
        ,a.acc_age_91_120d
        ,a.acc_age_121_150d
        ,a.acc_age_151_180d
        ,a.acc_age_181_360d
        ,a.acc_age_1_2y
        ,a.acc_age_2_3y
        ,a.acc_age_3_4y
        ,a.acc_age_4_5y
        ,a.acc_age_above_5y
        from (
                select 
                xx.report_date                              as report_date                         
               ,xx.comfid                                   as comfid               
               ,xx.accviewnum                               as accviewnum                       
               ,xx.providerfid                              as providerfid
               ,xx.providernum                              as providernum                         
               ,xx.bizlinenum                               as bizlinenum                       
               ,xx.currency                                 as currency                   
               ,xx.kmlx                                     as kmlx           
               ,( coalesce(xx.acc_age_0_30d,0.0)    
                + coalesce(xx.acc_age_31_60d,0.0)   
                + coalesce(xx.acc_age_61_90d,0.0)   
                + coalesce(xx.acc_age_91_120d,0.0)  
                + coalesce(xx.acc_age_121_150d,0.0) 
                + coalesce(xx.acc_age_151_180d,0.0) 
                + coalesce(xx.acc_age_181_360d,0.0) 
                + coalesce(xx.acc_age_1_2y,0.0)     
                + coalesce(xx.acc_age_2_3y,0.0)     
                + coalesce(xx.acc_age_3_4y,0.0)     
                + coalesce(xx.acc_age_4_5y,0.0)     
                + coalesce(xx.acc_age_above_5y,0.0) 
                )                                           as acc_age_total
               ,coalesce(xx.acc_age_0_30d,0.0)              as acc_age_0_30d
               ,coalesce(xx.acc_age_31_60d,0.0)             as acc_age_31_60d
               ,coalesce(xx.acc_age_61_90d,0.0)             as acc_age_61_90d
               ,coalesce(xx.acc_age_91_120d,0.0)            as acc_age_91_120d
               ,coalesce(xx.acc_age_121_150d,0.0)           as acc_age_121_150d
               ,coalesce(xx.acc_age_151_180d,0.0)           as acc_age_151_180d
               ,coalesce(xx.acc_age_181_360d,0.0)           as acc_age_181_360d
               ,coalesce(xx.acc_age_1_2y,0.0)               as acc_age_1_2y
               ,coalesce(xx.acc_age_2_3y,0.0)               as acc_age_2_3y
               ,coalesce(xx.acc_age_3_4y,0.0)               as acc_age_3_4y
               ,coalesce(xx.acc_age_4_5y,0.0)               as acc_age_4_5y
               ,coalesce(xx.acc_age_above_5y,0.0)           as acc_age_above_5y
               from
               (
                    select
                   '{TX_DATA_DATE}' as report_date
                   ,coalesce(comfid,'')       as  comfid
                   ,coalesce(accViewNum,'')   as  accViewNum
                   ,coalesce(providerFid,'')  as  providerFid
                   ,coalesce(providernum,'')  as  providernum
                   ,coalesce(bizlinenum,'')   as  bizlinenum
                   ,coalesce(currency,'')     as  currency
                   ,coalesce(kmlx,'')         as  kmlx
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) <= 30 then amt
                         else 0.0 end)  as acc_age_0_30d
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  30 and datediff('{TX_DATA_DATE}',bookeddate) <= 60 then amt
                         else 0.0 end)  as acc_age_31_60d
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  60 and datediff('{TX_DATA_DATE}',bookeddate) <= 90 then amt
                         else 0.0 end)  as acc_age_61_90d
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  90 and datediff('{TX_DATA_DATE}',bookeddate) <= 120 then amt
                         else 0.0 end)  as acc_age_91_120d
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  120 and datediff('{TX_DATA_DATE}',bookeddate) <= 150 then amt
                         else 0.0 end)  as acc_age_121_150d
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  150 and datediff('{TX_DATA_DATE}',bookeddate) <= 180 then amt
                         else 0.0 end)  as acc_age_151_180d
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  180 and datediff('{TX_DATA_DATE}',bookeddate) <= 365 then amt
                         else 0.0 end)  as acc_age_181_360d
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  365 and datediff('{TX_DATA_DATE}',bookeddate) <= 730 then amt
                         else 0.0 end)  as acc_age_1_2y
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  730 and datediff('{TX_DATA_DATE}',bookeddate) <= 1095 then amt
                         else 0.0 end)  as acc_age_2_3y
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  1095 and datediff('{TX_DATA_DATE}',bookeddate) <= 1460 then amt
                         else 0.0 end)  as acc_age_3_4y
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  1460 and datediff('{TX_DATA_DATE}',bookeddate) <= 1825 then amt
                         else 0.0 end)  as acc_age_4_5y
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  1825 then amt
                         else 0.0 end)  as acc_age_above_5y
                   from dmf_tmp.ebs_bp_cwaduit_coas_age_base_evd_tmp
                   group by 
                    comfid
                   ,accviewnum
                   ,providerfid
                   ,providernum
                   ,bizlinenum
                   ,currency
                   ,kmlx
             
                   ) xx
           
          ) a
          left join 
          (
              select coalesce(comfid,'') as comfid,max(coalesce(comnum,'')) as comnum,max(comname) as comname from (select * from dmf_tmp.ebs_bp_cwaduit_coas_acct_period_evidence_d_s where dt = '{TX_DATA_DATE}') xxx group by comfid
          ) b on a.comfid = b.comfid
          left join 
          (
              select 
              coalesce(providerfid,'') as providerfid,
              max(coalesce(providername,'')) as providername 
              from (
              select * 
              from dmf_tmp.ebs_bp_cwaduit_coas_acct_period_evidence_d_s 
              where dt = '{TX_DATA_DATE}'
              and  providernum is not null
              ) yyy group by providerfid
      
          ) c on a.providerfid = c.providerfid
          left join 
          (
              select 
              case when ( trim(bizlinenum)=''  or bizlinenum is  null) then ''  else bizlinenum   end as bizlinenum
              ,max(
                case   when ( trim(bizlinenum)<>''  and bizlinenum is not null) then coalesce(bizline, '')
                else ''
                end
                ) as bizline 
              from (select * from dmf_tmp.ebs_bp_cwaduit_coas_acct_period_evidence_d_s where dt = '{TX_DATA_DATE}') zzz group by  case when ( trim(bizlinenum)=''  or bizlinenum is  null) then ''  else bizlinenum   end
          ) d on a.bizlinenum = d.bizlinenum   
          left join
          (
             select accviewlongnum,accviewnum from 
                (
                select accviewlongnum,accviewnum,row_number() over (partition by accviewnum order by length(accviewlongnum) desc) as rownum 
                from dmf_tmp.ebs_bp_cwaduit_coas_acct_period_evidence_d_s
                where dt = '{TX_DATA_DATE}' and length(accviewlongnum) >0
                group by  accviewlongnum,accviewnum
                ) xx
                where xx.rownum = 1

          ) e on a.accViewNum = e.accViewNum
;
    """,
    
    "sql_28": """
use dmf_tmp;
drop table if exists dmf_tmp.ebs_bp_off_cwaduit_coas_age_base_evd;
    """,
	"sql_29": """
use dmf_tmp;
create table if not exists dmf_tmp.ebs_bp_off_cwaduit_coas_age_base_evd
    as
    select 
     coalesce(a.bookedDate ,'')      as bookedDate 
    ,coalesce(a.comfid     ,'')      as comfid     
    ,coalesce(a.accViewNum ,'')      as accViewNum 
    ,coalesce(a.providerFid,'')      as providerFid
    ,coalesce(a.providernum,'')      as providernum
    ,coalesce(a.bizlinenum ,'')      as bizlinenum 
    ,coalesce(a.currency   ,'')      as currency   
    ,coalesce(a.kmlx       ,'')      as kmlx       
    ,case when a.kmlx = '资产' then coalesce(a.dfamt,0.0)
          when a.kmlx = '负债' then coalesce(a.jfamt,0.0)
          when a.kmlx = '权益' then coalesce(a.jfamt,0.0)
          else 0.0 end as  amt        
    from (select * from dmf_tmp.ebs_bp_cwaduit_coas_evidence_class_day_sum ) a 
    inner join (select * from dmf_tmp.ebs_bp_cwaduit_coas_age_sum_evd where flag = '有抵消' )b
    on  a.comfid      = b.comfid     
    and a.accViewNum  = b.accViewNum 
    and a.providerFid = b.providerFid
    and a.providernum = b.providernum
    and a.bizlinenum  = b.bizlinenum
    and a.currency    = b.currency   
    and a.kmlx        = b.kmlx       
;
    """,
	"sql_30": """
use dmf_tmp;
drop table if exists dmf_tmp.ebs_bp_off_cwaduit_coas_age_base_evd_sum;
    """,
	"sql_31": """
use dmf_tmp;
create table if not exists dmf_tmp.ebs_bp_off_cwaduit_coas_age_base_evd_sum
    as
    select
     bookedDate
    ,comfid    
    ,accViewNum
    ,providerFid
    ,providernum
    ,bizlinenum
    ,currency  
    ,kmlx  
    ,sum(amt) as sum_amt
    from (
        select 
         b.bookedDate 
        ,a.comfid     
        ,a.accViewNum 
        ,a.providerFid
        ,a.providernum
        ,a.bizlinenum
        ,a.currency   
        ,a.kmlx  
        ,a.amt
        from dmf_tmp.ebs_bp_off_cwaduit_coas_age_base_evd a
        left join (
            select 
             bookedDate 
            ,comfid     
            ,accViewNum 
            ,providerFid
            ,providernum
            ,bizlinenum
            ,currency   
            ,kmlx  
            from dmf_tmp.ebs_bp_off_cwaduit_coas_age_base_evd
            group by 
             bookedDate 
            ,comfid     
            ,accViewNum 
            ,providerFid
            ,providernum
            ,bizlinenum
            ,currency   
            ,kmlx  
            ) b
        on  a.comfid       = b.comfid     
        and a.accViewNum   = b.accViewNum 
        and a.providerFid  = b.providerFid
        and a.providernum  = b.providernum
        and a.bizlinenum   = b.bizlinenum
        and a.currency     = b.currency   
        and a.kmlx         = b.kmlx        
        where a.bookeddate  <= b.bookeddate
        ) xx
    group by 
     bookedDate 
    ,comfid     
    ,accViewNum 
    ,providerFid
    ,providernum
    ,bizlinenum
    ,currency   
    ,kmlx  
    ;
    """,
	"sql_32": """
use dmf_tmp;
drop table if exists dmf_tmp.ebs_bp_off_cwaduit_coas_age_off_base_evd;
    """,
	"sql_33": """
use dmf_tmp;
create table if not exists dmf_tmp.ebs_bp_off_cwaduit_coas_age_off_base_evd
    as
    select 
       '{TX_DATA_DATE}' as bookedDate,
       comfid,
       accViewNum,
       providerFid,
       providernum,
       bizlinenum,
       currency,
       kmlx,
       sum(amt) as amt
       from 
       (
              select 
              a.bookedDate   as bookedDate 
             ,a.comfid       as comfid     
             ,a.accViewNum   as accViewNum 
             ,a.providerFid  as providerFid
             ,a.providernum  as providernum
             ,a.bizlinenum   as bizlinenum
             ,a.currency     as currency   
             ,a.kmlx         as kmlx       
             ,case when a.kmlx = '资产' then coalesce(a.jfamt,0.0)
                   when a.kmlx = '负债' then coalesce(a.dfamt,0.0)
                   when a.kmlx = '权益' then coalesce(a.dfamt,0.0)
                   else 0.0 end as  amt        
             from dmf_tmp.ebs_bp_cwaduit_coas_evidence_class_day_sum a
             inner join (
                select 
                comfid     
                ,accViewNum 
                ,providerFid
                ,providernum
                ,bizlinenum
                ,currency   
                ,kmlx   
                from dmf_tmp.ebs_bp_cwaduit_coas_age_sum_evd where flag = '有抵消' 
                group by
                comfid     
                ,accViewNum 
                ,providerFid
                ,providernum
                ,bizlinenum
                ,currency   
                ,kmlx   
                ) b
             on  a.comfid      = b.comfid     
             and a.accViewNum  = b.accViewNum 
             and a.providerFid = b.providerFid
             and a.providernum = b.providernum
             and a.bizlinenum  = b.bizlinenum
             and a.currency    = b.currency   
             and a.kmlx        = b.kmlx   
             where a.bookedDate <= '{TX_DATA_DATE}'

        ) xx
       group by 
       comfid,
       accViewNum,
       providerFid,
       providernum,
       bizlinenum,
       currency,
       kmlx
;
    """,
	"sql_34": """
use dmf_tmp;
drop table if exists dmf_tmp.ebs_bp_off_cwaduit_coas_age_evd_check_off;
    """,
	"sql_35": """
use dmf_tmp;
create table if not exists dmf_tmp.ebs_bp_off_cwaduit_coas_age_evd_check_off
    as
    select
     a.bookedDate 
    ,a.comfid     
    ,a.accViewNum 
    ,a.providerFid
    ,a.providernum
    ,a.bizlinenum
    ,a.currency   
    ,a.kmlx    
    ,(a.sum_amt - b.amt)  as check_amt
    from dmf_tmp.ebs_bp_off_cwaduit_coas_age_base_evd_sum a
    left join dmf_tmp.ebs_bp_off_cwaduit_coas_age_off_base_evd b
    on  a.comfid      = b.comfid     
    and a.accViewNum  = b.accViewNum 
    and a.providerFid = b.providerFid
    and a.providernum = b.providernum
    and a.bizlinenum  = b.bizlinenum
    and a.currency    = b.currency   
    and a.kmlx        = b.kmlx    
;
    """,
	"sql_36": """
use dmf_tmp;
drop table if exists dmf_tmp.ebs_bp_off_cwaduit_coas_age_evd_check_off_first;
    """,
	"sql_37": """
use dmf_tmp;
create table if not exists dmf_tmp.ebs_bp_off_cwaduit_coas_age_evd_check_off_first
    as
    select 
          x.bookedDate
         ,x.comfid     
         ,x.accViewNum 
         ,x.providerFid
         ,x.providernum
         ,x.bizlinenum
         ,x.currency   
         ,x.kmlx 
         ,x.check_amt
    from dmf_tmp.ebs_bp_off_cwaduit_coas_age_evd_check_off x
    inner join (
          select 
          min(bookedDate) as  bookedDate
         ,comfid     
         ,accViewNum 
         ,providerFid
         ,providernum
         ,bizlinenum
         ,currency   
         ,kmlx   
         from dmf_tmp.ebs_bp_off_cwaduit_coas_age_evd_check_off 
         where check_amt > 0.0
         group by 
          comfid     
         ,accViewNum 
         ,providerFid
         ,providernum
         ,bizlinenum
         ,currency   
         ,kmlx  
        ) y
    on      x.bookedDate     = y.bookedDate
        and x.comfid         = y.comfid     
        and x.accViewNum     = y.accViewNum 
        and x.providerFid    = y.providerFid
        and x.providernum    = y.providernum
        and x.bizlinenum     = y.bizlinenum
        and x.currency       = y.currency   
        and x.kmlx           = y.kmlx 
;

    """,
	"sql_38": """
use dmf_tmp;
drop table if exists dmf_tmp.ebs_bp_off_cwaduit_coas_age_base_evd_tmp;
    """,
	"sql_39": """
use dmf_tmp;
create table if not exists dmf_tmp.ebs_bp_off_cwaduit_coas_age_base_evd_tmp
    as
    select 
     a.bookedDate
    ,a.comfid     
    ,a.accViewNum 
    ,a.providerFid
    ,a.providernum
    ,a.bizlinenum
    ,a.currency   
    ,a.kmlx  
    ,case when a.bookedDate = b.bookedDate then b.check_amt 
          when a.bookedDate > b.bookedDate then a.amt
          else 0.0 
          end as amt
from dmf_tmp.ebs_bp_off_cwaduit_coas_age_base_evd a 
left join dmf_tmp.ebs_bp_off_cwaduit_coas_age_evd_check_off_first b
    on  a.comfid      = b.comfid     
    and a.accViewNum  = b.accViewNum 
    and a.providerFid = b.providerFid
    and a.providernum = b.providernum
    and a.bizlinenum  = b.bizlinenum
    and a.currency    = b.currency   
    and a.kmlx        = b.kmlx        
where a.bookedDate >= b.bookedDate
;
    """,
	"sql_40": """
use dmf_tmp;
drop table if exists dmf_tmp.ebs_bp_off_cwaduit_coas_age_tmp;
    """,
	"sql_41": """
use dmf_tmp;
create table if not exists dmf_tmp.ebs_bp_off_cwaduit_coas_age_tmp
    as
    select
         a.report_date
        ,a.comfid
        ,b.comnum
        ,b.comname
        ,a.accviewnum
        ,e.accviewlongnum
        ,a.providerfid
        ,a.providernum
        ,c.providername
        ,a.bizlinenum
        ,d.bizline
        ,a.currency
        ,a.kmlx
        ,-1 * a.acc_age_total      as acc_age_total                  
        ,-1 * a.acc_age_0_30d      as acc_age_0_30d                  
        ,-1 * a.acc_age_31_60d     as acc_age_31_60d                   
        ,-1 * a.acc_age_61_90d     as acc_age_61_90d                   
        ,-1 * a.acc_age_91_120d    as acc_age_91_120d                    
        ,-1 * a.acc_age_121_150d   as acc_age_121_150d                     
        ,-1 * a.acc_age_151_180d   as acc_age_151_180d                     
        ,-1 * a.acc_age_181_360d   as acc_age_181_360d                     
        ,-1 * a.acc_age_1_2y       as acc_age_1_2y                 
        ,-1 * a.acc_age_2_3y       as acc_age_2_3y                 
        ,-1 * a.acc_age_3_4y       as acc_age_3_4y                 
        ,-1 * a.acc_age_4_5y       as acc_age_4_5y                 
        ,-1 * a.acc_age_above_5y   as acc_age_above_5y                     
        from (
                select 
                xx.report_date                              as report_date                         
               ,xx.comfid                                   as comfid               
               ,xx.accviewnum                               as accviewnum                       
               ,xx.providerfid                              as providerfid
               ,xx.providernum                              as providernum                         
               ,xx.bizlinenum                               as bizlinenum                       
               ,xx.currency                                 as currency                   
               ,xx.kmlx                                     as kmlx           
               ,( coalesce(xx.acc_age_0_30d,0.0)    
                + coalesce(xx.acc_age_31_60d,0.0)   
                + coalesce(xx.acc_age_61_90d,0.0)   
                + coalesce(xx.acc_age_91_120d,0.0)  
                + coalesce(xx.acc_age_121_150d,0.0) 
                + coalesce(xx.acc_age_151_180d,0.0) 
                + coalesce(xx.acc_age_181_360d,0.0) 
                + coalesce(xx.acc_age_1_2y,0.0)     
                + coalesce(xx.acc_age_2_3y,0.0)     
                + coalesce(xx.acc_age_3_4y,0.0)     
                + coalesce(xx.acc_age_4_5y,0.0)     
                + coalesce(xx.acc_age_above_5y,0.0) 
                )                                           as acc_age_total
               ,coalesce(xx.acc_age_0_30d,0.0)              as acc_age_0_30d
               ,coalesce(xx.acc_age_31_60d,0.0)             as acc_age_31_60d
               ,coalesce(xx.acc_age_61_90d,0.0)             as acc_age_61_90d
               ,coalesce(xx.acc_age_91_120d,0.0)            as acc_age_91_120d
               ,coalesce(xx.acc_age_121_150d,0.0)           as acc_age_121_150d
               ,coalesce(xx.acc_age_151_180d,0.0)           as acc_age_151_180d
               ,coalesce(xx.acc_age_181_360d,0.0)           as acc_age_181_360d
               ,coalesce(xx.acc_age_1_2y,0.0)               as acc_age_1_2y
               ,coalesce(xx.acc_age_2_3y,0.0)               as acc_age_2_3y
               ,coalesce(xx.acc_age_3_4y,0.0)               as acc_age_3_4y
               ,coalesce(xx.acc_age_4_5y,0.0)               as acc_age_4_5y
               ,coalesce(xx.acc_age_above_5y,0.0)           as acc_age_above_5y
               from
               (
                    select
                   '{TX_DATA_DATE}' as report_date
                   ,coalesce(comfid,'')       as  comfid
                   ,coalesce(accViewNum,'')   as  accViewNum
                   ,coalesce(providerFid,'')  as  providerFid
                   ,coalesce(providernum,'')  as  providernum
                   ,coalesce(bizlinenum,'')   as  bizlinenum
                   ,coalesce(currency,'')     as  currency
                   ,case when kmlx = '资产' then '重分类负债'
                         when kmlx = '负债' then '重分类资产'
                         when kmlx = '权益' then '重分类权益' 
                         else kmlx end        as  kmlx
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) <= 30 then amt
                         else 0.0 end)  as acc_age_0_30d
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  30 and datediff('{TX_DATA_DATE}',bookeddate) <= 60 then amt
                         else 0.0 end)  as acc_age_31_60d
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  60 and datediff('{TX_DATA_DATE}',bookeddate) <= 90 then amt
                         else 0.0 end)  as acc_age_61_90d
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  90 and datediff('{TX_DATA_DATE}',bookeddate) <= 120 then amt
                         else 0.0 end)  as acc_age_91_120d
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  120 and datediff('{TX_DATA_DATE}',bookeddate) <= 150 then amt
                         else 0.0 end)  as acc_age_121_150d
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  150 and datediff('{TX_DATA_DATE}',bookeddate) <= 180 then amt
                         else 0.0 end)  as acc_age_151_180d
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  180 and datediff('{TX_DATA_DATE}',bookeddate) <= 365 then amt
                         else 0.0 end)  as acc_age_181_360d
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  365 and datediff('{TX_DATA_DATE}',bookeddate) <= 730 then amt
                         else 0.0 end)  as acc_age_1_2y
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  730 and datediff('{TX_DATA_DATE}',bookeddate) <= 1095 then amt
                         else 0.0 end)  as acc_age_2_3y
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  1095 and datediff('{TX_DATA_DATE}',bookeddate) <= 1460 then amt
                         else 0.0 end)  as acc_age_3_4y
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  1460 and datediff('{TX_DATA_DATE}',bookeddate) <= 1825 then amt
                         else 0.0 end)  as acc_age_4_5y
                   ,sum(case when datediff('{TX_DATA_DATE}',bookeddate) >  1825 then amt
                         else 0.0 end)  as acc_age_above_5y
                   from dmf_tmp.ebs_bp_off_cwaduit_coas_age_base_evd_tmp
                   group by 
                    comfid
                   ,accviewnum
                   ,providerfid
                   ,providernum
                   ,bizlinenum
                   ,currency
                   ,kmlx
             
                   ) xx
           
          ) a
          left join 
          (
              select coalesce(comfid,'') as comfid,max(coalesce(comnum,'')) as comnum,max(comname) as comname from (select * from dmf_tmp.ebs_bp_cwaduit_coas_acct_period_evidence_d_s where dt = '{TX_DATA_DATE}') xxx group by comfid
          ) b on a.comfid = b.comfid
          left join 
          (
              select coalesce(providerfid,'') as providerfid,
              max(coalesce(providername,'')) as providername 
              from 
              (select 
              * 
              from 
              dmf_tmp.ebs_bp_cwaduit_coas_acct_period_evidence_d_s 
              where dt = '{TX_DATA_DATE}' and  providernum is not null ) yyy group by providerfid
      
          ) c on a.providerfid = c.providerfid
          left join 
          (
              select 
              case when ( trim(bizlinenum)=''  or bizlinenum is  null) then ''  else bizlinenum   end as bizlinenum
              ,max(
                case   when ( trim(bizlinenum)<>''  and bizlinenum is not null) then coalesce(bizline, '')
                else ''
                end
                ) as bizline 
              from (select * from dmf_tmp.ebs_bp_cwaduit_coas_acct_period_evidence_d_s where dt = '{TX_DATA_DATE}') zzz group by  case when ( trim(bizlinenum)=''  or bizlinenum is  null) then ''  else bizlinenum   end
          ) d on a.bizlinenum = d.bizlinenum   
          left join
          (
             select accviewlongnum,accviewnum from 
                (
                select accviewlongnum,accviewnum,row_number() over (partition by accviewnum order by length(accviewlongnum) desc) as rownum 
                from dmf_tmp.ebs_bp_cwaduit_coas_acct_period_evidence_d_s
                where dt = '{TX_DATA_DATE}' and length(accviewlongnum) >0
                group by  accviewlongnum,accviewnum
                ) xx
                where xx.rownum = 1

          ) e on a.accViewNum = e.accViewNum
;
    """,
	"sql_42": """
use dmf_ada;
alter table dmf_ada.dmfada_adrpt_fi_acct_age_ebs_s_m drop if exists partition(dt = '{TX_DATA_DATE}');
    """,
	"sql_43": """
use dmf_ada;
insert into dmf_ada.dmfada_adrpt_fi_acct_age_ebs_s_m partition(dt = '{TX_DATA_DATE}')
  select
  b.*,
  (b.acc_age_0_30d+b.acc_age_31_60d+b.acc_age_61_90d)*b.hskmfx        as hs_acc_age_ud90,
  (b.acc_age_91_120d+b.acc_age_121_150d+b.acc_age_151_180d)*b.hskmfx  as hs_acc_age_91_180,
  b.acc_age_181_360d*b.hskmfx                                         as hs_acc_age_181_360,
  b.acc_age_1_2y*b.hskmfx                                             as hs_acc_age_1y_2y, 
  b.acc_age_2_3y*b.hskmfx                                             as hs_acc_age_2y_3y,
  b.acc_age_3_4y*b.hskmfx                                             as hs_acc_age_3y_4y,
  b.acc_age_4_5y*b.hskmfx                                             as hs_acc_age_4y_5y,
  b.acc_age_above_5y*b.hskmfx                                         as hs_acc_age_above_5y
  from(
  select 
  a.*,
  case when a.kmlx in ('负债','权益','重分类资产')  then -1
       else 1
       end as hskmfx
  from
  (
   select
   report_date      
  ,comfid           
  ,comnum           
  ,comname          
  ,accviewnum  
  ,accviewlongnum     
  ,providerfid      
  ,providernum
  ,providername     
  ,bizlinenum       
  ,bizline          
  ,currency         
  ,kmlx             
  ,cast(acc_age_total    as decimal(28,2)) as acc_age_total       
  ,cast(acc_age_0_30d    as decimal(28,2)) as acc_age_0_30d    
  ,cast(acc_age_31_60d   as decimal(28,2)) as acc_age_31_60d   
  ,cast(acc_age_61_90d   as decimal(28,2)) as acc_age_61_90d   
  ,cast(acc_age_91_120d  as decimal(28,2)) as acc_age_91_120d  
  ,cast(acc_age_121_150d as decimal(28,2)) as acc_age_121_150d 
  ,cast(acc_age_151_180d as decimal(28,2)) as acc_age_151_180d 
  ,cast(acc_age_181_360d as decimal(28,2)) as acc_age_181_360d 
  ,cast(acc_age_1_2y     as decimal(28,2)) as acc_age_1_2y     
  ,cast(acc_age_2_3y     as decimal(28,2)) as acc_age_2_3y     
  ,cast(acc_age_3_4y     as decimal(28,2)) as acc_age_3_4y     
  ,cast(acc_age_4_5y     as decimal(28,2)) as acc_age_4_5y     
  ,cast(acc_age_above_5y as decimal(28,2)) as acc_age_above_5y 
  from dmf_tmp.ebs_bp_cwaduit_coas_age_tmp

  union all

  select 
   report_date      
  ,comfid           
  ,comnum           
  ,comname          
  ,accviewnum  
  ,accviewlongnum      
  ,providerfid      
  ,providernum
  ,providername     
  ,bizlinenum       
  ,bizline          
  ,currency         
  ,kmlx             
  ,cast(acc_age_total    as decimal(28,2)) as acc_age_total       
  ,cast(acc_age_0_30d    as decimal(28,2)) as acc_age_0_30d    
  ,cast(acc_age_31_60d   as decimal(28,2)) as acc_age_31_60d   
  ,cast(acc_age_61_90d   as decimal(28,2)) as acc_age_61_90d   
  ,cast(acc_age_91_120d  as decimal(28,2)) as acc_age_91_120d  
  ,cast(acc_age_121_150d as decimal(28,2)) as acc_age_121_150d 
  ,cast(acc_age_151_180d as decimal(28,2)) as acc_age_151_180d 
  ,cast(acc_age_181_360d as decimal(28,2)) as acc_age_181_360d 
  ,cast(acc_age_1_2y     as decimal(28,2)) as acc_age_1_2y     
  ,cast(acc_age_2_3y     as decimal(28,2)) as acc_age_2_3y     
  ,cast(acc_age_3_4y     as decimal(28,2)) as acc_age_3_4y     
  ,cast(acc_age_4_5y     as decimal(28,2)) as acc_age_4_5y     
  ,cast(acc_age_above_5y as decimal(28,2)) as acc_age_above_5y 
  from dmf_tmp.ebs_bp_off_cwaduit_coas_age_tmp
  )a 
  )b
  
  ;
    """,
    
}

# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)