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
create table if not exists dmf_tmp.bp_cwaduit_coas_acct_period_evidence_d_s
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
     voucherstatus  int     comment '凭证状态',
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
drop table if exists dmf_tmp.bp_cwaduit_coas_acct_period_evidence_tmp;
    """,

    "sql_03": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_cwaduit_coas_acct_period_evidence_tmp
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
     voucherstatus  int     comment '凭证状态',
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

    "sql_04": """
use dmf_tmp;
alter table dmf_tmp.bp_cwaduit_coas_acct_period_evidence_d_s drop if exists partition(dt = '{TX_DATA_DATE}');
    """,
    "sql_05": """
use dmf_tmp;
set hive.auto.convert.join=false;
insert into dmf_tmp.bp_cwaduit_coas_acct_period_evidence_tmp partition(dt = '{TX_DATA_DATE}')



select
'' as id
,cast(voucher.fnumber  as string       )                       voucherNum
,substr(voucher.FBookedDate,1,10)                              bookedDate
,company.fid                                                   comFid
,cast(company.FNumber  as string       )                       comnum
,cast(company.FName_l2 as string       )                       comName
,concat(
   coalesce(accView.fnumber,'')
  ,case when costOrg.fid     is not null then '_' else '_0' end
  ,coalesce(costOrg.fnumber,'')
  ,case when bdBank.fid      is not null then '_' else '_0' end 
  ,coalesce(bdBank.fnumber,'')
  ,case when bankAcc.fid     is not null then '_' else '_0' end 
  ,coalesce(bankAcc.fnumber,'')
  ,case when provider.fid    is not null then '_' else '_0' end 
  ,coalesce(provider.fnumber,'')
  ,case when bizLine.fid     is not null then '_' else '_0' end 
  ,coalesce(regexp_replace(bizLine.flongnumber, '!', '.'),'')
  ,case when geProject.fid   is not null then '_' else '_0' end 
  ,coalesce(regexp_replace(geProject.flongnumber, '!', '.'),'')
  ,case when invoiceType.fid is not null then '_' else '_0' end 
  ,coalesce(invoiceType.fnumber ,'')
  )                                                            accViewGroup
,cast(accView.FDisplayName_l2 as string       )                accViewLongNum
,cast(accView.fnumber         as string       )                accViewNum
,accView.FDC                                                   accViewFDC
,costOrg.fid                                                   costOrgFid
,cast(costOrg.FDisplayName_l2   as string       )              costOrgName
,cast(costOrg.Fnumber           as string       )              costOrgNumber
,cast(bdBank.fname_l2           as string       )              dbBank
,cast(bankAcc.fname_l2          as string       )              bankAccount
,provider.fid                                                  providerFid
,provider.fnumber                                              providernum
,cast(provider.fname_l2         as string       )              providerName
,cast(bizLine.fname_l2          as string       )              bizLine
,cast(geProject.fname_l2        as string       )              projectName
,cast(invoiceType.fname_l2      as string       )              invoiceType
,vouEntry.FSeq                                                 entrySeq
,cast(vouEntry.FDescription     as string       )              voucherEDesc
,cast(vouAssRecord.FDESCRIPTION as string       )              assEntryDesc
,cast(currency.fname_l2         as string       )              currency
,case when  vouAssRecord.Fid is not null
      then case when vouEntry.FEntryDC=1
                then vouAssRecord.FOriginalAmount
           else null end
      else case when vouEntry.FEntryDC=1
                then vouEntry.FOriginalAmount
           else null end
       end                                                      jfOriAmt
,case when  vouAssRecord.Fid is not null
      then case when vouEntry.FEntryDC=0
                then vouAssRecord.FOriginalAmount
           else null end
      else case when vouEntry.FEntryDC=0
                then vouEntry.FOriginalAmount
           else null end
       end                                                      dfOriAmt
,case when  vouAssRecord.Fid is not null
      then case when vouEntry.FEntryDC=1
                then vouAssRecord.FLocalAmount
           else null end
      else case when vouEntry.FEntryDC=1
                then vouEntry.FLocalAmount
           else null end
       end                                                      jfLocalAmt
,case when  vouAssRecord.Fid is not null
      then case when vouEntry.FEntryDC=0
                then vouAssRecord.FLocalAmount
           else null end
      else case when vouEntry.FEntryDC=0
                then vouEntry.FLocalAmount
           else null end
       end                                                      dfLocalAmt
,voucher.fsourceSys                                             sourceSys
,voucher.fsourceType                                            sourceType
,cast(creator.fname_l2      as string       )                   creatorName
,voucher.FCreateTime                                            createTime
,voucher.FBizStatus                                             voucherStatus
,cast(voucher.FDescription  as string       )                   voucherDesc
,voucher.FAttachments                                           attachments
,voucher.FBizDate                                               bizDate
,voucher.FLastUpdateTime                                        lastUpdateTime
,cast(voucherTypes.fname_l2 as string       )                   voucherType
,cast(regexp_replace(bizLine.flongnumber, '!', '.') as string)  bizLineNum
,cast(geXZ.fname_l2         as string       )                   xzName
,cast(case when bizLine.fid is not null then concat(accView.fnumber,regexp_replace(bizLine.flongnumber, '!', '.'),'')
           else concat(accView.fnumber,'')
       end as string )                                          accBizLineNum
,cast(geCompany.fid      as string       )                      genCompanyfid
,cast(geCompany.fnumber  as string       )                      genCompanyNum
,cast(geCompany.fname_l2 as string       )                      genCompanyName
,cast(''   as string       )                                    remark
,''                                                             userId
from ( select * from odm.odm_fi_eas_t_gl_voucher_s_d where dt = '{TX_DATE}' ) voucher 
inner join ( select * from odm.odm_fi_eas_t_org_company_s_d where dt = '{TX_DATE}' ) company on voucher.FCompanyId = company.fid
inner join (select FOrgID,FType from odm.odm_fi_eas_t_pm_orgrange_a_d where FType=10 group by FOrgID,FType) orgRange on voucher.FCompanyId = orgRange.FOrgID 
left join odm.odm_fi_eas_t_bd_period_a_d period on voucher.FPeriodID = period.fid
left join odm.odm_fi_eas_t_bd_vouchertypes_a_d voucherTypes on voucher.FVoucherTypeID = voucherTypes.FID
left join ( select * from odm.odm_fi_eas_t_pm_user_s_d where dt = '{TX_DATE}' ) creator ON voucher.FCreatorID = creator.FID
left join odm.odm_fi_eas_t_gl_voucherentry_a_d vouEntry on voucher.fid = vouEntry.FBillId
left join ( select * from odm.odm_fi_eas_t_bd_currency_s_d where dt = '{TX_DATE}' ) currency on currency.fid = vouEntry.FCurrencyId
left join odm.odm_fi_eas_t_bd_accountview_a_d accView on vouEntry.FAccountID = accView.fid
left join odm.odm_fi_eas_t_gl_voucherassistrecord_a_d vouAssRecord on vouEntry.fid = vouAssRecord.FEntryId
left join odm.odm_fi_eas_t_bd_assistanthg_a_d assHG on vouAssRecord.FAssGrpId = assHG.fid
left join ( select * from odm.odm_fi_eas_t_org_costcenter_s_d where dt = '{TX_DATE}' ) costOrg on assHG.FCostOrgID = costOrg.fid
left join ( select * from odm.odm_fi_eas_t_bd_supplier_s_d where dt = '{TX_DATE}' ) provider on assHG.FProviderID = provider.fid
left join odm.odm_fi_eas_t_bd_accountbanks_a_d bankAcc on assHG.FBankAccountID = bankAcc.fid
left join odm.odm_if_eas_t_bd_bank_a_d bdBank on bdBank.fid = bankAcc.FBANK
left join ( select * from odm.odm_fi_eas_t_bd_generalasstacttype_s_d where dt = '{TX_DATE}' ) bizLine on assHG.FGeneralAssActType1ID = bizLine.fid
left join ( select * from odm.odm_fi_eas_t_bd_generalasstacttype_s_d where dt = '{TX_DATE}' ) invoiceType on assHG.FGeneralAssActType2ID = invoiceType.fid
left join ( select * from odm.odm_fi_eas_t_bd_generalasstacttype_s_d where dt = '{TX_DATE}' ) geProject on assHG.FGeneralAssActType5ID = geProject.fid
left join ( select * from odm.odm_fi_eas_t_bd_generalasstacttype_s_d where dt = '{TX_DATE}' ) geXZ on assHG.FGeneralAssActType4ID = geXZ.fid
left join odm.odm_fi_eas_t_org_baseunit_a_d geCompany on assHG.FCOMPANYORGID = geCompany.fid
where 1=1
and substr(voucher.FBookedDate,1,10)  <= '2020-12-31' --dyt:来源于金蝶的凭证只截止到20201231
union all
select
'' as                                                                    id
,'手动导入辅助账级'                                                      voucherNum
,concat(substr(pp.fnumber,1,4),'-',substr(pp.fnumber,5,2),'-01')         bookedDate
,com.fid                                                                 comFid
,cast(com.FNumber   as string       )                                    comnum
,cast(com.fname_l2  as string       )                                    comName
,concat(
   coalesce(acc.fnumber,'')
  ,case when cos.fid   is not null then '_' else '_0' end 
  ,coalesce(cos.fnumber,'')
  ,case when bank.fid  is not null then '_' else '_0' end 
  ,coalesce(bank.fnumber,'')
  ,case when acct.fid  is not null then '_' else '_0' end 
  ,coalesce(acct.fnumber,'')
  ,case when su.fid    is not null then '_' else '_0' end 
  ,coalesce(su.fnumber,'')
  ,case when g1.fid    is not null then '_' else '_0' end 
  ,coalesce(regexp_replace(g1.flongnumber, '!', '.'),'')
  ,case when g2.fid    is not null then '_' else '_0' end 
  ,coalesce(regexp_replace(g2.flongnumber, '!', '.'),'')
  ,case when g3.fid    is not null then '_' else '_0' end 
  ,coalesce(g3.fnumber,'')
    )                                                                    accViewGroup
,cast(acc.fname_l2  as string       )                                    accViewLongNum
,cast(acc.fnumber   as string       )                                    accViewNum
,acc.FDC                                                                 accViewFDC
,cos.fid                                                                 costOrgFid
,cast(cos.fname_l2  as string       )                                    costOrgName
,cast(cos.fnumber   as string       )                                    costOrgNumber
,cast(bank.fname_l2 as string       )                                    dbBank
,cast(acct.fname_l2 as string       )                                    bankAccount
,su.fid                                                                  providerFid
,su.fnumber                                                              providernum
,cast(su.fname_l2   as string       )                                    providerName
,cast(g1.fname_l2   as string       )                                    bizLine
,cast(g2.fname_l2   as string       )                                    projectName
,cast(g3.fname_l2   as string       )                                    invoiceType
,null                                                                    entrySeq
,null                                                                    voucherEDesc
,null                                                                    assEntryDesc
,cast(curr.fname_l2 as string       )                                    currency
,null                                                                    jfOriAmt
,null                                                                    dfOriAmt
,case when acc.FDC='1' then a.FBeginBalanceLocal else 0 end              jfLocalAmt
,case when acc.FDC='-1' then a.FBeginBalanceLocal else 0 end             dfLocalAmt
,null                                                                    sourceSys
,null                                                                    sourceType
,null                                                                    creatorName
,null                                                                    createTime
,null                                                                    voucherStatus
,null                                                                    voucherDesc
,null                                                                    attachments
,null                                                                    bizDate
,null                                                                    lastUpdateTime
,null                                                                    voucherType
,cast(regexp_replace(g1.flongnumber, '!', '.') as string       )         bizLineNum
,null                                                                    xzName
,null                                                                    accBizLineNum
,cast(geCompany.fid      as string       )                               genCompanyfid
,cast(geCompany.fnumber  as string       )                               genCompanyNum
,cast(geCompany.fname_l2 as string       )                               genCompanyName
,null                                                                    remark
,''                                                                      userId
from odm.odm_fi_eas_t_gl_initassistbalance_a_d a
left join (  select * from odm.odm_fi_eas_t_org_company_s_d where dt = '{TX_DATE}' ) com on com.fid = a.forgunitid
left join odm.odm_fi_eas_t_bd_accountview_a_d  acc on acc.fid = a.faccountid
left join ( select * from odm.odm_fi_eas_t_bd_currency_s_d where dt = '{TX_DATE}' ) curr on curr.fid = a.fcurrencyid
left join odm.odm_fi_eas_t_bd_systemstatusctrol_a_d ss on ss.FCOMPANYID = com.fid
left join odm.odm_fi_eas_t_bd_period_a_d pp on pp.fid = ss.FSTARTPERIODID
left join odm.odm_fi_eas_t_bd_assistanthg_a_d hg on hg.fid =  a.FASSISTGRPID
left join ( select * from odm.odm_fi_eas_t_org_costcenter_s_d where dt = '{TX_DATE}' ) cos on cos.fid = hg.FCOSTORGID
left join odm.odm_fi_eas_t_bd_accountbanks_a_d acct on acct.fid = hg.FBANKACCOUNTID
left join odm.odm_if_eas_t_bd_bank_a_d  bank on bank.fid = acct.FBANK
left join ( select * from odm.odm_fi_eas_t_bd_supplier_s_d where dt = '{TX_DATE}' ) su on su.fid = hg.FPROVIDERID
left join ( select * from odm.odm_fi_eas_t_bd_generalasstacttype_s_d where dt = '{TX_DATE}' ) g1 on g1.fid = hg.FGENERALASSACTTYPE1ID
left join ( select * from odm.odm_fi_eas_t_bd_generalasstacttype_s_d where dt = '{TX_DATE}' ) g2 on g2.fid = hg.FGENERALASSACTTYPE5ID
left join ( select * from odm.odm_fi_eas_t_bd_generalasstacttype_s_d where dt = '{TX_DATE}' ) g3 on g3.fid = hg.FGENERALASSACTTYPE2ID
left join odm.odm_fi_eas_t_org_baseunit_a_d geCompany on hg.FCOMPANYORGID = geCompany.fid
where a.fcurrencyid ='11111111-1111-1111-1111-111111111111DEB58FDC'
  and ss.FSYSTEMSTATUSID ='e45c1988-00fd-1000-e000-33d8c0a8100d02A5514C'
  and concat(substr(pp.fnumber,1,4),'-',substr(pp.fnumber,5,2),'-','01')<='{TX_DATA_DATE}'
union all
select
'' as id
,'手动导入科目级'                                                       voucherNum
,concat(substr(pp.fnumber,1,4),'-',substr(pp.fnumber,5,2),'-01')        bookedDate
,com.fid                                                                comFid
,cast(com.FNumber   as string       )                                   comnum
,cast(com.fname_l2  as string       )                                   comName
,concat(acc.fnumber,'_0_0_0_0_0_0_0')                                   accViewGroup
,cast(acc.fname_l2  as string       )                                   accViewLongNum
,cast(acc.fnumber   as string       )                                   accViewNum
,acc.FDC                                                                accViewFDC
,null                                                                   costOrgFid
,null                                                                   costOrgName
,null                                                                   costOrgNumber
,null                                                                   dbBank
,null                                                                   bankAccount
,null                                                                   providerFid
,null                                                                   providernum
,null                                                                   providerName
,null                                                                   bizLine
,null                                                                   projectName
,null                                                                   invoiceType
,null                                                                   entrySeq
,null                                                                   voucherEDesc
,null                                                                   assEntryDesc
,cast(curr.fname_l2 as string       )                                   currency
,null                                                                   jfOriAmt
,null                                                                   dfOriAmt
,case when acc.FDC='1' then a.FBeginBalanceLocal else 0 end             jfLocalAmt
,case when acc.FDC='-1' then a.FBeginBalanceLocal else 0 end            dfLocalAmt
,null                                                                   sourceSys
,null                                                                   sourceType
,null                                                                   creatorName
,null                                                                   createTime
,null                                                                   voucherStatus
,null                                                                   voucherDesc
,null                                                                   attachments
,null                                                                   bizDate
,null                                                                   lastUpdateTime
,null                                                                   voucherType
,null                                                                   bizLineNum
,null                                                                   xzName
,null                                                                   accBizLineNum
,null                                                                   genCompanyfid
,null                                                                   genCompanyNum
,null                                                                   genCompanyName
,null                                                                   remark
,''                                                                     userId
from odm.odm_fi_eas_t_gl_initaccountbalance_a_d a
left join odm.odm_fi_eas_t_gl_initassistbalance_a_d b on a.faccountid=b.faccountid and a.forgunitid=b.forgunitid
left join (  select * from odm.odm_fi_eas_t_org_company_s_d where dt = '{TX_DATE}' ) com on com.fid = a.forgunitid
left join odm.odm_fi_eas_t_bd_accountview_a_d  acc on acc.fid = a.faccountid
left join ( select * from odm.odm_fi_eas_t_bd_currency_s_d where dt = '{TX_DATE}' ) curr on curr.fid = a.fcurrencyid
left join odm.odm_fi_eas_t_bd_systemstatusctrol_a_d ss on ss.FCOMPANYID = com.fid
left join odm.odm_fi_eas_t_bd_period_a_d pp on pp.fid = ss.FSTARTPERIODID
where a.fcurrencyid ='11111111-1111-1111-1111-111111111111DEB58FDC'
  and ss.FSYSTEMSTATUSID ='e45c1988-00fd-1000-e000-33d8c0a8100d02A5514C'
  and acc.FIsLeaf ='1'
  and concat(substr(pp.fnumber,1,4),'-',substr(pp.fnumber,5,2),'-','01')<='{TX_DATA_DATE}'
  and b.faccountid is null
  ;
    """,
    "sql_06": """
use dmf_tmp;
alter table dmf_tmp.bp_cwaduit_coas_acct_period_evidence_d_s drop if exists partition(dt = '{TX_DATA_DATE}');
    """,
    "sql_07": """
use dmf_tmp;
insert into dmf_tmp.bp_cwaduit_coas_acct_period_evidence_d_s partition(dt = '{TX_DATA_DATE}')
select
      a.id             
     ,a.vouchernum     
     ,a.bookeddate     
     ,a.comfid         
     ,a.comnum         
     ,a.comname        
     ,a.accviewgroup   
     ,a.accviewlongnum 
     ,a.accviewnum     
     ,a.accviewfdc     
     ,a.costorgfid     
     ,a.costorgname    
     ,a.costorgnumber  
     ,a.dbbank         
     ,a.bankaccount    
     ,coalesce(b.providerfid,a.providerfid) as providerfid  
     ,a.providernum    
     ,a.providername   
     ,a.bizline        
     ,a.projectname    
     ,a.invoicetype    
     ,a.entryseq       
     ,a.voucheredesc   
     ,a.assentrydesc   
     ,a.currency       
     ,a.jforiamt       
     ,a.dforiamt       
     ,a.jflocalamt     
     ,a.dflocalamt     
     ,a.sourcesys      
     ,a.sourcetype     
     ,a.creatorname    
     ,a.createtime     
     ,a.voucherstatus  
     ,a.voucherdesc    
     ,a.attachments    
     ,a.bizdate        
     ,a.lastupdatetime 
     ,a.vouchertype    
     ,a.bizlinenum     
     ,a.xzname         
     ,a.accbizlinenum  
     ,a.gencompanyfid  
     ,a.gencompanynum  
     ,a.gencompanyname 
     ,a.remark         
     ,a.userid         
from (select * from dmf_tmp.bp_cwaduit_coas_acct_period_evidence_tmp where dt = '{TX_DATA_DATE}') a
left join 
(
    select providernum,max(providerfid) as providerfid
    from (
	    select providerfid,providernum  
		from dmf_tmp.bp_cwaduit_coas_acct_period_evidence_tmp
		where dt = '{TX_DATA_DATE}' group by providerfid,providernum 
		)x group by providernum having count(providerfid) >1
) b
on a.providernum = b.providernum

;
    """,
    "sql_08": """
use dmf_tmp;
drop table if exists dmf_tmp.bp_cwaduit_coas_evidence;
    """,
    "sql_09": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_cwaduit_coas_evidence
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
    bwb.currency,
    case when not (t.kmlx is null or (e.accViewNum = '') or (e.accViewNum = 'nan')  ) then t.kmlx
         when (t.kmlx is null or (e.accViewNum = '') or (e.accViewNum = 'nan')  ) and substr(accViewNum,1,1) in ('1','5') then '资产'
         when (t.kmlx is null or (e.accViewNum = '') or (e.accViewNum = 'nan')  ) and substr(accViewNum,1,1) = '2' then '负债'
         when (t.kmlx is null or (e.accViewNum = '') or (e.accViewNum = 'nan')  ) and substr(accViewNum,1,1) in ('3','4') then '权益'
         else '其他' end as kmlx
    from (select * from dmf_tmp.bp_cwaduit_coas_acct_period_evidence_d_s where dt = '{TX_DATA_DATE}') e
    left join dmf_add.dmfadd_add_fi_acct_tree_a_m t
    on e.accViewNum = t.kmbm
    left join (
      select 
       a.fid       as comfid
      ,a.fnumber   as comnum
      ,b.fname_l2  as currency 
      from (  select * from odm.odm_fi_eas_t_org_company_s_d where dt = '{TX_DATE}' ) a
      left join ( select * from odm.odm_fi_eas_t_bd_currency_s_d where dt = '{TX_DATE}' ) b
      on a.fbasecurrencyid = b.fid
      group by 
       a.fid     
      ,a.fnumber 
      ,b.fname_l2
      ) bwb
    on e.comfid = bwb.comfid
    where (e.accViewNum <> '') 
    and (e.accViewNum <> 'nan') 
    and (e.accViewNum is not null) 
    and COALESCE(e.voucherType,'') != '上季度调整冲销'
    and COALESCE(e.voucherStatus,'') != '2'
    and e.accViewNum != ''
    and COALESCE(e.voucherDesc,'') not like '%结转损益%'
;
    """,
    "sql_10": """
use dmf_tmp;
drop table if exists dmf_tmp.bp_cwaduit_coas_evidence_class_day_sum;
    """,
    "sql_11": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_cwaduit_coas_evidence_class_day_sum
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
    from dmf_tmp.bp_cwaduit_coas_evidence
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
drop table if exists dmf_tmp.bp_cwaduit_coas_age_sum_evd;
    """,
    "sql_13": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_cwaduit_coas_age_sum_evd
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
            from dmf_tmp.bp_cwaduit_coas_evidence_class_day_sum
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
drop table if exists dmf_tmp.bp_cwaduit_coas_age_base_evd;
    """,
    "sql_15": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_cwaduit_coas_age_base_evd
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
    from (select * from dmf_tmp.bp_cwaduit_coas_evidence_class_day_sum ) a
    inner join (select * from dmf_tmp.bp_cwaduit_coas_age_sum_evd where flag = '有余额' )b
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
drop table if exists dmf_tmp.bp_cwaduit_coas_age_base_evd_sum;
    """,
    "sql_17": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_cwaduit_coas_age_base_evd_sum
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
        from dmf_tmp.bp_cwaduit_coas_age_base_evd a
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
            from dmf_tmp.bp_cwaduit_coas_age_base_evd
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
drop table if exists dmf_tmp.bp_cwaduit_coas_age_off_base_evd;
;
    """,
	"sql_19": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_cwaduit_coas_age_off_base_evd
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
                   else 0.0 end as  amt        
             from dmf_tmp.bp_cwaduit_coas_evidence_class_day_sum a
             inner join (
                select 
                comfid     
                ,accViewNum 
                ,providerFid
                ,providernum
                ,bizlinenum
                ,currency   
                ,kmlx   
                from dmf_tmp.bp_cwaduit_coas_age_sum_evd where flag = '有余额' 
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
drop table if exists dmf_tmp.bp_cwaduit_coas_age_evd_check_off;
    """,
	"sql_21": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_cwaduit_coas_age_evd_check_off
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
    from dmf_tmp.bp_cwaduit_coas_age_base_evd_sum a
    left join dmf_tmp.bp_cwaduit_coas_age_off_base_evd b
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
drop table if exists dmf_tmp.bp_cwaduit_coas_age_evd_check_off_first;
    """,
	"sql_23": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_cwaduit_coas_age_evd_check_off_first
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
    from dmf_tmp.bp_cwaduit_coas_age_evd_check_off x
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
         from dmf_tmp.bp_cwaduit_coas_age_evd_check_off 
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
drop table if exists dmf_tmp.bp_cwaduit_coas_age_base_evd_tmp;
    """,
	"sql_25": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_cwaduit_coas_age_base_evd_tmp
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
from dmf_tmp.bp_cwaduit_coas_age_base_evd a 
left join dmf_tmp.bp_cwaduit_coas_age_evd_check_off_first b
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
drop table if exists dmf_tmp.bp_cwaduit_coas_age_tmp;
    """,
	"sql_27": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_cwaduit_coas_age_tmp
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
                   from dmf_tmp.bp_cwaduit_coas_age_base_evd_tmp
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
              select coalesce(comfid,'') as comfid,max(coalesce(comnum,'')) as comnum,max(comname) as comname from (select * from dmf_tmp.bp_cwaduit_coas_acct_period_evidence_d_s where dt = '{TX_DATA_DATE}') xxx group by comfid
          ) b on a.comfid = b.comfid
          left join 
          (
              select coalesce(providerfid,'') as providerfid,max(coalesce(providername,'')) as providername from (select * from dmf_tmp.bp_cwaduit_coas_acct_period_evidence_d_s where dt = '{TX_DATA_DATE}') yyy group by providerfid
      
          ) c on a.providerfid = c.providerfid
          left join 
          (
              select 
              coalesce(bizlinenum,'') as bizlinenum
              ,max(
                case when coalesce(bizlinenum,'') != '' then coalesce(bizline,'')
                else '' end 
                ) as bizline 
              from (select * from dmf_tmp.bp_cwaduit_coas_acct_period_evidence_d_s where dt = '{TX_DATA_DATE}') zzz group by bizlinenum
          ) d on a.bizlinenum = d.bizlinenum   
          left join
          (
             select accviewlongnum,accviewnum from 
                (
                select accviewlongnum,accviewnum,row_number() over (partition by accviewnum order by length(accviewlongnum) desc) as rownum 
                from dmf_tmp.bp_cwaduit_coas_acct_period_evidence_d_s
                where dt = '{TX_DATA_DATE}' and length(accviewlongnum) >0
                group by  accviewlongnum,accviewnum
                ) xx
                where xx.rownum = 1

          ) e on a.accViewNum = e.accViewNum
;
    """,
	"sql_28": """
use dmf_tmp;
drop table if exists dmf_tmp.bp_off_cwaduit_coas_age_base_evd;
    """,
	"sql_29": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_off_cwaduit_coas_age_base_evd
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
    from (select * from dmf_tmp.bp_cwaduit_coas_evidence_class_day_sum ) a
    inner join (select * from dmf_tmp.bp_cwaduit_coas_age_sum_evd where flag = '有抵消' )b
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
drop table if exists dmf_tmp.bp_off_cwaduit_coas_age_base_evd_sum;
    """,
	"sql_31": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_off_cwaduit_coas_age_base_evd_sum
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
        from dmf_tmp.bp_off_cwaduit_coas_age_base_evd a
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
            from dmf_tmp.bp_off_cwaduit_coas_age_base_evd
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
drop table if exists dmf_tmp.bp_off_cwaduit_coas_age_off_base_evd;
    """,
	"sql_33": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_off_cwaduit_coas_age_off_base_evd
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
             from dmf_tmp.bp_cwaduit_coas_evidence_class_day_sum a
             inner join (
                select 
                comfid     
                ,accViewNum 
                ,providerFid
                ,providernum
                ,bizlinenum
                ,currency   
                ,kmlx   
                from dmf_tmp.bp_cwaduit_coas_age_sum_evd where flag = '有抵消' 
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
drop table if exists dmf_tmp.bp_off_cwaduit_coas_age_evd_check_off;
    """,
	"sql_35": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_off_cwaduit_coas_age_evd_check_off
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
    from dmf_tmp.bp_off_cwaduit_coas_age_base_evd_sum a
    left join dmf_tmp.bp_off_cwaduit_coas_age_off_base_evd b
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
drop table if exists dmf_tmp.bp_off_cwaduit_coas_age_evd_check_off_first;
    """,
	"sql_37": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_off_cwaduit_coas_age_evd_check_off_first
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
    from dmf_tmp.bp_off_cwaduit_coas_age_evd_check_off x
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
         from dmf_tmp.bp_off_cwaduit_coas_age_evd_check_off 
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
drop table if exists dmf_tmp.bp_off_cwaduit_coas_age_base_evd_tmp;
    """,
	"sql_39": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_off_cwaduit_coas_age_base_evd_tmp
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
from dmf_tmp.bp_off_cwaduit_coas_age_base_evd a 
left join dmf_tmp.bp_off_cwaduit_coas_age_evd_check_off_first b
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
drop table if exists dmf_tmp.bp_off_cwaduit_coas_age_tmp;
    """,
	"sql_41": """
use dmf_tmp;
create table if not exists dmf_tmp.bp_off_cwaduit_coas_age_tmp
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
                   from dmf_tmp.bp_off_cwaduit_coas_age_base_evd_tmp
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
              select coalesce(comfid,'') as comfid,max(coalesce(comnum,'')) as comnum,max(comname) as comname from (select * from dmf_tmp.bp_cwaduit_coas_acct_period_evidence_d_s where dt = '{TX_DATA_DATE}') xxx group by comfid
          ) b on a.comfid = b.comfid
          left join 
          (
              select coalesce(providerfid,'') as providerfid,max(coalesce(providername,'')) as providername from (select * from dmf_tmp.bp_cwaduit_coas_acct_period_evidence_d_s where dt = '{TX_DATA_DATE}') yyy group by providerfid
      
          ) c on a.providerfid = c.providerfid
          left join 
          (
              select 
              coalesce(bizlinenum,'') as bizlinenum
              ,max(
                case when coalesce(bizlinenum,'') != '' then coalesce(bizline,'')
                else '' end 
                ) as bizline 
              from (select * from dmf_tmp.bp_cwaduit_coas_acct_period_evidence_d_s where dt = '{TX_DATA_DATE}') zzz group by bizlinenum
          ) d on a.bizlinenum = d.bizlinenum   
          left join
          (
             select accviewlongnum,accviewnum from 
                (
                select accviewlongnum,accviewnum,row_number() over (partition by accviewnum order by length(accviewlongnum) desc) as rownum 
                from dmf_tmp.bp_cwaduit_coas_acct_period_evidence_d_s
                where dt = '{TX_DATA_DATE}' and length(accviewlongnum) >0
                group by  accviewlongnum,accviewnum
                ) xx
                where xx.rownum = 1

          ) e on a.accViewNum = e.accViewNum
;
    """,
	"sql_42": """
use dmf_ada;
alter table dmf_ada.dmfada_adrpt_fi_acct_age_s_m drop if exists partition(dt = '{TX_DATA_DATE}');
    """,
	"sql_43": """
use dmf_ada;
insert into dmf_ada.dmfada_adrpt_fi_acct_age_s_m partition(dt = '{TX_DATA_DATE}')
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
  from dmf_tmp.bp_cwaduit_coas_age_tmp

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
  from dmf_tmp.bp_off_cwaduit_coas_age_tmp;
    """,
}

# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)