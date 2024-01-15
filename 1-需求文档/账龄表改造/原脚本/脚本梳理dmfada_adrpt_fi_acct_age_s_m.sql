表01：bp_cwaduit_coas_acct_period_evidence_d_s
表02：bp_cwaduit_coas_acct_period_evidence_tmp
表03：bp_cwaduit_coas_evidence
表04：bp_cwaduit_coas_evidence_class_day_sum 
表05：bp_cwaduit_coas_age_sum_evd
表06：bp_cwaduit_coas_age_base_evd
表07：bp_cwaduit_coas_age_base_evd_sum
表08：bp_cwaduit_coas_age_off_base_evd
表09：bp_cwaduit_coas_age_evd_check_off
表10：bp_cwaduit_coas_age_evd_check_off_first
表11：bp_cwaduit_coas_age_base_evd_tmp
表12：bp_cwaduit_coas_age_tmp
表13：bp_off_cwaduit_coas_age_base_evd
表14：bp_off_cwaduit_coas_age_base_evd_sum
表15：bp_off_cwaduit_coas_age_off_base_evd
表16：bp_off_cwaduit_coas_age_evd_check_off
表17：bp_off_cwaduit_coas_age_evd_check_off_first
表18：bp_off_cwaduit_coas_age_base_evd_tmp
表19：bp_off_cwaduit_coas_age_tmp
表20：dmfada_adrpt_fi_acct_age_s_m


"sql_01"
create：表01
表结构见下，这是金蝶凭证明细表结构
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

"sql_02"
drop：表02 每次都只保存最新分区的数据

"sql_03"
create：表02 表结构见下，这是金蝶凭证明细表结构
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

"sql_04"
alter：表01

"sql_05"
insert：表02
select：
首先取金蝶凭证明细的基础表，并用其他金蝶维表加工维度
from ( select * from odm.odm_fi_eas_t_gl_voucher_s_d where dt = '{TX_DATE}' ) voucher --这是金蝶凭证表，是快照表，每天都会抽取最新dt的数据
inner join ( select * from odm.odm_fi_eas_t_org_company_s_d where dt = '{TX_DATE}' ) company on voucher.FCompanyId = company.fid --与金蝶公司表（快照表，取最新dt）做关联，引入公司编码和公司名称字段
inner join (select FOrgID,FType from odm.odm_fi_eas_t_pm_orgrange_a_d where FType=10 group by FOrgID,FType) orgRange on voucher.FCompanyId = orgRange.FOrgID --金蝶组织范围表，这里关联完后并没有用到相关的字段
left join odm.odm_fi_eas_t_bd_period_a_d period on voucher.FPeriodID = period.fid --金蝶期间表（全量表），这里关联完后并没有用到相关的字段
left join odm.odm_fi_eas_t_bd_vouchertypes_a_d voucherTypes on voucher.FVoucherTypeID = voucherTypes.FID --凭证类型表（全量表），这里是因为引入凭证类型字段
left join ( select * from odm.odm_fi_eas_t_pm_user_s_d where dt = '{TX_DATE}' ) creator ON voucher.FCreatorID = creator.FID --金蝶用户表（快照表），引入凭证的创建人
left join odm.odm_fi_eas_t_gl_voucherentry_a_d vouEntry on voucher.fid = vouEntry.FBillId --金蝶凭证分录表，引入借贷方向与凭证的原币金额，以及其他的维度供下面的步骤关联用
left join ( select * from odm.odm_fi_eas_t_bd_currency_s_d where dt = '{TX_DATE}' ) currency on currency.fid = vouEntry.FCurrencyId --金蝶币种表（快照表），引入币种维度
left join odm.odm_fi_eas_t_bd_accountview_a_d accView on vouEntry.FAccountID = accView.fid --金蝶科目表，引入分录对应的科目编码
left join odm.odm_fi_eas_t_gl_voucherassistrecord_a_d vouAssRecord on vouEntry.fid = vouAssRecord.FEntryId --金蝶辅助账表，如果辅助账fid为空，引入原币金额，并引入下方维度
left join odm.odm_fi_eas_t_bd_assistanthg_a_d assHG on vouAssRecord.FAssGrpId = assHG.fid--金蝶辅助账横表，引入辅助账相关维度
left join ( select * from odm.odm_fi_eas_t_org_costcenter_s_d where dt = '{TX_DATE}' ) costOrg on assHG.FCostOrgID = costOrg.fid--使用辅助账id引入成本中心编码、名称、id
left join ( select * from odm.odm_fi_eas_t_bd_supplier_s_d where dt = '{TX_DATE}' ) provider on assHG.FProviderID = provider.fid--使用辅助账id引入客商编码、名称、id
left join odm.odm_fi_eas_t_bd_accountbanks_a_d bankAcc on assHG.FBankAccountID = bankAcc.fid--使用辅助账id引入银行账户名称
left join odm.odm_if_eas_t_bd_bank_a_d bdBank on bdBank.fid = bankAcc.FBANK--使用辅助账id引入银行名称
left join ( select * from odm.odm_fi_eas_t_bd_generalasstacttype_s_d where dt = '{TX_DATE}' ) bizLine on assHG.FGeneralAssActType1ID = bizLine.fid --使用辅助账id引入业务线编码
left join ( select * from odm.odm_fi_eas_t_bd_generalasstacttype_s_d where dt = '{TX_DATE}' ) invoiceType on assHG.FGeneralAssActType2ID = invoiceType.fid--辅助账id引入发票类型
left join ( select * from odm.odm_fi_eas_t_bd_generalasstacttype_s_d where dt = '{TX_DATE}' ) geProject on assHG.FGeneralAssActType5ID = geProject.fid--辅助账id引入项目类型
left join ( select * from odm.odm_fi_eas_t_bd_generalasstacttype_s_d where dt = '{TX_DATE}' ) geXZ on assHG.FGeneralAssActType4ID = geXZ.fid--辅助账id引入科目性质
left join odm.odm_fi_eas_t_org_baseunit_a_d geCompany on assHG.FCOMPANYORGID = geCompany.fid--使用辅助账id引入公司编码、名称、id
where 1=1
and substr(voucher.FBookedDate,1,10)  <= '{TX_DATA_DATE}'--只取截止到上月末时点的数据
union all
select：
取金蝶初始化余额表，取'手动导入辅助账级'   
from odm.odm_fi_eas_t_gl_initaccountbalance_a_d a --金蝶初始化账户余额表 是一个全量表
left join odm.odm_fi_eas_t_gl_initassistbalance_a_d b on a.faccountid=b.faccountid and a.forgunitid=b.forgunitid --金蝶初始化余额表，是一个全量表引入forgunitid字段
left join (  select * from odm.odm_fi_eas_t_org_company_s_d where dt = '{TX_DATE}' ) com on com.fid = a.forgunitid --金蝶公司表，因为公司名称、编码、id字段
left join odm.odm_fi_eas_t_bd_accountview_a_d  acc on acc.fid = a.faccountid --金蝶科目表，引入FIsLeaf字段
left join ( select * from odm.odm_fi_eas_t_bd_currency_s_d where dt = '{TX_DATE}' ) curr on curr.fid = a.fcurrencyid --金蝶币种表，引入币种名称字段
left join odm.odm_fi_eas_t_bd_systemstatusctrol_a_d ss on ss.FCOMPANYID = com.fid --金蝶系统状态表，引入FSYSTEMSTATUSID系统状态字段
left join odm.odm_fi_eas_t_bd_period_a_d pp on pp.fid = ss.FSTARTPERIODID --金蝶期间表，引入期间
where a.fcurrencyid ='11111111-1111-1111-1111-111111111111DEB58FDC' --这里只要(综合本位币)的数据
  and ss.FSYSTEMSTATUSID ='e45c1988-00fd-1000-e000-33d8c0a8100d02A5514C' --不知道是啥意思
  and acc.FIsLeaf ='1' --只取末级科目
  and concat(substr(pp.fnumber,1,4),'-',substr(pp.fnumber,5,2),'-','01')<='{TX_DATA_DATE}' --只取区间大于上月末分区的数据
  and b.faccountid is null --科目不为空
union all
select：
取金蝶初始化余额表，'手动导入科目级'   
from odm.odm_fi_eas_t_gl_initaccountbalance_a_d a --金蝶初始化账户余额表 是一个全量表
left join odm.odm_fi_eas_t_gl_initassistbalance_a_d b on a.faccountid=b.faccountid and a.forgunitid=b.forgunitid --金蝶初始化余额表，引入faccountid
left join (  select * from odm.odm_fi_eas_t_org_company_s_d where dt = '{TX_DATE}' ) com on com.fid = a.forgunitid --金蝶公司表，引入公司信息
left join odm.odm_fi_eas_t_bd_accountview_a_d  acc on acc.fid = a.faccountid--金蝶科目表，只取末级科目
left join ( select * from odm.odm_fi_eas_t_bd_currency_s_d where dt = '{TX_DATE}' ) curr on curr.fid = a.fcurrencyid
left join odm.odm_fi_eas_t_bd_systemstatusctrol_a_d ss on ss.FCOMPANYID = com.fid--金蝶系统状态表，引入FSYSTEMSTATUSID系统状态字段
left join odm.odm_fi_eas_t_bd_period_a_d pp on pp.fid = ss.FSTARTPERIODID--金蝶期间表，引入期间
where a.fcurrencyid ='11111111-1111-1111-1111-111111111111DEB58FDC'--这里只要(综合本位币)的数据
  and ss.FSYSTEMSTATUSID ='e45c1988-00fd-1000-e000-33d8c0a8100d02A5514C'--不知道是啥意思
  and acc.FIsLeaf ='1'--只取末级科目
  and concat(substr(pp.fnumber,1,4),'-',substr(pp.fnumber,5,2),'-','01')<='{TX_DATA_DATE}'--只取区间大于上月末分区的数据
  and b.faccountid is null

"sql_06"
alter：表01

"sql_07"
insert：表01
select
把表01中的字段select到，这时候会导致group by时数据被拆分。
from：表02
left join：表02
所以用left join这段做了一个只取最新版本客商id的逻辑（注意这里不是客商编码）

"sql_08"
drop：表03

"sql_09": 
create：表03
select
凭证日期
公司id
科目编码
供应商id
业务线编码
如果借方金额（本位币）为空 则=0.0--猜测是为了让数据后面可以正常求和，不受精度影响
如果贷方金额（本位币）为空 则=0.0
新增余额字段：使用借方金额（本位币）、贷方金额（本位币）做计算，将借方金额转为余额，贷加负号后转为余额
新增本位币字段
新增dxlx（猜测：抵消类型）字段：
借方金额大于0且贷方=0，或贷方金额小于0且为借方=0，为抵消类型为‘借正’
贷方金额大于0且借方=0，或借方金额小于0且为贷方=0，为抵消类型为‘贷正’
其他情况均为‘其他’（按理说不应该有其他的，因为金额只可能在借贷出现，且不能同时为零）
新增kmlx（猜测：科目类型）字段，逻辑如下：
when not (t.kmlx is null or (e.accViewNum = '') or (e.accViewNum = 'nan')  ) then t.kmlx
when (t.kmlx is null or (e.accViewNum = '') or (e.accViewNum = 'nan')  ) and substr(accViewNum,1,1) in ('1','5') then '资产'
when (t.kmlx is null or (e.accViewNum = '') or (e.accViewNum = 'nan')  ) and substr(accViewNum,1,1) = '2' then '负债'
when (t.kmlx is null or (e.accViewNum = '') or (e.accViewNum = 'nan')  ) and substr(accViewNum,1,1) in ('3','4') then '权益'
else '其他' end as kmlx
from：表01
left join：dmfadd_add_fi_acct_tree_a_m --科目树表
on：科目编码
带出：科目类型（kmlx）
left join：
	（
	select 
	from：odm_fi_eas_t_org_company_s_d  --金蝶公司表
	left join：odm_fi_eas_t_bd_currency_s_d  --金蝶币种表
	）
	on：公司id
	带出：这个公司id对应的本位币
过滤科目类型为空的数据，只保留	'资产''负债''权益'这三个类型的科目类型的数据
    and COALESCE(e.voucherType,'') != '上季度调整冲销'--凭证类型，这里要找需求方确认EBS怎么判断
    and COALESCE(e.voucherStatus,'') != '2'--凭证状态不等于2，不知道这个什么意思
    and e.accViewNum != ''--科目编码不能为空
    and COALESCE(e.voucherDesc,'') not like '%结转损益%'--科目描述不包含结转损益，这里要找需求方确认EBS怎么判断

"sql_10":
drop：表04

"sql_11":
create：表04
select 			--这一步做了凭证明细做了防空清洗后进行轻度汇总
coalesce(bookedDate,'')    as bookedDate,		--凭证日期
    coalesce(comfid,'')        as comfid,		--公司id
    coalesce(accViewNum,'')    as accViewNum,	--科目编码
    coalesce(providerFid,'')   as providerFid,	--供应商id
    coalesce(providernum,'')   as providernum,	--供应商编码
    coalesce(bizlinenum,'')    as bizlinenum,	--业务线编码
    sum(coalesce(amt,0.0))     as amt,			--金额
    sum(case when dxlx = '借正' then coalesce(amt,0.0) else 0.0 end ) as jfamt, --借方金额
    sum(case when dxlx = '贷正' then coalesce(amt,0.0) else 0.0 end ) as dfamt,	--贷方金额
    coalesce(currency,'')      as currency,		--币种
    coalesce(kmlx,'')          as kmlx			--科目类型

from：表03
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
	


"sql_12":
drop：表05

"sql_13"：--这个表比较关键 后面就要通过这张表做打标的'有余额'以及'有抵消'分开对数据进行加工
create：表05
select
取维度之外，
取金额并按照金额将数据打标，逻辑如下：
,amt        
,case when cast(s.amt as decimal(28,6)) >0 then '有余额' 
      when cast(s.amt as decimal(28,6)) <0 then '有抵消'
      else '平账' end as flag
（
	select 
	首先把表4全量字段都拿过来
	然后根据科目类型判断金额（amt）的计算逻辑
	如果科目类型为‘资产’ 则借-贷
	如果科目类型为‘负债’ 则贷-借
	如果科目类型为‘权益’ 则贷-借
	else就是0（这里的逻辑很奇怪，按理说不会再有else的情况了）
	from：表04
	group by 
				comfid,				--公司id
				accViewNum,			--科目编码
				providerFid,		--供应商id
				providernum,		--供应商编码
				bizlinenum,			--业务线编码
				currency,			--币种
				kmlx				--目录类型
）
where cast(s.amt as decimal(28,6)) != 0.0  --把平账的数据去掉


"sql_14"：
drop：表06

"sql_15"
create：表06--这一步取了凭证明细中'有余额'部分的凭证明细，并取bookedDate字段取到
--这里直接上sql
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
    from (select * from 表04 ) a --取轻度汇总后凭证明细的
    inner join (select * from 表05 where flag = '有余额' )b --只保留有余额的数据
    on  a.comfid      = b.comfid     
    and a.accViewNum  = b.accViewNum 
    and a.providerFid = b.providerFid
    and a.providernum = b.providernum
    and a.bizlinenum  = b.bizlinenum
    and a.currency    = b.currency   
    and a.kmlx        = b.kmlx

"sql_16"
drop：表07

"sql_17"--这段sql写的有点不合理，本质上是计算按维度汇总截止到最新记账日期的金额字段的汇总，用开窗就行
create：表07
select
     bookedDate
    ,comfid    
    ,accViewNum
    ,providerFid
    ,providernum
    ,bizlinenum
    ,currency  
    ,kmlx  
    ,sum(amt) as sum_amt --这里应该可以用开窗
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
        ,a.amt  --这个金额里面没有负数
        from 表06 a
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
            from 表06
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
        where a.bookeddate  <= b.bookeddate --截止到最新凭证日期金额的汇总
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

"sql_18"
drop：表08

"sql_19"--这段目前还不知道在干啥，感觉是前面"sql_17"与"sql_15"的结合体，后面只在"sql_21"用这个表了做关联
create：表08
    select 
       '{TX_DATA_DATE}' as bookedDate,--这里是唯一一个不同之处，这里将上月月末时点（数据日期）强行转化为了凭证日期
       comfid,
       accViewNum,
       providerFid,
       providernum,
       bizlinenum,
       currency,
       kmlx,
       sum(amt) as amt --这里也没有负数
       from 
       (---这一层像"sql_15"
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
             from 表04 a
             inner join (
                select 
                comfid     
                ,accViewNum 
                ,providerFid
                ,providernum
                ,bizlinenum
                ,currency   
                ,kmlx   
                from 表05 where flag = '有余额' 
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
             where a.bookedDate <= '{TX_DATA_DATE}'--计算了截止到上月末时点的累计数据
        ) xx
       group by 
       comfid,
       accViewNum,
       providerFid,
       providernum,
       bizlinenum,
       currency,
       kmlx
	   

"sql_20"
drop：表09

"sql_21"
create：表09--这一段用表07和表08做了映射，使用两边的金额字段做了计算，直接上sql
    select
     a.bookedDate 
    ,a.comfid     
    ,a.accViewNum 
    ,a.providerFid
    ,a.providernum
    ,a.bizlinenum
    ,a.currency   
    ,a.kmlx    
    ,(a.sum_amt - b.amt)  as check_amt --这个sql的目的所在，使用截止到上月末（数据日期）的金额累计值与截止到最新时点的金额累计值做计算
    from 表07 a
    left join 表08 b
    on  a.comfid      = b.comfid     
    and a.accViewNum  = b.accViewNum 
    and a.providerFid = b.providerFid
    and a.providernum = b.providernum
    and a.bizlinenum  = b.bizlinenum
    and a.currency    = b.currency   
    and a.kmlx        = b.kmlx 

"sql_22"
drop：表10

"sql_23"--这个有点没看懂，应该是只取上月末时点到最新凭证日期有变化的数据，直接上sql
create：表10
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
    from 表09 x
    inner join (
          select 
          min(bookedDate) as  bookedDate --这里有点不懂，为啥要取min，就是本月内最小的日期？
         ,comfid     
         ,accViewNum 
         ,providerFid
         ,providernum
         ,bizlinenum
         ,currency   
         ,kmlx   
         from 表09 
         where check_amt > 0.0 --这里只取"sql_21"中check_amt大于零的数据，也就是最新凭证日期到上月末日期有变化的数据
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


"sql_24"
drop：表11

"sql_25"
create：表11--这段sql是检查相同维度下的金额汇总是否在上月末时点到最新的凭证日期有变化，如果没有变化就还用上月末时点的金额
select
select 
     a.bookedDate
    ,a.comfid     
    ,a.accViewNum 
    ,a.providerFid
    ,a.providernum
    ,a.bizlinenum
    ,a.currency   
    ,a.kmlx  
    ,case when a.bookedDate = b.bookedDate then b.check_amt --如果两边日期一致，说明在这个月产生了没有变化，则取check_amt（这段逻辑没看懂）
          when a.bookedDate > b.bookedDate then a.amt --如果大于上月末时点的数据，则说明这个月没变，直接取余额
          else 0.0 
          end as amt
from 表06 a 
left join 表10 b
    on  a.comfid      = b.comfid     
    and a.accViewNum  = b.accViewNum 
    and a.providerFid = b.providerFid
    and a.providernum = b.providernum
    and a.bizlinenum  = b.bizlinenum
    and a.currency    = b.currency   
    and a.kmlx        = b.kmlx        
where a.bookedDate >= b.bookedDate --只要这个月变化过的数据


"sql_26"
drop：表12

"sql_27"
create：表12--用表11的数据的凭证日期与上月末时点日期计算各个凭证的账龄，最后利用表01的明细数据补充维度，（公司名称、供应商名称、业务线名称等）
select 
from：表11
left join：表01
left join：表01
left join：表01
left join：表01

"sql_28"
drop：表13

"sql_29"
create：表13--这一步取了凭证明细中'有抵消'部分的凭证明细，并取bookedDate字段取到
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
    from (select * from dmf_tmp.bp_cwaduit_coas_evidence_class_day_sum ) a --取轻度汇总后凭证明细的
    inner join (select * from dmf_tmp.bp_cwaduit_coas_age_sum_evd where flag = '有抵消' )b --只取有'有抵消'的数据
    on  a.comfid      = b.comfid     
    and a.accViewNum  = b.accViewNum 
    and a.providerFid = b.providerFid
    and a.providernum = b.providernum
    and a.bizlinenum  = b.bizlinenum
    and a.currency    = b.currency   
    and a.kmlx        = b.kmlx
from：表04
inner join：表05

"sql_30"
create：表14

"sql_31"
create：表14--取截止到最新凭证金额的汇总 这段与"sql_17"逻辑相同，只不过是换成了计算有抵消的数据
select
from：表13
left join：表13

"sql_32"
drop：表15

"sql_33"--这一段计算与前面'sql_19'的逻辑相似，只不过是计算了有抵消的数据，计算了截止到上月末时点的累计数据
create：表15
select
from：表04
inner join：表05

"sql_34"
drop：表16

"sql_35"
create：表16--这一段计算与前面'sql_21'的逻辑相似，是要计算截止到上月末时点到最新凭证时间的数据是否有变化，最后计算变化的金额
select
from：表14
left join：表15

"sql_36"
drop：表17

"sql_37"
create：表17--这一段计算与前面'sql_23'的逻辑相似，目的是只取上月有变化的金额
select
from：表16
inner join：表16

"sql_38"
drop：表18

"sql_39"
create：表18
 select 
     a.bookedDate
    ,a.comfid     
    ,a.accViewNum 
    ,a.providerFid
    ,a.providernum
    ,a.bizlinenum
    ,a.currency   
    ,a.kmlx  
    ,case when a.bookedDate = b.bookedDate then b.check_amt  --从这里开始抵消掉的金额已经用尽，开始出现账龄
          when a.bookedDate > b.bookedDate then a.amt --用尽后的所有金额
          else 0.0 
          end as amt
from 表13 a 
left join 表17 b
    on  a.comfid      = b.comfid     
    and a.accViewNum  = b.accViewNum 
    and a.providerFid = b.providerFid
    and a.providernum = b.providernum
    and a.bizlinenum  = b.bizlinenum
    and a.currency    = b.currency   
    and a.kmlx        = b.kmlx        
where a.bookedDate >= b.bookedDate


"sql_40"
drop：表19

"sql_41"--这里把抵消的金额数据只都做了负数的处理，其他和有余额的那段处理逻辑一样
create：表19
select
from：表18
left join：表01
left join：表01
left join：表01
left join：表01

"sql_42"
alter：表20

"sql_43"--最后把有余额和有抵消的数据union在一起
insert：表20
select：
from：表12
union all--最后把有余额和有抵消的数据union在一起
select：
from：表19

表12追溯链路：表11、表06、表04、表03、表01
表19追溯链路：表18、表13、表04、表03、表01