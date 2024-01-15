#!/usr/bin/perl
########################################################################################################################
#  Creater        :wanglixin16
#  Creation Time  :2018-07-20
#  Description    :dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d 集团资金占用日报
#                 :report_key='CAPITAL_OCCUPATION',资金占用; operate_org_cd='JDJR',京东金融; currency_cd='CNY',人民币
#  Modify By      :王立新
#  Modify Time    :2018-09-17
#                  20190924   wanglixin ODM_FI_INVESTOR_LOAN_S_D 表新增create_date进行时间限定，否则会有T+0数据
#  Modify Content :
#  Script Version :1.0.3
########################################################################################################################
use strict;
use jrjtcommon;
use un_pswd;
use Common::Hive;
use zjcommon;

##############################################
#默认STINGER运行,失败后HIVE运行,可更改Runner和Retry_Runner
#修改最终生成表库名和表名
##############################################

my $Runner = "STINGER";
my $Retry_Runner = "HIVE";
my $DB = "";
my $TABLE = "";
##############################################

if ( $#ARGV < 0 ) { exit(1); }
my $CONTROL_FILE = $ARGV[0];
my $JOB = substr(${CONTROL_FILE}, 4, length(${CONTROL_FILE})-17);

#当日 yyyy-mm-dd
my $TX_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-12, 4).'-'.substr(${CONTROL_FILE},length(${CONTROL_FILE})-8, 2).'-'.substr(${CONTROL_FILE},length(${CONTROL_FILE})-6, 2);

my $TXDATE = substr($TX_DATE, 0, 4).substr($TX_DATE, 5, 2).substr($TX_DATE, 8, 2);                        #当日 yyyymmdd
my $TX_MONTH = substr($TX_DATE, 0, 4).'-'.substr($TX_DATE, 5, 2);                                          #当日所在月 yyyy-mm
my $TXMONTH = substr($TX_DATE, 0, 4).substr($TX_DATE, 5, 2);                                               #当日所在月 yyyymm
my $TX_PREV_DATE = getPreviousDate($TX_DATE);                                                               #前一天 yyyy-mm-dd
my $TX_NEXT_DATE = getNextDate($TX_DATE);                                                                   #下一天 yyyy-mm-dd
my $TXPDATE = substr(${TX_PREV_DATE},0,4).substr(${TX_PREV_DATE},5,2).substr(${TX_PREV_DATE},8,2);        #前一天 yyyymmdd
my $TXNDATE = substr(${TX_NEXT_DATE},0,4).substr(${TX_NEXT_DATE},5,2).substr(${TX_NEXT_DATE},8,2);        #下一天 yyyymmdd
my $CURRENT_TIME = getNowTime();
my $TX_YEAR = substr($TX_DATE, 0, 4);#当年 yyyy

########################################################################################################################
# Write SQL For Your APP
sub getsql
{
    my @SQL_BUFF=();
#########################################################################################
####################################以下为SQL编辑区######################################
#########################################################################################

$SQL_BUFF[1]=qq(
set mapred.job.name=dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d1;

use dmf_tmp;
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d
like dmf_alm.dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d
;

);

$SQL_BUFF[2]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d2;


use dmf_tmp;

-- 759  白条赊销转信贷 在 a_fact_ftp_jd_bsc_10_01_i_d.pl(白条资金占用)中生成,在a_fact_ftp_jd_bsc_10_i_d.pl中调用插入到'CAPITAL_OCCUPATION' = report_key 的数据中


-- 银行流贷基本信息 dmt_ftp_jd_alm_bank_loan_info_s_d
FROM dmf_bc.dmfbc_alm_dm_01_bank_loan_info_s_d
INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)   -- 再保理
SELECT
    dt      AS etl_dt
    ,'CAPITAL_OCCUPATION' AS report_key
    ,dt     AS date_id
    ,'JDJR' AS operate_org_cd
    ,'CNY'  AS currency_cd
    ,'202'  AS item_key
    ,CAST(SUM(SHOULD_PAY_AMT)  AS BIGINT )  AS item_valn
    ,''     AS item_vals
    ,dt
WHERE dt = '$TX_DATE'
 AND Loan_Id  NOT IN ('0000000046BANK_LOAN_BL','0000000039BANK_LOAN_BL','0000000022BANK_LOAN_BL','0000000036BANK_LOAN_BL')
 AND LOAN_TYPE='5'
 AND TO_DATE(end_dT)>DT
GROUP BY dt
INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)    -- 供应链流贷
SELECT
    dt AS etl_dt
    ,'CAPITAL_OCCUPATION' AS report_key
    ,dt     AS date_id
    ,'JDJR' AS operate_org_cd
    ,'CNY'  AS currency_cd
    ,'204'  AS item_key
    ,CAST(SUM(should_pay_amt) AS BIGINT )  AS item_valn
    ,''     AS item_vals
    ,dt
WHERE dt='$TX_DATE'
  AND Loan_Id NOT IN ('0000000046BANK_LOAN_BL','0000000039BANK_LOAN_BL','0000000022BANK_LOAN_BL','0000000036BANK_LOAN_BL')
  AND LOAN_TYPE <> '5'
  AND MY_MAIN_NAME = '上海邦汇商业保理有限公司'
GROUP BY dt
INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)    -- 消费金融业务流贷
SELECT
    dt AS etl_dt
    ,'CAPITAL_OCCUPATION' AS report_key
    ,dt     AS date_id
    ,'JDJR' AS operate_org_cd
    ,'CNY'  AS currency_cd
    ,'509'  AS item_key
    ,CAST(SUM(should_pay_amt) AS BIGINT )  AS item_valn
    ,''     AS item_vals
    ,dt
WHERE dt='$TX_DATE'
  AND Loan_Id NOT IN ('0000000046BANK_LOAN_BL','0000000039BANK_LOAN_BL','0000000022BANK_LOAN_BL','0000000036BANK_LOAN_BL')
  AND LOAN_TYPE <> '5'
  AND MY_MAIN_NAME = '北京京汇小额贷款有限公司'
GROUP BY dt
INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)    -- 未分业务线流贷
SELECT
    dt AS etl_dt
    ,'CAPITAL_OCCUPATION' AS report_key
    ,dt     AS date_id
    ,'JDJR' AS operate_org_cd
    ,'CNY'  AS currency_cd
    ,'733'  AS item_key
    ,CAST(SUM(should_pay_amt) AS BIGINT )  AS item_valn
    ,''     AS item_vals
    ,dt
WHERE dt='$TX_DATE'
  AND Loan_Id NOT IN ('0000000046BANK_LOAN_BL','0000000039BANK_LOAN_BL','0000000022BANK_LOAN_BL','0000000036BANK_LOAN_BL')
  AND LOAN_TYPE <> '5'
  AND MY_MAIN_NAME NOT IN ('上海邦汇商业保理有限公司','北京京汇小额贷款有限公司')
GROUP BY dt
;

-- 云工厂基本信息  dmt_ftp_jd_alm_cloud_plat_info_s_d
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='$TX_DATE')
select dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'724' as item_key
      ,SUM(Should_Pay_Amt) as item_valn
      ,'' as item_vals
  from dmf_bc.dmfbc_alm_dm_01_cloud_plat_info_s_d
 where dt='$TX_DATE'
 group by dt
;

-- FICC台账基本信息   dmt_ftp_jd_alm_ficc_loan_info_s_d
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='$TX_DATE')
select dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'725' as item_key
      ,SUM(cur_bal) as item_valn
      ,'' as item_vals
  from dmf_bc.dmfbc_alm_dm_01_ficc_loan_info_s_d
 where dt='$TX_DATE'
 group by dt
;

);

$SQL_BUFF[3]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d3;

use dmf_tmp;

INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='$TX_DATE')
SELECT
    t1.dt
    ,'CAPITAL_OCCUPATION' AS report_key
    ,t1.dt  AS date_id
    ,'JDJR' AS operate_org_cd
    ,'CNY'  AS currency_cd
    ,CASE WHEN NVL(t3.merchantName,'') NOT LIKE '力蕴汽车' THEN '403'
          WHEN NVL(t3.merchantName,'') LIKE '力蕴汽车' THEN '404'
     END    AS item_key
    ,CAST(SUM(t1.shouldpayamount) AS BIGINT ) AS item_valn
    ,''     AS item_vals
FROM (
    SELECT * FROM odm.ODM_CF_PLUS_PLAN_S_D
    WHERE dt = '$TX_DATE'
      AND to_date(createdate)<=dt
) t1
INNER JOIN (
    SELECT *
    FROM odm.ODM_CF_PLUS_LOAN_ORDER_S_D
    WHERE dt = '$TX_DATE'
      AND status =3
      AND to_date(completetime)<=dt
) t2
  ON t1.loanno=t2.loanid
INNER JOIN (
    SELECT
         merchantno as merchantCode
        ,merchantnoname as merchantName
    FROM ODM.ODM_CF_PLUS_MERCHANTNO_S_D
    WHERE dt = '$TX_DATE'
    GROUP BY merchantno,merchantnoname
)  t3
  ON t1.merchantCode=t3.merchantCode
GROUP BY t1.dt
    ,CASE WHEN NVL(t3.merchantName,'') NOT LIKE '力蕴汽车' THEN '403'
          WHEN NVL(t3.merchantName,'') LIKE '力蕴汽车' THEN '404'
     END
;


-- 京农贷账户信息  dmfbc_alm_dm_01_jnd_acct_info_s_d
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='$TX_DATE')
select a.dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,a.dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'701' as item_key
      ,SUM(a.Should_Pay_Amt) as item_valn
      ,'' as item_vals
  from (select * from dmf_bc.dmfbc_alm_dm_01_jnd_acct_info_s_d  where dt = '$TX_DATE' ) A
 group by a.dt
;

--20181026 wanglixin16 更新逻辑 金条账户信息
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select t1.dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,t1.dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,case when INVESTOR_TYPE='0' then '409' else '405' end as item_key   --金条-内外部出资
      ,SUM(Should_Pay_Amt) as item_valn 
      ,'' as item_vals
      ,'$TX_DATE'
FROM 
     dmf_bc.dmfbc_alm_dm_01_jt_acct_info_s_d t1
WHERE t1.dt = '$TX_DATE'
GROUP BY t1.dt,case when INVESTOR_TYPE='0' then '409' else '405' end
;

);

$SQL_BUFF[4]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d4;

use dmf_tmp;

-- 保理基本信息   DMT_FTP_JD_ALM_LP_GUARANTEE_INFO_S_D
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='$TX_DATE')
select a.dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,a.dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'102' -- 外部保理
      ,CAST(SUM(unret_pri_new)  AS BIGINT ) as item_valn
      ,'' as item_vals
  from (
          SELECT
             k1.*,
             CASE WHEN k1.currency='USD' THEN  k1.unret_pri*k2.exchg_rate ELSE k1.unret_pri END  as   unret_pri_new
          FROM
             (select
                     *
                from
                     idm.idm_f02_sf_ordr_dtl_s_d
                 where DT = '$TX_DATE'
                       AND biz_line in('外部保理')
              ) k1
         left join
             (select * from dmf_dim.dmfdim_dim_exchg_rate_i_d where DT = '$TX_DATE') k2
          on k1.dt=k2.dt and k1.currency=k2.currency
        ) a
group by a.dt
;

--20181016 wanglixin16 更新内部保理逻辑
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='$TX_DATE')
SELECT
         A.DT
        ,'CAPITAL_OCCUPATION' as report_key
        ,a.dt as date_id
        ,'JDJR' as operate_org_cd
        ,'CNY' as currency_cd
        ,'101'                  -- 内部保理
        ,CAST(SUM(unret_pri_new)  AS BIGINT )    as item_valn
       ,'' as item_vals
    FROM
        (
          SELECT
             k1.*,
             CASE WHEN k1.currency='USD' THEN  k1.unret_pri*k2.exchg_rate ELSE k1.unret_pri END  as   unret_pri_new
          FROM
             (select
                     *
                from
                     idm.idm_f02_sf_ordr_dtl_s_d
                 where DT = '$TX_DATE'
                       AND biz_line in('内部保理')
              ) k1
         left join
             (select * from dmf_dim.dmfdim_dim_exchg_rate_i_d where DT = '$TX_DATE') k2
          on k1.dt=k2.dt and k1.currency=k2.currency
        ) A
    GROUP BY
        A.DT
;

--20181016 wanglixin16 订单池
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='$TX_DATE')
SELECT
         A.DT
        ,'CAPITAL_OCCUPATION' as report_key
        ,a.dt as date_id
        ,'JDJR' as operate_org_cd
        ,'CNY' as currency_cd
        ,'109'                  -- 订单池
        ,CAST(SUM(CASE
            WHEN A.currency='USD' THEN  unret_pri*6.596
            ELSE unret_pri END )  AS BIGINT )    AS item_valn
        ,'' as item_vals
    FROM
        idm.idm_f02_sf_ordr_dtl_s_d A
    WHERE
        A.DT='$TX_DATE'
        AND biz_line='订单池融资'
    GROUP BY
        A.DT
;

--20181016 wanglixin16 老金采
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='$TX_DATE')
select
         DT
        ,'CAPITAL_OCCUPATION' as report_key
        ,dt as date_id
        ,'JDJR' as operate_org_cd
        ,'CNY' as currency_cd
        ,'105'
        ,sum(nvl(order_amt, 0) - nvl(refund_prcp_amt,0) - nvl(repay_prcp_amt,0)) as item_valn
        ,'' as item_vals
    from
        odm.ODM_CF_QY_IOU_RECEIPT_S_D
    WHERE
        dt= '$TX_DATE'
        and jd_order_type = '20096'
        and to_date(
            tx_time
        ) <= dt
    GROUP BY
        DT
;

--20181016 wanglixin16 新金采
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='$TX_DATE')
select
      etl_dt,
      report_key,
      date_id,
      operate_org_cd,
      currency_cd,
      '110', --新金采
      sum(item_valn) item_valn,
      '' as item_vals
from
(select etl_dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,etl_dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,SUM(jc_unret_pri) as item_valn
  from sdm.sdm_f02_sf_jdjc_prod_tx_sum_a_d
    where etl_dt='$TX_DATE'
     group by etl_dt
union all
select
         DT as etl_dt
        ,'CAPITAL_OCCUPATION' as report_key
        ,dt as date_id
        ,'JDJR' as operate_org_cd
        ,'CNY' as currency_cd
        ,-(sum(nvl(order_amt, 0) - nvl(refund_prcp_amt,0) - nvl(repay_prcp_amt,0))) as item_valn
    from
        odm.ODM_CF_QY_IOU_RECEIPT_S_D
    WHERE
        dt= '$TX_DATE'
        and jd_order_type = '20096'
        and to_date(
            tx_time
        ) <= dt
    GROUP BY
        DT
) tt
group by
      etl_dt,
      report_key,
      date_id,
      operate_org_cd,
      currency_cd
;

-- 经分供应链贷款表 idm.idm_f02_sf_ordr_dtl_s_d
from idm.idm_f02_sf_ordr_dtl_s_d
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'104' as item_key -- 动产质押
      ,SUM(unret_pri) as item_valn
      ,'' as item_vals
      ,'$TX_DATE'
 where dt='$TX_DATE' and biz_line like '%动产%'
 group by dt
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'103' as item_key
      ,SUM(unret_pri) as item_valn
      ,'' as item_vals
      ,'$TX_DATE'
 where dt='$TX_DATE' and biz_line='内部小贷'
 group by dt
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'111' as item_key
      ,SUM(unret_pri) as item_valn
      ,'' as item_vals
      ,'$TX_DATE'
 where dt='$TX_DATE' and biz_line in ('外部小贷','快银BOSS贷')
 group by dt
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'770' as item_key  --快银
      ,cast(SUM(unret_pri) as bigint) as item_valn
      ,'' as item_vals
      ,'$TX_DATE'
 where dt='$TX_DATE' and biz_line='快银'
 group by dt
;


-- 信托&过桥基本信息    dmfbc_alm_dm_01_trust_loan_info_s_d
INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d PARTITION(dt)
SELECT
    dt      AS etl_dt
    ,'CAPITAL_OCCUPATION' AS report_key
    ,dt     AS date_id
    ,'JDJR' AS operate_org_cd
    ,'CNY'  AS currency_cd
    ,'207'  AS item_key
    ,SUM(Should_Pay_Amt) AS item_valn
    ,''     AS item_vals
    ,'$TX_DATE'
FROM dmf_bc.dmfbc_alm_dm_01_trust_loan_info_s_d
WHERE dt='$TX_DATE'
GROUP BY dt
;

--20181030 wanglixin8 将日立再保理数据置为0
INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d PARTITION(dt) values ('$TX_DATE','CAPITAL_OCCUPATION','$TX_DATE','JDJR','CNY','208',0.00,'','$TX_DATE')
;


INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
SELECT
    dt as etl_dt
    ,'CAPITAL_OCCUPATION' as report_key
    ,dt as date_id
    ,'JDJR' as operate_org_cd
    ,'CNY' as currency_cd
    ,'205' as item_key
    ,SUM(SHOULD_PAY_AMT)+2900000000  as item_valn
    ,'' as item_vals
    ,'$TX_DATE'
FROM dmf_bc.dmfbc_alm_dm_01_company_financing_info_s_d
WHERE DT = '$TX_DATE'
GROUP BY dt
;

-- 联合贷基本信息  dmt_ftp_jd_alm_union_loan_info_s_d
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select '$TX_DATE' as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,'$TX_DATE' as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'207' as item_key -- 信托&联合贷
      ,item_valn as item_valn
      ,'' as item_vals
      ,'$TX_DATE'
 from dim.dim_ftp_jd_alm_dim_capital_occupation_a_d
 where item_key='207'
;


insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'706' as item_key -- 京农贷_联合贷
      ,SUM(Should_Pay_Amt) as item_valn
      ,'' as item_vals
      ,dt
 from dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d
 where dt='$TX_DATE' and UNION_TYPE='4' 
 -- AND Investment_Code IN ('30017842' ,'30025355' ,'30026632')
 group by dt
;

--修改人：王立新；修改时间：2018-09-17；内容：odm.ODM_FI_INVESTOR_LOAN_S_D表新增条件TO_DATE(PLAT_CREATE_TIME) >='2018-08-01'
INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt = '$TX_DATE')
SELECT
    '$TX_DATE' AS etl_dt
    ,'CAPITAL_OCCUPATION' AS report_key
    ,'$TX_DATE' AS date_id
    ,'JDJR' AS operate_org_cd
    ,'CNY'  AS currency_cd
    ,'505'  AS item_key -- 大白_联合贷
    ,429878443.16 + CAST(B.loan_amount- A.CancelPrincipal-A.PaidPrincipal -A.ReverseUnpaidPrincipal -A.UnderPrincipal AS FLOAT ) AS item_valn
    ,''     AS item_vals
 FROM
        (SELECT
            SUM(LOAn_amount)     AS   loan_amount
        FROM
            odm.ODM_FI_INVESTOR_LOAN_S_D  B
        WHERE
            B.DT='$TX_DATE'
            and to_date(B.create_time)<=dt           -- 去除T+0 数据
            and to_date(B.record_update_time)<=dt    -- 去除T+0 数据
            AND  b.INVESTOR_ID   IN (
               SELECT
                        investor_id
                    FROM
                        odm.ODM_FI_PLAT_INVESTOR_S_D
                    WHERE
                        DT='$TX_DATE'
                        AND investor_type='0'
                        AND  PLAT_id IN (
                            '2' ,'5' ,'6' ,'7' ,'8' ,'9' ,'10' ,'11' ,'12' ,'13' ,'14' ,'15' ,'16' ,'17' ,'18' ,'19' ,'20' ,'21' ,'22' ,'23' ,'24' ,'25' ,'201'
                        )         )
            AND  status in(
                'Success','Cancel'
            )
            AND TO_DATE(
                B.PLAT_CREATE_TIME
            )<=B.DT
            AND  TO_DATE(
                PLAT_CREATE_TIME
            ) >='2018-08-01'  ) B
    LEFT JOIN
        (
            SELECT
                SUM(CASE
                    WHEN a.business_type ='Cancel'
                    AND a.money_type='CancelPrincipal' THEN COALESCE(a.amount,
                    0.0)
                    ELSE 0.0
                END)  AS CancelPrincipal ,
                SUM(CASE
                    WHEN a.business_type ='Repayment'
                    AND a.money_type='PaidPrincipal' THEN COALESCE(a.amount,
                    0.0)
                    ELSE 0.0
                END)   AS PaidPrincipal,
                SUM(CASE
                    WHEN a.business_type ='Refund'
                    AND a.money_type='ReverseUnpaidPrincipal' THEN COALESCE(a.amount,
                    0.0)
                    ELSE 0.0
                END)  AS ReverseUnpaidPrincipal,
                SUM(CASE
                    WHEN a.business_type ='UnderRepayment'
                    AND a.money_type='Principal' THEN COALESCE(a.amount,
                    0.0)
                    ELSE 0.0
                END)   AS UnderPrincipal
            FROM
                odm.ODM_FI_INVESTOR_FLOW_S_D A
            WHERE
                A.DT='$TX_DATE'
                AND TO_DATE(a.create_time)>= '2018-08-01' and TO_DATE(a.create_time)<=A.DT
                AND  a.business_type IN (
                    'Interest','Repayment','Refund','Cancel','Loan','Reverse'
                )
                AND a.investor_id  IN (
                    SELECT
                        investor_id
                    FROM
                        odm.ODM_FI_PLAT_INVESTOR_S_D
                    WHERE
                        DT='$TX_DATE'
                        AND investor_type='0'
                        AND  PLAT_id IN (
                            '2' ,'5' ,'6' ,'7' ,'8' ,'9' ,'10' ,'11' ,'12' ,'13' ,'14' ,'15' ,'16' ,'17' ,'18' ,'19' ,'20' ,'21' ,'22' ,'23' ,'24' ,'25' ,'201'
                        )
                )
            ) A
                ON 1=1
;

INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select
     '$TX_DATE'   AS etl_dt
    ,'CAPITAL_OCCUPATION' AS report_key
    ,'$TX_DATE'  AS date_id
    ,'JDJR' AS operate_org_cd
    ,'CNY'  AS currency_cd
    ,'508'  AS item_key -- 金条_联合贷
    ,SUM(t1.Should_Pay_Amt) AS item_valn
    ,''     AS item_vals
    ,t1.dt
FROM 
     dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d t1
WHERE t1.dt = '$TX_DATE' AND t1.UNION_TYPE = '1'
GROUP BY t1.dt
;


);

$SQL_BUFF[5]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d5;

use dmf_tmp;

-- sdm_f02_cf_xbt_ordr_dtl_s_d 表结构变更
from (select t1.dt
             ,CASE
                  WHEN t2.biz_nm4 not in ('到家', '商城', '扶贫专项', '大额订单') and t1.PROD_TYPE <> '农村' THEN '402' -- 小白(除商城白条)
              END as item_key1
             ,CASE
                  WHEN t1.PROD_TYPE ='农村' and t2.biz_nm4 in ( '商城')  THEN '702' -- 乡村白条(商城)
                  WHEN t1.PROD_TYPE ='农村' and t2.biz_nm4 not in ( '商城')  THEN '703' -- 乡村白条(非商城)
                  ELSE '0-0'
              END as item_key2
             ,unpayoff_prin
        from (select * from sdm.sdm_f02_cf_xbt_ordr_dtl_s_d where dt ='$TX_DATE' ) t1
    inner join dim.dim_bt_tran_jf_a_d t2
          on t1.biz_id = t2.biz_id4
       where  to_date(loan_time) <= DT ) t1
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select t1.dt as etl_dt       ,'CAPITAL_OCCUPATION' as report_key
      ,t1.dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,item_key1 as item_key
      ,SUM(unpayoff_prin) as item_valn
      ,'' as item_vals
      ,'$TX_DATE'
 group by dt, item_key1
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select t1.dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,t1.dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,item_key2 as item_key
      ,SUM(unpayoff_prin) as item_valn
      ,'' as item_vals
      ,'$TX_DATE'
where item_key2 in ( '702','703')
 group by dt, item_key2
;

-- 752 小白条(商城出资)
from (
    select t2.biz_nm4,T1.PROD_TYPE, sum(unpayoff_prin) as unpayoff_prin
      from ( SELECT *
             FROM sdm.sdm_f02_cf_xbt_ordr_dtl_s_d
             where dt= '$TX_DATE'
                   and to_date(loan_time) <= DT
                 ) t1
         left join
               dim.dim_bt_tran_jf_a_d t2
                   on t1.biz_id = t2.biz_id4
    group by t2.biz_nm4,T1.PROD_TYPE
) t
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select '$TX_DATE' as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,'$TX_DATE' as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'752' as item_key    -- 小白条(商城出资)
      ,SUM(unpayoff_prin) as item_valn
      ,'' as item_vals
      ,'$TX_DATE'
where t.biz_nm4 in ('到家', '商城', '扶贫专项', '大额订单')
--20181017 wanglixin16 更新小白(商城白条)逻辑
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select
       '$TX_DATE' as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,'$TX_DATE' as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'401' as item_key    -- 小白（商城白条-个人白条）
      ,sum(unpayoff_prin)  as item_valn
      ,'' as item_vals
      ,'$TX_DATE'
    where
        t.biz_nm4 in (
            '商城'
        )
        and  T.PROD_TYPE<>'农村'
--20181017 wanglixin16 新增小白（商城白条-其他）
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select
       '$TX_DATE' as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,'$TX_DATE' as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'410' as item_key    -- 小白（商城白条-其他）
      ,sum(unpayoff_prin)  as item_valn
      ,'' as item_vals
      ,'$TX_DATE'
    where
        t.biz_nm4 in (
             '到家', '扶贫专项', '大额订单'
        )
        and  T.PROD_TYPE<>'农村'
;


-- dwd_actv_bt_plus_plan_s_d    754 大白条(商城出资)
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='$TX_DATE')
select t1.dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,t1.dt as date_id       ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'754' as item_key    -- 小白条(商城出资)
       ,sum(nvl(shouldpayamount,0)) as item_valn
      ,'' as item_vals
from( select a.*
      from( select *
            from odm.ODM_CF_PLUS_PLAN_S_D
            where dt= '$TX_DATE'
                  and to_date(createdate) <= dt
               ) a
      inner join( select *
                  from odm.ODM_CF_PLUS_LOAN_ORDER_S_D
                  where dt= '$TX_DATE'
                        and status = 3
                        and to_date(completetime) <= dt
                    ) c
        on a.merchantorderid = c.merchantorderid
      inner join ( select *
                   from dim.dim_whip_merchant_cate_a_d
                   where producttype not in (4,5,6,7)
                         and merchantno in ('2810000001'
                                           ,'3730000001'
                                           ,'3850000001'
                                           ,'2800000001'
                                           ,'3850000003','3850000004'
                                             )
                     ) b
       on a.merchantCode = b.merchantno
     ) t1
group by dt;

);

$SQL_BUFF[6]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d6;

use dmf_tmp;

-- dwd_actv_bt_plus_plan_s_d    755 老金采(商城出资)
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='$TX_DATE')
select t1.dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,t1.dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'755' as item_key    -- 小白条(商城出资)
       ,sum(nvl(order_amt,0) - nvl(refund_prcp_amt,0) - nvl(repay_prcp_amt,0)) as item_valn
      ,'' as item_vals
from odm.ODM_CF_QY_IOU_RECEIPT_S_D t1
WHERE dt= '$TX_DATE'
      and jd_order_type = '20096'
      and to_date(tx_time) <= dt
GROUP BY t1.DT;



-- dwd_actv_bt_plus_plan_s_d
-- 749  集团今日往来流入
-- 750  集团今日往来流出
--修改人：王立新；修改时间：2018-09-17；修改内容：将本币和外币取数逻辑进行更改。
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='$TX_DATE') --插入749
select '$TX_DATE' as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,'$TX_DATE' as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'749'
      ,allocate_amt_sum_rm as item_valn
      ,'' as item_vals
from dmf_bc.dmfbc_alm_dm_01_group_funds_info_s_d
  where  dt='$TX_DATE'
;
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='$TX_DATE') --插入750
select '$TX_DATE' as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,'$TX_DATE' as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'750'
      ,allocate_amt_sum_wb as item_valn
      ,'' as item_vals
from dmf_bc.dmfbc_alm_dm_01_group_funds_info_s_d
  where  dt='$TX_DATE'
;

);


$SQL_BUFF[7]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d7;

use dmf_tmp;

-- dmt_ftp_jd_alm_cloud_plat_info_s_d   PROJECT_NAME LIKE '%信托%'    730  信托借款
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='$TX_DATE')
select dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'730' as item_key
       ,sum(A.Should_Pay_Amt) as item_valn
      ,'' as item_vals
FROM dmf_bc.dmfbc_alm_dm_01_cloud_plat_info_s_d A
WHERE dt= '$TX_DATE'
      and PROJECT_NAME LIKE '%信托%'
      and TO_DATE(A.END_DT)>=A.DT
GROUP BY dt
;


);



$SQL_BUFF[8]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d8;

use dmf_tmp;

-- 集团占款费用配置表数据 dim_ftp_jd_alm_dim_group_fee
from dim.dim_ftp_jd_alm_dim_group_fee t
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select '$TX_DATE' as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,'$TX_DATE' as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'753' as item_key    -- 白条ABS商城持有
      ,sum(fee_amt) as item_valn
      ,'' as item_vals
      ,'$TX_DATE'
WHERE FEE_START_DT<='$TX_DATE' AND FEE_END_DT>='$TX_DATE'
--  and fee_name in ('白条5号-江苏京东出资 HT201611230001','白条6号-江苏京东出资 HT201612130001')
  and split(t.fee_name, ' ')[1] in (
        select  distinct abs_id
        FROM    dmf_bc.dmfbc_alm_dm_01_abs_loan_info_s_d b
        WHERE   ABS_TYPE='2' AND TO_DATE(Maturity_Dt)>DT
            AND DT='$TX_DATE'
            and  b.abs_prin-(b.ACTU_FINANCING_PRIN-b.SHOULD_PAY_AMT)>0
        )
;
from dim.dim_ftp_jd_alm_dim_group_fee t
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select '$TX_DATE' as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,'$TX_DATE' as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'756' as item_key    -- 商城固定垫资
      ,SUM(CASE WHEN FEE_DIRECTION=2.0 THEN -1*FEE_AMT
                ELSE FEE_AMT
           END) AS item_valn
      ,'' as item_vals
      ,'$TX_DATE'
WHERE FEE_START_DT<='$TX_DATE'
  AND FEE_END_DT>='$TX_DATE'
   and( fee_name like '%京农贷泗洪县放款额+水电煤保证金%' or fee_name like '%集团代垫费用%')
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select '$TX_DATE' as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,'$TX_DATE' as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'758' as item_key    -- 白条资产回购
      ,sum(FEE_AMT) as item_valn
      ,'' as item_vals
      ,'$TX_DATE'
WHERE FEE_START_DT<='$TX_DATE'
  AND FEE_END_DT>='$TX_DATE'
  and fee_name like '%金融回购%'
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select '$TX_DATE' as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,'$TX_DATE' as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'760' as item_key    -- 老金采余额调整
      ,sum(FEE_AMT) as item_valn
      ,'' as item_vals
      ,'$TX_DATE'
WHERE FEE_START_DT<='$TX_DATE'
  AND FEE_END_DT>='$TX_DATE'
  AND fee_name = '企业白条余额调整'
  and FEE_DIRECTION=2
;

-- ABS基本信息
from dmf_bc.dmfbc_alm_dm_01_abs_loan_info_s_d
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'751' as item_key    -- 白条ABS今日摊还
      ,SUM(CASE WHEN abs_prin-(ACTU_FINANCING_PRIN-SHOULD_PAY_AMT)<0 THEN abs_prin
                else ACTU_FINANCING_PRIN-SHOULD_PAY_AMT
           END) as item_valn
      ,'' as item_vals
      ,dt
WHERE DT='$TX_DATE'
  AND ABS_TYPE='2'
  AND TO_DATE(Maturity_Dt)>DT
  AND currency_cd= 'CNY'
GROUP BY DT
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'757' as item_key    -- 白条ABS发行额
      ,SUM(CASE WHEN abs_prin-(ACTU_FINANCING_PRIN-SHOULD_PAY_AMT)<0
                THEN 0 ELSE abs_prin END)
          AS item_valn
      ,'' as item_vals
      ,dt
WHERE DT='$TX_DATE'
  AND TO_DATE(Maturity_Dt)>DT
  AND ABS_TYPE='2'
GROUP BY DT
--修改人：王立新；修改时间：2018-09-17；修改内容：201、206、501 取各项ABS金额是 加个筛选条件 TO_DATE( maturity_dt )>DT
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,CASE ABS_TYPE
          WHEN '1' THEN '507' -- 金条
          WHEN '6' THEN '' -- 农村金融,待定
       END as item_key
      ,CAST( SUM(case when should_pay_amt<0 then 0 else should_pay_amt end )  AS BIGINT )  as item_valn
      ,'' as item_vals
      ,dt
 where dt='$TX_DATE'
   and ABS_TYPE IN ('1') AND SHOULD_PAY_AMT>0  AND  TO_DATE(maturity_dt)>DT
 group by dt,
        CASE ABS_TYPE
            WHEN '1' THEN '507' -- 金条
            WHEN '6' THEN '' -- 农村金融【待定】
        END
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select dt as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,CASE ABS_TYPE
          WHEN '2' THEN '501' -- 小白
          WHEN '4' THEN '201' -- 保理
          WHEN '7' THEN '206' -- 小贷
       END as item_key
      ,CAST( SUM(case when should_pay_amt<0 then 0 else should_pay_amt end )  AS BIGINT ) as item_valn
      ,'' as item_vals
      ,dt
 where dt='$TX_DATE' and TO_DATE( maturity_dt )>DT
   and ABS_TYPE IN ('2', '4', '7') AND SHOULD_PAY_AMT>0  AND  TO_DATE(maturity_dt)>DT
 group by dt,
        CASE ABS_TYPE
            WHEN '2' THEN '501' -- 小白
            WHEN '4' THEN '201' -- 保理
            WHEN '7' THEN '206' -- 小贷
        END
;


INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d PARTITION(dt) values ('$TX_DATE','CAPITAL_OCCUPATION','$TX_DATE','JDJR','CNY','107',0.00,'','$TX_DATE') ;
INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d PARTITION(dt) values ('$TX_DATE','CAPITAL_OCCUPATION','$TX_DATE','JDJR','CNY','407',0.00,'','$TX_DATE') ;
INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d PARTITION(dt) values ('$TX_DATE','CAPITAL_OCCUPATION','$TX_DATE','JDJR','CNY','704',0.00,'','$TX_DATE') ;


-- 修改人王立新，修改日期2018-09-17；修改内容：727 垫资业务取  dim_ftp_jd_alm_dim_capital_occupation_a_d表内的 727 项目
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select '$TX_DATE' as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,'$TX_DATE' as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'727'  as item_key  -- 财富管理-垫资业务
      ,item_valn as item_valn
      ,'' as item_vals
      ,'$TX_DATE'
  from dim.dim_ftp_jd_alm_dim_capital_occupation_a_d
   where item_key='727'
;

--集团结算占用金额,dmt_ftp_jd_alm_group_funds_info_s_d.groups_amt
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt)
select '$TX_DATE' as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,'$TX_DATE' as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,'761'  as item_key  --集团结算占用金额
      ,groups_amt as item_valn
      ,'' as item_vals
      ,'$TX_DATE'
  from dmf_bc.dmfbc_alm_dm_01_group_funds_info_s_d
   where dt='$TX_DATE'
;

);



$SQL_BUFF[9]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d9;

use dmf_tmp;

INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='$TX_DATE')
SELECT
    '$TX_DATE'  AS etl_dt                       --数据日期
    ,'CAPITAL_OCCUPATION' AS report_key         --报表关键字
    ,'$TX_DATE' AS date_id                      --日期
    ,'JDJR' AS operate_org_cd                   --部门
    ,'CNY'  AS currency_cd                      --币种
    ,'502'  AS item_key                         --报表项关键字
    ,net_amt item_valn                          --数值型值
    ,'' AS item_vals                            --字符型值
FROM dmf_alm.dmfalm_alm_rpt_01_accsale_tran_crdt_s_d A
WHERE dt = '$TX_DATE' and loan_type='1'
;

--修改人：王立新；修改日期：2018-09-17；内容：新增TO_DATE(PLAT_CREATE_TIME) >='2018-08-01'
INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='$TX_DATE')
SELECT
    '$TX_DATE'  AS etl_dt                       --数据日期
    ,'CAPITAL_OCCUPATION' AS report_key         --报表关键字
    ,'$TX_DATE' AS date_id                      --日期
    ,'JDJR' AS operate_org_cd                   --部门
    ,'CNY'  AS currency_cd                      --币种
    ,'503'  AS item_key                         --报表项关键字
    ,(item_valn1 - item_valn2) AS item_valn     --数值型值
    ,'' AS item_vals                            --字符型值
FROM (
SELECT
        4767742299.37+CAST(t1.loan_amount- t2.CancelPrincipal-t2.PaidPrincipal -t2.ReverseUnpaidPrincipal -t2.UnderPrincipal AS FLOAT ) AS item_valn1
    FROM
        (     SELECT
            SUM(LOAn_amount)     AS   loan_amount
        FROM
            odm.ODM_FI_INVESTOR_LOAN_S_D k2
        WHERE
            DT = '$TX_DATE'
            and to_date(create_time)<=dt           -- 去除T+0 数据
            and to_date(record_update_time)<=dt    -- 去除T+0 数据
            AND k2.INVESTOR_ID IN (
                SELECT
                    investor_id
                FROM
                    odm.ODM_FI_PLAT_INVESTOR_S_D
                WHERE
                    DT='$TX_DATE'
                    AND investor_type='0'
                    AND (
                        PLAT_id IN (
                            '3' ,'101' ,'102' ,'301' ,'302' ,'303' ,'304' ,'305' ,'306' ,'307' ,'308' ,'309' ,'310' ,'311' ,'312' ,'313' ,'314' ,'315' ,'316' ,'317' ,'318' ,'319' ,'320' ,'321' ,'322' ,'323' ,'324' ,'325' ,'326' ,'327' ,'328' ,'329' ,'330'
                        )
                        OR   INVESTOR_ID     IN (
                            'inv_sdtoxd_cqxd'
                        )
                    )
            )
            AND status in(
                'Success','Cancel'
            )
            AND TO_DATE(
                PLAT_CREATE_TIME
            )<=DT
            AND  TO_DATE(
                PLAT_CREATE_TIME
            ) >='2018-08-01'
        ) t1
    LEFT JOIN
        (
            SELECT
                SUM(CASE
                    WHEN business_type = 'Cancel'
                    and money_type = 'CancelPrincipal' THEN COALESCE(amount,
                    0.0)
                    ELSE 0.0
                END)   AS CancelPrincipal         ,
                SUM(CASE
                    WHEN business_type = 'Repayment'
                    and money_type = 'PaidPrincipal' THEN COALESCE(amount,
                    0.0)
                    ELSE 0.0
                END)   AS PaidPrincipal         ,
                SUM(CASE
                    WHEN business_type = 'Refund'
                    AND money_type='ReverseUnpaidPrincipal' THEN coalesce(amount,
                    0.0)
                    ELSE 0.0
                END)  AS ReverseUnpaidPrincipal         ,
                SUM(CASE
                    WHEN business_type ='UnderRepayment'
                    AND money_type='Principal' THEN COALESCE(amount,
                    0.0)
                    ELSE 0.0
                END)    AS UnderPrincipal
            FROM
                odm.ODM_FI_INVESTOR_FLOW_S_D k1
            where
                dt = '$TX_DATE'
                AND TO_DATE(
                    create_time
                )<= DT
                AND TO_DATE(
                    create_time
                )>='2018-08-01'
                AND business_type in (
                    'Interest','Repayment','Refund','Cancel','Loan','Reverse'
                )
                AND k1.investor_id  IN (
                    SELECT
                        investor_id
                    FROM
                        odm.ODM_FI_PLAT_INVESTOR_S_D
                    WHERE
                        DT='$TX_DATE'
                        AND investor_type='0'
                        AND (
                            PLAT_id IN (
                                '3' ,'101' ,'102' ,'301' ,'302' ,'303' ,'304' ,'305' ,'306' ,'307' ,'308' ,'309' ,'310' ,'311' ,'312' ,'313' ,'314' ,'315' ,'316' ,'317' ,'318' ,'319' ,'320' ,'321' ,'322' ,'323' ,'324' ,'325' ,'326' ,'327' ,'328' ,'329' ,'330'
                            )
                            OR   INVESTOR_ID     IN (
                                'inv_sdtoxd_cqxd'
                            )
                        )
                )
            ) t2
                ON 1=1
) aa
LEFT JOIN (
    SELECT item_valn AS item_valn2
    FROM dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d
    WHERE dt = '$TX_DATE'
      AND item_key = '502'
) bb   ON 1= 1
;


);


$SQL_BUFF[10]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d10;

use dmf_tmp;
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='CURR')
select etl_dt, report_key, date_id, operate_org_cd,
       currency_cd, a.item_key, a.item_valn, item_vals
  from dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d a
 where dt = '$TX_DATE' and NVL(a.item_key, '') <> '' and
       a.item_key not IN (select b.item_key
                            from dim.dim_ftp_jd_alm_dim_capital_occupation_a_d b
                         )
;

insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='CURR')
select '$TX_DATE' as etl_dt
      ,'CAPITAL_OCCUPATION' as report_key
      ,'$TX_DATE' as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,item_key
      ,item_valn
      ,'' as item_vals
 from dim.dim_ftp_jd_alm_dim_capital_occupation_a_d
-- DMT.dmt_ftp_jd_alm_data_supplement_item_kv_s_d
;


use dmf_alm;
insert overwrite table dmf_alm.dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d partition(dt='$TX_DATE')
select etl_dt
      ,report_key       -- '报表关键字'
      ,date_id          -- '日期'
      ,operate_org_cd   -- '部门'
      ,currency_cd      -- '币种'
      ,item_key         -- '报表项关键字'
      ,sum(item_valn)   -- '数值型值'
      ,item_vals        -- '字符型值'
from  dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d
where dt='CURR'
group by etl_dt
      ,report_key       -- '报表关键字'
      ,date_id          -- '日期'
      ,operate_org_cd   -- '部门'
      ,currency_cd      -- '币种'
      ,item_key         -- '报表项关键字'
      ,item_vals        -- '字符型值'
;

);

$SQL_BUFF[11]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d11;

use dmf_tmp;
drop table dmf_tmp.tmp_dmfalm_alm_rpt_01_grp_cap_ocup_fee_s_d
;

);

#############################################################################################
########################################以上为SQL编辑区######################################
#############################################################################################

    return @SQL_BUFF;
}

########################################################################################################################

sub main
{
    my $ret;

    my @sql_buff = getsql();

    for (my $i = 0; $i <= $#sql_buff; $i++) {
        $ret = Common::Hive::run_hive_sql($sql_buff[$i], ${Runner}, ${Retry_Runner});

        if ($ret != 0) {
            print getCurrentDateTime("SQL_BUFF[$i] Execute Failed");
            return $ret;
        }
        else {
            print getCurrentDateTime("SQL_BUFF[$i] Execute Success");
        }
    }

    return $ret;
}

########################################################################################################################
# program section
# To see if there is one parameter,
print getCurrentDateTime(" Startup Success ..");
print "JOB          : $JOB\n";
print "TX_DATE      : $TX_DATE\n";
print "TXDATE       : $TXDATE\n";
print "Target TABLE : $TABLE\n";

my $rc = main();
if ( $rc != 0 ) {
    print getCurrentDateTime("Task Execution Failed"),"\n";
} else{
    print getCurrentDateTime("Task Execution Success"),"\n";
}
exit($rc);