#!/usr/bin/perl
########################################################################################################################
#  Creater        :wanglixin16
#  Creation Time  :2018-09-26
#  Description    :dmfalm_alm_rpt_01_roll_bdgt_s_d 滚动预算报表
#                 :report_key='ROLLING_BUDGET',资金占用; operate_org_cd='JDJR',京东金融; currency_cd='CNY',人民币
#  Modify By      :
#  Modify Time    :
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
set mapred.job.name=dmfalm_alm_rpt_01_roll_bdgt_s_d1;

use dmf_tmp;
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d
like dmf_alm.dmfalm_alm_rpt_01_roll_bdgt_s_d
;

);


$SQL_BUFF[2]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d2;


use dmf_tmp;

-- 插入供应链金融信息
INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
SELECT                                     --'经营现金流入额'
     a.dt      AS etl_dt
    ,'ROLLING_BUDGET' AS report_key
    ,a.dt     AS date_id
    ,'JDJR' AS operate_org_cd
    ,'CNY'  AS currency_cd
    ,A.item_key AS item_key
    ,CAST( B.lunret_pri-A.lunret_pri+B.unret_pri-A.unret_pri   AS BIGINT)   AS item_valn
    ,'1'     AS item_vals
FROM
        (SELECT
           dt,
           case when biz_line='内部保理' then '101'
                when biz_line='订单池融资' and cust_name like '%华为%' then '102'
                when biz_line='订单池融资' and cust_name not like '%华为%' then '102A'
                when biz_line='内部小贷' then '103'
                when biz_line='外部保理' then '106'
                when biz_line in ('外部小贷','快银BOSS贷','快银') then '107'
                when biz_line='动产质押' then '108'
              else '999'
           end AS item_key,
           CAST(SUM(CASE
                WHEN DATE_ADD(DT,
                -1)=to_date( start_time ) THEN  unret_pri_new
                ELSE 0
            END ) AS BIGINT ) AS lunret_pri,

            CAST( SUM(CASE
                WHEN DATE_ADD(DT,
                -1)<>to_date( start_time )
                and to_date( start_time )<> DT   THEN  unret_pri_new
                ELSE 0
            END ) AS BIGINT  ) AS unret_pri
        FROM
                (SELECT
                        k1.*,
                        CASE WHEN k1.currency='USD' THEN  k1.unret_pri*k2.exchg_rate ELSE k1.unret_pri END  as   unret_pri_new
                   FROM
                        (select
                                *
                           from
                                idm.idm_f02_sf_ordr_dtl_s_d
                           where DT = '$TX_DATE'
                                 AND biz_line in('动产质押','外部小贷','内部小贷','外部保理','内部保理','快银','订单池融资','快银BOSS贷')
                         ) k1
                 left join
                          (select * from dmf_dim.dmfdim_dim_exchg_rate_i_d where DT = '$TX_DATE') k2
                        on k1.dt=k2.dt and k1.currency=k2.currency
                ) M
         GROUP BY dt,
           case when biz_line='内部保理' then '101'
                when biz_line='订单池融资' and cust_name like '%华为%' then '102'
                when biz_line='订单池融资' and cust_name not like '%华为%' then '102A'
                when biz_line='内部小贷' then '103'
                when biz_line='外部保理' then '106'
                when biz_line in ('外部小贷','快银BOSS贷','快银') then '107'
                when biz_line='动产质押' then '108'
              else '999'
           end
        )A
  JOIN
        (
            SELECT
                case when biz_line='内部保理' then '101'
                     when biz_line='订单池融资' and cust_name like '%华为%' then '102'
                     when biz_line='订单池融资' and cust_name not like '%华为%' then '102A'
                     when biz_line='内部小贷' then '103'
                     when biz_line='外部保理' then '106'
                     when biz_line in ('外部小贷','快银BOSS贷','快银') then '107'
                     when biz_line='动产质押' then '108'
                    else '999'
                end AS item_key,
                CAST(SUM( CASE
                    WHEN TO_DATE(A.start_time)=DT THEN  unret_pri_new
                    ELSE 0
                  END  ) AS BIGINT ) AS Lunret_pri,
                CAST(SUM(CASE
                    WHEN TO_DATE(A.start_time)<>DT THEN  unret_pri_new
                    ELSE 0
                  END   ) AS BIGINT ) AS unret_pri
            FROM
                (SELECT
                        k1.*,
                        CASE WHEN k1.currency='USD' THEN  k1.unret_pri*k2.exchg_rate ELSE k1.unret_pri END  as   unret_pri_new
                   FROM
                        (select
                                *
                           from
                                idm.idm_f02_sf_ordr_dtl_s_d
                           where DT = '$TX_PREV_DATE'
                                 AND biz_line in('动产质押','外部小贷','内部小贷','外部保理','内部保理','快银','订单池融资','快银BOSS贷')
                         ) k1
                 left join
                          (select * from dmf_dim.dmfdim_dim_exchg_rate_i_d where DT = '$TX_PREV_DATE') k2
                        on k1.dt=k2.dt and k1.currency=k2.currency
                ) A
          GROUP BY
           case when biz_line='内部保理' then '101'
                when biz_line='订单池融资' and cust_name like '%华为%' then '102'
                when biz_line='订单池融资' and cust_name not like '%华为%' then '102A'
                when biz_line='内部小贷' then '103'
                when biz_line='外部保理' then '106'
                when biz_line in ('外部小贷','快银BOSS贷','快银') then '107'
                when biz_line='动产质押' then '108'
               else '999'
           end
        )B
            ON a.item_key=b.item_key
;

-- 插入金采流入
INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
SELECT
     a.etl_dt      AS etl_dt
    ,'ROLLING_BUDGET' AS report_key
    ,a.etl_dt     AS date_id
    ,'JDJR' AS operate_org_cd
    ,'CNY'  AS currency_cd
    ,'104' AS item_key
    ,sum(JC_REFUND_AMT_TODAY+JC_REPAY_PRI_TODAY)   AS item_valn
    ,'1'     AS item_vals
from
    sdm.sdm_f02_sf_jdjc_prod_tx_sum_a_d a
where etl_dt='$TX_DATE'
group by a.etl_dt
;

-- 插入金采流出
INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
SELECT
     a.etl_dt      AS etl_dt
    ,'ROLLING_BUDGET' AS report_key
    ,a.etl_dt     AS date_id
    ,'JDJR' AS operate_org_cd
    ,'CNY'  AS currency_cd
    ,'131' AS item_key
    ,sum(JC_ORDER_AMT_TODAY-jc_refund_amt_today)   AS item_valn
    ,'2'     AS item_vals
from
    sdm.sdm_f02_sf_jdjc_prod_tx_sum_a_d a
where etl_dt='$TX_DATE'
group by a.etl_dt
;

INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
SELECT                                     --'经营现金流出额'
            dt      AS etl_dt
           ,'ROLLING_BUDGET' AS report_key
           ,dt     AS date_id
           ,'JDJR' AS operate_org_cd
           ,'CNY'  AS currency_cd
           ,case when biz_line='内部保理' then '128'
                 when biz_line='订单池融资' and cust_name like '%华为%' then '129'
                 when biz_line='订单池融资' and cust_name not like '%华为%' then '129A'
                 when biz_line='内部小贷' then '130'
                 when biz_line='外部保理' then '133'
                 when biz_line in ('外部小贷','快银BOSS贷','快银') then '134'
                 when biz_line='动产质押' then '135'
                else '999'
             end AS item_key
            ,CAST(SUM(amount_new)  AS BIGINT ) AS item_valn
            ,2 as  item_vals
  FROM
      (SELECT
             k1.*,
             CASE WHEN k1.currency='USD' THEN  k1.unret_pri*k2.exchg_rate ELSE k1.unret_pri END  as   amount_new
        FROM
             (select
                     *
                from
                     idm.idm_f02_sf_ordr_dtl_s_d
                 where DT = '$TX_DATE'
                       AND biz_line in('动产质押','外部小贷','内部小贷','外部保理','内部保理','快银','订单池融资','快银BOSS贷')
                       AND to_date(start_time)='$TX_DATE'
                       AND cast(unret_pri as float)<>0
              ) k1
         left join
             (select * from dmf_dim.dmfdim_dim_exchg_rate_i_d where DT = '$TX_DATE') k2
          on k1.dt=k2.dt and k1.currency=k2.currency
      ) A
   GROUP BY dt,
            case when biz_line='内部保理' then '128'
                 when biz_line='订单池融资' and cust_name like '%华为%' then '129'
                 when biz_line='订单池融资' and cust_name not like '%华为%' then '129A'
                 when biz_line='内部小贷' then '130'
                 when biz_line='外部保理' then '133'
                 when biz_line in ('外部小贷','快银BOSS贷','快银') then '134'
                 when biz_line='动产质押' then '135'
                else '999'
            end
;

);

$SQL_BUFF[3]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d3;


use dmf_tmp;

INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
select
       '$TX_DATE'      AS etl_dt
      ,'ROLLING_BUDGET' AS report_key
      ,'$TX_DATE'     AS date_id
      ,'JDJR' AS operate_org_cd
      ,'CNY'  AS currency_cd
      ,'115'
      ,sum(CAST( B.lunpayoff_prin-A.lunpayoff_prin  AS BIGINT))   AS item_valn
      ,'1'
  FROM
        (
         SELECT
                dt,
                sum(case when to_date(create_dt)='$TX_DATE'  then 0 else should_pay_amt end) AS lunpayoff_prin
           FROM
                dmf_bc.dmfbc_alm_dm_01_jt_acct_info_s_d
           WHERE
                 DT='$TX_DATE' 
            group by dt
        )a
   JOIN
        (
            SELECT
                CAST(SUM( should_pay_amt) AS BIGINT ) AS Lunpayoff_prin
            FROM
                dmf_bc.dmfbc_alm_dm_01_jt_acct_info_s_d A
            WHERE
                DT='$TX_PREV_DATE'
        )b
            on 1=1
;


INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
SELECT                                                   ------'金条~ 经营现金流出额'
         '$TX_DATE'       AS etl_dt
        ,'ROLLING_BUDGET' AS report_key
        ,'$TX_DATE'       AS date_id
        ,'JDJR'           AS operate_org_cd
        ,'CNY'            AS currency_cd
        ,'142'            AS item_key
        ,CAST(sum(should_pay_amt) AS BIGINT )  AS item_valn
        ,'2'              AS item_vals
    FROM
        dmf_bc.dmfbc_alm_dm_01_jt_acct_info_s_d t1
    where
        dt='$TX_DATE'
        and to_date(create_dt)=dt
        and cast(should_pay_amt as float)<>0
;

INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d  partition(dt='$TX_DATE')
SELECT                                          ---'经营现金流入额-校园白条和小白条'
       '$TX_DATE'      AS etl_dt
      ,'ROLLING_BUDGET' AS report_key
      ,'$TX_DATE'     AS date_id
      ,'JDJR' AS operate_org_cd
      ,'CNY'  AS currency_cd
      ,case when a.Member_Type='3' then '113' else '112' end as item_key
      ,sum(CAST( B.lunpayoff_prin-A.lunpayoff_prin  AS BIGINT))  AS item_valn
      ,'1'     AS item_vals
  FROM
        (
         SELECT
                dt,
                Member_Type,
                sum(case when to_date(create_dt)='$TX_DATE'  then 0 else should_pay_amt end) AS lunpayoff_prin
           FROM
                dmf_bc.dmfbc_alm_dm_01_bt_loan_info_s_d
           WHERE
                 DT='$TX_DATE' 
            group by dt,Member_Type
        )a
   JOIN
        (
            SELECT
                  Member_Type,
                  CAST(SUM( should_pay_amt) AS BIGINT ) AS Lunpayoff_prin
            FROM
                dmf_bc.dmfbc_alm_dm_01_bt_loan_info_s_d A
            WHERE
                DT='$TX_PREV_DATE'
            group by Member_Type
        )b
            on a.Member_Type=b.Member_Type
  GROUP BY case when a.Member_Type='3' then '113' else '112' end
;

INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d  partition(dt='$TX_DATE')
SELECT
       '$TX_DATE'      AS etl_dt
      ,'ROLLING_BUDGET' AS report_key
      ,'$TX_DATE'     AS date_id
      ,'JDJR' AS operate_org_cd
      ,'CNY'  AS currency_cd
      ,item_key
      ,item_valn
      ,item_vals
FROM
(SELECT      ---'经营现金流出额-校园白条和小白条'
        case when prod_type='校园' then '140' else '139' end as item_key
        ,CAST(SUM(unpayoff_prin) as float) as item_valn
        ,'2' as item_vals
    FROM
        sdm.sdm_f02_cf_xbt_ordr_dtl_s_d
    WHERE
        DT='$TX_DATE'
        AND to_date(loan_time)='$TX_DATE'
        and cast(unpayoff_prin as float)<>0
     GROUP BY case when prod_type='校园' then '140' else '139' end
 ) tt
;

);

$SQL_BUFF[4]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d4;


use dmf_tmp;

INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
SELECT
       '$TX_DATE'      AS etl_dt
      ,'ROLLING_BUDGET' AS report_key
      ,'$TX_DATE'     AS date_id
      ,'JDJR' AS operate_org_cd
      ,'CNY'  AS currency_cd
      ,item_key
      ,item_valn
      ,item_vals
FROM
(select
        '114' as item_key,                 --流入-分期业务
        CAST( BB.lunpayoff_prin-AA.lunpayoff_prin+BB.unpayoff_prin-AA.unpayoff_prin   AS BIGINT) as item_valn ,
        '1'     AS item_vals
    from
        (SELECT
            CAST(SUM(CASE
                WHEN DATE_ADD(A.DT,
                -1)=to_date( C.createdate ) THEN  a.shouldpayamount
                ELSE 0
            END ) AS BIGINT ) AS lunpayoff_prin,
            CAST( SUM(CASE
                WHEN DATE_ADD(A.DT,
                -1)<>to_date( C.createdate  )
                and to_date( C.createdate  )<> A.DT   THEN  a.shouldpayamount
                ELSE 0
            END ) AS BIGINT  ) AS unpayoff_prin
        FROM
            (select
                *
            from
                odm.ODM_CF_PLUS_PLAN_S_D
            where
                dt='$TX_DATE'
                and to_date(
                    createdate
                )<=dt ) a
        inner join
            (
                select
                    *
                from
                    odm.ODM_CF_PLUS_LOAN_ORDER_S_D
                WHERE
                    dt='$TX_DATE'
                    and status =3
                    and to_date(
                        completetime
                    )<=dt
            )C
                on a.loanno=c.loanid
            )AA
    JOIN
        (
            SELECT
                CAST(SUM( CASE
                    WHEN TO_DATE( E.createdate )=E.DT THEN  D.shouldpayamount
                    ELSE 0
                END  ) AS BIGINT ) AS Lunpayoff_prin,
                CAST(SUM(CASE
                    WHEN TO_DATE( E.createdate )<>E.DT THEN  D.shouldpayamount
                    ELSE 0
                END   ) AS BIGINT ) AS unpayoff_prin
            FROM
                (select
                    *
                from
                    odm.ODM_CF_PLUS_PLAN_S_D
                where
                    dt='$TX_PREV_DATE'
                    and to_date(
                        createdate
                    )<=dt ) D
            inner join
                (
                    select
                        *
                    from
                        odm.ODM_CF_PLUS_LOAN_ORDER_S_D
                    WHERE
                        dt='$TX_PREV_DATE'
                        and status =3
                        and to_date(
                            completetime
                        )<=dt
                )E
                    on D.loanno=E.loanid
                )BB
                    ON 1=1
UNION ALL
select
        '141' as item_key,
        CAST(SUM( CASE WHEN TO_DATE(C.createdate )=C.DT THEN  A.shouldpayamount ELSE 0 END  ) AS BIGINT ) as item_valn,
        '2'     AS item_vals
    FROM
        (select
                *
          from
               odm.ODM_CF_PLUS_PLAN_S_D
           where
            dt='$TX_DATE'
            and to_date(createdate)<=dt
         ) a
    inner join
        (
            select
                   *
             from
                  odm.ODM_CF_PLUS_LOAN_ORDER_S_D
              WHERE
                dt='$TX_DATE'
                and status =3
                and to_date(completetime)<=dt
        )C
            on a.loanno=c.loanid
 ) tt
;

);

$SQL_BUFF[5]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d5;


use dmf_tmp;


INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
SELECT       ---'供应链金融-筹资活动现金流入额-保理ABS和小贷ABS'
       '$TX_DATE'      AS etl_dt
      ,'ROLLING_BUDGET' AS report_key
      ,'$TX_DATE'     AS date_id
      ,'JDJR' AS operate_org_cd
      ,'CNY'  AS currency_cd
      ,case when abs_type='4' then '201' --保理ABS
           when abs_type='7' then '204' --小贷ABS
            when abs_type='1' then '210' --金条ABS
            when abs_type='2' then '209' --白条ABS
            when abs_type='6' then '218' --农贷ABS
         end as item_key
      ,cast(sum(abs_prin) as float) as item_valn
      ,'1' as item_vals
 FROM dmf_bc.dmfbc_alm_dm_01_abs_loan_info_s_d
    where
        dt='$TX_DATE'
        and to_date(start_dt)=dt
        and to_date(finish_date)>=dt
    GROUP BY
            case when abs_type='4' then '201'
                when abs_type='7' then '204'
                 when abs_type='1' then '210'
                 when abs_type='2' then '209'
                 when abs_type='6' then '218'
           end
;
INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
SELECT
        '$TX_DATE'      AS etl_dt
       ,'ROLLING_BUDGET' AS report_key
       ,'$TX_DATE'     AS date_id
       ,'JDJR' AS operate_org_cd
       ,'CNY'  AS currency_cd
       ,case when a.abs_type='1' then '233' --'金条ABS摊还'
             when a.abs_type='2' then '231' --'白条ABS摊还'
             when a.abs_type='4' then '232' --'保理ABS摊还'
             when a.abs_type='6' then '235' --'农贷ABS摊还'
             when a.abs_type='7' then '234' --'小贷ABS摊还'
             when a.abs_type not in('1','2','4','6','7') then '236' --'其他ABS摊还'
          end as item_key
       , cast((a.abspayamt-b.abspayamt) as float) as item_valn
       ,'2' as item_vals
 from
        (select
            abs_type,
            cast(sum(actu_financing_prin-should_pay_amt) as bigint) as abspayamt
        from
            dmf_bc.dmfbc_alm_dm_01_abs_loan_info_s_d
        WHERE
            dt='TX_DATE'
         group by abs_type)a
    join
        (
            select
                abs_type,
                cast(sum(actu_financing_prin-should_pay_amt) as bigint) as abspayamt
            from
                dmf_bc.dmfbc_alm_dm_01_abs_loan_info_s_d
            where
                dt='TX_PREV_DATE'
             group by abs_type
        )b
            on a.abs_type=b.abs_type
;

);


$SQL_BUFF[6]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d6;


use dmf_tmp;

INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
SELECT
       '$TX_DATE'     AS etl_dt
      ,'ROLLING_BUDGET' AS report_key
      ,'$TX_DATE'     AS date_id
      ,'JDJR' AS operate_org_cd
      ,'CNY'  AS currency_cd
      ,'217' as item_key  --消金-农贷联合贷
      ,CAST(sum(order_amt*investment_ratio/100) AS FLOAT ) as item_valn
      ,'1' as item_vals
   FROM
        dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d   A
    where
          DT='$TX_DATE'
          and  to_date(create_dt)='$TX_DATE'
          and  union_type='4'
          and cast(should_pay_amt as int)<>0
;


INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
SELECT
       '$TX_DATE'     AS etl_dt
      ,'ROLLING_BUDGET' AS report_key
      ,'$TX_DATE'     AS date_id
      ,'JDJR' AS operate_org_cd
      ,'CNY'  AS currency_cd
      ,'212' as item_key  --消金-金条联合贷表内
      ,CAST(sum(should_pay_amt) AS FLOAT ) as item_valn
      ,'1' as item_vals
FROM dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d  B
 WHERE
      B.DT='$TX_DATE' and  to_date(create_dt)='$TX_DATE'
      and  union_type='1'
      and under_ratio>0
      and cast(should_pay_amt as int)<>0
;
INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
SELECT
       '$TX_DATE'     AS etl_dt
      ,'ROLLING_BUDGET' AS report_key
      ,'$TX_DATE'     AS date_id
      ,'JDJR' AS operate_org_cd
      ,'CNY'  AS currency_cd
      ,'241' as item_key  --消金-金条联合贷表外
      ,CAST(sum(should_pay_amt) AS FLOAT ) as item_valn
      ,'1' as item_vals
FROM dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d  B
 WHERE
      B.DT='$TX_DATE' and  to_date(create_dt)='$TX_DATE'
      and  union_type='1'
      and (under_ratio=0 or under_ratio is null)
      and cast(should_pay_amt as int)<>0
;



INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt)
SELECT
       '$TX_DATE'     AS etl_dt
      ,'ROLLING_BUDGET' AS report_key
      ,'$TX_DATE'     AS date_id
      ,'JDJR' AS operate_org_cd
      ,'CNY'  AS currency_cd
      ,'242' as item_key  --消金-小白联合贷表内
      ,CAST(SUM(should_pay_amt) AS FLOAT ) as item_valn
      ,'1' as item_vals
      ,'$TX_DATE'
FROM dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d  B
 WHERE
      B.DT='$TX_DATE' and  to_date(create_dt)='$TX_DATE'
      and  union_type='2'
      and under_ratio>0
      and cast(should_pay_amt as int)<>0
;
INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt)
SELECT
       '$TX_DATE'     AS etl_dt
      ,'ROLLING_BUDGET' AS report_key
      ,'$TX_DATE'     AS date_id
      ,'JDJR' AS operate_org_cd
      ,'CNY'  AS currency_cd
      ,'213' as item_key  --消金-小白联合贷表外
      ,CAST(SUM(should_pay_amt) AS FLOAT ) as item_valn
      ,'1' as item_vals
      ,'$TX_DATE'
FROM dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d  B
 WHERE
      B.DT='$TX_DATE' and  to_date(create_dt)='$TX_DATE'
      and union_type='2'
      and (under_ratio=0 or under_ratio is null)
      and cast(should_pay_amt as int)<>0
;

INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
SELECT
       '$TX_DATE'     AS etl_dt
      ,'ROLLING_BUDGET' AS report_key
      ,'$TX_DATE'     AS date_id
      ,'JDJR' AS operate_org_cd
      ,'CNY'  AS currency_cd
      ,'214' as item_key  --消金-大白联合贷
      ,CAST(sum(should_pay_amt) AS FLOAT ) as item_valn
      ,'1' as item_vals
FROM
     dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d  B
 WHERE
      B.DT='$TX_DATE' and  to_date(create_dt)='$TX_DATE'
      and  union_type='3'
      and cast(should_pay_amt as int)<>0
;
);

$SQL_BUFF[7]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d7;

use dmf_tmp;

INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
select
       '$TX_DATE'     AS etl_dt
      ,'ROLLING_BUDGET' AS report_key
      ,'$TX_DATE'     AS date_id
      ,'JDJR' AS operate_org_cd
      ,'CNY'  AS currency_cd
      ,'118' as item_key            --流入-农村金融-京农贷
      ,CAST( b.lunpayoff_prin-a.lunpayoff_prin  AS BIGINT)  as   item_valn
      ,'1' as item_vals
 from
     (SELECT
            dt,
            sum(case when to_date(create_dt)='$TX_DATE'  then 0 else should_pay_amt end) AS lunpayoff_prin
        FROM
             dmf_bc.dmfbc_alm_dm_01_jnd_acct_info_s_d
        WHERE
              DT='$TX_DATE'
         group by dt
      )a
  JOIN
      (
        SELECT
               CAST(SUM( should_pay_amt) AS BIGINT ) AS Lunpayoff_prin
          FROM
               dmf_bc.dmfbc_alm_dm_01_jnd_acct_info_s_d A
         WHERE
               DT='$TX_PREV_DATE'
      )b
   on 1=1
;

INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
select
       '$TX_DATE'     AS etl_dt
      ,'ROLLING_BUDGET' AS report_key
      ,'$TX_DATE'     AS date_id
      ,'JDJR' AS operate_org_cd
      ,'CNY'  AS currency_cd
      ,'145' as item_key            --流出-农村金融-京农贷
      ,sum(order_amt*(Investment_Ratio/100))  as item_valn
      ,'2' as item_vals
from
       dmf_bc.dmfbc_alm_dm_01_jnd_acct_info_s_d
 where dt='$TX_DATE'
       and to_date(create_dt)=dt
       and cast(should_pay_amt as int)<>0

;
);


$SQL_BUFF[8]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d8;


use dmf_tmp;


INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
select
       '$TX_DATE'     AS etl_dt
      ,'ROLLING_BUDGET' AS report_key
      ,'$TX_DATE'     AS date_id
      ,'JDJR' AS operate_org_cd
      ,'CNY'  AS currency_cd
      ,'243' as item_key  --流出-金条联合贷表内
      ,item_valn  as item_valn
      ,'2' as item_vals
    from
        (
         SELECT
                sum(CAST( B.lunpayoff_prin-A.lunpayoff_prin  AS BIGINT))    AS item_valn
           FROM
                (SELECT
                       dt,
                       sum(case when to_date(create_dt)='$TX_DATE'  then 0 else should_pay_amt end) AS lunpayoff_prin
                   FROM
                        dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d
                   WHERE
                         DT='$TX_DATE'
                         and  union_type='1'
                         and under_ratio>0
                   group by dt
                 )a
             JOIN
                 (
                   SELECT
                          CAST(SUM( should_pay_amt) AS BIGINT ) AS Lunpayoff_prin
                     FROM
                          dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d A
                    WHERE
                          DT='$TX_PREV_DATE'
                          and  union_type='1'
                          and under_ratio>0
                 )b
              on 1=1
        ) t2
;


INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
select
       '$TX_DATE'     AS etl_dt
      ,'ROLLING_BUDGET' AS report_key
      ,'$TX_DATE'     AS date_id
      ,'JDJR' AS operate_org_cd
      ,'CNY'  AS currency_cd
      ,'244' as item_key  --流出-金条联合贷表外
      ,item_valn  as item_valn
      ,'2' as item_vals
    from
        (
         SELECT
                sum(CAST( B.lunpayoff_prin-A.lunpayoff_prin  AS BIGINT))    AS item_valn
           FROM
                (SELECT
                       dt,
                       sum(case when to_date(create_dt)='$TX_DATE'  then 0 else should_pay_amt end) AS lunpayoff_prin
                   FROM
                        dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d
                   WHERE
                         DT='$TX_DATE'
                         and  union_type='1'
                         and (under_ratio=0 or under_ratio is null)
                   group by dt
                 )a
             JOIN
                 (
                   SELECT
                          CAST(SUM( should_pay_amt) AS BIGINT ) AS Lunpayoff_prin
                     FROM
                          dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d A
                    WHERE
                          DT='$TX_PREV_DATE'
                          and  union_type='1'
                          and (under_ratio=0 or under_ratio is null)
                 )b
              on 1=1
        ) t2
;


INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
select
       '$TX_DATE'     AS etl_dt
      ,'ROLLING_BUDGET' AS report_key
      ,'$TX_DATE'     AS date_id
      ,'JDJR' AS operate_org_cd
      ,'CNY'  AS currency_cd
      ,'245' as item_key  --流出-小白联合贷表内
      ,item_valn  as item_valn
      ,'2' as item_vals
    from
       (
         SELECT
                sum(CAST( B.lunpayoff_prin-A.lunpayoff_prin  AS BIGINT))    AS item_valn
           FROM
                (SELECT
                       dt,
                       sum(case when to_date(create_dt)='$TX_DATE'  then 0 else should_pay_amt end) AS lunpayoff_prin
                   FROM
                        dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d
                   WHERE
                         DT='$TX_DATE'
                         and  union_type='2'
                         and under_ratio>0
                   group by dt
                 )a
             JOIN
                 (
                   SELECT
                          CAST(SUM( should_pay_amt) AS BIGINT ) AS Lunpayoff_prin
                     FROM
                          dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d A
                    WHERE
                          DT='$TX_PREV_DATE'
                          and  union_type='2'
                          and under_ratio>0
                 )b
              on 1=1
        ) t2
;


INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
SELECT
         '$TX_DATE'     AS etl_dt
        ,'ROLLING_BUDGET' AS report_key
        ,'$TX_DATE'     AS date_id
        ,'JDJR' AS operate_org_cd
        ,'CNY'  AS currency_cd
        ,'246' as item_key  --流出-小白联合贷表外
        ,sum(CAST( B.lunpayoff_prin-A.lunpayoff_prin  AS BIGINT))    AS item_valn
        ,'2' as item_vals
  FROM
       (SELECT
              dt,
              sum(case when to_date(create_dt)='$TX_DATE'  then 0 else should_pay_amt end) AS lunpayoff_prin
          FROM
               dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d
          WHERE
                DT='$TX_DATE'
                and  union_type='2'
                and (under_ratio=0 or under_ratio is null)
          group by dt
        )a
    JOIN
        (
          SELECT
                 CAST(SUM( should_pay_amt) AS BIGINT ) AS Lunpayoff_prin
            FROM
                 dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d A
           WHERE
                 DT='$TX_PREV_DATE'
                 and  union_type='2'
                 and (under_ratio=0 or under_ratio is null)
        )b
     on 1=1
;

INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
SELECT
         '$TX_DATE'     AS etl_dt
        ,'ROLLING_BUDGET' AS report_key
        ,'$TX_DATE'     AS date_id
        ,'JDJR' AS operate_org_cd
        ,'CNY'  AS currency_cd
        ,'247' as item_key  --流出-大白联合贷
        ,sum(CAST( B.lunpayoff_prin-A.lunpayoff_prin  AS BIGINT))    AS item_valn
        ,'2' as item_vals
  FROM
        (SELECT
            dt,
            sum(case when to_date(create_dt)='$TX_DATE'  then 0 else should_pay_amt end) AS lunpayoff_prin
        FROM
            dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d
        WHERE
            DT='$TX_DATE'  and  union_type='3'
         group by dt
        )a
   JOIN
        (
            SELECT
                CAST(SUM( should_pay_amt) AS BIGINT ) AS Lunpayoff_prin
            FROM
                dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d A
            WHERE
                DT='$TX_PREV_DATE' and  union_type='3'
        )b
            on 1=1
;

INSERT INTO TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
select
       '$TX_DATE'     AS etl_dt
      ,'ROLLING_BUDGET' AS report_key
      ,'$TX_DATE'     AS date_id
      ,'JDJR' AS operate_org_cd
      ,'CNY'  AS currency_cd
      ,'248' as item_key  --流出-农贷联合贷
      ,CAST( B.lunpayoff_prin-A.lunpayoff_prin+B.unpayoff_prin-A.unpayoff_prin   AS BIGINT) as item_valn
      ,'2' as item_vals
FROM
        (SELECT
            CAST(SUM(CASE
                WHEN DATE_ADD(DT,
                -1)=to_date( create_dt ) THEN  SHOULD_PAY_AMT
                ELSE 0
            END ) AS BIGINT ) AS lunpayoff_prin,
            CAST( SUM(CASE
                WHEN DATE_ADD(DT,
                -1)<>to_date( create_dt )
                and to_date( create_dt )<> DT   THEN  SHOULD_PAY_AMT
                ELSE 0
            END ) AS BIGINT  ) AS unpayoff_prin
        from
            dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d   A
        where
            dt='$TX_DATE'
            AND (Under_Ratio is not null or Under_Ratio<>0)
            AND Investment_Code IN (
                'inv_guilinbk' ,'inv_huishangbk' ,'inv_shanghaibk1' ,'inv_hknsgs_jt' ,'inv_jt_zjtxt1' ,'inv_xianbk' ,'inv_zrxt6' ,'inv_cjbk_zx' ,'inv_sh_jx' ,'inv_shyh_trvl' ,'inv_shyh_zf' ,'inv_cjyh_dycd' ,'inv_sh_btp' ,'inv_shyh_edu' ,'inv_xbyc_chjbankGS' ,'inv_zx_chjbankGS' ,'inv_hkns_jhxbk' ,'inv_hknsgs_xb' ,'inv_jhxbk_gsbk' ,'inv_sdtoxd_gsbank' ,'inv_sdtoxd_shyh' ,'inv_sdtoxd_xian' ,'inv_shyh_1hd' ,'inv_shyh_btfq' ,'inv_shyh_btzf' ,'inv_shyh_cxsx' ,'inv_shyh_qletc' ,'inv_shyh_qx' ,'inv_shyh_smf' ,'inv_shyh_yhqsm' ,'inv_shyh_ylzds' ,'inv_shyh_zc' ,'inv_shyhxgs_qqg' ,'inv_shyh_dyf' ,'inv_qqg_gsbk' ,'inv_zxxsk_gsbk' ,'30017842' ,'30025355' ,'30026632'
            )
            AND UNION_TYPE='4' )A
    JOIN
        (
            SELECT
                CAST(SUM( CASE
                    WHEN TO_DATE(A.create_dt)=DT THEN  SHOULD_PAY_AMT
                    ELSE 0
                END  ) AS BIGINT ) AS Lunpayoff_prin,
                CAST(SUM(CASE
                    WHEN TO_DATE(A.create_dt)<>DT THEN  SHOULD_PAY_AMT
                    ELSE 0
                END   ) AS BIGINT ) AS unpayoff_prin
            from
                dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d   A
            where
                dt='$TX_PREV_DATE'
                AND Under_Ratio<>0
                AND Investment_Code IN (
                    'inv_guilinbk' ,'inv_huishangbk' ,'inv_shanghaibk1' ,'inv_hknsgs_jt' ,'inv_jt_zjtxt1' ,'inv_xianbk' ,'inv_zrxt6' ,'inv_cjbk_zx' ,'inv_sh_jx' ,'inv_shyh_trvl' ,'inv_shyh_zf' ,'inv_cjyh_dycd' ,'inv_sh_btp' ,'inv_shyh_edu' ,'inv_xbyc_chjbankGS' ,'inv_zx_chjbankGS' ,'inv_hkns_jhxbk' ,'inv_hknsgs_xb' ,'inv_jhxbk_gsbk' ,'inv_sdtoxd_gsbank' ,'inv_sdtoxd_shyh' ,'inv_sdtoxd_xian' ,'inv_shyh_1hd' ,'inv_shyh_btfq' ,'inv_shyh_btzf' ,'inv_shyh_cxsx' ,'inv_shyh_qletc' ,'inv_shyh_qx' ,'inv_shyh_smf' ,'inv_shyh_yhqsm' ,'inv_shyh_ylzds' ,'inv_shyh_zc' ,'inv_shyhxgs_qqg' ,'inv_shyh_dyf' ,'inv_qqg_gsbk' ,'inv_zxxsk_gsbk' ,'30017842' ,'30025355' ,'30026632'
                )
                AND UNION_TYPE='4'
        )B
            ON 1=1
;
);

$SQL_BUFF[9]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d9;

use dmf_alm;
insert overwrite table dmf_alm.dmfalm_alm_rpt_01_roll_bdgt_s_d partition(dt='$TX_DATE')
select etl_dt
      ,report_key       -- '报表关键字'
      ,date_id          -- '日期'
      ,operate_org_cd   -- '部门'
      ,currency_cd      -- '币种'
      ,item_key         -- '报表项关键字'
      ,sum(item_valn)   -- '数值型值'
      ,item_type        -- '标识1,流入,2,流出'
from  dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d
where dt='$TX_DATE'
group by etl_dt
      ,report_key
      ,date_id
      ,operate_org_cd
      ,currency_cd
      ,item_key
      ,item_type
;

);

$SQL_BUFF[10]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d10;

use dmf_tmp;
drop table dmf_tmp.tmp_dmfalm_alm_rpt_01_roll_bdgt_s_d
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