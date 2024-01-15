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