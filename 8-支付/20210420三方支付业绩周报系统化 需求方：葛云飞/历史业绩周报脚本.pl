#!/usr/bin/perl
########################################################################################################################
#  Creater        :
#  Creation Time  :
#  Description    :
#  Modify By      :
#  Modify Time    :
#  Modify Content :
#  Script Version :1.0.3
########################################################################################################################
use strict;
use jrjtcommon;
use Common::Hive;
use zjcommon;

##############################################
#默认STINGER运行，失败后HIVE运行，可更改Runner和Retry_Runner
#修改最终生成表库名和表名
##############################################

my $Runner = "STINGER";
my $Retry_Runner = "HIVE";
my $DB = "";
my $TABLE = "";
#########################################################################################
#############################日期变量区##################################################
#########################################################################################

if ( $#ARGV < 0 ) { exit(1); }
my $CONTROL_FILE = $ARGV[0];

###########################################
#运行参数
my $RUN_PARAM = $ARGV[1];
my $JOB = substr(${CONTROL_FILE}, 4, length(${CONTROL_FILE})-17);#作业名称

#数据日期，格式 yyyy-mm-dd
my $TX_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-12, 4).'-'.substr(${CONTROL_FILE},length(${CONTROL_FILE})-8, 2).'-'.substr(${CONTROL_FILE},length(${CONTROL_FILE})-6, 2);
#数据日期，格式 yyyymmdd
my $TXDATE = substr($TX_DATE, 0, 4).substr($TX_DATE, 5, 2).substr($TX_DATE, 8, 2);
my $TX_MONTH = substr($TX_DATE, 0, 4).'-'.substr($TX_DATE, 5, 2);                                          #数据日期所在月，格式： yyyy-mm
my $TXMONTH = substr($TX_DATE, 0, 4).substr($TX_DATE, 5, 2);                                               #数据日期所在月，格式： yyyymm
my $TX_PREV_DATE = getPreviousDate($TX_DATE);                                                               #前一天，格式：yyyy-mm-dd
my $TX_NEXT_DATE = getNextDate($TX_DATE);                                                                   #下一天，格式： yyyy-mm-dd
my $TXPDATE = substr(${TX_PREV_DATE},0,4).substr(${TX_PREV_DATE},5,2).substr(${TX_PREV_DATE},8,2);        #前一天，格式： yyyymmdd
my $TXNDATE = substr(${TX_NEXT_DATE},0,4).substr(${TX_NEXT_DATE},5,2).substr(${TX_NEXT_DATE},8,2);        #下一天，格式： yyyymmdd
my $CURRENT_TIME = getNowTime();
my $TX_YEAR = substr($TX_DATE, 0, 4);#当年 yyyy
my $TX_1B_DATE = addDay($TX_DATE, -1);

########################################################################################################################
# Write SQL For Your APP
sub getsql
{
    my @SQL_BUFF=();
    #########################################################################################
    ####################################以下为SQL编辑区######################################
    #sql编写区，多个sql按 $SQL_BUFF[0],$SQL_BUFF[1],$SQL_BUFF[2]...新建，按数字下标顺序运行,需要几个sql就使用几个$SQL_BUFF[x]变量
    #在sql中可以使用大量的日期变量，例如：
    #select * from app.a_01_pop_info_basic  where dt='$TX_DATE'
    #日期变量的所有日期都是根据当前任务数据日期计算而来，如果当前的数据日期为：2017-10-09
    #则上述语句程序解析后，实际运行语句为： select * from app.a_01_pop_info_basic where dt='2017-10-09'
    #日期变量列表见日期变量区
    #########################################################################################
    #示例sql:使用了数据日期内置变量：$TX_DATE
 $SQL_BUFF[1]=qq(
  set mapred.job.name=dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_01;
  use dmf_tmp;
  ---------------三大基础数据
  drop table dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_01;
  create table dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_01
  as 
  ------------交易额
  select 
   '$TX_1B_DATE'                                                                                                                                             as dim_day
  ,case when mer_big_type in ('外部收单-线下','外部发卡-银联','跨境业务','行业解决方案','利息收入','支付集团业务') then mer_big_type
        when mer_big_type in ('外部发卡-封闭卡','生活应用','智能出行','其他业务')                                  then '其他业务'
     else mer_big_type      end                                                                                                                              as fir_lvl_name 
  ,case when mer_big_type in ('外部收单-线下','外部发卡-银联','跨境业务','行业解决方案')             then mer_sma_type
        when mer_big_type='支付集团业务' and mer_sma_type in ('线上C端','线上C端-全球购')  then '支付集团业务-C端'
        when mer_big_type='支付集团业务' and mer_sma_type='线上B端'                             then '支付集团业务-B端'
        when mer_big_type='支付集团业务' and mer_sma_type='线下-商城聚合'                            then '支付集团业务-聚合'
        when mer_big_type in ('外部发卡-封闭卡','生活应用','智能出行','其他业务','利息收入')         then mer_big_type
        when mer_big_type='外部收单-线上' and mer_sma_type in('线上-外部C端','线上C端')              then '线上-外部C端'
     else mer_sma_type      end                                                                                                                             as sec_lvl_name      
  ,'交易额'                                                                                                                                                 as index_name
  ,sum(case when dim_day_id='$TX_1B_DATE' then nvl(order_amt,0) end )                                                                                       as index_value  
  ,sum(case when substr(dim_day_id,1,7)=substr('$TX_1B_DATE',1,7)   and dim_day_id<='$TX_1B_DATE' then nvl(order_amt,0) end)             
                                                                                                                                                            as mtd
  ,sum(nvl(order_amt,0) )                                                                                                                                   as ytd
  from dmf_bc.dmfbc_oar_bc_pay_sis_fin_rpt_i_d pp
  where substr(dim_day_id,1,4)=substr('$TX_1B_DATE',1,4) 
  and dim_day_id<='$TX_1B_DATE'
  and dept_nm='支付业务部' 
  and mer_big_type <>'TO C-用户运营'
  group by                                                                                                                            
  case when mer_big_type in ('外部收单-线下','外部发卡-银联','跨境业务','行业解决方案','利息收入','支付集团业务') then mer_big_type
        when mer_big_type in ('外部发卡-封闭卡','生活应用','智能出行','其他业务') then '其他业务'
     else mer_big_type      end                                                                                                                                   
  ,case when mer_big_type in ('外部收单-线下','外部发卡-银联','跨境业务','行业解决方案') then mer_sma_type
        when mer_big_type='支付集团业务' and mer_sma_type in ('线上C端','线上C端-全球购')  then '支付集团业务-C端'
        when mer_big_type='支付集团业务' and mer_sma_type='线上B端'                             then '支付集团业务-B端'
        when mer_big_type='支付集团业务' and mer_sma_type='线下-商城聚合'                            then '支付集团业务-聚合'
        when mer_big_type in ('外部发卡-封闭卡','生活应用','智能出行','其他业务','利息收入')         then mer_big_type
        when mer_big_type='外部收单-线上' and mer_sma_type in('线上-外部C端','线上C端')              then '线上-外部C端'
     else mer_sma_type      end  
;     
  -----------收入
  insert into table dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_01
  select 
   '$TX_1B_DATE'                                                                                              as dim_day
   ,case when mer_big_type in ('外部收单-线下','外部发卡-银联','跨境业务','行业解决方案','利息收入','支付集团业务') then mer_big_type
        when mer_big_type in ('外部发卡-封闭卡','生活应用','智能出行','其他业务') then '其他业务'
     else mer_big_type      end                                                                                                                                   as fir_lvl_name 
  ,case when mer_big_type in ('外部收单-线下','外部发卡-银联','跨境业务','行业解决方案') then mer_sma_type
        when mer_big_type='支付集团业务' and mer_sma_type in ('线上C端','线上C端-全球购')  then '支付集团业务-C端'
        when mer_big_type='支付集团业务' and mer_sma_type='线上B端'                             then '支付集团业务-B端'
        when mer_big_type='支付集团业务' and mer_sma_type='线下-商城聚合'                            then '支付集团业务-聚合'
        when mer_big_type in ('外部发卡-封闭卡','生活应用','智能出行','其他业务','利息收入')         then mer_big_type
        when mer_big_type='外部收单-线上' and mer_sma_type in('线上-外部C端','线上C端')              then '线上-外部C端'
     else mer_sma_type      end                                                                                                                                   as sec_lvl_name  
  ,'收入'                                                                                                  as index_name
  ,sum(case when dim_day_id='$TX_1B_DATE' then nvl(mer_fee,0) end )                                        as index_value  
  ,sum(case when substr(dim_day_id,1,7)=substr('$TX_1B_DATE',1,7)  and dim_day_id<='$TX_1B_DATE' then nvl(mer_fee,0) end)      as mtd
  ,sum(nvl(mer_fee,0))                                                                                     as ytd
  from dmf_bc.dmfbc_oar_bc_pay_sis_fin_rpt_i_d  pp
  where substr(dim_day_id,1,4)=substr('$TX_1B_DATE',1,4)
  and dim_day_id<='$TX_1B_DATE'
  and dept_nm='支付业务部'  
  and mer_big_type <>'TO C-用户运营'
  group by 
   case when mer_big_type in ('外部收单-线下','外部发卡-银联','跨境业务','行业解决方案','利息收入','支付集团业务') then mer_big_type
        when mer_big_type in ('外部发卡-封闭卡','生活应用','智能出行','其他业务') then '其他业务'
     else mer_big_type      end                                                                                                                                   
  ,case when mer_big_type in ('外部收单-线下','外部发卡-银联','跨境业务','行业解决方案') then mer_sma_type
        when mer_big_type='支付集团业务' and mer_sma_type in ('线上C端','线上C端-全球购')  then '支付集团业务-C端'
        when mer_big_type='支付集团业务' and mer_sma_type='线上B端'                             then '支付集团业务-B端'
        when mer_big_type='支付集团业务' and mer_sma_type='线下-商城聚合'                            then '支付集团业务-聚合'
        when mer_big_type in ('外部发卡-封闭卡','生活应用','智能出行','其他业务','利息收入')         then mer_big_type
        when mer_big_type='外部收单-线上' and mer_sma_type in('线上-外部C端','线上C端')              then '线上-外部C端'
     else mer_sma_type      end  
  ;
  
  ------------交易笔数
 insert into table dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_01
  select 
   '$TX_1B_DATE'                                                                                                                                             as dim_day
   ,case when mer_big_type in ('外部收单-线下','外部发卡-银联','跨境业务','行业解决方案','利息收入','支付集团业务') then mer_big_type
        when mer_big_type in ('外部发卡-封闭卡','生活应用','智能出行','其他业务') then '其他业务'
     else mer_big_type      end                                                                                                                                   as fir_lvl_name 
  ,case when mer_big_type in ('外部收单-线下','外部发卡-银联','跨境业务','行业解决方案') then mer_sma_type
        when mer_big_type='支付集团业务' and mer_sma_type in ('线上C端','线上C端-全球购')  then '支付集团业务-C端'
        when mer_big_type='支付集团业务' and mer_sma_type='线上B端'                             then '支付集团业务-B端'
        when mer_big_type='支付集团业务' and mer_sma_type='线下-商城聚合'                            then '支付集团业务-聚合'
        when mer_big_type in ('外部发卡-封闭卡','生活应用','智能出行','其他业务','利息收入')         then mer_big_type
        when mer_big_type='外部收单-线上' and mer_sma_type in('线上-外部C端','线上C端')              then '线上-外部C端'
     else mer_sma_type      end                                                                                                                                   as sec_lvl_name        
  ,'交易笔数'                                                                                                                                               as index_name
  ,sum(case when dim_day_id='$TX_1B_DATE' then nvl(trade_num,0) end )                                    as index_value  
  ,sum(case when substr(dim_day_id,1,7)=substr('$TX_1B_DATE',1,7)   and dim_day_id<='$TX_1B_DATE' then nvl(trade_num,0) end)             
                                                                                                                                                            as mtd
  ,sum(nvl(trade_num,0) )                                                                                as ytd
  from dmf_bc.dmfbc_oar_bc_pay_sis_fin_rpt_i_d pp
  where substr(dim_day_id,1,4)=substr('$TX_1B_DATE',1,4) 
  and dim_day_id<='$TX_1B_DATE'
  and dept_nm='支付业务部'  
  and mer_big_type <>'TO C-用户运营'
  group by 
   case when mer_big_type in ('外部收单-线下','外部发卡-银联','跨境业务','行业解决方案','利息收入','支付集团业务') then mer_big_type
        when mer_big_type in ('外部发卡-封闭卡','生活应用','智能出行','其他业务') then '其他业务'
     else mer_big_type      end                                                                                                                                  
  ,case when mer_big_type in ('外部收单-线下','外部发卡-银联','跨境业务','行业解决方案') then mer_sma_type
        when mer_big_type='支付集团业务' and mer_sma_type in ('线上C端','线上C端-全球购')  then '支付集团业务-C端'
        when mer_big_type='支付集团业务' and mer_sma_type='线上B端'                             then '支付集团业务-B端'
        when mer_big_type='支付集团业务' and mer_sma_type='线下-商城聚合'                            then '支付集团业务-聚合'
        when mer_big_type in ('外部发卡-封闭卡','生活应用','智能出行','其他业务','利息收入')         then mer_big_type
        when mer_big_type='外部收单-线上' and mer_sma_type in('线上-外部C端','线上C端')              then '线上-外部C端'
     else mer_sma_type      end  
;     
 
  -----------成本（不含税）
  insert into table dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_01
  select 
   '$TX_1B_DATE'                                                                                              as dim_day
    ,case when mer_big_type in ('外部收单-线下','外部发卡-银联','跨境业务','行业解决方案','利息收入','支付集团业务') then mer_big_type
        when mer_big_type in ('外部发卡-封闭卡','生活应用','智能出行','其他业务') then '其他业务'
     else mer_big_type      end                                                                                                                                   as fir_lvl_name 
  ,case when mer_big_type in ('外部收单-线下','外部发卡-银联','跨境业务','行业解决方案') then mer_sma_type
        when mer_big_type='支付集团业务' and mer_sma_type in ('线上C端','线上C端-全球购')  then '支付集团业务-C端'
        when mer_big_type='支付集团业务' and mer_sma_type='线上B端'                             then '支付集团业务-B端'
        when mer_big_type='支付集团业务' and mer_sma_type='线下-商城聚合'                            then '支付集团业务-聚合'
        when mer_big_type in ('外部发卡-封闭卡','生活应用','智能出行','其他业务','利息收入')         then mer_big_type
        when mer_big_type='外部收单-线上' and mer_sma_type in('线上-外部C端','线上C端')              then '线上-外部C端'
     else mer_sma_type      end                                                                                                                                   as sec_lvl_name  
  ,'通道成本'                                                                                              as index_name
  ,sum(case when dim_day_id='$TX_1B_DATE' then nvl(bank_fee,0) end )/1.06                                  as index_value  
  ,sum(case when substr(dim_day_id,1,7)=substr('$TX_1B_DATE',1,7) and dim_day_id<='$TX_1B_DATE' then nvl(bank_fee,0) end )/1.06          as mtd
  ,sum(nvl(bank_fee,0))/1.06                                                                               as ytd
  from dmf_bc.dmfbc_oar_bc_pay_sis_fin_rpt_i_d pp
  where substr(dim_day_id,1,4)=substr('$TX_1B_DATE',1,4)
  and dim_day_id<='$TX_1B_DATE'
  and dept_nm='支付业务部' 
  and mer_big_type <>'TO C-用户运营' 
  group by 
      case when mer_big_type in ('外部收单-线下','外部发卡-银联','跨境业务','行业解决方案','利息收入','支付集团业务') then mer_big_type
        when mer_big_type in ('外部发卡-封闭卡','生活应用','智能出行','其他业务') then '其他业务'
     else mer_big_type      end                                                                                                                                    
  ,case when mer_big_type in ('外部收单-线下','外部发卡-银联','跨境业务','行业解决方案') then mer_sma_type
        when mer_big_type='支付集团业务' and mer_sma_type in ('线上C端','线上C端-全球购')  then '支付集团业务-C端'
        when mer_big_type='支付集团业务' and mer_sma_type='线上B端'                             then '支付集团业务-B端'
        when mer_big_type='支付集团业务' and mer_sma_type='线下-商城聚合'                            then '支付集团业务-聚合'
        when mer_big_type in ('外部发卡-封闭卡','生活应用','智能出行','其他业务','利息收入')         then mer_big_type
        when mer_big_type='外部收单-线上' and mer_sma_type in('线上-外部C端','线上C端')              then '线上-外部C端'
     else mer_sma_type      end  
;     
   )
 ;
  $SQL_BUFF[2]=qq(
  set mapred.job.name=dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_02;
  use dmf_tmp;
  drop table dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_02;
  create table dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_02
  as 
  -----插入交易额、收入、成本(不含税)、交易笔数
  select 
    dim_day           as dim_day        --日期
   ,fir_lvl_name      as fir_lvl_name   --一级业务名称
   ,sec_lvl_name      as sec_lvl_name   --二级业务线名称
   ,index_name        as index_name     --指标名
   ,index_value       as index_value    --指标值
   ,mtd               as mtd            --本月累计值
   ,ytd               as ytd            --年累计值 
  from 
   dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_01
  ;
  --不含税收入
  insert into table dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_02
  select 
    dim_day                                                                        as dim_day         --日期
   ,fir_lvl_name                                                                   as fir_lvl_name   --一级业务名称
   ,sec_lvl_name                                                                   as sec_lvl_name   --二级业务名称
   ,'不含税收入'                                                                   as index_name      --指标名
   ,index_value/1.06                                                               as index_value     --指标值
   ,mtd/1.06                                                                       as mtd             --本月累计值
   ,ytd/1.06                                                                       as ytd             --年累计值 
  from dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_01
  where index_name='收入'
  ;
  --营业税金及附加
  insert into table dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_02
  select 
    dim_day                                                                                                     as dim_day         --日期
   ,fir_lvl_name                                                                   as fir_lvl_name   --一级业务名称
   ,sec_lvl_name                                                                   as sec_lvl_name   --二级业务名称
   ,'营业税金及附加'                                                                                            as index_name      --指标名
   ,index_value/1.06*nvl(t2.business_taxes_surcharges,0)                                                        as index_value     --指标值
   ,mtd/1.06*nvl(t2.business_taxes_surcharges,0)                                                                as mtd             --本月累计值
   ,ytd/1.06*nvl(t2.business_taxes_surcharges,0)                                                                as ytd             --年累计值 
  from (select * from dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_01 where index_name='收入')t1
  left join (select month_id,max(index_value) as business_taxes_surcharges
               from dmf_add.dmfadd_oar_add_pay_fin_int_line_i_m where trim(depart_code)='9310000' 
              and trim(index_desc)='营业税金及附加比率'
              group by month_id
              ) t2 
   on substr(t1.dim_day,1,7)=t2.month_id
              
  ;
)
;
  $SQL_BUFF[3]=qq(
  set mapred.job.name=dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_03;
  use dmf_tmp;
  drop table dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_03;
  create table dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_03
  as 
  -----插入交易额、收入、成本(不含税)、不含税收入、营业税金及附加
  select 
    dim_day           as dim_day        --日期
   ,fir_lvl_name      as fir_lvl_name   --一级业务名称
   ,sec_lvl_name      as sec_lvl_name   --二级业务名称
   ,index_name        as index_name     --指标名
   ,index_value       as index_value    --指标值
   ,mtd               as mtd            --本月累计值
   ,ytd               as ytd            --年累计值 
  from 
   dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_02
  ;
  --毛利
  insert into table dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_03
  select 
    dim_day                                                                        as dim_day         --日期
   ,fir_lvl_name                                                                   as fir_lvl_name   --一级业务名称
   ,sec_lvl_name                                                                   as sec_lvl_name   --二级业务名称
   ,'毛利'                                                                         as index_name      --指标名
   ,sum(case when index_name='不含税收入' then index_value else 0 end
    -
    case when index_name='通道成本' then index_value else 0 end 
    -
    case when index_name='营业税金及附加' then index_value else 0 end)              as index_value     --指标值
   ,sum(case when index_name='不含税收入' then mtd else 0 end
    -
    case when index_name='通道成本' then mtd else 0 end 
    -
    case when index_name='营业税金及附加' then mtd else 0 end)                      as mtd     --本月累计值  
   ,sum(case when index_name='不含税收入' then ytd else 0 end
    -
    case when index_name='通道成本' then ytd else 0 end 
    -
    case when index_name='营业税金及附加' then ytd else 0 end)                      as ytd     --年累计值 
  from dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_02
  where index_name in ('不含税收入','通道成本','营业税金及附加')
  group by dim_day,fir_lvl_name,sec_lvl_name
  ;

)
;

  $SQL_BUFF[4]=qq(
  set mapred.job.name=dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_04;
  use dmf_tmp;
  drop table dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_04;
  create table dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_04
  as 
  select 
    dim_day                                                                                                 as dim_day
   ,fir_lvl_name                                                                   as fir_lvl_name   --一级业务名称
   ,sec_lvl_name                                                                   as sec_lvl_name   --二级业务名称
   ,case 
       when sec_lvl_name='支付集团业务-C端'  then '001'
       when sec_lvl_name='支付集团业务-B端'  then '002'
       when sec_lvl_name='支付集团业务-聚合' then '003'
       when sec_lvl_name='线上-外部C端'      then '004'
       when sec_lvl_name='线上-通道业务'     then '005'
       when sec_lvl_name='牌照收单'          then '006'
       when sec_lvl_name='外部发卡-银联'     then '007'
       when sec_lvl_name='外部收单-跨境B'    then '008'
       when sec_lvl_name='外部收单-跨境C'    then '009'
       when sec_lvl_name='聚合支付'          then '010'
       when sec_lvl_name='利息收入'          then '011'
       when sec_lvl_name='利息收入'          then '012'
       when sec_lvl_name='外部发卡-封闭卡'   then '013'
       when sec_lvl_name='生活应用'          then '014'
       when sec_lvl_name='智能出行'          then '015'
       when sec_lvl_name='其他业务'          then '016'
    end as  order_sec_lvl  
 ,case 
     when index_name='交易额'                   then '006'
     when index_name='收入'                     then '007'
     when index_name='不含税收入'               then '008'
     when index_name='通道成本'                 then '009'
     when index_name='营业税金及附加'           then '010'
     when index_name='毛利'                     then '011'
     when index_name='交易笔数'                 then '013'
     end                                                                                                    as order_index_name
   ,index_name                 as index_name          --指标名
   ,sum(nvl(index_value,0))    as index_value         --指标值
   ,sum(nvl(mtd,0))            as mtd                 --本月累计值
   ,sum(nvl(ytd,0))            as ytd                 --年累计值
  from dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_03
  group by 
     dim_day                                                                                                 
 ,fir_lvl_name                                                                   
 ,sec_lvl_name                                                                     
 ,case 
       when sec_lvl_name='支付集团业务-C端'  then '001'
       when sec_lvl_name='支付集团业务-B端'  then '002'
       when sec_lvl_name='支付集团业务-聚合' then '003'
       when sec_lvl_name='线上-外部C端'      then '004'
       when sec_lvl_name='线上-通道业务'     then '005'
       when sec_lvl_name='牌照收单'          then '006'
       when sec_lvl_name='外部发卡-银联'     then '007'
       when sec_lvl_name='外部收单-跨境B'    then '008'
       when sec_lvl_name='外部收单-跨境C'    then '009'
       when sec_lvl_name='聚合支付'          then '010'
       when sec_lvl_name='利息收入'          then '011'
       when sec_lvl_name='利息收入'          then '012'
       when sec_lvl_name='外部发卡-封闭卡'   then '013'
       when sec_lvl_name='生活应用'          then '014'
       when sec_lvl_name='智能出行'          then '015'
       when sec_lvl_name='其他业务'          then '016'
    end  
 ,case 
     when index_name='交易额'                   then '006'
     when index_name='收入'                     then '007'
     when index_name='不含税收入'               then '008'
     when index_name='通道成本'                 then '009'
     when index_name='营业税金及附加'           then '010'
     when index_name='毛利'                     then '011'
     when index_name='交易笔数'                 then '013'
     end                                                                                                    
   ,index_name                  
  ;

)
;

  $SQL_BUFF[5]=qq(
 set mapred.job.name=dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_05;
  use dmf_tmp;
  drop table dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_05;
  create table dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_05
  as 
   select
    dim_day                    as dim_day             --日期
   ,fir_lvl_name                                                                   as fir_lvl_name   --一级业务名称
   ,sec_lvl_name                                                                   as sec_lvl_name   --二级业务名称
   ,order_sec_lvl                                                                  as order_sec_lvl
   ,order_index_name           as order_index_name    --指标排序
   ,index_name                 as index_name          --指标名
   ,index_value                as index_value         --指标值
   ,mtd                        as mtd                 --本月累计值
   ,ytd                        as ytd                 --年累计值
from dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_04 
union all
select
    dim_day                    as dim_day             --日期
   ,'部门合计'                 as fir_lvl_name        --一级业务名称
   ,'部门合计'                 as sec_lvl_name        --二级业务名称
   ,'100'                      as order_sec_lvl
   ,order_index_name           as order_index_name    --指标排序
   ,index_name                 as index_name          --指标名
   ,sum(index_value)           as index_value         --指标值
   ,sum(mtd)                   as mtd                 --本月累计值
   ,sum(ytd)                   as ytd                 --年累计值
from dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_04 
group by 
 dim_day
,order_index_name
,index_name

;
)
; 

  $SQL_BUFF[6]=qq(
 set mapred.job.name=dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_06;
 use dmf_bi;
 insert overwrite table dmf_bi.dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d partition (dt) 
   select
    default.sysdate()                  as etl_dt              --etl日期
   ,dim_day                    as dim_day             --日期
   ,'9310000'                  as depart_code         --部门编码
   ,'支付业务部'               as depart_name         --部门名称
   ,cast(null as string)       as fir_lvl_code
   ,cast(null as string)       as order_fir_lvl
   ,fir_lvl_name                                                                   as fir_lvl_name   --一级业务名称
   ,cast(null as string)       as sec_lvl_code
   ,order_sec_lvl                                                                  as order_sec_lvl
   ,sec_lvl_name                                                                   as sec_lvl_name   --二级业务名称
   ,order_index_name           as order_index_name    --指标排序
   ,index_name                 as index_name          --指标名
   ,index_value                as index_value         --指标值
   ,mtd                        as mtd                 --本月累计值
   ,ytd                        as ytd                 --年累计值
   ,dim_day                    as dt
from dmf_tmp.dmf_tmp_dmfbi_oar_bi_pay_sis_fin_report_dtl_i_d_05 
;
)
;  
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

