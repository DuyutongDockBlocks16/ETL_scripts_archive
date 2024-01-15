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
my $TX_10B_DATE = addDay($TX_DATE, -90);
my $TX_40B_DATE = addDay($TX_DATE, -100);

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
set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_01;
use dmf_dev;
----------------------------------------20190124确认交易量用的订单总金额，白条分收入用的是支付金额
------------------------------------------------------------------------数据整理，首先，交易量剔除,订单笔数调整(订单笔数调整参照交易量（上述1-3），与交易量同步变动。)
----------------------------------------①分类为“收单”，交易名称为“银行卡企业充值”，部门“支付业务部”。
----------------------------------------②分类为“收单”，交易名称为“银行卡个人充值”，商户号为“22843769”的部分。
----------收单业务
 drop table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_01 ;
 create table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_01
 as       
 select 
    acc_date                                                                                as dim_day_id 
   ,mer_id          
   ,trade_type      
   ,trade_name      
   ,product_id      
   ,pay_tools
   ,mer_date        
   ,acc_date        
   ,case when trade_name='银行卡企业充值' then 0                                                              --分类为“收单”，交易名称为“银行卡企业充值”，部门“支付业务部”
         when trade_name='银行卡个人充值' and mer_id='22843769' then 0                                        --分类为“收单”，交易名称为“银行卡个人充值”，商户号为“22843769”的部分
         else nvl(order_amt,0)/100  end                                                      as order_amt   
   ,nvl(trade_amt,0)/100                                                                     as trade_amt         
   ,nvl(mer_fee,0)/100                                                                       as mer_fee       
   ,nvl(bank_fee,0)/100                                                                      as bank_fee        
   ,case when trade_name='银行卡企业充值' then 0                                                              --分类为“收单”，交易名称为“银行卡企业充值”，部门“支付业务部”
         when trade_name='银行卡个人充值' and mer_id='22843769' then 0                                        --分类为“收单”，交易名称为“银行卡个人充值”，商户号为“22843769”的部分
         else nvl(trade_num,0)  end                                                          as trade_num 
   ,flag            
   ,'收单'                                                                                   as bus_flag
   ,dept_nm         
   ,mer_big_type    
   ,mer_sma_type    
   ,mer_ownr        
   ,prod_type       
   ,sys_src
   ,card_type   
 from dmf_dev.dmfbc_oar_bc_pay_fin_stl_i_d 
 where dt>='$TX_40B_DATE'
  and acc_date >= '$TX_10B_DATE' 
  and acc_date<='$TX_PREV_DATE'
  and flag in ('zd', 'td') 
 ;
-----------------------③分类为“代付”，交易名称为“结算后出款”，计提银行成本为“0”的部分。
 ----------------------代付业务
insert into table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_01
select 
    acc_date                                                                                as dim_day_id 
   ,mer_id         
   ,trade_type      
   ,trade_name      
   ,product_id      
   ,'代付'                                                                                           as pay_tools       
   ,mer_date        
   ,acc_date        
   ,case when trade_name='结算后出款' and nvl(bank_fee,0) =0 then 0 else nvl(order_amt,0)/100 end   as order_amt   --分类为“代付”，交易名称为“结算后出款”，计提银行成本为“0”的部分
   ,nvl(trade_amt,0)/100                                                                            as trade_amt   --分类为“代付”，交易名称为“结算后出款”，计提银行成本为“0”的部分     
   ,nvl(mer_fee,0)/100                                                                              as mer_fee       
   ,nvl(bank_fee,0)/100                                                                             as bank_fee         
   ,case when trade_name='结算后出款' and nvl(bank_fee,0) =0 then 0 else nvl(trade_num,0) end   as trade_num   --分类为“代付”，交易名称为“结算后出款”，计提银行成本为“0”的部分        
   ,flag                                                                                            as flag
   ,'代付'                                                                                          as bus_flag
   ,dept_nm         
   ,mer_big_type    
   ,mer_sma_type    
   ,mer_ownr        
   ,prod_type       
   ,sys_src
   ,card_type   
 from dmf_dev.dmfbc_oar_bc_pay_fin_stl_i_d 
 where dt>='$TX_40B_DATE'
  and acc_date >= '$TX_10B_DATE' 
  and acc_date<='$TX_PREV_DATE'
  and flag in ('df') 
  ;
 ------------------------------------------④分类为“钱包”，支付工具为“账户余额”，商户手续费为“0”的部分。
 ----------------------钱包业务
  ----------------------钱包业务  and mer_id<>'110583859001'  --20190218 yjb add
insert into table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_01
select 
    mer_date                                                                                    as dim_day_id 
   ,mer_id  
   ,trade_type      
   ,trade_name      
   ,product_id      
   ,pay_tools       
   ,mer_date        
   ,acc_date        
   ,case when pay_tools='钱包余额' and nvl(mer_fee,0)=0 and mer_id<>'110583859001' then 0 else  nvl(order_amt,0)/100 end   as order_amt    --分类为“钱包”，支付工具为“账户余额”，商户手续费为“0”的部分
   ,nvl(trade_amt,0)/100                                                                         as trade_amt    --分类为“钱包”，支付工具为“账户余额”，商户手续费为“0”的部分     
   ,nvl(mer_fee,0)/100                                                                           as mer_fee       
   ,nvl(bank_fee,0)/100                                                                          as bank_fee          
   ,case when pay_tools='钱包余额' and nvl(mer_fee,0)=0 and mer_id<>'110583859001'  then 0 else  nvl(trade_num,0) end       as trade_num    --分类为“钱包”，支付工具为“账户余额”，商户手续费为“0”的部分     
   ,flag                                                                                         as flag
   ,'钱包'                                                                                       as bus_flag
   ,dept_nm         
   ,mer_big_type    
   ,mer_sma_type    
   ,mer_ownr        
   ,prod_type       
   ,sys_src
   ,card_type   
 from dmf_dev.dmfbc_oar_bc_pay_fin_stl_i_d 
 where dt>='$TX_40B_DATE'
  and mer_date >= '$TX_10B_DATE' 
  and mer_date<='$TX_PREV_DATE'
  and flag in ('qb') 
  ;
------------------------全款结算
insert into table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_01
select 
    mer_date                                                                                    as dim_day_id 
   ,mer_id   
   ,trade_type      
   ,trade_name      
   ,product_id      
   ,pay_tools       
   ,mer_date        
   ,acc_date        
   ,case when flag='qk' then  nvl(order_amt,0)/100  else order_amt end                           as order_amt   
   ,case when flag='qk' then  nvl(trade_amt,0)/100  else trade_amt end                           as trade_amt         
   ,case when flag='qk' then  nvl(mer_fee,0)/100   else  nvl(mer_fee,0) end                      as mer_fee       
   ,case when flag='qk' then  nvl(bank_fee,0)/100  else  nvl(bank_fee,0) end                     as bank_fee        
   ,nvl(trade_num,0)                                                                             as trade_num    
   ,flag                                                                                         as flag
   ,case when flag='qk' then '全款结算' else '业务系统'               end                        as bus_flag
   ,dept_nm         
   ,mer_big_type    
   ,mer_sma_type    
   ,mer_ownr        
   ,prod_type       
   ,sys_src
   ,card_type   
 from dmf_dev.dmfbc_oar_bc_pay_fin_stl_i_d 
 where dt>='$TX_40B_DATE'
  and acc_date >= '$TX_10B_DATE' 
  and acc_date<='$TX_PREV_DATE'
  and flag in ('qk') 
  ; 
) 
;
      
$SQL_BUFF[2]=qq(
set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_02;
use dmf_dev;
---------------------------------------------------------⑤支付工具为“优惠券”，商户手续费为“0”的部分。
---------------------------------------------------------⑥交易名称为“B2B提现”，部门“支付业务部”、业务所属“体系外”。
---------------------------------------------------------⑦支付工具为“京东白条”， 部门“支付业务部”、业务大类“跨境业务”之外。
--⑦支付工具为“京东白条”， （部门“支付业务部”、业务大类“跨境业务”、）
--Or  （支付集团业务，线上C端-全球购）之外。这个跟白条是同一个，对吗？

----------全部业务
 drop table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_02 ;
 create table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_02
 as  
select 
    dim_day_id
   ,mer_id          
   ,trade_type      
   ,trade_name      
   ,product_id      
   ,case when pay_tools='优惠券' then '快捷' else pay_tools         end as pay_tools      
   ,mer_date        
   ,acc_date    
   ,case when pay_tools='优惠券'  then 0                                                             --支付工具为“优惠券”，商户手续费为“0”的部分
         when trade_name='B2B提现' and dept_nm='支付业务部' and mer_ownr='体系外'       then 0                      --交易名称为“B2B提现”，部门“支付业务部”、业务所属“体系外”
         else order_amt                    
          end                                                   as order_amt
   ,trade_amt                                                                                  as trade_amt         
   ,mer_fee       
   ,bank_fee    
   ,case when pay_tools='优惠券'  then 0                                                             --支付工具为“优惠券”，商户手续费为“0”的部分
         when trade_name='B2B提现' and dept_nm='支付业务部' and mer_ownr='体系外'       then 0                      --交易名称为“B2B提现”，部门“支付业务部”、业务所属“体系外”
         else trade_num                    
          end                                                                                 as trade_num      
   ,flag
   ,bus_flag
   ,dept_nm         
   ,mer_big_type    
   ,mer_sma_type    
   ,mer_ownr        
   ,prod_type       
   ,sys_src 
  ,card_type   
from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_01
where pay_tools<>'白条'
union all
select 
    dim_day_id
   ,mer_id          
   ,trade_type      
   ,trade_name      
   ,product_id      
   ,case when pay_tools='优惠券' then '快捷' else pay_tools         end as pay_tools      
   ,mer_date        
   ,acc_date    
   ,case when pay_tools='白条'     and dept_nm='支付业务部'  and 
          (mer_big_type='跨境业务' or (mer_big_type='支付集团业务' and mer_sma_type='线上C端-全球购')
                                   or (mer_big_type='支付商城业务' and mer_sma_type='线上-商城C端-全球购')) then order_amt                     --支付工具为“京东白条”， 部门“支付业务部”、业务大类“跨境业务”之外
         else 0  end                                                                   as order_amt
   ,trade_amt                                                                                  as trade_amt         
   ,mer_fee       
   ,bank_fee    
   ,case when pay_tools='白条'     and dept_nm='支付业务部'  and 
          (mer_big_type='跨境业务' or (mer_big_type='支付集团业务' and mer_sma_type='线上C端-全球购')
                                   or (mer_big_type='支付商城业务' and mer_sma_type='线上-商城C端-全球购')) then trade_num                     --支付工具为“京东白条”， 部门“支付业务部”、业务大类“跨境业务”之外
         else 0  end                                                                   as trade_num        
   ,flag
   ,bus_flag
   ,dept_nm         
   ,mer_big_type    
   ,mer_sma_type    
   ,mer_ownr        
   ,prod_type       
   ,sys_src
   ,card_type   
from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_01
where pay_tools='白条'
;
)
;

$SQL_BUFF[3]=qq(
set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_03;
use dmf_dev;
-----------------------------------------------------------------------交易量调整------------------------------------------
--2、 其次，交易量调整，方法如下：   都需要确认
--①分类是“代付”的订单金额*-1；分类为“钱包”，交易名称为“代付”的订单金额*-1 。
--②产品类型为“京东金融-小金库”支付工具为“代付”需要调减交易量，取数方法三账入账明细表，分类“钱包”，支付工具“小金库”交易量合计金额。
--③交易名称为“代付至银行卡-退票（新）（D104）”、“交易撤销（U100）”、“退款受理”（W100）的订单金额为负数，正退为“退单”，其余订单金额取绝对值，正退为“正单”。（可用来校验二-2-①） 经沟通这条废弃
 drop table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_03 ;
 create table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_03
 as  
select 
    dim_day_id
   ,mer_id           
   ,trade_type      
   ,trade_name      
   ,product_id      
   ,pay_tools       
   ,mer_date        
   ,acc_date    
   ,case when bus_flag='代付' then order_amt*(-1) 
         when bus_flag='钱包' and trade_type in ('T100', 'D104', 'D105', 'T103', 'T101', 'T104', 'T105') then order_amt*(-1)
         else order_amt                                                                                                       end  as order_amt
   ,trade_amt                                                                                                         as trade_amt
   ,mer_fee       
   ,bank_fee        
   ,case when bus_flag='代付' then trade_num 
         when bus_flag='钱包' and trade_type in ('T100', 'D104', 'D105', 'T103', 'T101', 'T104', 'T105') then trade_num
         else trade_num                                                                                                       end  as trade_num 
   ,flag
   ,bus_flag
   ,dept_nm         
   ,mer_big_type    
   ,mer_sma_type    
   ,mer_ownr        
   ,prod_type       
   ,sys_src
   ,card_type   
from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_02
union all 
select 
    dim_day_id
   ,mer_id           
   ,trade_type      
   ,trade_name      
   ,product_id      
   ,'代付'                    as pay_tools       
   ,mer_date        
   ,acc_date    
   ,order_amt*(-1)            as order_amt
   ,trade_amt*(-1)            as trade_amt                                                                                                    
   ,0 as mer_fee       
   ,0 as bank_fee        
   ,trade_num*(-1)            as trade_num
   ,flag
   ,bus_flag
   ,'财富管理部'      as dept_nm         
   ,'小金库'          as mer_big_type    
   ,'小金库'          as mer_sma_type    
   ,cast(null as string)              as mer_ownr        
   ,'京东金融-小金库' as prod_type       
   ,sys_src 
   ,card_type   
from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_02
where bus_flag='钱包'
and pay_tools='小金库'

;
)
;
$SQL_BUFF[4]=qq(
set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_04;
use dmf_dev;
drop table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_04 ;
create table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_04
as  
---------------------------------用a_wd_order_fin_new_s_d 里面的逻辑替换脚本依赖，缩短时间
select 
 days
,sum(case when in_out='商城' then ord_amount end)/sum(ord_amount) as sc_rate   --商城比例
,sum(case when in_out='外单' and on_off='线上' then ord_amount end)/sum(ord_amount) as on_rate   --线上比例
,sum(case when in_out='外单' and on_off='线下' then ord_amount end)/sum(ord_amount) as off_rate  --线下比例
from 
(
select 
uniorder.order_id,
uniorder.ord_amount,
uniorder.days,
unitype.on_off,
uniorder.in_out
from
    (
    select 
    to_date(created_date) as days,
    case when (merchant_second_no='104100548991020' or merchant_second_name like '%京东商城%') then '商城' else '外单' end as in_out,
    merchant_order_id as order_id,
    amount/100 as ord_amount
      ,merchant_second_no
    from odm.ODM_APP_JR_CTRADE_ORDER_0000_I_D     --订单表
    where dt>='$TX_40B_DATE'
    and to_date(created_date)<='$TX_PREV_DATE'
    and to_date(created_date)>='$TX_10B_DATE' 
    and order_status=2
    and trade_type in('JDQP','JDQP_ASYNC')
    and success_time is not null
    and merchant_no in('110298203006','110298203001','110298203002','110298203007')
    and merchant_second_name != '双十一闪付压测'
    and merchant_second_no!='815111654112471'
    ) as uniorder
    
left outer join
    (
    select merchantorderid as re_order_id
    from odm.ODM_PAY_WD_REFUNDACCEPT_0000_I_D   --银联退款表
    where dt>='2017-01-01'
    and createtime>='2017-01-01'
    and refstatus = 1
    group by merchantorderid
    ) as unireorder
on uniorder.order_id=unireorder.re_order_id

left outer join

    (
    select outtradeno,
    nvl(if(terminaltype in ('08','07'),'线上','线下'),'线下') as on_off
    from odm.ODM_CF_BAILIAN_TRADE_REQ_I_D    --白条闪付交易请求表
    where dt>='2017-01-01'
    group by outtradeno,nvl(if(terminaltype in ('08','07'),'线上','线下'),'线下')
    ) as unitype
on uniorder.order_id = unitype.outtradeno
      left outer join

    (
    select third_merchant_no 
    from odm.ODM_PAY_JHZF_PPMS_EFFECT_PRODUCT_I_D   --生效的产品表
    where third_merchant_no is not null
    group by third_merchant_no
    ) as jhzf
on uniorder.merchant_second_no=jhzf.third_merchant_no
 where unireorder.re_order_id is null
   and  jhzf.third_merchant_no is null) pp
   where pp.days between '$TX_10B_DATE' and '$TX_PREV_DATE'
group by days
;
)
;

$SQL_BUFF[5]=qq(
set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_05;
use dmf_dev;
-----------------------------------------------------------------------交易量业务线之间调整------------------------------------------
--①部门“支付业务部”、业务归属 “体系内”、业务大类 “支付集团业务”、业务小类“线上C端”中支付工具为“代扣”“B2B业务”的，
--调整为部门“支付业务部”、业务归属 “体系内”、业务大类 “支付集团业务”、业务小类“线上-商城B端”。
--②部门“支付业务部”、业务归属 “体系内”、业务大类 “支付集团业务”、业务小类为“线上C端”中产品类型为“资金归集”的（商户号110194601001），
--调整为部门“支付业务部”、业务归属 “体系内”、业务大类 “支付集团业务”、业务小类“线上-商城B端”。
--③部门“支付业务部”、业务归属 “体系外”、业务大类“外部发卡-银联”、业务小类“外部发卡-银联”、产品类型“京东支付-银联闪付”按照订单比例还原至部门“支付业务部”、
--业务归属 “体系内”、业务大类 “支付集团业务”、业务小类“线上C端”中。
 drop table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_05 ;
 create table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_05
 as  
select
    dim_day_id 
   ,mer_id          
   ,trade_type      
   ,trade_name      
   ,product_id      
   ,pay_tools       
   ,mer_date        
   ,acc_date    
   ,case when dept_nm ='支付业务部' and mer_ownr='体系外' and mer_big_type='外部发卡-银联'  and mer_sma_type='外部发卡-银联' and prod_type='京东支付-银联闪付'
         then order_amt*(1-sc_rate)
         when dept_nm ='支付业务部' and mer_big_type='生活应用' and pay_tools in ('代付','企业充值')
         then 0
         else order_amt                                                                                end    as order_amt
   ,trade_amt                                                                                                 as trade_amt   
   ,case when dept_nm ='支付业务部' and mer_ownr='体系外' and mer_big_type='外部发卡-银联'  and mer_sma_type='外部发卡-银联' and prod_type='京东支付-银联闪付'
         then mer_fee*(1-sc_rate)
         else mer_fee                                                                                end    as mer_fee       
   ,case when dept_nm ='支付业务部' and mer_ownr='体系外' and mer_big_type='外部发卡-银联'  and mer_sma_type='外部发卡-银联' and prod_type='京东支付-银联闪付'
         then bank_fee*(1-sc_rate)
         else bank_fee                                                                                end    as bank_fee               
   ,case when dept_nm ='支付业务部' and mer_ownr='体系外' and mer_big_type='外部发卡-银联'  and mer_sma_type='外部发卡-银联' and prod_type='京东支付-银联闪付'
         then trade_num*(1-sc_rate)
         else trade_num                                                                                end    as trade_num      
   ,flag
   ,bus_flag
   ,dept_nm         
   ,mer_big_type    
   ,case when ((dept_nm ='支付业务部' and mer_ownr='体系内' and mer_big_type='支付集团业务'  and mer_sma_type='线上C端' and pay_tools in ('代付','网关'))
              or (dept_nm ='支付业务部' and mer_ownr='体系内' and mer_big_type='支付商城业务'  and mer_sma_type='线上-商城C端' and pay_tools in ('代付','网关'))
              or mer_id='110194601001'  --商城资金归集 
              or (dept_nm ='支付业务部' and mer_ownr='体系内' and mer_big_type='支付集团业务'  and mer_sma_type in ('线上C端-全球购','线下-商城聚合') and pay_tools in ('代付'))
              or (dept_nm ='支付业务部' and mer_ownr='体系内' and mer_big_type='支付商城业务'  and mer_sma_type in ('线上-商城C端-全球购','线下-商城聚合') and pay_tools in ('代付'))              
               )        
         then '线上B端' 
         else mer_sma_type  end  as  mer_sma_type
   ,mer_ownr        
   ,prod_type       
   ,sys_src
   ,card_type   
from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_03 p1
left join dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_04 p2 on p1.dim_day_id=p2.days
union all
select
    dim_day_id 
   ,mer_id          
   ,trade_type      
   ,trade_name      
   ,product_id      
   ,pay_tools       
   ,mer_date        
   ,acc_date    
   ,order_amt*p2.sc_rate                                                                              as order_amt
   ,trade_amt                                                                                         as trade_amt         
   ,mer_fee*p2.sc_rate                                                                                as mer_fee       
   ,bank_fee*p2.sc_rate                                                                               as bank_fee        
   ,trade_num*p2.sc_rate                                                                              as trade_num    
   ,flag
   ,bus_flag
   ,dept_nm         
   ,'支付集团业务'                                                                                    as mer_big_type    
   ,'线上C端'                                                                                        as mer_sma_type
   ,'体系内'                                                                                          as mer_ownr        
   ,prod_type       
   ,sys_src 
   ,card_type   
from (select *from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_03 
where dept_nm ='支付业务部' 
  and mer_ownr='体系外' 
  and mer_big_type='外部发卡-银联'  
  and mer_sma_type='外部发卡-银联' 
  and prod_type='京东支付-银联闪付') p1
left join dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_04 p2 on p1.dim_day_id=p2.days
;
)
;


$SQL_BUFF[6]=qq(
set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_06;
use dmf_dev;
---------管报&业绩报表调整步骤  
--一、交易规模&订单笔数
----直接采用产品明细表部门为“支付业务部”交易规模&交易笔数，增加白条还款规模。


drop table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_06 ;
create table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_06
as 
select
    acc_date                                                                                         as dim_day_id 
   ,cast(null as string)                                                                                             as mer_id          
   ,'白条还款'                                                                                       as trade_type      
   ,'白条还款'                                                                                       as trade_name      
   ,cast(null as string)                                                                                             as product_id      
   ,'白条还款'                                                                                       as pay_tools       
   ,cast(null as string)                                                                                             as mer_date        
   ,cast(null as string)                                                                                             as acc_date    
   ,sum(nvl(trade_amt,0))                                                                            as order_amt
   ,0                                                                                                as trade_amt         
   ,cast(null as string)                                                                                             as mer_fee       
   ,cast(null as string)                                                                                             as bank_fee        
   ,cast(null as string)                                                                                             as trade_num    
   ,'白条还款'                                                                                       as flag
   ,'白条还款'                                                                                       as bus_flag
   ,'支付业务部'                                                                                     as dept_nm         
   ,'支付集团业务'                                                                                   as mer_big_type    
   ,'线上B端'                                                                                   as mer_sma_type
   ,'体系内'                                                                                         as mer_ownr        
   ,'白条还款'                                                                                       as prod_type       
   ,'调整项'                                                                                         as sys_src
   ,cast(null as string)                                                                                             as card_type   
from 
(  --账单还款
  select 
   to_date(payTime)            as acc_date
  ,sum(nvl(payedloanamt,0)+nvl(payedplanfee,0)+nvl(payeddayamt,0)+nvl(payednextdayamt,0)+nvl(payedoveramt,0)+nvl(payednextoveramt,0))           as trade_amt
  FROM odm.ODM_CF_BILL_REPAY_DETAIL_0000_I_D
  where 1=1
  and to_date(payTime)>='$TX_10B_DATE'
  and to_date(payTime)<='$TX_PREV_DATE'
  and  bizcode in ('1','4','18')
  and syscode='1'
  and (payMoneyType not in ('OFFLINE','REFUNDREPAY','HKQ') OR payMoneyType  IS null)
  group by to_date(payTime)
  union all
  select 
   to_date(payTime)            as acc_date
  ,sum(nvl(currefundamount,0))   as trade_amt
  FROM odm.ODM_CF_BILL_REAL_REFUND_0000_S_D   --账单真实退款表
  where dt=date_sub('$TX_DATE',1)
  and to_date(payTime)>='$TX_10B_DATE'
  and to_date(payTime)<='$TX_PREV_DATE'
  and  bizcode in ('1','4','18')
  and syscode='1'
  and (payMoneyType not in ('OFFLINE','REFUNDREPAY','HKQ') OR payMoneyType  IS null)
  group by to_date(payTime)
  union all
  --订单还款
  SELECT 
  to_date(payTime)            as acc_date
  ,sum(nvl(payedloanamount,0)+nvl(payedplanamount,0)+nvl(payedoveramount,0)+nvl(exceedamount,0))   as trade_amt     --+nvl(payeddayamount,0)
  FROM odm.ODM_CF_REPAY_S_D   --白条还款近线明细
  where dt = date_sub('$TX_DATE',1)
  and to_date(payTime)>='$TX_10B_DATE'
  and to_date(payTime)<='$TX_PREV_DATE'
  and status!=0   
  and syscode='1'
  and bizcode in ('1','4','18')
  and (sourceType not in ('offline') or sourceType  is null)
  group by to_date(payTime)
  ) ss  
 group by acc_date
 ;
 )
 ;
 
$SQL_BUFF[7]=qq(
set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_07;
use dmf_dev;
drop table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_07 ;
create table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_07
as 
--二、收入调整
--①部门“支付业务部”、业务归属 “体系外”、业务大类“外部发卡-银联”、业务小类“外部发卡-银联”、产品类型“京东支付-银联闪付”按照订单比例还原至部门“支付业务部”、业务归属 “体系内”、业务大类 “支付集团业务”、业务小类“线上C端”中。
---   银联闪付收入成本调整放在上一个BUFF[5]中了
--②部门“支付业务部”、业务归属 “体系内”、支付工具“小金库”收入调减需要还原至财富管理部。（目前仅有商城业务+全球购）
--③支付工具“京东白条”、部门“支付业务部”、业务归属 “体系内”、业务大类“支付集团业务”、业务小类“线上C端-全球购”收入金额*1%/2.2%进行调减还原至消费金融部。
--④支付工具“京东白条”、部门“支付业务部”、业务归属 “体系外”，交易规模*0.4%/1.06调减收入还原至消费金融部。（目前有外部收单&外部发卡）。
--⑤商户号110646950001，支付工具“京东白条”， 部门“支付业务部”收入调减还原至消费金融部。
--三、成本调整
--部门“支付业务部”、业务归属 “体系外”、业务大类“外部发卡-银联”、业务小类“外部发卡-银联”、产品类型“京东支付-银联闪付”按照订单比例还原至部门“支付业务部”、业务归属 “体系内”、业务大类 “支付集团业务”、业务小类“线上C端”中。
select
    dim_day_id 
   ,mer_id          
   ,trade_type      
   ,trade_name      
   ,product_id      
   ,pay_tools       
   ,mer_date        
   ,acc_date    
   ,0                      as order_amt
   ,0                      as trade_amt         
   ,mer_fee*(-1)           as mer_fee     
   ,0                      as bank_fee        
   ,0                      as trade_num    
   ,flag
   ,bus_flag
   ,dept_nm         
   ,mer_big_type    
   ,mer_sma_type
   ,mer_ownr        
   ,prod_type       
   ,sys_src
   ,card_type   
from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_05
where dept_nm='支付业务部' 
  and mer_ownr='体系内'
  and pay_tools='小金库'
union all
select
    dim_day_id 
   ,mer_id          
   ,trade_type      
   ,trade_name      
   ,product_id      
   ,pay_tools       
   ,mer_date        
   ,acc_date    
   ,0                                 as order_amt
   ,0                                 as trade_amt         
   ,mer_fee*(-1)*0.01/0.022           as mer_fee     
   ,0                                 as bank_fee        
   ,0                                 as trade_num    
   ,flag
   ,bus_flag
   ,dept_nm         
   ,mer_big_type    
   ,mer_sma_type
   ,mer_ownr        
   ,prod_type       
   ,sys_src 
   ,card_type   
from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_05
where dept_nm='支付业务部' 
  and mer_ownr='体系内'
  and mer_big_type='支付集团业务'
  and mer_sma_type='线上C端-全球购'
  and pay_tools='白条'
--union all
--20190916yjb根据刚刚沟通，8月管报和业绩月报 外部线上C的差异主要由于如下两个原因导致：
--1、白条外单调整，管报未调整，业绩周报中进行调整；烦请后续从业绩周报中去除该调整项，调整内容如下：
--支付工具“京东白条”、部门“支付业务部”、业务归属 “体系外”，交易规模（用支付金额，影响的是收入，跟目前表对不上）*0.4%/1.06调减收入还原至消费金融部。（目前有外部收单&外部发卡）
--select
--    dim_day_id 
--   ,mer_id          
--   ,trade_type      
--   ,trade_name      
--   ,product_id      
--   ,pay_tools       
--   ,mer_date        
--   ,acc_date    
--   ,0                                 as order_amt
--   ,0                                 as trade_amt         
--   ,trade_amt*(-1)*0.004              as mer_fee     
--   ,0                                 as bank_fee        
--   ,0                                 as trade_num    
--   ,flag
--   ,bus_flag
--   ,dept_nm         
--   ,mer_big_type    
--   ,mer_sma_type
--   ,mer_ownr        
--   ,prod_type       
--   ,sys_src   
--   ,card_type   
--from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_05
--where dept_nm='支付业务部' 
--  and mer_ownr='体系外'
--  and pay_tools='白条'
 union all
select
    dim_day_id 
   ,mer_id          
   ,trade_type      
   ,trade_name      
   ,product_id      
   ,pay_tools       
   ,mer_date        
   ,acc_date    
   ,0                                 as order_amt
   ,0                                 as trade_amt         
   ,mer_fee*(-1)                      as mer_fee     
   ,0                                 as bank_fee        
   ,0                                 as trade_num    
   ,flag
   ,bus_flag
   ,dept_nm         
   ,mer_big_type    
   ,mer_sma_type
   ,mer_ownr        
   ,prod_type       
   ,sys_src  
   ,card_type   
from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_05
where dept_nm='支付业务部' 
  and pay_tools='白条'
  and mer_id='110646950001'
;
)
;
$SQL_BUFF[8]=qq(
set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_08;
use dmf_dev;
---------------------封闭卡数据
drop table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_08;
create table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_08 as 
SELECT  dim_day_id,
        merchant_accept, 
        sum(case when type IN ('II','RI') then amount else 0 end) as t_recharge_amount,
        sum(case when type IN ( 'S', 'RS', 'VS', 'RVS') then amount else 0 end) as t_sale_amount,
        sum(case when type IN ('D','RD') then amount else 0 end) as t_refund_amount
from  
      (SELECT  t1.created_date as dim_day_id,
               t1.trade_no,
               t1.type,
               t1.merchant_accept, 
               t1.amount 
        FROM   (SELECT substr(created_date,1,10) as created_date,
                       trade_no,
                       type,
                       term_no,
                       batch_no,
                       store_no,
                       merchant_accept,
                       amount 
                FROM odm.ODM_PAY_CARD_ORDER_013_S_D  --卡交易订单表
                WHERE dt=date_sub('$TX_DATE',1)
                AND type IN ( 'S', 'RS', 'VS', 'RVS','D','RD','II','RI' )
                AND status = '1'
                AND account_type = 'CNY'
                AND channel = '1101'
                and substr(created_date,1,10) >='$TX_10B_DATE'
                and substr(created_date,1,10)<='$TX_PREV_DATE'
                ) t1
                join 
                (SELECT distinct Substr(created_date,1,10) as created_date, batch_no, term_no, store_no, merchant_accept
                FROM odm.ODM_PAY_SETTLEMENT_FLOW_013_I_D   --结算流水表
                where status > 0 
                and Substr(created_date,1,10) >='$TX_10B_DATE'
                and substr(created_date,1,10)<='$TX_PREV_DATE'
                ) t2
                ON t1.term_no = t2.term_no
                AND t1.batch_no = t2.batch_no
                AND t1.store_no = t2.store_no
                AND t1.merchant_accept = t2.merchant_accept
                and t1.created_date=t2.created_date
        UNION ALL
        SELECT Substr(created_date,1,10) AS  dim_day_id,
               trade_no,
               type,
               merchant_accept,
               amount
        FROM  odm.ODM_PAY_CARD_ORDER_013_S_D   --卡交易订单表
        WHERE  dt=date_sub('$TX_DATE',1)
        AND type IN ( 'S', 'RS', 'VS', 'RVS','D','RD','II','RI' )
        AND status = '1'
        AND account_type = 'CNY'
        AND channel = '2202'
        AND Substr(created_date,1,10) >='$TX_10B_DATE'
        and substr(created_date,1,10)<='$TX_PREV_DATE'
      ) t
LEFT JOIN (SELECT trade_no
          FROM   odm.ODM_PAY_RECONCILIATION_DIFFERENCE_013_S_D    --对账处理表
          WHERE  dt=date_sub('$TX_DATE',1)
          AND check_status = '0') r
ON t.trade_no = r.trade_no
WHERE  r.trade_no IS null
group by dim_day_id,merchant_accept
;

)
;

$SQL_BUFF[9]=qq(
set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_09;
use dmf_dev;
drop table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_09;
create table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_09 
as 
select 
    dim_day_id               as dim_day_id
   ,cast(null as string)                     as mer_id          
   ,cast(null as string)                     as trade_type      
   ,cast(null as string)                     as trade_name      
   ,cast(null as string)                     as product_id      
   ,cast(null as string)                     as pay_tools       
   ,cast(null as string)                     as mer_date        
   ,cast(null as string)                     as acc_date    
   ,sum(nvl(d.t_sale_amount,0))     as order_amt                       --交易额
   ,0                               as trade_amt
   ,sum(abs(nvl(case a.commission_type 
        when '1' then d.t_recharge_amount*a.commission_percent/100
--        when '2' then e.t_activation_amount*a.commission_percent/100
        when '3' then (d.t_sale_amount+d.t_refund_amount)*a.commission_percent/100
        when '4' then d.t_sale_amount*a.commission_percent/100
        else 0 end,0)))                               
                                    as mer_fee                         --商户手续费
   ,0                               as bank_fee                        --银行成本
   ,0                               as trade_num                       --交易笔数
   ,'封闭卡'                        as flag
   ,'封闭卡'                        as bus_flag
   ,'支付业务部'                    as dept_nm         
   ,'外部发卡-封闭卡'               as mer_big_type    
   ,'外部发卡-封闭卡'               as mer_sma_type
   ,'体系外'                        as mer_ownr        
   ,'封闭卡'                        as prod_type       
   ,'调整项'                        as sys_src 
   ,cast(null as string)                            as card_type   
from (select * from odm.ODM_PAY_MERCHANT_BASE_013_S_D where dt=date_sub('$TX_DATE',1)) a    --商户基础信息
 join dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_08 d
on a.code= d.merchant_accept
group by d.dim_day_id

union all
select
    e.active_date                   as dim_day_id
   ,cast(null as string)                            as mer_id          
   ,cast(null as string)                            as trade_type      
   ,cast(null as string)                            as trade_name      
   ,cast(null as string)                            as product_id      
   ,cast(null as string)                            as pay_tools       
   ,cast(null as string)                            as mer_date        
   ,cast(null as string)                            as acc_date  
   ,0                               as order_amt                       --交易金额
   ,0                               as trade_amt
   ,sum(abs(nvl(case a.commission_type 
       when '2' then e.t_activation_amount*a.commission_percent/100
        else 0 end,0)))             as mer_fee                         --商户手续费 
   ,0                               as bank_fee                        --银行成本
   ,0                               as trade_num                       --交易笔数
   ,'封闭卡'                        as flag
   ,'封闭卡'                        as bus_flag
   ,'支付业务部'                    as dept_nm         
   ,'外部发卡-封闭卡'               as mer_big_type    
   ,'外部发卡-封闭卡'               as mer_sma_type
   ,'体系外'                        as mer_ownr        
   ,'封闭卡'                        as prod_type       
   ,'调整项'                        as sys_src
   ,cast(null as string)                            as card_type   
from (select * from odm.ODM_PAY_MERCHANT_BASE_013_S_D where dt=date_sub('$TX_DATE',1)) a    --商户基础信息
 join 
    (select
        substr(active_date,1,10) as active_date,
        merchant_code,
        sum(card_denomination) as t_activation_amount
    from(
    select *,row_number() over(partition by id order by modified_time desc ) as rn
    from odm.ODM_PAY_ZH_ORDER_SELL_CARD_013_S_D   --封闭卡售卡记录表
    where dt=date_sub('$TX_DATE',1) 
    and substr(active_date,1,10) >='$TX_10B_DATE' 
    and substr(active_date,1,10)<='$TX_PREV_DATE' and activation='1' ) s
    where rn=1 
    group by merchant_code,substr(active_date,1,10)
    ) e
on a.code= e.merchant_code
group by e.active_date
;
)
;

$SQL_BUFF[10]=qq(
set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_10;

----------------pos 算法还原其他集市表a_payment_pos_bank_i_d
use dmf_dev;
drop table dmf_dev.dyt_dmf_dev_pos_my2_tradeorder;
create table dmf_dev.dyt_dmf_dev_pos_my2_tradeorder as 
select * from (
    select *,row_number() over(partition by payid order by updatetime desc) as rn from odm.ODM_PAY_TRADEORDER_001_I_D   --POS订单表
) a 
where rn = 1
;
use dmf_dev;
drop table dmf_dev.dyt_dmf_dev_pos_my2_paymentresult;
create table dmf_dev.dyt_dmf_dev_pos_my2_paymentresult as 
select * from (
    select *,row_number() over(partition by payid order by updatetime desc) as rn from odm.ODM_PAY_PAYMENTRESULT_001_S_D --POS支付结果表
    where dt =date_sub('$TX_DATE',1) 
) a 
where rn = 1
;

drop table if exists dmf_dev.dyt_dmf_dev_pos_pos_fin_list;
create table dmf_dev.dyt_dmf_dev_pos_pos_fin_list as
select 
    '建行' as bankname,
    '正向交易' as s_type,
    to_date(o.createtime) as dim_day_id,
    sum(o.trxamount) as amt
from dmf_dev.dyt_dmf_dev_pos_my2_tradeorder o 
join dmf_dev.dyt_dmf_dev_pos_my2_paymentresult pr on pr.payId = o.payId
where 
    o.orderSource in ('POS','POS_EXEMPT') 
    and substr(o.createtime,1,10) >='$TX_10B_DATE'
    and substr(o.createtime,1,10)<='$TX_PREV_DATE'
    and pr.payType='PURCHASE'
    and o.orderStatus='SUCCESS' 
    and pr.frpCode='8002003' 
    and pr.bankCustomerCode in ('105100000004796','105100000004797','105100000004990','105100000004991','105100000004992','105100000004993','105100000004994','105100000004995','105100000005009','105100000005010','105100000005011','105100000005024','105100000005025','105100000005026','105100000005027','105100000005028','105100000005029','105100000005030','105100000005031','105100000005032','105100000005033','105100000005034','105100000005035','105100000005036','105100000005037','105100000005038','105100000005039','105100000005040','105100000005041','105100000005042','105100000005043','105100000005044','105100000005045','105100000005046','105100000005047','105100000005048','105100000005049','105100000005050','105100000005051','105100000005052','105100000005053','105100000005054','105100000005055','105100000005056','105100000005057','105100000005058','105100000005059','105100000005060','105100000005061','105100000005062','105100000005063','105100000005064','105100000005065','105100000005066','105100000005067','105100000005068','105100000005069','105100000005070','105100000005071','105100000005072','105100000005073','105100000005074','105100000005075','105100000005076','105100000005077','105100000005078','105100000005079','105100000005080','105100000005081','105100000005082','105100000005083','105100000005084','105100000005085','105100000005086','105100000005087','105100000005088','105100000005089','105100000005090','105100000005091','105100000005092','105100000005093','105100000005094','105100000005095','105100000005096','105100000005097','105100000005098','105100000005099','105100000005100','105100000005101','105100000005102','105100000005103','105100000005104','105100000005105','105100000005106','105100000005107','105100000005108','105100000005109','105100000005110','105100000005111','105100000005112','105100000005113','105100000005114','105100000005115','105100000005116','105100000005117','105100000005118','105100000005119','105100000005120','105100000005121','105100000005122','105100000005123','105100000005124','105100000005125','105100000005126','105100000005127','105100000005128','105100000005129','105100000005130','105100000005131','105100000005132','105100000005133','105100000005134','105100000005135','105100000005136','105100000005137','105100000005138','105100000005139','105100000005140','105100000005141','105100000005142','105100000005143','105100000005144','105100000005145','105100000005146','105100000005147','105100000005148','105100000005149','105100000005150','105100000005151','105100000005152','105100000005153','105100000005154','105100000005155','105100000005156','105100000005157','105100000005158','105100000005159','105100000005160','105100000005161','105100000005162','105100000005163','105100000005164','105100000005165','105100000005166','105100000005167','105100000005168','105100000005169','105100000005170','105100000005171','105100000005172','105100000005173','105100000005174','105100000005175','105100000005176','105100000005177','105100000005178','105100000005179','105100000005180','105100000005181','105100000005182','105100000005183','105100000005184','105100000005185','105100000005186','105100000005187','105100000005188','105100000005189','105100000005190','105100000005191','105100000005192','105100000005193','105100000005194','105100000005195','105100000005196','105100000005197','105100000005198','105100000005199','105100000005200','105100000005201','105100000005202','105100000005203','105100000005204','105100000005205','105100000005206','105100000005207','105100000005208','105100000005209','105100000005210','105100000005211','105100000005212','105100000005213','105100000005214','105100000005215','105100000005216','105100000005217','105100000005218','105100000005219','105100000005220','105100000005221','105100000005222','105100000005223','105100000005224','105100000005225','105100000005226','105100000005227','105100000005228','105100000005229','105100000005230','105100000005231','105100000005232','105100000005233','105100000005234','105100000005235','105100000005236','105100000005237','105100000005238','105100000005239','105100000005240','105100000005241','105100000005242','105100000005243','105100000005244','105100000005245','105100000005246','105100000005247','105100000005248','105100000005249','105100000005250','105100000005251','105100000005252','105100000005253','105100000005254','105100000005255','105100000005256','105100000005257','105100000005258','105100000005259')
group by to_date(o.createtime)
;

insert into table dmf_dev.dyt_dmf_dev_pos_pos_fin_list
select 
    '农行' as bankname,
    '正向交易' as s_type,
    to_date(o.createtime) as dim_day_id,
    sum(o.trxamount) as amt 
from dmf_dev.dyt_dmf_dev_pos_my2_tradeorder o 
join dmf_dev.dyt_dmf_dev_pos_my2_paymentresult pr on pr.payId = o.payId
where 
    o.orderSource in ('POS','POS_EXEMPT')
    and substr(o.createtime,1,10) >='$TX_10B_DATE'
    and substr(o.createtime,1,10)<='$TX_PREV_DATE'
    and pr.payType='PURCHASE'
    and o.orderStatus='SUCCESS' 
    and pr.frpCode='8002008'
    and pr.bankCustomerCode in ('113110053990050','113110053990051','113110053990052','113110053990053','113110053990054','113110053990055','113110053990056','113110053990057','113110053990058','113110053990059','113110053990060','113110053990061','113110053990062','113110053990063','113110053990064','113110053990065','113110053990066','113110053990067','113110053990068','113110053990069','113110053990070','113110053990071','113110053990072','113110053990073','113110053990074','113110053990075','113110053990076','113110053990077','113110053990078','113110053990079','113110053990080','113110053990081','113110053990082','113110053990083','113110053990084','113110053990085','113110053990086','113110053990087','113110053990088','113110053990089','113110053990090','113110053990091','113110053990092','113110053990093','113110053990094','113110053990095','113110053990096','113110053990097','113110053990098','113110053990099','113110053990100','113110053990101','113110053990102','113110053990103','113110053990104','113110053990105','113110053990106','113110053990107','113110053990108','113110053990109','113110053990110','113110053990111','113110053990112','113110053990113','113110053990114','113110053990115','113110053990116','113110053990117','113110053990118','113110053990119','113110053990120','113110053990121','113110053990122','113110053990123','113110053990124','113110053990125','113110053990126','113110053990127','113110053990128','113110053990129','113110053990130','113110053990131','113110053990132','113110053990133','113110053990134','113110053990135','113110053990136','113110053990137','113110053990138','113110053990139','113110053990140','113110053990141','113110053990142','113110053990143','113110053990144','113110053990145','113110053990146','113110053990147','113110053990148','113110053990149','113110053990150','113110053990151','113110053990152')
group by to_date(o.createtime)
;

insert into table dmf_dev.dyt_dmf_dev_pos_pos_fin_list
select 
    '交行' as bankname,
    '正向交易' as s_type,
    to_date(o.createtime) as dim_day_id,
    sum(o.trxamount) as amt
from dmf_dev.dyt_dmf_dev_pos_my2_tradeorder o 
join dmf_dev.dyt_dmf_dev_pos_my2_paymentresult pr on pr.payId = o.payId
where 
    o.orderSource in ('POS','POS_EXEMPT') 
    and substr(o.createtime,1,10) >='$TX_10B_DATE'
    and substr(o.createtime,1,10)<='$TX_PREV_DATE'
    and pr.payType='PURCHASE'
    and o.orderStatus='SUCCESS' 
    and pr.frpCode='8002009'
    and pr.bankCustomerCode in ('301110054111960')
group by  to_date(o.createtime)
; 

insert into table dmf_dev.dyt_dmf_dev_pos_pos_fin_list
select 
    case when frpCode ='8002003' then '建行'   -- 数据来源 mydb-posbase-01.db.jdfin.local:3306:pos_base(mysql)  frpCode代表银行编码 8002003=建行 8002008=农行 8002009=交行
         when frpCode ='8002008' then '农行'
         when frpCode ='8002009' then '交行'
         end as  bankname,
    '退款' as s_type,
    to_date(createTime) as dim_day_id,
    sum(amount)  as amt
from odm.ODM_PAY_REFUND_DETAIL_I_D 
where 
    source in ('POS','WEB','COD','PAR') 
    and substr(createtime,1,10) >='$TX_10B_DATE'
    and substr(createtime,1,10)<='$TX_PREV_DATE'   
    and status='SUCCESS' 
    and refundPaymentNo is null
group by 
    to_date(createTime),
    case when frpCode ='8002003' then '建行'
         when frpCode ='8002008' then '农行'
         when frpCode ='8002009' then '交行'
    end
;

use dmf_dev;
drop table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_10;
create table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_10 
as 
select      
    dim_day_id                                      as dim_day_id
   ,cast(null as string)                            as mer_id          
   ,cast(null as string)                            as trade_type      
   ,cast(null as string)                            as trade_name      
   ,cast(null as string)                            as product_id      
   ,'pos'                                           as pay_tools       
   ,cast(null as string)                            as mer_date        
   ,cast(null as string)                            as acc_date  
   ,sum(amt)                                        as order_amt                       --交易金额
   ,0                                               as trade_amt
   ,sum(amt) * 0.0015                               as mer_fee                         --商户手续费 
   ,0                                               as bank_fee                        --银行成本
   ,0                                               as trade_num                       --交易笔数
   ,'pos'                                           as flag
   ,'pos'                                           as bus_flag
   ,'支付业务部'                                    as dept_nm         
   ,'支付集团业务'                                  as mer_big_type    
   ,'线上C端'                                  as mer_sma_type
   ,'体系内'                                        as mer_ownr        
   ,'pos'                                           as prod_type       
   ,'调整项'                                        as sys_src 
   ,cast(null as string)                            as card_type   
from  dmf_dev.dyt_dmf_dev_pos_pos_fin_list
group by dim_day_id
;
)
;

$SQL_BUFF[11]=qq(
set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_11;
use dmf_dev;
drop table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_11;
create table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_11
as 
select      
    dim_day_txdate                  as dim_day_id
   ,cast(null as string)                            as mer_id          
   ,'线下导入收入'                  as trade_type      
   ,'线下导入收入'                  as trade_name      
   ,cast(null as string)                            as product_id      
   ,cast(null as string)                            as pay_tools       
   ,dim_day_txdate                  as mer_date        
   ,dim_day_txdate                  as acc_date  
   ,0                               as order_amt                       --交易金额
   ,0                               as trade_amt
   ,day_value*10000                 as mer_fee                         --商户手续费 
   ,0                               as bank_fee                        --银行成本
   ,0                               as trade_num                       --交易笔数
   ,'线下导入收入'                  as flag
   ,'线下导入收入'                  as bus_flag
   ,depart_name                     as dept_nm         
   ,case when fir_lvl_name in ('TO C-用户运营','利息收入') then fir_lvl_name 
         when fir_lvl_name in ('支付集团业务-B端','支付商城业务-B端') then '支付集团业务'
        end                      as mer_big_type    
   ,case when fir_lvl_name in ('TO C-用户运营','利息收入') then fir_lvl_name 
         when fir_lvl_name in ('支付集团业务-B端','支付商城业务-B端') then '线上B端'
        end                         as mer_sma_type
   ,case when fir_lvl_name in ('TO C-用户运营','利息收入') then '体系外' 
         when fir_lvl_name in ('支付集团业务-B端','支付商城业务-B端') then '体系内'
        end                         as mer_ownr        
   ,case when fir_lvl_name in ('TO C-用户运营','利息收入') then fir_lvl_name 
         when fir_lvl_name in ('支付集团业务-B端','支付商城业务-B端') then '线上B端'
        end                         as prod_type       
   ,'线下导入收入'                  as sys_src
   ,cast(null as string)                            as card_type   
from (select
     dim_day_txdate,
     month_id,
     days_num,     
     depart_code,
     depart_name,
     fir_lvl_code,
     case when fir_lvl_name='TOC-用户运营' then 'TO C-用户运营' else  fir_lvl_name end  as fir_lvl_name,
     sec_lvl_code,
     sec_lvl_name,
     order_index_name,
     index_name,
     index_desc,
     index_value/days_num*1.06 as day_value,             --变成税前的
     index_value
     from 
(select * from dmf_add.dmfadd_oar_add_pay_fin_int_line_i_m where trim(depart_code)='9310000' and trim(order_index_name) in ('007','008')  --dmfadd_oar_add_pay_fin_int_line_a_m 支付BigBoss线下数据导入
  and trim(fir_lvl_code) in ('fin9320006','fin9320008','fin9320010')) p1
left join dim.dim_day p2 on p1.month_id=substr(p2.dim_day_txdate,1,7)
) pp 
where
 pp.dim_day_txdate>='$TX_10B_DATE' and
 pp.dim_day_txdate<='$TX_PREV_DATE'

;
)
;
$SQL_BUFF[12]=qq(
set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_12;
use dmf_dev;
drop table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_12 ;
create table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_12
as 
select date(dim_day_id) as dim_day_id,mer_id,trade_type,trade_name,product_id,pay_tools,mer_date,acc_date,order_amt,trade_amt,cast(mer_fee as double),cast(bank_fee as double),cast(trade_num as integer),flag,bus_flag,dept_nm,mer_big_type,mer_sma_type,mer_ownr,prod_type,sys_src,card_type from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_05                     --主体业务
union all                                                                                                                                             
select date(dim_day_id) as dim_day_id,mer_id,trade_type,trade_name,product_id,pay_tools,mer_date,acc_date,order_amt,trade_amt,cast(mer_fee as double),cast(bank_fee as double),cast(trade_num as integer),flag,bus_flag,dept_nm,mer_big_type,mer_sma_type,mer_ownr,prod_type,sys_src,card_type from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_06                     --白条还款
union all                                                                                                                                             
select date(dim_day_id) as dim_day_id,mer_id,trade_type,trade_name,product_id,pay_tools,mer_date,acc_date,order_amt,trade_amt,cast(mer_fee as double),cast(bank_fee as double),cast(trade_num as integer),flag,bus_flag,dept_nm,mer_big_type,mer_sma_type,mer_ownr,prod_type,sys_src,card_type from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_07                     --调减收入
union all                                                                                                                                            
select date(dim_day_id) as dim_day_id,mer_id,trade_type,trade_name,product_id,pay_tools,mer_date,acc_date,order_amt,trade_amt,cast(mer_fee as double),cast(bank_fee as double),cast(trade_num as integer),flag,bus_flag,dept_nm,mer_big_type,mer_sma_type,mer_ownr,prod_type,sys_src,card_type from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_09                     --封闭卡
union all                                                                                                                                            
select date(dim_day_id) as dim_day_id,mer_id,trade_type,trade_name,product_id,pay_tools,mer_date,acc_date,order_amt,trade_amt,cast(mer_fee as double),cast(bank_fee as double),cast(trade_num as integer),flag,bus_flag,dept_nm,mer_big_type,mer_sma_type,mer_ownr,prod_type,sys_src,card_type from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_10                     --调整项POS
union all                                                                                                                                           
select date(dim_day_id) as dim_day_id,mer_id,trade_type,trade_name,product_id,pay_tools,mer_date,acc_date,order_amt,trade_amt,cast(mer_fee as double),cast(bank_fee as double),cast(trade_num as integer),flag,bus_flag,dept_nm,mer_big_type,mer_sma_type,mer_ownr,prod_type,sys_src,card_type from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_11                     --线下导入数据 POP结算、TO-C运营、利息收入
;
)
;
$SQL_BUFF[13]=qq(
set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_13;
use dmf_dev;
drop table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_13 ;
create table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_13
as
select 
   dim_day_id                                  --统计日期维度
  ,mer_id                                      --二级商户号id
  ,trade_type                                  --清结算系统交易类型
  ,trade_name                                  --交易类型名称
  ,product_id                                  --产品编号
  ,pay_tools                                   --支付工具
  ,mer_date                                    --商户交易日期
  ,acc_date                                    --记账日期
  ,sum(nvl(order_amt,0))      as order_amt     --订单总金额
  ,sum(nvl(trade_amt,0))      as trade_amt     --支付金额                  --收单、代付、钱包、调整项（POS、封闭卡、白条还款）都用交易金额，全款结算用订单金额
  ,sum(nvl(mer_fee,0))        as mer_fee       --商户手续费
  ,sum(nvl(bank_fee,0))       as bank_fee      --银行成本
  ,sum(nvl(trade_num,0))      as trade_num     --交易笔数
  ,flag                                        --标识 
  ,bus_flag  
  ,dept_nm                                     --部门
  ,mer_big_type                                --业务大类
  ,case when mer_id='110583859001' and  pay_tools='钱包余额' then '线上B端' 
        when mer_sma_type in ('线上C端','线上-商城C端') and pay_tools='代付' then '线上B端'
     else mer_sma_type end as mer_sma_type      --业务小类
  ,mer_ownr                                    --业务归属
  ,prod_type                                   --产品类型
  ,sys_src                                     --商户来源
  ,case when card_type='CR' then '贷记卡' 
        when card_type='DE' then '借记卡' 
        when card_type='PR' then '预付卡' 
        when card_type='QC' then '准贷记卡' 
        when card_type='OD' then '境外借记卡' 
        when card_type='OC' then '境外信用卡'  
     else '其他' end          as card_type     --卡类型
  ,cast(null as string)                       as reserved1     --预留字段1
  ,cast(null as string)                       as reserved2     --预留字段2
  ,cast(null as string)                       as reserved3     --预留字段3
  ,cast(null as string)                       as reserved4     --预留字段4
  ,cast(null as string)                       as reserved5     --预留字段5   
  ,dim_day_id                 as dt
from 
dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_12
where 1=1
and dim_day_id>='$TX_10B_DATE'
and dim_day_id<='$TX_PREV_DATE'
group by 
 dim_day_id  
,mer_id      
,trade_type  
,trade_name  
,product_id  
,pay_tools   
,mer_date    
,acc_date
,flag
,bus_flag            
,dept_nm         
,mer_big_type    
,case when mer_id='110583859001' and  pay_tools='钱包余额' then '线上B端' 
        when mer_sma_type in ('线上C端','线上-商城C端') and pay_tools='代付' then '线上B端'
     else mer_sma_type end    
,mer_ownr        
,prod_type           
,sys_src
,case when card_type='CR' then '贷记卡' 
        when card_type='DE' then '借记卡' 
        when card_type='PR' then '预付卡' 
        when card_type='QC' then '准贷记卡' 
        when card_type='OD' then '境外借记卡' 
        when card_type='OC' then '境外信用卡'  
     else '其他' end        
;
)
;

$SQL_BUFF[14]=qq(
set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_14;
use dmf_dev;
-----------------------------把通道成本的作业内容融合到该作业当中
------增加鹏华的通道成本费用
drop table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_14 ;
create table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_14
as
select 
   to_date(ordr_time)                         as dim_day_id
  ,cast(null as string)                       as mer_id                                      --二级商户号id
  ,cast(null as string)                       as trade_type                                  --清结算系统交易类型
  ,cast(null as string)                       as trade_name                                  --交易类型名称
  ,cast(null as string)                       as product_id                                  --产品编号
  ,cast(null as string)                       as pay_tools                                   --支付工具
  ,cast(null as string)                       as mer_date                                    --商户交易日期
  ,cast(null as string)                       as acc_date                                    --记账日期
  ,0                                          as order_amt                                   --订单总金额
  ,0                                          as trade_amt                                   --支付金额                  --收单、代付、钱包、调整项（POS、封闭卡、白条还款）都用交易金额，全款结算用订单金额
  ,0                                          as mer_fee                                     --商户手续费
  ,sum(tx_amt)*0.0004                         as bank_fee                                    --银行成本
  ,0                                          as trade_num                                   --交易笔数
  ,'zd'                                       as flag                                        --标识 
  ,'鹏华'                                     as bus_flag                                    --业务标识
  ,'财富管理部'                               as dept_nm                                     --部门
  ,'小金库'                                   as mer_big_type                                --业务大类
  ,'小金库'                                   as mer_sma_type                                --业务小类
  ,cast(null as string)                       as mer_ownr                                    --业务归属
  ,'小金库'                                   as prod_type                                   --产品类型
  ,'鹏华'                                     as sys_src                                     --数据来源
  ,cast(null as string)                       as card_type                                   --卡类型
  ,cast(null as string)                       as reserved1                                   --预留字段1
  ,cast(null as string)                       as reserved2                                   --预留字段2
  ,cast(null as string)                       as reserved3                                   --预留字段3
  ,cast(null as string)                       as reserved4                                   --预留字段4
  ,cast(null as string)                       as reserved5                                   --预留字段5   
  ,to_date(ordr_time)                         as dt
from idm.idm_f02_fin_xjk_tx_dtl_s_d
  where dt='$TX_PREV_DATE'
    and tx_type='purch'
    and sec_biz_type_code='IN000001'
    and ordr_status_code>=3          ----用结算日期 周六日没数据 原先是6 sett_time 改成 ordr_time
    and substr(ordr_time,1,10)>='$TX_10B_DATE'
    and substr(ordr_time,1,10)<='$TX_PREV_DATE'
    and prod_code in ('J50060002','J50060000','J50060001')
    group by to_date(ordr_time)
;
)
;

$SQL_BUFF[15]=qq(
set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_15;
use dmf_dev;
------拆分消费金融 白条、金条还款额占比
drop table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_15;
create table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_15
as 
--白条还款表
select 
  substr(pay_time,1,10)                                               as repay_date
 ,'白条'                                                              as flag 
 ,sum(nvl(paid_prin,0)+nvl(paid_plan_fee_amt,0)+nvl(paid_day_fee_amt,0)+nvl(paid_over_amt,0)-nvl(prin_exceed_amt,0)-nvl(fee_exceed_amt,0)-nvl(day_exceed_amt,0)-nvl(over_exceed_amt,0)) as payed_amount 
 from idm.idm_f02_cf_xbt_repay_dtl_s_d
where dt='$TX_PREV_DATE'
and substr(pay_time,1,10)<='$TX_PREV_DATE'
and substr(pay_time,1,10)>='$TX_10B_DATE'
group by substr(pay_time,1,10)
union all 
--金条还款表
select substr(paytime,1,10)                                                     as repay_date
      ,'金条'                                                               as flag 
      ,sum(payedloanamount+payeddayamount+payedoveramount+payedplanamount)  as payed_amount
     from odm.ODM_CF_JT_REPAYMENTDETAIL_0000_S_D
where dt='$TX_PREV_DATE'
 and to_date(createdate)<='$TX_PREV_DATE' 
 and payenum not in (147,567)
 and (syscode=1 and bizcode=13)
and to_date(paytime)<='$TX_PREV_DATE'
and to_date(paytime)>='$TX_10B_DATE'
group by substr(paytime,1,10)
;

use dmf_dev;
------拆分消费金融 白条、金条还款额占比
drop table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_151;
create table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_151
as 
select 
     tt.repay_date 
    ,sum(case when flag='白条'  then payed_amount else 0 end)/sum(payed_amount) as bt_rate   --白条比例
    ,sum(case when flag='金条'  then payed_amount else 0 end)/sum(payed_amount) as jt_rate   --金条比例
 from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_15 tt
 group by tt.repay_date
 ;
)
;


$SQL_BUFF[16]=qq(
set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_16;
use dmf_dev;
------拆分消费金融 白条、金条还款额占比
drop table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_16;
create table dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_16
as 
select 
   dim_day_id                                  --统计日期维度
  ,mer_id                                      --二级商户号id
  ,trade_type                                  --清结算系统交易类型
  ,trade_name                                  --交易类型名称
  ,product_id                                  --产品编号
  ,pay_tools                                   --支付工具
  ,mer_date                                    --商户交易日期
  ,acc_date                                    --记账日期
  ,order_amt                                   --订单总金额
  ,trade_amt                                   --支付金额                  --收单、代付、钱包、调整项（POS、封闭卡、白条还款）都用交易金额，全款结算用订单金额
  ,mer_fee                                     --商户手续费
  ,bank_fee                                    --银行成本
  ,trade_num                                   --交易笔数
  ,flag                                        --标识 
  ,bus_flag  
  ,dept_nm                                     --部门
  ,mer_big_type                                --业务大类
  ,mer_sma_type                                --业务小类
  ,mer_ownr                                    --业务归属
  ,prod_type                                   --产品类型
  ,sys_src                                     --商户来源
  ,card_type                                   --卡类型
  ,reserved1                                   --预留字段1
  ,reserved2                                   --预留字段2
  ,reserved3                                   --预留字段3
  ,reserved4                                   --预留字段4
  ,reserved5                                   --预留字段5 
  ,dt                                          --时间分区  
from 
dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_13
where mer_sma_type<>'白条还款'
union all
select 
   dim_day_id                                       --统计日期维度
  ,mer_id                                           --二级商户号id
  ,trade_type                                       --清结算系统交易类型
  ,trade_name                                       --交易类型名称
  ,product_id                                       --产品编号
  ,pay_tools                                        --支付工具
  ,mer_date                                         --商户交易日期
  ,acc_date                                         --记账日期
  ,order_amt                                        --订单总金额
  ,trade_amt                                        --支付金额                  --收单、代付、钱包、调整项（POS、封闭卡、白条还款）都用交易金额，全款结算用订单金额
  ,mer_fee                                          --商户手续费
  ,bank_fee*t2.bt_rate          as bank_fee         --银行成本
  ,trade_num                                        --交易笔数
  ,flag                                             --标识 
  ,concat(bus_flag,'-白条还款') as bus_flag  
  ,dept_nm                                          --部门
  ,'白条'                       as mer_big_type     --业务大类
  ,mer_sma_type                                     --业务小类
  ,mer_ownr                                         --业务归属
  ,prod_type                                        --产品类型
  ,sys_src                                          --商户来源
  ,card_type                                        --卡类型
  ,reserved1                                        --预留字段1
  ,reserved2                                        --预留字段2
  ,reserved3                                        --预留字段3
  ,reserved4                                        --预留字段4
  ,reserved5                                        --预留字段5 
  ,dt                                               --时间分区  
from 
(select *from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_13
where mer_sma_type='白条还款') t1
left join dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_151 t2 
  on t1.dim_day_id=t2.repay_date
union all
select 
   dim_day_id                                       --统计日期维度
  ,mer_id                                           --二级商户号id
  ,trade_type                                       --清结算系统交易类型
  ,trade_name                                       --交易类型名称
  ,product_id                                       --产品编号
  ,pay_tools                                        --支付工具
  ,mer_date                                         --商户交易日期
  ,acc_date                                         --记账日期
  ,0                   as order_amt                 --订单总金额
  ,0                   as trade_amt                 --支付金额                  --收单、代付、钱包、调整项（POS、封闭卡、白条还款）都用交易金额，全款结算用订单金额
  ,0                   as mer_fee                   --商户手续费
  ,bank_fee*t2.jt_rate as bank_fee                  --银行成本
  ,0                   as trade_num                 --交易笔数
  ,flag                                             --标识 
  ,concat(bus_flag,'-白条还款') as bus_flag  
  ,dept_nm                                          --部门
  ,'金条'                       as mer_big_type     --业务大类
  ,mer_sma_type                                     --业务小类
  ,mer_ownr                                         --业务归属
  ,prod_type                                        --产品类型
  ,sys_src                                          --商户来源
  ,card_type                                        --卡类型
  ,reserved1                                        --预留字段1
  ,reserved2                                        --预留字段2
  ,reserved3                                        --预留字段3
  ,reserved4                                        --预留字段4
  ,reserved5                                        --预留字段5 
  ,dt                                               --时间分区  
from 
(select *from dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_13
where mer_sma_type='白条还款') t1
left join dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_151 t2 
  on t1.dim_day_id=t2.repay_date
  ;
)
;

$SQL_BUFF[17]=qq(
set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_17;
use dmf_bi;
insert overwrite table dmfbi_profits_pay_sfjh_pay_cnt_data_dtl_inds_i_d partition (dt)
select 
   dim_day_id                                  --统计日期维度
  ,mer_id                                      --二级商户号id
  ,trade_type                                  --清结算系统交易类型
  ,trade_name                                  --交易类型名称
  ,product_id                                  --产品编号
  ,pay_tools                                   --支付工具
  ,mer_date                                    --商户交易日期
  ,acc_date                                    --记账日期
  ,sum(nvl(order_amt,0))      as order_amt     --订单总金额
  ,sum(nvl(trade_amt,0))      as trade_amt     --支付金额                  --收单、代付、钱包、调整项（POS、封闭卡、白条还款）都用交易金额，全款结算用订单金额
  ,sum(nvl(mer_fee,0))        as mer_fee       --商户手续费
  ,sum(nvl(bank_fee,0))       as bank_fee      --银行成本
  ,sum(nvl(trade_num,0))      as trade_num     --交易笔数
  ,flag                                        --标识 
  ,bus_flag  
  ,dept_nm                                     --部门
  ,mer_big_type                                --业务大类
  ,mer_sma_type                                --业务小类
  ,mer_ownr                                    --业务归属
  ,prod_type                                   --产品类型
  ,sys_src                                     --商户来源
  ,card_type                                   --卡类型
  ,reserved1                                   --预留字段1
  ,reserved2                                   --预留字段2
  ,reserved3                                   --预留字段3
  ,reserved4                                   --预留字段4
  ,reserved5                                   --预留字段5   
  ,dt
from 
dmf_dev.dyt_dmf_dev_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_16
group by 
 dim_day_id  
,mer_id      
,trade_type  
,trade_name  
,product_id  
,pay_tools   
,mer_date    
,acc_date
,flag
,bus_flag            
,dept_nm         
,mer_big_type    
,mer_sma_type    
,mer_ownr        
,prod_type           
,sys_src
,card_type
,reserved1
,reserved2
,reserved3
,reserved4
,reserved5
,dt       
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

