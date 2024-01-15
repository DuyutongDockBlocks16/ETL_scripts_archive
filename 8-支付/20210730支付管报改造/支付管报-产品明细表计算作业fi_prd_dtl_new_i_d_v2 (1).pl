#!/usr/bin/perl
########################################################################################################################
#  Creater        :
#  Creation Time  :
#  Description    :caixue
#  Modify By      :R2020121473721新增业务由于交易额重复需要剔除，枚举值product_id='BANKAGENTPAY'对应的交易额剔除，支付产品明细表及管报集市的交易额数据，需要剔除
#  Modify Time    :2021-02-03
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

 $SQL_BUFF[0]=qq(
            
        
      --  由于支付管理业务线更名，把涉及“商城”字样更新为“集团”字样，因此支付产品明细表中名称调整如下：
      --  1、  业务大类“支付商城业务”全部更改为 “支付集团业务”
      --  2、  业务小类为“线上-商城C端”“线上-商城B端”“线上-商城C端-全球购”全部更改为“线上C端”“ 线上B端”“ 线上C端-全球购”。


        set mapred.max.split.size=64000000; 

        use dmf_tmp;
        drop table dmf_tmp.tmp_zfgb_update_a;
        create table dmf_tmp.tmp_zfgb_update_a
        as 
        select
        joinkey,
        change_code,
        id,
        trade_date,
        trans_dt,
        trade_category,
        mer_id,
        mer_name,
        trade_type,
        pay_tools,
        card_type,
        order_amt,
        case when product_id='BANKAGENTPAY' then 0 else trade_amt end trade_amt,
        mer_fee,
        bank_fee,
        cal_cost,
        case when product_id='BANKAGENTPAY' then 0 else trade_num end trade_num,
        product_id,
        created_date,
        source,
        modified_date,
        owner,
        ou,
        ou_name,
        service_type,
        department, 
        case when service_owner = '支付商城业务' then '支付集团业务' else service_owner end as service_owner,
        case when service_category = '线上-商城C端' then '线上C端' 
           when service_category = '线上-商城B端' then '线上B端' 
           when service_category = '线上-商城C端-全球购' then '线上C端-全球购'
          else service_category end as service_category,
        product_type,
        bank_mer_id,
        bank_mer_name,
        send_bank_code,
        recv_bank_code,
        create_dt,
        dt,
        data_type
        from dmf_bc.dmfbc_mrpt_pay_fi_zfgb_transaction_i_d
        where dt='$TX_DATE';

        
        use dmf_tmp;
        drop table if exists dmf_tmp.tmp_sis_account_result_01;
        create table dmf_tmp.tmp_sis_account_result_01
        as
        select
          data_type,    
          trade_date,
          trade_category,
          trade_type,
          mer_id,
          mer_name,
          pay_tools,
          card_type,  
            --分类为“收单”，交易名称为“银行卡企业充值”，部门“支付业务部”。成本不为0，干掉。
          case when department='支付业务部' and trade_category='收单' and trade_type='P101' and cal_cost<>0 then 0
               when trade_category='收单' and trade_type='P101' and cal_cost=0 then 0
               when trade_category='代付' and trade_type='T103' and cal_cost=0 then 0
               when trade_category='钱包' and pay_tools='ACCT' and mer_fee=0 and mer_id<>'110583859001' then 0
               when trade_category='收单' and trade_type='P102' and mer_id='22843769' then 0
               when pay_tools='CUPN' and mer_fee=0 then 0
               when department = '支付业务部' and service_owner='生活应用' and pay_tools = 'TRAN' then 0
          else order_amt end as order_amt,
          case when trade_category='收单' and trade_type='P101' and cal_cost=0 then 0
               when trade_category='代付' and trade_type='T103' and cal_cost=0 then 0
               when trade_category='钱包' and pay_tools='ACCT' and mer_fee=0 and mer_id<>'110583859001' then 0
               when trade_category='收单' and trade_type='P102' and mer_id='22843769' then 0
               when  pay_tools='CUPN' and mer_fee=0 then 0
          else trade_amt end as trade_amt,
          mer_fee,
          case when trade_category='收单' and trade_type='P101' and cal_cost=0 then 0
               when trade_category='代付' and trade_type='T103' and cal_cost=0 then 0
               when trade_category='钱包' and pay_tools='ACCT' and mer_fee=0 and mer_id<>'110583859001' then 0
               when trade_category='收单' and trade_type='P102' and mer_id='22843769' then 0
               when  pay_tools='CUPN' and mer_fee=0 then 0
          else bank_fee end as bank_fee,
          case when trade_category='收单' and trade_type='P101' and cal_cost=0 then 0
               when trade_category='代付' and trade_type='T103' and cal_cost=0 then 0
               when trade_category='钱包' and pay_tools='ACCT' and mer_fee=0 and mer_id<>'110583859001' then 0
               when trade_category='收单' and trade_type='P102' and mer_id='22843769' then 0
               when  pay_tools='CUPN' and mer_fee=0 then 0
          else cal_cost end as cal_cost,
          case when department='支付业务部' and trade_category='收单' and trade_type='P101' and cal_cost<>0 then 0 
               when trade_category='收单' and trade_type='P101' and cal_cost=0 then 0
               when trade_category='代付' and trade_type='T103' and cal_cost=0 then 0
               when trade_category='钱包' and pay_tools='ACCT' and mer_fee=0 and mer_id<>'110583859001' then 0
               when trade_category='收单' and trade_type='P102' and mer_id='22843769' then 0
               when  pay_tools='CUPN' and mer_fee=0 then 0
               when department = '支付业务部' and service_owner='生活应用' and pay_tools = 'TRAN' then 0
          else trade_num end as trade_num,
          source,
          owner,
          ou_name,
          case when trade_type in('W100','U100','D104') then '退单' else '正单' end as zt_flag,
          department,
          case when service_owner = '支付商城业务' then '支付集团业务'
               when  department='支付业务部' and service_owner='外部收单-线上' and service_category='线上-通道业务' and pay_tools  in('WECHAT','ALIPAY','CARD')  then '外部收单-线下' else service_owner end as service_owner,
          case when service_category = '线上-商城C端' then '线上C端' 
               when service_category = '线上-商城B端' then '线上B端' 
               when service_category = '线上-商城C端-全球购' then '线上C端-全球购' 
               when  department='支付业务部' and service_owner='外部收单-线上' and service_category='线上-通道业务' and pay_tools  in('WECHAT','ALIPAY','CARD') then '牌照收单' else service_category end as service_category,
          case when  department='支付业务部' and service_owner='外部收单-线上' and service_category='线上-通道业务' and pay_tools  in('WECHAT','ALIPAY','CARD') then '线下C端' else product_type end as product_type,
          bank_mer_id,
          bank_mer_name,
          send_bank_code,
          recv_bank_code
        from dmf_tmp.tmp_zfgb_update_a
        where 1=1 
        and trade_date<>'00000000'
        and trade_date>='20190101'
        and source='result';
        
    );;;

 $SQL_BUFF[1]=qq(
            
        set mapred.job.name=fi_prd_dtl_i_d_01;

        -- 在银河上，创建临时表默认是 orc列式存储，会进行压缩，如果字段值为null，则在读取时会报错
        -- 所以先创建表指定textfile格式，再重新插入

        use dmf_tmp;
        drop table if exists dmf_tmp.tmp_sis_account_result_02;
        create table dmf_tmp.tmp_sis_account_result_02
        (`trade_date` string,
          `trade_category` string,
          `trade_type` string,
          `mer_id` string,
          `mer_name` string,
          `pay_tools` string,
          `card_type` string,
          `order_amt` decimal(30,12),
          `trade_amt` decimal(30,12),
          `mer_fee` decimal(30,12),
          `bank_fee` decimal(30,12),
          `cal_cost` decimal(30,12),
          `trade_num` decimal(30,12),
          `owner` string,
          `ou_name` string,
          `zt_flag` string,
          `department` string,
          `service_owner` string,
          `service_category` string,
          `product_type` string,
          `bank_mer_id` string,
          `bank_mer_name` string,
          `send_bank_code` string,
          `recv_bank_code` string)
        stored as textfile;

        insert into dmf_tmp.tmp_sis_account_result_02
        select
          trade_date,
          trade_category,
          trade_type,--交易名称
          mer_id,
          mer_name,
          case when pay_tools='CUPN' then 'EXPR' else pay_tools end as pay_tools,
          card_type,
          case when pay_tools='CUPN' then 0 
               when zt_flag='正单' and data_type=1 then  abs(order_amt)
               when zt_flag='正单' and data_type=2 then (-1)*abs(order_amt) --冲销数据，传一个与有效数据相反的值过去
               when zt_flag='退单' and data_type=1 then (-1)*abs(order_amt)
               when zt_flag='退单' and data_type=2 then  abs(order_amt) --冲销数据，传一个与有效数据相反的值过去 
          else order_amt end as order_amt,
          trade_amt,
          mer_fee,
          bank_fee,
          cal_cost,
          case when zt_flag='正单' and data_type=1 then  abs(trade_num)
               when zt_flag='正单' and data_type=2 then (-1)*abs(trade_num) --冲销数据，传一个与有效数据相反的值过去
               when zt_flag='退单' and data_type=1 then (-1)*abs(trade_num)
               when zt_flag='退单' and data_type=2 then  abs(trade_num) --冲销数据，传一个与有效数据相反的值过去
          else trade_num end as trade_num, 
          owner,
          ou_name,
          zt_flag,
        case  when product_type='POP商户' and mer_fee<>0      then '支付业务部' 
              else department
         end as  department,
        case  when product_type='POP商户' and mer_fee<>0      then '外部收单-线上'       
                else service_owner
         end as  service_owner,
        case when product_type='POP商户' and mer_fee<>0     then '线上-通道业务' 
               else service_category
         end as  service_category,
        case  when product_type='POP商户' and mer_fee<>0    then '线上-通道业务-POP代付有收入' 
               else product_type
        end as product_type,
        bank_mer_id,
        bank_mer_name,
        send_bank_code,
        recv_bank_code
        from dmf_tmp.tmp_sis_account_result_01;


    );;;
$SQL_BUFF[2]=qq(
            
        set mapred.job.name=fi_prd_dtl_i_d_02;
        use dmf_tmp;
        drop table if exists dmf_tmp.tmp_sis_account_result_04;
        create table dmf_tmp.tmp_sis_account_result_04
        as
        select
          trade_date,
          trade_category,
          trade_type,--交易名称
          mer_id,
          mer_name,
          pay_tools,
          card_type,
          case when trade_type='T101' and department='支付业务部' and service_owner<>'支付集团业务' then 0 
               else order_amt end as order_amt,
          trade_amt,
          mer_fee,
          bank_fee,
          cal_cost,
          case when trade_type='T101' and department='支付业务部' and service_owner<>'支付集团业务' then 0 
             else trade_num end as trade_num,
          owner,
          ou_name,
          zt_flag,
          department,
          service_owner,
          service_category,
          product_type,
          bank_mer_id,
          bank_mer_name,
          send_bank_code,
          recv_bank_code
        from dmf_tmp.tmp_sis_account_result_02
        where pay_tools<>'JIOU'
        union all
        select
          trade_date,
          trade_category,
          trade_type,--交易名称
          mer_id,
          mer_name,
          pay_tools,
          card_type,
        -- ⑦支付工具为“京东白条”， 部门“支付业务部”、业务大类“跨境业务”，以及业务大类“支付集团业务”，业务分类“线上C端-全球购”之外。
          case when department='支付业务部' and service_owner='跨境业务' then order_amt
               when department='支付业务部' and service_owner='支付集团业务' and service_category='线上C端-全球购' then order_amt
               when department<>'支付业务部' then order_amt
          else 0 end as order_amt,
          trade_amt,
          mer_fee,
          bank_fee,
          cal_cost,
         -- ⑦支付工具为“京东白条”， 部门“支付业务部”、业务大类“跨境业务”，以及业务大类“支付集团业务”，业务分类“线上C端-全球购”之外。
          case when department='支付业务部' and service_owner='跨境业务' then trade_num
               when department='支付业务部' and service_owner='支付集团业务' and service_category='线上C端-全球购' then trade_num
               when department<>'支付业务部' then trade_num
          else 0 end as trade_num,
          owner,
          ou_name,
          zt_flag,
          department,
          service_owner,
          service_category,
          product_type,
          bank_mer_id,
          bank_mer_name,
          send_bank_code,
          recv_bank_code
        from dmf_tmp.tmp_sis_account_result_02
        where pay_tools='JIOU';

        
    );;;

  $SQL_BUFF[3]=qq(
            
        set mapred.job.name=fi_prd_dtl_i_d_03;
        use dmf_tmp;
        drop table if exists dmf_tmp.tmp_sis_account_result_05;
        create table dmf_tmp.tmp_sis_account_result_05
        as
        select
          trade_date,
          trade_category,
          trade_type,
          mer_id,
          mer_name,
          pay_tools,
          card_type,
          order_amt,  
          trade_amt,
          mer_fee,
          bank_fee,
          cal_cost,
          trade_num,
          zt_flag,
          case  when mer_id='110583859001' then '支付业务部'     else department     end as department,
          case  when mer_id='110583859001' then '支付集团业务'    else service_owner  end as service_owner,
          case when department='支付业务部' and service_owner = '支付集团业务' and pay_tools in ('TRAN','NETB') then '线上B端'
               when department='支付业务部' and service_owner = '支付集团业务' and service_category = '线上C端' and mer_id = '110194601001' then '线上B端'
              -- when service_category='线上C端' then '线上-外部C端'
               when mer_id='110583859001' then '线上B端'
               else service_category end as service_category,
          product_type,
          bank_mer_id,
          bank_mer_name,
          send_bank_code,
          recv_bank_code,
          ou_name
        from dmf_tmp.tmp_sis_account_result_04
        union all
        select
          trade_date,
          trade_category,
          trade_type,
          mer_id,
          mer_name,
          '代付'             as pay_tools,
          card_type,
          order_amt*(-1) as order_amt,
          trade_amt*(-1) as trade_amt,
          0 as mer_fee,
          bank_fee*(-1) as bank_fee,
          cal_cost*(-1) as cal_cost,
          0 as trade_num,
          zt_flag,
          '财富管理部'      as department,
          '小金库'          as service_owner,
          '小金库' as service_category,
          '京东金融-小金库' as product_type,
          bank_mer_id,
          bank_mer_name,
          send_bank_code,
          recv_bank_code,
          ou_name
        from dmf_tmp.tmp_sis_account_result_04
        where trade_category='钱包'
        and pay_tools='XJK';

        
    );;;

 $SQL_BUFF[4]=qq(
            
        set mapred.job.name=fi_prd_dtl_i_d_04;

        use dmf_tmp;
        drop table if EXISTS dmf_tmp.sis_gross_settle_report_01;
        create table dmf_tmp.sis_gross_settle_report_01
        as 
        select
         trade_date,
         mer_id,
         mer_name,
         case when pay_tools='CUPN' then 'EXPR' else pay_tools end as pay_tools,
         card_type,
         owner,
         ou_name,
         ou,
         trade_amt,
         mer_fee,
         trade_num,
         case  when mer_id='110583859001' then '支付业务部'
             when product_type='POP商户' and mer_fee<>0      then '支付业务部' 
             else department   end as department,
         case  when mer_id='110583859001' then '支付集团业务'  
             when product_type='POP商户' and mer_fee<>0      then '外部收单-线上' 
         else service_owner  end as service_owner,
         case when department='支付业务部' and service_owner = '支付集团业务' and pay_tools in ('TRAN','NETB') then '线上B端'
               when department='支付业务部' and service_owner = '支付集团业务' and service_category = '线上C端' and mer_id = '110194601001' then '线上B端'
             --  when service_category='线上C端' then '线上-外部C端'
               when mer_id='110583859001' then '线上B端'
               when product_type='POP商户' and mer_fee<>0     then '线上-通道业务' 
               else service_category end as service_category,
         case  when product_type='POP商户' and mer_fee<>0    then '线上-通道业务-POP代付有收入' 
         else product_type end as product_type,
          bank_mer_id,
          bank_mer_name,
          send_bank_code,
          recv_bank_code,
         case when trade_category in('代付退票','退单') then '退单' else '正单' end as zt_flag,
         data_type,
         case when trade_category in('代付','代付退票') then '代付' else '收单' end as trade_category
        from dmf_tmp.tmp_zfgb_update_a
        where 1=1 
        and source='report'
        and owner<>'京东商城';

        
    );;;
$SQL_BUFF[5]=qq(
            
        set mapred.job.name=fi_prd_dtl_i_d_05;
        use dmf_tmp;
        drop table if exists dmf_tmp.tmp_zhifu_guanbao_result_report_01;
        create table dmf_tmp.tmp_zhifu_guanbao_result_report_01
        as
        select
          trade_date,
          mer_id,
          mer_name,
          trade_category,
          pay_tools,
          card_type,
          order_amt,
          trade_amt,
          mer_fee,
          bank_fee,
          cal_cost,
          trade_num,
          zt_flag,
          department,
          service_owner,
          service_category,
          product_type,
          bank_mer_id,
          bank_mer_name,
          send_bank_code,
          recv_bank_code,
          ou_name,
          '账扣' as source_flag
        from dmf_tmp.tmp_sis_account_result_05
        union all
        select
          trade_date,
          mer_id,
          mer_name,
          trade_category,
          pay_tools,
          card_type,
          0 as order_amt,
          0 as trade_amt,
          mer_fee,
          0 as bank_fee,
          0 as cal_cost,
          0 as trade_num,
          zt_flag,
          department,
          service_owner,
          service_category,
          product_type,
          bank_mer_id,
          bank_mer_name,
          send_bank_code,
          recv_bank_code,
          ou_name,
          '后收' as source_flag
        from dmf_tmp.sis_gross_settle_report_01;
        
    );;;



    # 计算商城比例，老模型下线，采用新模型
        # use dmf_tmp;
        # drop table dmf_tmp.tmp_zhifu_guanbao_sc_rate;
        # create table dmf_tmp.tmp_zhifu_guanbao_sc_rate
        # as  
        # select
        # days 
        # ,sum(case when in_out='商城' then ord_amount end)/sum(ord_amount) as sc_rate   --商城比例
        # ,sum(case when in_out='外单' and on_off1='线上' then ord_amount end)/sum(ord_amount) as on_rate   --线上比例
        # ,sum(case when in_out='外单' and on_off1='线下' then ord_amount end)/sum(ord_amount) as off_rate  --线下比例
        # from dmf_bc.DMFBC_BC_WD_ORDR_FIN_S_D a
        # where dt= '$TX_DATE'
        # and days>='2020-01-01'
        # and days<='$TX_DATE'
        # and paytype in ('JDQP')
        # and re_order_id is null
        # group by days;


 $SQL_BUFF[7]=qq(
            
        set mapred.job.name=fi_prd_dtl_i_d_07;

        use dmf_tmp;
        drop table if exists dmf_tmp.tmp_tx_ordr_dtl_sum_a;
        create table dmf_tmp.tmp_tx_ordr_dtl_sum_a
        as
        select  
        case when  sec_merchant_no   in('110333663001','110333663002','110333663003','110333663012','104100548991020','104100554111402','104100554111403') 
              or   sec_merchant_nm like '%京东商城%' then '商城' else '外单' end as in_out,
        case when online_offline_code ='online' then '线上' else '线下' end as on_off1,
        dt,
        sum(pay_order_amt) as ord_amount
        from idm.idm_f02_pay_wd_tx_ordr_dtl_i_d  where dt >= '2020-02-01'
        and dt <= '{TX_DATE}'
         and prod_code like '%JDQP%'  --交易类型
        and refund_flag=0 --无退款
        group by case when sec_merchant_no   in('110333663001','110333663002','110333663003','110333663012','104100548991020','104100554111402','104100554111403') or  sec_merchant_nm like '%京东商城%' then '商城' else '外单' end ,
        case when online_offline_code ='online' then '线上' else '线下' end,dt;


        drop table dmf_tmp.tmp_zhifu_guanbao_sc_rate;
        create table dmf_tmp.tmp_zhifu_guanbao_sc_rate
        as  
        select 
        dt as days
        ,sum(case when in_out='商城' then ord_amount end)/sum(ord_amount) as sc_rate   --商城比例
        ,sum(case when in_out='外单' and on_off1='线上' then ord_amount end)/sum(ord_amount) as on_rate   --线上比例
        ,sum(case when in_out='外单' and on_off1='线下' then ord_amount end)/sum(ord_amount) as off_rate  --线下比例
        from  dmf_tmp.tmp_tx_ordr_dtl_sum_a
        group by dt;


        
    );;;
$SQL_BUFF[8]=qq(
            
        set mapred.job.name=fi_prd_dtl_i_d_08;
        use dmf_tmp;
        drop table if exists dmf_tmp.tmp_zhifu_guanbao_result_report_02;
        create table dmf_tmp.tmp_zhifu_guanbao_result_report_02
        as
        select
          trade_date,
          mer_id,
          mer_name,
          trade_category,
          pay_tools,
          card_type,
          case when  department ='支付业务部'  and service_owner='外部发卡-银联'  and service_category='外部发卡-银联' and product_type='京东支付-银联闪付' 
          then order_amt*(nvl(rate.sc_rate,0.0)) 
          else order_amt end as order_amt,
          case when  department ='支付业务部'  and service_owner='外部发卡-银联'  and service_category='外部发卡-银联' and product_type='京东支付-银联闪付' 
          then trade_amt*(nvl(rate.sc_rate,0.0)) 
          else trade_amt end as trade_amt,
          case when  department ='支付业务部'  and service_owner='外部发卡-银联'  and service_category='外部发卡-银联' and product_type='京东支付-银联闪付' 
          then mer_fee*(nvl(rate.sc_rate,0.0)) 
          else mer_fee end as mer_fee, 
          case when  department ='支付业务部'  and service_owner='外部发卡-银联'  and service_category='外部发卡-银联' and product_type='京东支付-银联闪付' 
          then bank_fee*(nvl(rate.sc_rate,0.0)) 
          else bank_fee end as bank_fee,
          case when  department ='支付业务部'  and service_owner='外部发卡-银联'  and service_category='外部发卡-银联' and product_type='京东支付-银联闪付' 
          then cal_cost*(nvl(rate.sc_rate,0.0)) 
          else cal_cost end as cal_cost,
          case when  department ='支付业务部'  and service_owner='外部发卡-银联'  and service_category='外部发卡-银联' and product_type='京东支付-银联闪付' 
          then trade_num*(nvl(rate.sc_rate,0.0)) 
          else trade_num end as trade_num,
          zt_flag,
          department,
          case when  department ='支付业务部'  and service_owner='外部发卡-银联'  and service_category='外部发卡-银联' and product_type='京东支付-银联闪付' 
          then '支付集团业务' else service_owner end as service_owner,
          case when  department ='支付业务部'  and service_owner='外部发卡-银联'  and service_category='外部发卡-银联' and product_type='京东支付-银联闪付' 
          then '线上C端' else service_category end as service_category,
          case when  department ='支付业务部'  and service_owner='外部发卡-银联'  and service_category='外部发卡-银联' and product_type='京东支付-银联闪付' 
          then '银联闪付规模在商城和C端线上&线下之间调整' else product_type end as product_type,
          bank_mer_id,
          bank_mer_name,
          send_bank_code,
          recv_bank_code,
          ou_name,
          source_flag
        from  dmf_tmp.tmp_zhifu_guanbao_result_report_01 report
        left join dmf_tmp.tmp_zhifu_guanbao_sc_rate rate
        on from_unixtime(unix_timestamp(report.trade_date,'yyyymmdd'),'yyyy-mm-dd')=rate.days;

        insert into  dmf_tmp.tmp_zhifu_guanbao_result_report_02
        select
          trade_date,
          mer_id,
          mer_name,
          trade_category,
          pay_tools,
          card_type,
          order_amt*(1-nvl(rate.sc_rate,0.0)),
          trade_amt*(1-nvl(rate.sc_rate,0.0)),
          mer_fee*(1-nvl(rate.sc_rate,0.0)),
          bank_fee*(1-nvl(rate.sc_rate,0.0)),
          cal_cost*(1-nvl(rate.sc_rate,0.0)),
          trade_num*(1-nvl(rate.sc_rate,0.0)),
          zt_flag,
          department,
          service_owner,
          service_category,
          product_type,
          bank_mer_id,
          bank_mer_name,
          send_bank_code,
          recv_bank_code,
          ou_name,
          source_flag
         from  dmf_tmp.tmp_zhifu_guanbao_result_report_01 report
         left join dmf_tmp.tmp_zhifu_guanbao_sc_rate rate
         on from_unixtime(unix_timestamp(report.trade_date,'yyyymmdd'),'yyyy-mm-dd')=rate.days
        where department ='支付业务部'  and service_owner='外部发卡-银联'  and service_category='外部发卡-银联' and product_type='京东支付-银联闪付';

        -- 按维度汇总
        -- department,service_owner,service_category,product_type,zt_flag,pay_tools,trade_category,card_type,ou_name,trade_date, 
        --  bank_mer_id,
        --  bank_mer_name,
        --  send_bank_code,
        --  recv_bank_code,source_flag

        use dmf_tmp;
        drop table if exists dmf_tmp.tmp_prd_dtl_group_all_a;
        create table dmf_tmp.tmp_prd_dtl_group_all_a
        as
        select 
        trade_date,
        mer_id,
        mer_name,
        department,
        service_owner,
        service_category,
        product_type,
        zt_flag,
        CASE    
            WHEN pay_tools = 'ACCT'   THEN '账户余额'
            WHEN pay_tools = 'ALIPAY' THEN '支付宝'
            WHEN pay_tools = 'AUTH'   THEN '预授权'
            WHEN pay_tools = 'B2CN'   THEN '网关内卡'  --网关内卡？
            WHEN pay_tools = 'CARD'   THEN '银行卡'
            WHEN pay_tools = 'COLL'   THEN '代扣'
            WHEN pay_tools = 'CUPN'   THEN '优惠劵'
            WHEN pay_tools = 'ENFL'   THEN '企业充值'
            WHEN pay_tools = 'EXPR'   THEN '快捷支付'
            WHEN pay_tools = 'GJK'    THEN '国际卡'
            WHEN pay_tools = 'JINCAI' THEN '金采'
            WHEN pay_tools = 'JIOU'   THEN '京东白条'
            WHEN pay_tools = 'JQH'    THEN '借钱花'
            WHEN pay_tools = 'NETB'   THEN 'B2B业务'
            WHEN pay_tools = 'POST'   THEN '邮政汇款'
            WHEN pay_tools = 'TRAN'   THEN '代付'  --代付/付款
            WHEN pay_tools = 'WECHAT' THEN '微信'
            WHEN pay_tools = 'XJK'    THEN '小金库'
            WHEN pay_tools = 'XJKLC'  THEN '小金库理财'
            WHEN pay_tools = 'PPCD'   THEN '预付费卡'
            ELSE pay_tools 
        END AS pay_tools,
        round(sum(order_amt)/100,2) as trade_amt,
        round(sum(trade_num),2) as trade_num,
        round(sum(mer_fee/1.06/100),2) as income_without_tax,
        round(sum(cal_cost/1.06/100),2) as cost_without_tax,
        'cwmart' as maker,
        trade_category,
        card_type,
        ou_name,
        bank_mer_id,
        bank_mer_name,
        send_bank_code,
        recv_bank_code,
        source_flag
        from dmf_tmp.tmp_zhifu_guanbao_result_report_02
        group by mer_id,mer_name,department,service_owner,service_category,product_type,zt_flag,pay_tools,trade_category,card_type,ou_name,trade_date,bank_mer_id,bank_mer_name,send_bank_code,recv_bank_code,source_flag;

        
    );;;

  $SQL_BUFF[9]=qq(
            
        set mapred.job.name=fi_prd_dtl_i_d_09;
        insert OVERWRITE table dmf_bc.dmfbc_mrpt_pay_fi_prd_dtl_cus_i_d PARTITION (dt='$TX_DATE')
        select
        trade_date,
        department,
        service_owner,
        service_category,
        product_type,
        zt_flag,
        pay_tools,
        round(sum(trade_amt),2) as trade_amt,
        round(sum(trade_num),2) as trade_num,
        round(sum(income_without_tax),2) as income_without_tax,
        round(sum(cost_without_tax),2) as cost_without_tax,
        maker,
        trade_category,
        card_type,
        ou_name
        from dmf_tmp.tmp_prd_dtl_group_all_a
        group by department,service_owner,service_category,product_type,zt_flag,pay_tools,trade_category,card_type,ou_name,trade_date,maker;



        set mapred.job.name=fi_prd_dtl_i_d_10;
        insert OVERWRITE table dmf_bc.dmfbc_mrpt_pay_fi_prd_dtl_i_d PARTITION (dt='$TX_DATE')
        select
        trade_date,
        department,
        service_owner,
        service_category,
        product_type,
        zt_flag,
        pay_tools,
        sum(trade_amt) as trade_amt,
        sum(trade_num) as trade_num,
        sum(income_without_tax) as income_without_tax,
        sum(cost_without_tax) as cost_without_tax,
        maker,
        trade_category
        from dmf_bc.dmfbc_mrpt_pay_fi_prd_dtl_cus_i_d
        where dt='$TX_DATE'
        group by department,service_owner,service_category,product_type,zt_flag,pay_tools,trade_category,trade_date,maker;

        
    );;;

 $SQL_BUFF[10]=qq(

        set mapred.job.name=fi_prd_dtl_i_d_10;

            
        -- 关联业务线编码表
        -- 支付业务部，按之前方式关联，其他业务部，只关联到部门即可

        -- 第一步关联，即业务大类和业务小类都能匹配到的情况
        use dmf_tmp;
        drop table if exists dmf_tmp.tmp_prd_dtl_group_manage_code_pay_01;
        create table dmf_tmp.tmp_prd_dtl_group_manage_code_pay_01
        as
        select 
        a.*,
        b.business_manage_code as business_manage_code,
        case when b.business_manage_code is null then a.service_owner else a.service_category end as business_manage_name
        from dmf_tmp.tmp_prd_dtl_group_all_a a 
        left join dmf_add.dmfadd_add_pay_sis_business_manage_code_a_d b
        on a.department=b.department
        and a.service_owner = b.service_owner
        and a.service_category = b.service_category;
        


        -- 第二步关联，如果 第一步没有关联上，则分支付业务部和其他事业部

        use dmf_tmp;
        drop table if exists dmf_tmp.tmp_prd_dtl_group_manage_code_a;
        create table dmf_tmp.tmp_prd_dtl_group_manage_code_a
        as
        select 
        a.trade_date,
        a.mer_id,
        a.mer_name,
        a.department,
        a.service_owner,
        a.service_category,
        a.product_type,
        a.zt_flag,
        a.pay_tools,
        a.trade_amt,
        a.trade_num,
        a.income_without_tax,
        a.cost_without_tax,
        a.maker,
        a.trade_category,
        a.card_type,
        a.ou_name,
        a.bank_mer_id,
        a.bank_mer_name,
        a.send_bank_code,
        a.recv_bank_code,
        a.source_flag,
        case when  b.business_manage_code is null then 'YWX.992.08.19'
        else b.business_manage_code end as business_manage_code,
        a.business_manage_name
        from dmf_tmp.tmp_prd_dtl_group_manage_code_pay_01 a 
        left join dmf_add.dmfadd_add_pay_sis_business_manage_code_a_d b
        on a.department=b.department
        and a.service_owner = b.service_owner
        where a.business_manage_code is null
        and a.department = '支付业务部'
        union all
        select 
        a.trade_date,
        a.mer_id,
        a.mer_name,
        a.department,
        a.service_owner,
        a.service_category,
        a.product_type,
        a.zt_flag,
        a.pay_tools,
        a.trade_amt,
        a.trade_num,
        a.income_without_tax,
        a.cost_without_tax,
        a.maker,
        a.trade_category,
        a.card_type,
        a.ou_name,
        a.bank_mer_id,
        a.bank_mer_name,
        a.send_bank_code,
        a.recv_bank_code,
        a.source_flag,
        case when  b.business_manage_code is null then 'YWX.992.08.19'
        else b.business_manage_code end as business_manage_code,
        a.business_manage_name
        from dmf_tmp.tmp_prd_dtl_group_manage_code_pay_01 a 
        left join dmf_add.dmfadd_add_pay_sis_business_manage_code_a_d b
        on a.department=b.service_owner
        where a.business_manage_code is null
        and a.department <> '支付业务部'
        union all
        select * from dmf_tmp.tmp_prd_dtl_group_manage_code_pay_01
        where business_manage_code is not null;



        
    );;;
$SQL_BUFF[11]=qq(
        
      set mapred.job.name=fi_prd_dtl_i_d_11;

      insert overwrite table dmf_rpt.dmfrpt_mrpt_pay_fi_prd_dtl_bank_i_d partition (dt = '$TX_DATE')
      select
        a.trade_date,
        a.department,
        a.service_owner,
        a.service_category,
        nvl(a.product_type,''),
        a.zt_flag,
        a.pay_tools,
        a.trade_amt,
        a.trade_num,
        a.income_without_tax,
        a.cost_without_tax,
        a.maker,
        a.trade_category,
        a.bank_mer_name,
        a.bank_mer_id,
        a.send_bank_code,
        a.recv_bank_code,
        a.card_type,
        a.mer_id,
        a.mer_name,
        a.source_flag,
        a.business_manage_code as manage_line_code
        from dmf_tmp.tmp_prd_dtl_group_manage_code_a a
        where a.trade_amt <> 0 or a.trade_num <> 0 or a.income_without_tax <> 0 or a.cost_without_tax <> 0;
 
    );;;

$SQL_BUFF[12]=qq(

        set mapred.job.name=fi_prd_dtl_i_d_12;
        use dmf_tmp;    
        drop  table  if exists dmf_tmp.tmp_zfgb_push_01;
        create table dmf_tmp.tmp_zfgb_push_01
        as
        select
        'JDF.09'                    as subject_manage_code,
        '不含税收入'                 as subject_manage_name,
        business_manage_code,
        business_manage_name,
        income_without_tax as  manage_report_amount,
        trade_date as  voucher_date, --凭证日期
        ou_name as supplier_name,
        concat('$TX_DATE','_产品明细表基础数据_收入') as  entry_mark,
        concat('$TX_DATE','_产品明细表基础数据_收入') as  supple_mark,
        trade_date as  business_date, --业务日期，也就是交易日期
        case when department='支付业务部' then '记' else '部门间业务重分类' end as voucher_type_name,
        case when department='支付业务部' then '01' else '58' end as  voucher_type_code,
        department,
        service_owner,
        service_category
        from dmf_tmp.tmp_prd_dtl_group_manage_code_a
        where 1=1
        and department='支付业务部'
        union all
        select
        'JDF.10'  as subject_manage_code,
        '通道成本' as subject_manage_name,
         business_manage_code,
         business_manage_name,
        cost_without_tax as  manage_report_amount,
        trade_date as  voucher_date,
        ou_name as supplier_name,
        concat('$TX_DATE','_产品明细表基础数据_成本') as  entry_mark,
        concat('$TX_DATE','_产品明细表基础数据_成本') as  supple_mark,
        trade_date as  business_date,
        case when department='支付业务部' then '记' else '部门间业务重分类' end as voucher_type_name,
        case when department='支付业务部' then '01' else '58' end as  voucher_type_code,
        department,
        service_owner,
        service_category
        from dmf_tmp.tmp_prd_dtl_group_manage_code_a
        where 1=1
        and department='支付业务部'
        --交易量
        union all
        select
        'JDF.02' as subject_manage_code,--预算科目编码
        '交易量'  as subject_manage_name,--预算科目名称
        business_manage_code,
        business_manage_name,
        trade_amt as  manage_report_amount,
        trade_date as  voucher_date,
        null as supplier_name,
        concat('$TX_DATE','_产品明细表基础数据_交易量') as  entry_mark,
        concat('$TX_DATE','_产品明细表基础数据_交易量') as  supple_mark,
        trade_date as  business_date,
        case when department='支付业务部' then '记' else '部门间业务重分类' end as voucher_type_name,
        case when department='支付业务部' then '01' else '58' end as  voucher_type_code,
        department,
        service_owner,
        service_category
        from dmf_tmp.tmp_prd_dtl_group_manage_code_a
        where 1=1
        and department='支付业务部';


    );;;


$SQL_BUFF[13]=qq(

        set mapred.job.name=fi_prd_dtl_i_d_13;
        --通用业务线编码转换
        --如果匹配到通用业务线编码，但是业务线名称不同，则会导致uuid重复，此处将匹配到通用编码的业务线名称统一转换成“数科内部其他”，
        --同时如果支付业务部的匹配到通用编码时，做记录，说明财务提供的业务线编码不全。
        insert OVERWRITE table dmf_bc.dmfbc_mrpt_pay_fi_biz_line_mntr_i_d PARTITION (dt='$TX_DATE')
        select 
        subject_manage_code,--预算科目编码
        subject_manage_name,--预算科目名称
        business_manage_code,
        case when business_manage_code='YWX-992.08.19' then '数科内部其他' else business_manage_name end as business_manage_name,
        manage_report_amount,
        voucher_date,
        supplier_name,
        entry_mark,
        supple_mark,
        business_date,
        voucher_type_name,
        voucher_type_code,
        department,
        service_owner,
        service_category
        from dmf_tmp.tmp_zfgb_push_01;


    );;;

$SQL_BUFF[14]=qq(

        set mapred.job.name=fi_prd_dtl_i_d_14;
        use dmf_tmp;
        drop  table  if exists dmf_tmp.tmp_zhifu_guanbao_push_sum;
        create table dmf_tmp.tmp_zhifu_guanbao_push_sum
        as
        select
        subject_manage_code,
        subject_manage_name,
        business_manage_code,
        business_manage_name,
        sum(manage_report_amount) as manage_report_amount,
        entry_mark,
        supple_mark,
        voucher_type_code,
        voucher_type_name,
        supplier_name,
        voucher_date,
        business_date
        from dmf_bc.dmfbc_mrpt_pay_fi_biz_line_mntr_i_d
        where dt='$TX_DATE'
        group by 
        subject_manage_code,
        subject_manage_name,
        business_manage_code,
        business_manage_name,
        entry_mark,
        supple_mark,
        voucher_type_code,
        voucher_type_name,
        supplier_name,
        voucher_date,
        business_date;




    );;;


# 2020-05-16 防重key新增 business_manage_name 字段，因为集市规则，有可能 一个业务线编码对应两个业务线名称
$SQL_BUFF[15]=qq(


        -- 备份数据
        -- use dmf_tmp;

        -- drop table if exists dmf_tmp.tmp_mrpt_pay_fi_push_to_sk_bak;
        -- create table dmf_tmp.tmp_mrpt_pay_fi_push_to_sk_bak
        -- as
        -- select * from dmf_bc.dmfbc_mrpt_pay_fi_push_to_sk_i_d
        -- where dt >='2020-04-01';

        set mapred.job.name=fi_prd_dtl_i_d_15;
        
        insert OVERWRITE table dmf_bc.dmfbc_mrpt_pay_fi_push_to_sk_i_d PARTITION (dt='$TX_DATE')
        select
        '1',
        '网银在线（北京）科技有限公司',
        '101056',
        push.subject_manage_code,
        push.subject_manage_name,
        push.business_manage_code,
        push.business_manage_name,
        dmf_bc.dmdictdesc('zfgb_department_manage','支付业务部','code'),
        dmf_bc.dmdictdesc('zfgb_department_manage','支付业务部','name'),
        null,
        null,
        null,
        push.business_manage_code as business_account_code,
        push.business_manage_name as business_account_name,
        null,
        null,
        3,
        null,
        null,
        null,
        push.manage_report_amount,
        null,
        null,
        null,
        push.voucher_date,
        null,
        null,
        null,
        null,
        case when account.supplier_code is null then 'KS999999'
        else account.supplier_code end ,
        push.supplier_name,
        null,
        null,
        null,
        null,
        null,
        push.entry_mark,
        push.supple_mark,
        'BB01',
        '人民币',
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        push.business_date,
        push.voucher_type_code,
        push.voucher_type_name,
        null,
        null,
        null,
        null,
        null,
        '$TX_DATE',
        default.getmd5('1','101056','X0000019',push.subject_manage_code,push.business_manage_code,push.business_manage_name,push.business_date,'$TX_DATE',push.supplier_name,push.voucher_type_code) as uuid,
        FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss'),
        'cwmart',
        FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss'),
        null,
        0
        from  dmf_tmp.tmp_zhifu_guanbao_push_sum push
        left join  
		(
		select * from 
			(
				select *,row_number() over(partition by supplier_name order by id) rn 
				from odm.odm_fi_eas_daily_details_account_s_d where dt='$TX_DATE'
			) a 
          where a.rn=1
		)  account
        on push.supplier_name = account.supplier_name
        where nvl(push.manage_report_amount,0)<>0;
    );;;


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

