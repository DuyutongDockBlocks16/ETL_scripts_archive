#!/usr/bin/perl
########################################################################################################################
#  Creater        :
#  Creation Time  :
#  Description    :
#  Modify By      :    zzl
#  Modify Time    :    20200205
#  Modify Content :    在每月的1号，2号，3号，4号，都把 上个月 2号至今的数据取一遍
#
#  2020-07修改，模型需要新增银行名称，银行商户号，结算行，发卡行四个维度
#  在临时表中添加 银行商户号，结算行，发卡行 三个维度，在 中间正式层表中添加 银行商户号 银行名称，结算行，发卡行 四个维度
#  同时模型需要刷数，但是考虑到每天跑数各种关联，速度太慢，将考虑使用id关联的方式刷数，其他字段不变
#    bank_mer_id string comment '银行商户号',
#    bank_mer_name    string '银行名称'
#    send_bank_code string comment '结算行',
#    recv_bank_code string comment '发卡行'
#
#
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


        -- 备份 20190101依赖的中间层数据，因为要刷数，

        set mapred.job.name=dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_00;
  
        insert OVERWRITE table dmf_tmp.dmftmp_mrpt_pay_fi_zfgb_transaction_i_d PARTITION (dt='$TX_DATE',source='result')
        select
        id,
        trade_date,
        trade_category,
        mer_id,
        mer_name,
        trade_type,
        pay_tools,
        card_type,
        order_amt,
        trade_amt,
        mer_fee,
        bank_fee,
        cal_cost,
        trade_num,
        product_id,
        created_date,
        modified_date,
        '' as owner,
        '' as ou,
        '' as ou_name,
        '' as service_type,
        '' as department,
        '' as service_owner,
        '' as service_category,
        '' as product_type,
        bank_mer_id,
        send_bank_code,
        recv_bank_code
        from (
            select *,row_number() over(partition by id order by modified_date desc) as rn 
            from odm.odm_pay_sis_fi_sis_account_result_i_d
            where dt >= 
            case when day('$TX_DATE') = 1 
                 -- in (1,2,3,4)   添加字段，在7月3号跑2号的数据时候，会造成大量数据冲销，因本月打标没有延迟，2号3号4号没必要取上月整月数据。
                 then date_add(from_unixtime(unix_timestamp(trunc(add_months('$TX_DATE',-1),'MM'),'yyyy-mm-dd'),'yyyy-mm-dd'),1)
            else '$TX_DATE' end 
            and dt <= '$TX_DATE'
        ) t where t.rn = 1;


);;;

    $SQL_BUFF[1]=qq(

        set mapred.job.name=dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_01;
        
        --全款结算报表
        insert OVERWRITE table dmf_tmp.dmftmp_mrpt_pay_fi_zfgb_transaction_i_d PARTITION (dt='$TX_DATE',source='report')
        select
        id,
        mer_date,--全款结算报表的业务日期即结算日期。此逻辑由清结算研发提供
        trade_type, --全款结算报表的trade_type实际上就是 trade_category
        mer_id,
        mer_name,
        null,
        pay_tools,
        card_type,
        0,
        trade_amt,
        mer_fee,
        trade_num,
        0,
        0,
        null,
        created_date,
        modified_date,
        null as owner,
        null as ou,
        null as ou_name,
        null as service_type,
        null as department,
        null as service_owner,
        null as service_category,
        null as product_type,
        null as bank_mer_id,
        null as send_bank_code,
        null as recv_bank_code
        from (
            select *,row_number() over(partition by id order by modified_date desc) as rn 
            from  odm.odm_pay_sis_fi_sis_gross_settle_report_i_d
            where dt >= 
            case when day('$TX_DATE') = 1 
                 -- in (1,2,3,4)   添加字段，在7月3号跑2号的数据时候，会造成大量数据冲销，因本月打标没有延迟，2号3号4号没必要取上月整月数据。
                 then date_add(from_unixtime(unix_timestamp(trunc(add_months('$TX_DATE',-1),'MM'),'yyyy-mm-dd'),'yyyy-mm-dd'),1)
                 else '$TX_DATE' 
            end 
            and dt <= '$TX_DATE'

        ) t where t.rn =1;

);;;
    

        $SQL_BUFF[2]=qq(

        set mapred.job.name=dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_02;

        use dmf_tmp;
        drop table if exists dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_mark_01;
        create table dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_mark_01
        AS
        select
        zfgb.id,
        zfgb.trade_date,
        zfgb.trade_category,
        zfgb.mer_id,
        zfgb.mer_name,
        zfgb.trade_type,
        zfgb.pay_tools,
        zfgb.card_type,
        zfgb.order_amt,
        zfgb.trade_amt,
        zfgb.mer_fee,
        zfgb.bank_fee,
        zfgb.cal_cost,
        zfgb.trade_num,
        zfgb.product_id,
        zfgb.created_date,
        zfgb.modified_date,
        zfgb.source,
        case when info.owner is null or info.owner='' then '自营' else info.owner end as owner,
        case when info.owner='京东金融' or info.owner='京东商城' then info.ou else prm.acounting_code end as ou,
        case when info.owner='京东金融' or info.owner='京东商城' then mapping.ou_name else prm.acounting_name end as ou_name,
        zfgb.service_type,
        p.department,p.service_owner,p.service_type as service_category,p.product_type,
        zfgb.bank_mer_id,
        issuer.issuer_name as bank_mer_name,
        send_bank_code,
        recv_bank_code
        from dmf_tmp.dmftmp_mrpt_pay_fi_zfgb_transaction_i_d zfgb
        left join (select * from (select *,row_number() over(partition by id order by modified_date desc) as rn from odm.odm_pay_sis_fi_sis_mer_base_info_i_d where dt <='$TX_DATE') t1 where rn=1) info on zfgb.mer_id=info.mer_id
        left join (select * from (select *,row_number() over(partition by id order by modified_date desc) as rn from odm.odm_pay_sis_fi_ou_name_mapping_i_d where dt <='$TX_DATE') t2 where rn=1) mapping on info.ou=mapping.ou
        left join (select * from (select *,row_number() over(partition by id order by modified_date desc) as rn from odm.odm_pay_sis_fi_kingdee_account_prm_i_d where dt <='$TX_DATE') t3 where rn=1 and account_type='mer_id') prm on zfgb.mer_id=prm.account_idx
        left join (select * from (select *,row_number() over(partition by id order by modified_date desc) as rn from odm.odm_pay_sis_fi_merid_mapping_i_d where dt <='$TX_DATE') t3 where rn=1) p on zfgb.mer_id=p.mer_id
        left join (select * from odm.ODM_PAY_CHNL_CHANNEL_ISSUER_S_D where dt = '$TX_DATE') issuer on zfgb.recv_bank_code = issuer.issuer_code
        where zfgb.dt='$TX_DATE';

);;;
    
     $SQL_BUFF[3]=qq(

        set mapred.job.name=dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_03;
        --打其余标签

        use dmf_tmp;
        drop table if exists dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_mark_02;
        create table dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_mark_02
        AS
        select
        id,
        trade_date,
        trade_category,
        mer_id,
        mer_name,
        trade_type,
        pay_tools,
        card_type,
        order_amt,
        trade_amt,
        mer_fee,
        bank_fee,
        cal_cost,
        trade_num,
        product_id,
        created_date,
        modified_date,
        source,
        owner,
        ou,
        ou_name,
        case when (service_category is null or service_category='') and  product_id in ('JDPAY01','JDPAY02','JDPAY03','SUBWAYPAY') 
             and pay_tools  in ('EXPR','JIOU','ACCT','XJK')     then '支付业务部' 
             else department
         end as  department,
        case when (service_category is null or service_category='') and  product_id in ('JDPAY01','JDPAY02','JDPAY03','SUBWAYPAY') 
             and pay_tools  in ('EXPR','JIOU','ACCT','XJK')  then '外部收单-线上'      
            else service_owner
         end as  service_owner,
        case when (service_category is null or service_category='') and  product_id in ('JDPAY01','JDPAY02','JDPAY03','SUBWAYPAY') 
             and pay_tools  in ('EXPR','JIOU','ACCT','XJK')   then '线上-外部C端' 
             else service_category
         end as  service_category,
        product_type,
        bank_mer_id,
        bank_mer_name,
        send_bank_code,
        recv_bank_code
        from dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_mark_01;
);;;

    


    $SQL_BUFF[4]=qq(

        set mapred.job.name=dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_04;
        --打其余标签

        use dmf_tmp;
        drop table if exists dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_mark_03;
        create table dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_mark_03
        AS
        select
        id,
        trade_date,
        trade_category,
        mer_id,
        mer_name,
        trade_type,
        pay_tools,
        card_type,
        order_amt,
        trade_amt,
        mer_fee,
        bank_fee,
        cal_cost,
        trade_num,
        product_id,
        created_date,
        modified_date,
        source,
        owner,
        ou,
        ou_name,
        case when (service_category is null or service_category='') and pay_tools  in('WECHAT','ALIPAY','CARD') 
        then '支付业务部' 
        else department end as  department,
        case when (service_category is null or service_category='') and  pay_tools  in('WECHAT','ALIPAY','CARD')
        then '外部收单-线下'       
        else service_owner end as  service_owner,
        case when (service_category is null or service_category='') and  pay_tools  in('WECHAT','ALIPAY','CARD')
        then '牌照收单' else service_category
        end as  service_category,
        case when (service_category is null or service_category='') and  pay_tools  in('WECHAT','ALIPAY','CARD')
        then '线下C端' else product_type
        end as  product_type,
        bank_mer_id,
        bank_mer_name,
        send_bank_code,
        recv_bank_code
        from dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_mark_02;
    );;;

    $SQL_BUFF[5]=qq(

        set mapred.job.name=dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_05;

        use dmf_tmp;
        drop table if exists dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_mark_04;
        create table dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_mark_04
        AS
        select
        id,
        trade_date,
        trade_category,
        mer_id,
        mer_name,
        trade_type,
        pay_tools,
        card_type,
        order_amt,
        trade_amt,
        mer_fee,
        bank_fee,
        cal_cost,
        trade_num,
        product_id,
        created_date,
        modified_date,
        source,
        owner,
        ou,
        ou_name,
        case when (service_category is null or service_category='') then '支付业务部' 
        else department end as  department,
        case when (service_category is null or service_category='') then '外部收单-线上'             
        else service_owner  end as  service_owner,
        case when (service_category is null or service_category='') then '线上-通道业务' 
        else service_category end as  service_category,
        product_type,
        bank_mer_id,
        bank_mer_name,
        send_bank_code,
        recv_bank_code
        from dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_mark_03;
    );;;

    $SQL_BUFF[6]=qq(

        set mapred.job.name=dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_06;

        use dmf_tmp;
        DROP TABLE IF EXISTS dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_01;
        CREATE TABLE IF NOT EXISTS dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_01 
        AS
        SELECT
        default.getmd5(t.source,t.id) AS joinkey,
        default.getmd5(id,trade_date,trade_category,mer_id,mer_name,trade_type,pay_tools,card_type,order_amt,trade_amt,mer_fee,bank_fee,cal_cost,trade_num,product_id,
        created_date,
        owner,
        ou,
        ou_name,
        department,
        service_owner,
        service_category,
        product_type,
        bank_mer_id,
        bank_mer_name,
        send_bank_code,
        recv_bank_code) AS change_code,
        id,
        trade_date,
        from_unixtime(unix_timestamp(trade_date,'yyyymmdd'),'yyyy-mm-dd HH:MM:ss') as trans_dt,
        trade_category,
        mer_id,
        mer_name,
        trade_type,
        pay_tools,
        card_type,
        order_amt,
        trade_amt,
        mer_fee,
        bank_fee,
        cal_cost,
        trade_num,
        product_id,
        created_date,
        source,
        modified_date,
        owner,
        ou,
        ou_name,
        department,
        service_owner,
        service_category,
        product_type,
        bank_mer_id,
        bank_mer_name,
        send_bank_code,
        recv_bank_code
        FROM dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_mark_04 t;
    );;;


     $SQL_BUFF[7]=qq(

        set mapred.job.name=dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_07;

        --历史流水全量有效数据
        use dmf_tmp;
        DROP TABLE IF EXISTS dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_his;
        CREATE TABLE IF NOT EXISTS dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_his 
        AS
        select * FROM (
        select * ,row_number() over (partition by joinkey order by dt desc) as rn
        FROM dmf_bc.dmfbc_mrpt_pay_fi_zfgb_transaction_i_d WHERE dt<'$TX_DATE' AND data_type=1
        ) a 
        where rn = 1;

    );;;

    $SQL_BUFF[8]=qq(

        set mapred.job.name=dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_08;
        --比较T-1日增量流水与T-2日之前历史流水，若数据发生变化则以T-1日报表数据为准
        INSERT OVERWRITE TABLE dmf_bc.dmfbc_mrpt_pay_fi_zfgb_transaction_i_d PARTITION (dt='$TX_DATE',data_type=1)
        SELECT
        C.joinkey,
        C.change_code,
        C.id,
        C.trade_date,
        C.trans_dt,
        C.trade_category,
        C.mer_id,
        C.mer_name,
        C.trade_type,
        C.pay_tools,
        C.card_type,
        C.order_amt,
        C.trade_amt,
        C.mer_fee,
        C.bank_fee,
        C.cal_cost,
        C.trade_num,
        C.product_id,
        C.created_date,
        C.source,
        C.modified_date,
        C.owner,
        C.ou,
        C.ou_name,
        null as service_type,
        C.department,
        C.service_owner,
        C.service_category,
        C.product_type,
        FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss') AS create_dt,
        C.bank_mer_id,
        C.bank_mer_name,
        C.send_bank_code,
        C.recv_bank_code
        FROM dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_his H
        FULL OUTER JOIN dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_01 C
        ON H.joinkey=C.joinkey
        WHERE (H.change_code<>C.change_code) OR (H.joinkey IS NULL);

    );;;
    

        $SQL_BUFF[9]=qq(

        set mapred.job.name=dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_09;
       --比较T-1日增量流水与T-2日之前历史流水，若数据发生变化则冲销T-2日之前最新记录
        INSERT OVERWRITE TABLE dmf_bc.dmfbc_mrpt_pay_fi_zfgb_transaction_i_d PARTITION (dt='$TX_DATE',data_type=2)
        SELECT
        H.joinkey,
        H.change_code,
        H.id,
        H.trade_date,
        H.trans_dt,
        H.trade_category,
        H.mer_id,
        H.mer_name,
        H.trade_type,
        H.pay_tools,
        H.card_type,
        -H.order_amt,
        -H.trade_amt,
        -H.mer_fee,
        -H.bank_fee,
        -H.cal_cost,
        -H.trade_num,
        H.product_id,
        H.created_date,
        H.source,
        H.modified_date,
        H.owner,
        H.ou,
        H.ou_name,
        null as service_type,
        H.department,
        H.service_owner,
        H.service_category,
        H.product_type,
        FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss') AS create_dt,
        H.bank_mer_id,
        H.bank_mer_name,
        H.send_bank_code,
        H.recv_bank_code
        FROM dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_his H
        FULL OUTER JOIN dmf_tmp.tmp_dmfbc_mrpt_pay_fi_zfgb_transaction_i_d_01 C
        ON H.joinkey=C.joinkey
        WHERE H.change_code<>C.change_code;

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

