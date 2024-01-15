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
    return locals()
sql_map={
    # ATTENTION:   ！！！！ sql_01  因为系统按字典顺序进行排序，小于10 的一定要写成0加编号，否则会顺序混乱，数据出问题，切记，切记！！！！

#将所有的alter操作放在一个sql段中，并去掉所有的setjobname参数。后面注意修改sql_XX的序号
"sql_01": """
    use dmf_gj;

    --删除明细凭证表T-1分区
    --创建明细凭证表T-1分区

    alter table dmf_gj.dmfgj_gj_pay_fi_journal_i_d drop partition(dt='{TX_DATE}',biz_type='100205',borrow_loan_status='1');
    alter table dmf_gj.dmfgj_gj_pay_fi_journal_i_d drop partition(dt='{TX_DATE}',biz_type='100205',borrow_loan_status='0');

    --删除日汇总凭证表T-1分区
    --创建日汇总凭证表T-1分区

    alter table dmf_gj.dmfgj_gj_pay_fi_voucher_i_d drop partition(dt='{TX_DATE}',biz_type='100205');
    alter table dmf_gj.dmfgj_gj_pay_fi_voucher_i_d add partition(dt='{TX_DATE}',biz_type='100205');

     --删除日汇总凭证推送表T-1分区
    --创建日汇总凭证推送表T-1分区

    alter table dmf_gj.dmfgj_gj_pay_fi_hs_voucher_100205_i_d drop partition(dt='{TX_DATE}');
    alter table dmf_gj.dmfgj_gj_pay_fi_hs_voucher_100205_i_d add partition(dt='{TX_DATE}');

    --删除异常交易明细表T-1分区
    --创建异常交易明细表T-1分区

    alter table dmf_gj.dmfgj_ex_check_pay_detail_transaction_ex_000011_i_d drop partition(dt='{TX_DATE}',biz_type='100205');
    alter table dmf_gj.dmfgj_ex_check_pay_detail_transaction_ex_000011_i_d add partition(dt='{TX_DATE}',biz_type='100205');

    --删除异常明细凭证表T-1分区
    alter table dmf_gj.dmfgj_ex_check_pay_detail_voucher_ex_000011_i_d drop partition(dt='{TX_DATE}',biz_type='100205',borrow_loan_status='1');
    alter table dmf_gj.dmfgj_ex_check_pay_detail_voucher_ex_000011_i_d drop partition(dt='{TX_DATE}',biz_type='100205',borrow_loan_status='0');

    --删除明细凭证与日凭证关系表T-1分区
    alter table dmf_gj.dmfgj_ex_check_fi_hs_detail_day_voucher_relation_i_d drop partition(dt='{TX_DATE}',biz_type='100205');
    alter table dmf_gj.dmfgj_ex_check_fi_hs_detail_day_voucher_relation_i_d add partition(dt='{TX_DATE}',biz_type='100205');
""",

#之前的逻辑，去掉setjobname参数即可
"sql_02": """
    use dmf_tmp;

    drop table dmf_tmp.tmp_gj_transaction_detail_with_datasource_100205_i_d;
    create table dmf_tmp.tmp_gj_transaction_detail_with_datasource_100205_i_d
    as
    select t1.data_source_em from dmf_dim.dmfdim_gj_hs_fi_hs_dual_a_d t lateral view dmf_bc.dmbiztype('100205') t1 as data_source_em;

""",

#新增的sql段用于交易明细数据检查，后面注意修改业务小类的编码防止出现相同的临时表
"sql_03": """
    use dmf_tmp;
    set mapred.max.split.size=16000000;
    set hive.stats.autogather = false;
    set hive.execution.engine=tez;
    -- 交易明细数据检查，暂存临时表

    DROP TABLE IF EXISTS dmf_tmp.tmp_dmftmp_ex_check_fi_hs_transaction_detail_ex_i_d_100205;   -- 修改临时表名 100205
    create table dmf_tmp.tmp_dmftmp_ex_check_fi_hs_transaction_detail_ex_i_d_100205
    as
    select
    t1.*
    ,t2.check_result
    ,t2.error_codes
    from (
            select *
            from dmf_gj.dmfgj_gj_pay_fi_hs_transaction_detail_i_d
            where dt = '{TX_DATE}'
            and trans_amt != 0
            and data_source_em in (select data_source_em from dmf_tmp.tmp_gj_transaction_detail_with_datasource_100205_i_d)
            --and biz_type is not null
         ) t1
         lateral view dmf_bc.dmdetailcheck (biz_type,biz_line,data_source_em,trans_type,origin_id,trans_no,loan_no,plan_no,trans_amt,trans_dt,company_no1,company_no2,company_no3,company_no4,company_no5,company_no6,company_no7,company_no8,company_no9,currency,has_tax,tax,product_no,pay_no,serial_no,merchant_no,borrow_bank_acct,loan_bank_acct,project_no,section_no,spare_no,vir_merchant,dt)
         t2 as data_check, check_result, error_codes;
""",

#新增的sql段，将交易明细异常数据插入到正式表中
"sql_04": """
    use dmf_gj;
    set hive.stats.autogather = false;
    set mapred.max.split.size=16000000;

-- 交易明细异常数据，保存入库

    insert overwrite table dmf_gj.dmfgj_ex_check_pay_detail_transaction_ex_000011_i_d partition (dt, biz_type)
    select
    origin_id
    ,serial_no
    ,biz_line
    ,product_no
    ,trans_type
    ,company_no1
    ,company_no2
    ,company_no3
    ,company_no4
    ,company_no5
    ,borrow_bank_acct
    ,loan_bank_acct
    ,trans_dt
    ,trans_amt
    ,trans_no
    ,pay_no
    ,loan_no
    ,plan_no
    ,data_source_em
    ,customer_no
    ,merchant_no
    ,section_no
    ,pay_enum
    ,direction
    ,loan_type
    ,create_dt
    ,order_no
    ,project_no
    ,spare_no
    ,vir_merchant
    ,company_no6
    ,company_no7
    ,company_no8
    ,company_no9
    ,currency
    ,has_tax
    ,tax
    ,source_table                                          -- add by xiaojia 20210115 增加字段
    ,source_table_dt                                       -- add by xiaojia 20210115 增加字段
    ,repay_no                                              -- add by xiaojia 20210115 增加字段
    ,refund_no                                             -- add by xiaojia 20210115 增加字段
    ,bill_no                                               -- add by xiaojia 20210115 增加字段
    ,sett_id                                               -- add by xiaojia 20210115 增加字段
    ,fee_id                                                -- add by xiaojia 20210115 增加字段
    ,fee_type                                              -- add by xiaojia 20210115 增加字段
    ,sett_scenes                                           -- add by xiaojia 20210115 增加字段
    ,sett_biz_type                                         -- add by xiaojia 20210115 增加字段
    ,writeoff_status                                       -- add by xiaojia 20210115 增加字段
    ,product_id                                            -- add by xiaojia 20210115 增加字段
    ,sku_id                                                -- add by xiaojia 20210115 增加字段
    ,error_codes                                           -- add by xiaojia 20210115 增加字段
    ,dt
    ,biz_type
    from dmf_tmp.tmp_dmftmp_ex_check_fi_hs_transaction_detail_ex_i_d_100205
    where check_result = '0';
""",

#新增的sql段，将正常的交易明细数据生成明细凭证并保存在临时表中
"sql_05": """
    set mapred.max.split.size=16000000;
    set hive.stats.autogather = false;
    use dmf_tmp;

    -- 生成明细凭证，注意排除金额为0的数据，暂存临时表

    DROP TABLE IF EXISTS dmf_tmp.tmp_dmftmp_ex_check_fi_hs_journal_000011_i_d_00_100205;   -- 修改临时表名 100205
    create table         dmf_tmp.tmp_dmftmp_ex_check_fi_hs_journal_000011_i_d_00_100205
    as
    select
         t.origin_id
        ,t.superior_biz_type
        ,t.biz_line
        ,t.product_no
        ,t.buss_link_name
        ,t.company_no
        ,t.receipt_type
        ,t.fund_type
        ,t.trans_dt
        ,t.trans_amt
        ,t.subject_no
        ,t.direction_em
        ,t.aux_subject_1
        ,t.aux_subject_2
        ,t.aux_subject_3
        ,t.voucher_id
        ,t.record_id
        ,t.serial_no
        ,t.trans_no
        ,t.pay_no
        ,t.loan_no
        ,t.plan_no
        ,t.data_source_em
        ,t.trans_type
        ,t.positive_or_negative
        ,t.ysyf_id
        ,t.md5
        ,t.subject_type
        ,t.create_dt
        ,t.trans_id
        ,t.bank_code
        ,t.bank_name
        ,t.buss_link_code
        ,t.currency
        ,t.rule_id
        ,t.src_trans_amt
        ,t.tax
        ,t.manual_dt
        ,t.voucher_type
        ,t.aux_subject_1_type
        ,t.aux_subject_2_type
        ,t.aux_subject_3_type
        ,t.cal_trans_amt
        ,concat(t.origin_id,'_',t.ysyf_id) as union_id    -- add by xiaojia 20210115 增加唯一键
        ,t0.source_table                                   -- add by xiaojia 20210115 增加字段
        ,t0.source_table_dt                                -- add by xiaojia 20210115 增加字段
        ,t0.repay_no                                       -- add by xiaojia 20210115 增加字段
        ,t0.refund_no                                      -- add by xiaojia 20210115 增加字段
        ,t0.bill_no                                        -- add by xiaojia 20210115 增加字段
        ,t0.sett_id                                        -- add by xiaojia 20210115 增加字段
        ,t0.fee_id                                         -- add by xiaojia 20210115 增加字段
        ,t0.fee_type                                       -- add by xiaojia 20210115 增加字段
        ,t0.sett_scenes                                    -- add by xiaojia 20210115 增加字段
        ,t0.sett_biz_type                                  -- add by xiaojia 20210115 增加字段
        ,t0.writeoff_status                                -- add by xiaojia 20210115 增加字段
        ,substr(t.manual_dt, 0, 6) as period_code          -- add by xiaojia 20210115 增加账期字段  5K环境时间格式不一样
        ,t0.product_id                                     -- add by xiaojia 20210115 增加字段
        ,t0.sku_id                                         -- add by xiaojia 20210115 增加字段
        ,t.dt
        ,t.biz_type
        ,t.borrow_loan_status
    FROM dmf_tmp.tmp_dmftmp_ex_check_fi_hs_transaction_detail_ex_i_d_100205 t0 --注意修改业务小类编码
	lateral view dmf_bc.dmjournal(biz_type,biz_line,data_source_em,trans_type,origin_id,trans_no,loan_no,plan_no,trans_amt,trans_dt,company_no1,company_no2,company_no3,company_no4,company_no5,company_no6,company_no7,company_no8,company_no9,currency,has_tax,tax,product_no,pay_no,serial_no,merchant_no,borrow_bank_acct,loan_bank_acct,project_no,section_no,spare_no,vir_merchant,dt,'1')
		   t as origin_id,superior_biz_type,biz_line,product_no,buss_link_name,company_no,receipt_type,fund_type,trans_dt,trans_amt,subject_no,direction_em,aux_subject_1,aux_subject_2,aux_subject_3,voucher_id,record_id,serial_no,trans_no,pay_no,loan_no,plan_no,data_source_em,trans_type,positive_or_negative,ysyf_id,md5,subject_type,create_dt,trans_id,bank_code,bank_name,buss_link_code,currency,rule_id,src_trans_amt,tax,manual_dt,voucher_type,aux_subject_1_type,aux_subject_2_type,aux_subject_3_type,cal_trans_amt,dt,biz_type,borrow_loan_status
	where t0.check_result = '1'
""",

#新增sql段，对明细凭证进行UTDF检查打标，将结果存入临时表中
"sql_06": """
    set mapred.max.split.size=16000000;
    set hive.stats.autogather = false;
    set hive.merge.mapfiles = true;
    set hive.merge.mapredfiles = true;
    set hive.merge.size.per.task = 256000000;
    set hive.merge.smallfiles.avgsize=256000000;
    use dmf_tmp;

    -- 明细凭证UTDF检查，暂存临时表

    DROP TABLE IF EXISTS dmf_tmp.tmp_dmftmp_ex_check_fi_hs_journal_000011_i_d_01_100205;   -- 修改临时表名 100205
    create table         dmf_tmp.tmp_dmftmp_ex_check_fi_hs_journal_000011_i_d_01_100205
    as
    select
     t.origin_id
    ,t.superior_biz_type
    ,t.biz_line
    ,t.product_no
    ,t.buss_link_name
    ,t.company_no
    ,t.receipt_type
    ,t.fund_type
    ,t.trans_dt
    ,t.trans_amt
    ,t.subject_no
    ,t.direction_em
    ,t.aux_subject_1
    ,t.aux_subject_2
    ,t.aux_subject_3
    ,t.voucher_id
    ,t.record_id
    ,t.serial_no
    ,t.trans_no
    ,t.pay_no
    ,t.loan_no
    ,t.plan_no
    ,t.data_source_em
    ,t.trans_type
    ,t.positive_or_negative
    ,t.ysyf_id
    ,t.md5
    ,t.subject_type
    ,t.create_dt
    ,t.trans_id
    ,t.bank_code
    ,t.bank_name
    ,t.buss_link_code
    ,t.currency
    ,t.rule_id
    ,t.src_trans_amt
    ,t.tax
    ,t.manual_dt
    ,t.voucher_type
    ,t.aux_subject_1_type
    ,t.aux_subject_2_type
    ,t.aux_subject_3_type
    ,t.cal_trans_amt
    ,t.union_id                       -- add by xiaojia 20210115 增加字段
    ,t.source_table                   -- add by xiaojia 20210115 增加字段
    ,t.source_table_dt                -- add by xiaojia 20210115 增加字段
    ,t.repay_no                       -- add by xiaojia 20210115 增加字段
    ,t.refund_no                      -- add by xiaojia 20210115 增加字段
    ,t.bill_no                        -- add by xiaojia 20210115 增加字段
    ,t.sett_id                        -- add by xiaojia 20210115 增加字段
    ,t.fee_id                         -- add by xiaojia 20210115 增加字段
    ,t.fee_type                       -- add by xiaojia 20210115 增加字段
    ,t.sett_scenes                    -- add by xiaojia 20210115 增加字段
    ,t.sett_biz_type                  -- add by xiaojia 20210115 增加字段
    ,t.writeoff_status                -- add by xiaojia 20210115 增加字段
    ,t.period_code                    -- add by xiaojia 20210115 增加字段
    ,t.product_id                     -- add by xiaojia 20210115 增加字段
    ,t.sku_id                         -- add by xiaojia 20210115 增加字段
    ,t.dt
    ,t.biz_type
    ,t.borrow_loan_status
    ,case when aux_subject_1_type = '2#2' then aux_subject_1
          when aux_subject_2_type = '2#2' then aux_subject_2
          when aux_subject_3_type = '2#2' then aux_subject_3
          else ''
          end as supplier_id
    ,udtf_table.data_check
    ,udtf_table.check_res_codes
    from dmf_tmp.tmp_dmftmp_ex_check_fi_hs_journal_000011_i_d_00_100205 t  --注意修改业务大类及业务小类编码
         lateral view dmf_bc.dmjournalcheck(origin_id,superior_biz_type,biz_line,product_no,buss_link_name,company_no,receipt_type,fund_type,trans_dt,trans_amt,subject_no,direction_em,aux_subject_1,aux_subject_2,aux_subject_3,voucher_id,record_id,serial_no,trans_no,pay_no,loan_no,plan_no,data_source_em,trans_type,positive_or_negative,ysyf_id,md5,subject_type,create_dt,trans_id,bank_code,bank_name,buss_link_code,currency,rule_id,src_trans_amt,tax,manual_dt,voucher_type,aux_subject_1_type,aux_subject_2_type,aux_subject_3_type,cal_trans_amt,dt,biz_type,borrow_loan_status)
         udtf_table as data_check, check_result, check_res_codes;
""",

#新增sql段，对明细凭证进行检查，公司主体、供应商字段检查，暂存临时表
"sql_07": """
    set mapred.max.split.size=16000000;
    set hive.stats.autogather = false;
    use dmf_tmp;

    -- 凭证明细检查，公司主体、供应商字段检查，暂存临时表

    DROP TABLE IF EXISTS dmf_tmp.tmp_dmftmp_ex_check_fi_hs_journal_000011_i_d_02_100205;  -- 修改临时表名 100205
    create table         dmf_tmp.tmp_dmftmp_ex_check_fi_hs_journal_000011_i_d_02_100205
    as
    select
     t.origin_id
    ,t.superior_biz_type
    ,t.biz_line
    ,t.product_no
    ,t.buss_link_name
    ,t.company_no
    ,t.receipt_type
    ,t.fund_type
    ,t.trans_dt
    ,t.trans_amt
    ,t.subject_no
    ,t.direction_em
    ,t.aux_subject_1
    ,t.aux_subject_2
    ,t.aux_subject_3
    ,t.voucher_id
    ,t.record_id
    ,t.serial_no
    ,t.trans_no
    ,t.pay_no
    ,t.loan_no
    ,t.plan_no
    ,t.data_source_em
    ,t.trans_type
    ,t.positive_or_negative
    ,t.ysyf_id
    ,t.md5
    ,t.subject_type
    ,t.create_dt
    ,t.trans_id
    ,t.bank_code
    ,t.bank_name
    ,t.buss_link_code
    ,t.currency
    ,t.rule_id
    ,t.src_trans_amt
    ,t.tax
    ,t.manual_dt
    ,t.voucher_type
    ,t.aux_subject_1_type
    ,t.aux_subject_2_type
    ,t.aux_subject_3_type
    ,t.cal_trans_amt
    ,t.union_id
    ,t.source_table
    ,t.source_table_dt
    ,t.repay_no
    ,t.refund_no
    ,t.bill_no
    ,t.sett_id
    ,t.fee_id
    ,t.fee_type
    ,t.sett_scenes
    ,t.sett_biz_type
    ,t.writeoff_status
    ,t.period_code
    ,t.product_id
    ,t.sku_id
    ,t.dt
    ,t.biz_type
    ,t.borrow_loan_status
    ,t.data_check
    ,case when
            data_check = 1
            then
                CONCAT_WS (
                    ',',
                    if(check_res_codes = '', null, check_res_codes),
                    if(((t4.id is null) and (t.aux_subject_1_type = '2#2' or t.aux_subject_2_type = '2#2' or t.aux_subject_3_type = '2#2')), '3002', null)
                )
            else ''
        end as error_codes
    from dmf_tmp.tmp_dmftmp_ex_check_fi_hs_journal_000011_i_d_01_100205 t    --注意修改业务大类及业务小类编码
    left join
         (
           select p1.id,p3.rn
            from
              (select * from dmf_dim.dmfdim_dim_fi_subject_merchant_h_d where end_dt = '4712-12-31') p1
            join
              ( select supplier_id
               from dmf_tmp.tmp_dmftmp_ex_check_fi_hs_journal_000011_i_d_01_100205
               where data_check = 1 and supplier_id != ''
               group by supplier_id
               ) p2
             on p1.id= p2.supplier_id
            join
              (select
                 cast(idx as string) rn
               from
                (select split(space(datediff('2022-09-27','2020-01-01')), ' ')  as x) t  --不需要修改
              lateral view
              posexplode(x) pe as idx, ele
              ) p3
            on 1=1
          ) t4
    on t.data_check = 1 and t.supplier_id != '' and concat(t.supplier_id,cast(cast(round(rand()*999,0) as int) as string)) = concat(t4.id,rn);

""",

#新增sql段，筛选异常明细凭证，只取origin_id和error_codes
"sql_08": """
    use dmf_tmp;
    set hive.stats.autogather = false;

    -- 筛选异常凭证明细数据

    DROP TABLE IF EXISTS dmf_tmp.tmp_dmftmp_ex_check_fi_hs_journal_000011_i_d_03_100205;  -- 修改临时表名 100205
    create table         dmf_tmp.tmp_dmftmp_ex_check_fi_hs_journal_000011_i_d_03_100205
    as
    select
     t.origin_id
    ,t.error_codes
    from
    dmf_tmp.tmp_dmftmp_ex_check_fi_hs_journal_000011_i_d_02_100205 t    --注意修改业务大类及业务小类编码
    where t.error_codes != '';

""",

#新增sql段，将异常明细凭证插入到正式结果表中
"sql_09": """
    set mapred.max.split.size=16000000;
    set hive.stats.autogather = false;
    use dmf_gj;

-- 保存异常明细凭证数据
    insert overwrite table dmf_gj.dmfgj_ex_check_pay_detail_voucher_ex_000011_i_d partition (dt, biz_type, borrow_loan_status)
    select
     t.origin_id
    ,t.superior_biz_type
    ,t.biz_line
    ,t.product_no
    ,t.buss_link_name
    ,t.company_no
    ,t.receipt_type
    ,t.fund_type
    ,t.trans_dt
    ,t.trans_amt
    ,t.subject_no
    ,t.direction_em
    ,t.aux_subject_1
    ,t.aux_subject_2
    ,t.aux_subject_3
    ,t.voucher_id
    ,t.record_id
    ,t.serial_no
    ,t.trans_no
    ,t.pay_no
    ,t.loan_no
    ,t.plan_no
    ,t.data_source_em
    ,t.trans_type
    ,t.positive_or_negative
    ,t.ysyf_id
    ,t.md5
    ,t.subject_type
    ,t.create_dt
    ,t.trans_id
    ,t.bank_code
    ,t.bank_name
    ,t.buss_link_code
    ,t.currency
    ,t.rule_id
    ,t.src_trans_amt
    ,t.tax
    ,t.manual_dt    --add
    ,t.voucher_type --add
    ,t.aux_subject_1_type
    ,t.aux_subject_2_type
    ,t.aux_subject_3_type
    ,t.cal_trans_amt
    ,t.union_id
    ,t.source_table
    ,t.source_table_dt
    ,t.repay_no
    ,t.refund_no
    ,t.bill_no
    ,t.sett_id
    ,t.fee_id
    ,t.fee_type
    ,t.sett_scenes
    ,t.sett_biz_type
    ,t.writeoff_status
    ,t.period_code
    ,t.product_id
    ,t.sku_id
    ,t.error_codes
    ,t.dt
    ,t.biz_type
    ,t.borrow_loan_status
    from
    dmf_tmp.tmp_dmftmp_ex_check_fi_hs_journal_000011_i_d_02_100205 t      --注意修改业务大类及业务小类编码
    where exists (select 1 from dmf_tmp.tmp_dmftmp_ex_check_fi_hs_journal_000011_i_d_03_100205 where origin_id = t.origin_id);
""",

#下面的逻辑对应修改前生成明细凭证的sql段，需要新增字段并修改from后面的表。注意需要先行新增业务大类明细凭证的字段！！！
"sql_10": """
    set mapred.max.split.size=16000000;
    set hive.stats.autogather = false;
    use dmf_gj;

-- 保存正常凭证明细数据
    insert overwrite table dmf_gj.dmfgj_gj_pay_fi_journal_i_d partition (dt, biz_type, borrow_loan_status)
    select
     t.origin_id
    ,t.superior_biz_type
    ,t.biz_line
    ,t.product_no
    ,t.buss_link_name
    ,t.company_no
    ,t.receipt_type
    ,t.fund_type
    ,t.trans_dt
    ,t.trans_amt
    ,t.subject_no
    ,t.direction_em
    ,t.aux_subject_1
    ,t.aux_subject_2
    ,t.aux_subject_3
    ,t.voucher_id
    ,t.record_id
    ,t.serial_no
    ,t.trans_no
    ,t.pay_no
    ,t.loan_no
    ,t.plan_no
    ,t.data_source_em
    ,t.trans_type
    ,t.positive_or_negative
    ,t.ysyf_id
    ,t.md5
    ,t.subject_type
    ,t.create_dt
    ,t.trans_id
    ,t.bank_code
    ,t.bank_name
    ,t.buss_link_code
    ,t.currency
    ,t.rule_id
    ,t.src_trans_amt
    ,t.tax
    ,t.manual_dt    --add
    ,t.voucher_type --add
    ,t.aux_subject_1_type
    ,t.aux_subject_2_type
    ,t.aux_subject_3_type
    ,t.cal_trans_amt
    ,t.union_id                       -- 本次新增字段
    ,t.source_table                   -- 本次新增字段
    ,t.source_table_dt                -- 本次新增字段
    ,t.repay_no                       -- 本次新增字段
    ,t.refund_no                      -- 本次新增字段
    ,t.bill_no                        -- 本次新增字段
    ,t.sett_id                        -- 本次新增字段
    ,t.fee_id                         -- 本次新增字段
    ,t.fee_type                       -- 本次新增字段
    ,t.sett_scenes                    -- 本次新增字段
    ,t.sett_biz_type                  -- 本次新增字段
    ,t.writeoff_status                -- 本次新增字段
    ,t.period_code                    -- 本次新增字段
    ,t.product_id                     -- 本次新增字段
    ,t.sku_id                         -- 本次新增字段
    ,t.dt
    ,t.biz_type
    ,t.borrow_loan_status
    from
    dmf_tmp.tmp_dmftmp_ex_check_fi_hs_journal_000011_i_d_02_100205 t
    where not exists (select 1 from dmf_tmp.tmp_dmftmp_ex_check_fi_hs_journal_000011_i_d_03_100205 where origin_id = t.origin_id);
""",

#修改前生成汇总凭证的sql段，新增字段union_id、detail_union_ids.注意需要先行新增业务大类汇总凭证的字段！！！
"sql_11": """
    use dmf_gj;
    set hive.stats.autogather = false;
--将明细凭证汇总dmftmp_ex_check_fi_hs_voucher_100205_i_d

    insert overwrite table dmf_gj.dmfgj_gj_pay_fi_voucher_i_d partition (dt,biz_type)
    select
        superior_biz_type
        ,biz_line
        ,product_no
        ,buss_link_name
        ,company_no
        ,receipt_type
        ,fund_type
        ,trans_dt
        ,case when positive_or_negative = '0' then abs(sum(trans_amt))
              when positive_or_negative = '1' then abs(sum(trans_amt))*-1
              when positive_or_negative = '2' then sum(trans_amt)
              when positive_or_negative = '3' then sum(trans_amt)*-1
              else sum(trans_amt)
              end as trans_amt
        ,subject_no
        ,direction_em
        ,aux_subject_1
        ,aux_subject_2
        ,aux_subject_3
        ,positive_or_negative
        ,ysyf_id
        ,md5 as voucher_id
        ,md5
        ,FROM_UNIXTIME(UNIX_TIMESTAMP()) as create_dt
        ,trans_id
        ,bank_code
        ,bank_name
        ,buss_link_code
        ,currency
        ,rule_id -- add by xiaojia 凭证轧差使用
        ,case when positive_or_negative = '0' then abs(sum(src_trans_amt))
              when positive_or_negative = '1' then -abs(sum(src_trans_amt))
              when positive_or_negative = '2' then sum(src_trans_amt)
              when positive_or_negative = '3' then -sum(src_trans_amt)
              else sum(src_trans_amt)
              end as src_trans_amt -- add by xiaojia 凭证轧差使用
        ,tax
        ,manual_dt
        ,voucher_type
        ,dmf_bc.getmd5(concat_ws(',',superior_biz_type,biz_line,product_no,buss_link_name,company_no,receipt_type,fund_type,trans_dt,subject_no,direction_em
                 ,aux_subject_1,aux_subject_2,aux_subject_3,positive_or_negative,ysyf_id,md5,trans_id,bank_code,bank_name,buss_link_code,currency,rule_id,tax,dt,biz_type,manual_dt,voucher_type)  -- 增加金额字段
                       ) as union_id  --异常凭证改造新增字段 20210319改造
		,cast(null as string) as detail_union_ids --20210319改造
        ,dt
        ,biz_type
    FROM dmf_gj.dmfgj_gj_pay_fi_journal_i_d
    WHERE dt = '{TX_DATE}' and biz_type = '100205'
    GROUP BY superior_biz_type,biz_line,product_no,buss_link_name,company_no,receipt_type,fund_type,trans_dt,subject_no,direction_em
                 ,aux_subject_1,aux_subject_2,aux_subject_3,positive_or_negative,ysyf_id,md5,trans_id,bank_code,bank_name,buss_link_code,currency,rule_id,tax,dt,biz_type,manual_dt,voucher_type;

""",

#新增的sql段，将结果插入到明细凭证与日凭证关系表。
"sql_12": """
    set hive.execution.engine=tez;
    set mapred.max.split.size=8000000;
    set hive.stats.autogather = false;
    use dmf_gj;

--建立明细凭证与日凭证关系表
    insert overwrite table dmf_gj.dmfgj_ex_check_fi_hs_detail_day_voucher_relation_i_d partition (dt,biz_type)
    select
        dmf_bc.getmd5(concat_ws(',',superior_biz_type,biz_line,product_no,buss_link_name,company_no,receipt_type,fund_type,trans_dt,subject_no,direction_em
                 ,aux_subject_1,aux_subject_2,aux_subject_3,positive_or_negative,ysyf_id,md5,trans_id,bank_code,bank_name,buss_link_code,currency,rule_id,tax,dt,biz_type,manual_dt,voucher_type)  -- 增加金额字段
                       ) as day_voucher_id,
        union_id as detail_voucher_id,
        dt,
        biz_type
    from dmf_gj.dmfgj_gj_pay_fi_journal_i_d
    where  dt = '{TX_DATE}' and biz_type = '100205' ;
""",

#将汇总凭证拆分，按业务小类存储。新增字段union_id。注意需要先行新增拆分业务小类后的汇总凭证模型的字段！！！
"sql_13": """
    use dmf_gj;
    set hive.stats.autogather = false;

--将日汇总凭证表数据按照业务小类拆分存储，后续分别创建推送作业，将数据推送至核算系统，避免个别业务线数据延迟导致核算全量业务无数据可用的情况

    INSERT OVERWRITE TABLE dmf_gj.dmfgj_gj_pay_fi_hs_voucher_100205_i_d PARTITION (dt='{TX_DATE}')
    SELECT
         superior_biz_type  AS superior_biz_type
        ,biz_type           AS biz_type
        ,biz_line           AS biz_line
        ,product_no         AS product_no
        ,buss_link_name     AS buss_link_name
        ,company_no         AS company_no
        ,receipt_type       AS receipt_type
        ,fund_type          AS fund_type
        ,trans_dt           AS trans_dt
        ,trans_amt          AS trans_amt
        ,subject_no         AS subject_no
        ,direction_em       AS direction_em
        ,aux_subject_1      AS aux_subject_1
        ,aux_subject_2      AS aux_subject_2
        ,aux_subject_3      AS aux_subject_3
        ,positive_or_negative AS positive_or_negative
        ,ysyf_id            AS ysyf_id
        ,voucher_id         AS voucher_id
        ,md5                AS md5
        ,create_dt          AS create_dt
        ,trans_id           AS trans_id
        ,bank_code          AS bank_code
        ,bank_name          AS bank_name
        ,buss_link_code     AS buss_link_code
        ,currency           AS currency
        ,manual_dt          AS manual_dt
        ,voucher_type       AS voucher_type
        ,union_id           AS union_id        -- 异常凭证改造新增字段
    FROM dmf_gj.dmfgj_gj_pay_fi_voucher_i_d
    WHERE dt='{TX_DATE}' and biz_type='100205';

""",

}
# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)
