set mapred.job.name=dmfbc_oar_bc_pay_sis_fin_rpt_i_d_11;
use dmf_tmp;
drop table dmf_tmp.dmf_tmp_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_11;
create table dmf_tmp.dmf_tmp_dmfbc_oar_bc_pay_sis_fin_rpt_i_d_11
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
(select * from dmf_add.dmfadd_oar_add_pay_fin_int_line_i_m 
where trim(depart_code)='9310000' and trim(order_index_name) in ('007','008')  --dmfadd_oar_add_pay_fin_int_line_a_m 支付BigBoss线下数据导入
  and trim(fir_lvl_code) in ('fin9320006','fin9320008','fin9320010')) p1
left join dim.dim_day p2 on p1.month_id=substr(p2.dim_day_txdate,1,7)
) pp 
where
 pp.dim_day_txdate>='$TX_10B_DATE' and
 pp.dim_day_txdate<='$TX_PREV_DATE'