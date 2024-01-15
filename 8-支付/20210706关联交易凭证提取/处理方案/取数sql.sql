---需求背景：报告部需要所有京东科技与京东集团做关联交易的EBS凭证明细。
---所有过滤条件均与报告部周泉确认完毕

use dmf_dev;
drop table if exists dmf_dev.zfsj_connected_trans_dyt_20210706;
create table         dmf_dev.zfsj_connected_trans_dyt_20210706
as
select
*
from
odm.odm_fi_ebs_cux_jdtmas_journal_details_v_i_d voucher
inner join dmf_add.dmfadd_add_audit_rltv_cert_tab_com_a_d company --周泉给出的一个客商表，用这个表框定集团主体客商
on voucher.vendor_siet_code=company.vendor_site_code---改表为专用此需求专用的报表
where 1=1
and voucher.period_name not like '%A'---不用调整期间
and (voucher.coa_acc_code like '5%' or voucher.coa_acc_code like '6%')---只要损益类的科目，开头为5或6
and voucher.coa_acc_code not in ('650100900000')---不要'以前年度损益调整'这个科目的数据
and substr(voucher.default_effective_date,0,10)>='2021-04-01'---卡凭证日期为Q2的数据
and substr(voucher.default_effective_date,0,10) <'2021-07-01'