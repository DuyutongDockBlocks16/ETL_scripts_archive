select *
from
(select *
,row_number() over(partition by pay_num order by ordr_modify_time desc) as rn
from idm.idm_f02_pay_sfjh_tx_dtl_i_d
where dt<=default.sysdate(-1) and prod_type in ('双屏收银机及配件','自助收银机','青鸾app','智能POS及配件')
and ordr_type in ('PAY') and ordr_status='SUCCESS') a
where rn=1 and is_yn=1