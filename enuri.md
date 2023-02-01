```python

# == 에누리 분석 ==
# 월별 한도에누리 사용률
# 등급별 한도에누리 사용률
# 등급별/월별 한도에누리 사용률
# 점포별 한도에누리 사용률(대형/중소형)

  with enuri_standard as (
      select a.dpstr_cust_no                                               -- 고객번호
           , a.lmt_slct_yy                                                 -- 한도선정년도
           , a.cust_grde_cd                                                -- 우수고객등급
           , a.max_rdcr                                                    -- 최대에누리율
           , ifnull(a.lmt_bsc_amt + a.lmt_add_amt, 0) as lmt_ttl_amt       -- 지급에누리총액(한도 + 추가)
           , ifnull(a.lmt_bsc_amt, 0) as lmt_bsc_amt                       -- 한도기본금액(한도에누리)
           , ifnull(a.lmt_add_amt, 0) as lmt_add_amt                       -- 한도추가금액(추가에누리: 스타포인트를 한도에누리로 전환하여 기존 한도에 추가 or Upsell 프로모션으로 위한 추가 지급)
           , ifnull(a.use_amt - a.cncl_amt, 0) as used_enuri_amt           -- 사용에누리액(사용금액 - 취소금액)
           , ifnull(a.use_amt, 0) as use_amt                               -- 사용금액
           , ifnull(-a.cncl_amt, 0) as cncl_amt                            -- 취소금액
           , ifnull(a.rmnd_amt, 0) as rmnd_amt                             -- 잔여금액
      from TB_DWCU_NWRDCTLMTCUST_M a
      where 1 = 1
          and a.lmt_slct_yy = 2022
          and ifnull(a.lmt_bsc_amt + a.lmt_add_amt, 0) != 0
  ), enuri_used_log as (
      select b.trns_dt                                                     -- 거래일자
           , b.cstr_cd
           , b.pos_no
           , b.rcpt_no
           , b.cpn_evt_no
           , b.dpstr_cust_no
           , ifnull(b.rdct_lmt_amt, 0) as rdct_lmt_amt_day                 -- 에누리한도금액(일자별)
           , ifnull(b.gs_slng_amt, 0) as gs_slng_amt                       -- 총매출금액
           , ifnull(b.rdct_obj_amt, 0) as rdct_obj_amt                     -- 에누리대상금액
           , ifnull(b.rdct_amt, 0) as rdct_amt                             -- 에누리금액
           , ifnull(b.pslng_amt, 0) as pslng_amt                           -- 순매출금액
      from TB_DWSL_NWRDCTCPNUSE_L b
      where 1 = 1
          and b.trns_dt between '2021-12-01' and '2022-11-30'
          and b.cpn_evt_no like '9%'                                       -- 우수고객 한도/상시
          and b.rdct_lmt_amt != 0
  ), enuri_cncl_log as (
      select c.trns_dt
           , c.cstr_cd
           , c.pos_no
           , c.rcpt_no
           , c.cpn_evt_no
           , c.dpstr_cust_no
           , ifnull(-c.rdct_lmt_amt, 0) as rdct_lmt_amt_day                -- 에누리한도금액 중 취소금액(일자별)
           , ifnull(c.gs_slng_amt, 0) as gs_slng_amt                       -- 총매출금액
           , ifnull(c.rdct_obj_amt, 0) as rdct_obj_amt                     -- 에누리대상금액
           , ifnull(c.rdct_amt, 0) as rdct_amt                             -- 에누리금액
           , ifnull(c.pslng_amt, 0) as pslng_amt                           -- 순매출금액
      from TB_DWSL_NWRDCTCPNUSECANC_L c
      where 1 = 1
          and c.trns_dt between '2021-12-01' and '2022-11-30'
          and c.cpn_evt_no like '9%'                                       -- 우수고객 한도/상시
          and c.rdct_lmt_amt != 0
  )
  select t1.dpstr_cust_no                                                  -- 고객번호
       , t1.lmt_slct_yy                                                    -- 한도선정년도
       , t1.cust_grde_cd                                                   -- 우수고객등급
       , t1.max_rdcr                                                       -- 최대에누리율
       , t1.lmt_ttl_amt                                                    -- 지급에누리총액(한도 + 추가)
       , t1.lmt_bsc_amt                                                    -- 한도기본금액(한도에누리)
       , t1.lmt_add_amt                                                    -- 한도추가금액(추가에누리: 스타포인트를 한도에누리로 전환하여 기존 한도에 추가 or Upsell 프로모션으로 위한 추가 지급)
       , t1.used_enuri_amt                                                 -- 사용에누리액(사용금액 - 취소금액)
       , t1.use_amt                                                        -- 사용금액
       , t1.cncl_amt                                                       -- 취소금액
       , t1.rmnd_amt                                                       -- 잔여금액
       , t2.trns_dt                                                        -- 거래일자
       , t2.cstr_cd
       , t2.pos_no
       , t2.rcpt_no
       , t2.cpn_evt_no
       , t2.rdct_lmt_amt_day                                               -- 에누리한도금액(일자별)
       , t2.gs_slng_amt                                                    -- 총매출금액
       , t2.rdct_obj_amt                                                   -- 에누리대상금액
       , t2.rdct_amt                                                       -- 에누리금액
       , t2.pslng_amt                                                      -- 순매출금액
  from enuri_standard t1
  left join ((select b.* from enuri_used_log b) union (select c.* from enuri_cncl_log c)) t2
      on t1.dpstr_cust_no = t2.dpstr_cust_no
  where 1 = 1
--        and t1.dpstr_cust_no = '1234137591031'
  order by 1, 12
  
# Result:
![image](https://user-images.githubusercontent.com/67835149/215926581-e72906c9-b9f3-48a9-acd4-1fe7680a9c37.png)
