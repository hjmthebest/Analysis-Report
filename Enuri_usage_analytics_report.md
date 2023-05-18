```python
with enuri_standard as (
    select a.dpstr_cust_no                                                            -- 고객번호
         , t2.cstr_cd
         , nvl(t3.excll_cust_grde_mdcls_cd,'Customer') as excll_cust_grde_mdcls_cd
         , nvl(t3.excll_cust_grde_mdcls_nm,'Customer') as excll_cust_grde_mdcls_nm
         , max(a.max_rdcr) as max_rdcr                                                -- 최대에누리율
         , sum(a.lmt_bsc_amt) + sum(a.lmt_add_amt) as lmt_ttl_amt  -- 지급에누리총액(한도 + 추가)
         , sum(a.lmt_bsc_amt) as lmt_bsc_amt                                          -- 한도기본금액(한도에누리)
         , sum(a.lmt_add_amt) as lmt_add_amt                                          -- 한도추가금액(추가에누리: AVENUEL POINT를 한도에누리 전환하여 기존 한도에 추가 or Upsell 프로모션 위한 추가 지급)
         , sum(use_amt) -sum(cncl_amt) as use_amt
         , sum(rmnd_amt) as rmnd_amt
    from lpciddw.TB_DWCU_NWRDCTLMTCUST_M a                                            -- DWCU_신에누리한도고객_마스터
    left outer join lpciddm.tb_dmcu_yrexcllcustgrdeinfo_i t1
        on a.dpstr_cust_no = t1.dpstr_cust_no and t1.excll_cust_slct_yy = to_char(sysdate-1,'YYYY')  -- 어제일자 당해년도 등급만 확인
    join lpciddm.tb_dmct_excllcustsmcls_c t2
       on t1.now_excll_cust_grde_smcls_cd = t2.excll_cust_grde_smcls_cd
    join lpciddm.tb_dmct_excllcustmdcls_c  t3
       on t2.excll_cust_grde_mdcls_cd = t3.excll_cust_grde_mdcls_cd
    where 1 = 1
        and t2.excll_cust_grde_mdcls_cd in ('1119','1221','122A','1222','122B','1223','1224','155A','1551')  -- 우수고객 필터링
        and a.cpn_evt_no = '999999990010'                                                                    -- 우수고객 한도
        and a.lmt_st_dt between concat(substring('2023-01-01',1,4),'-01-01') and concat(substring('2023-12-31',1,4),'-12-31')
	  group by a.dpstr_cust_no                                          -- 백화점고객번호
           , t2.cstr_cd
           , nvl(t3.excll_cust_grde_mdcls_cd,'Customer')
           , nvl(t3.excll_cust_grde_mdcls_nm,'Customer')

), enuri_used_log as (
    select  b.cstr_cd
          , b.dpstr_cust_no
          , b.rdct_lmt_amt as rdct_lmt_amt_day            -- 에누리한도금액(일자별)
          , b.gs_slng_amt as gs_slng_amt                  -- 총매출금액
          , b.rdct_obj_amt as rdct_obj_amt                -- 에누리대상금액
          , b.rdct_amt as rdct_amt                        -- 에누리금액
          , b.pslng_amt as pslng_amt                      -- 순매출금액
    from lpciddw.TB_DWSL_NWRDCTCPNUSE_L b                                   -- DWSL_신에누리쿠폰사용_내역
    where 1 = 1
        and b.trns_dt between '2023-01-01' and '2023-12-31'         -- 거래일자 범위 
        and b.cpn_evt_no = '999999990010'    
        and b.use_rsn_cd  = '00'
                                                  -- 우수고객 한도
), enuri_cncl_log as (
    select c.cstr_cd
         , c.dpstr_cust_no
         , -c.rdct_lmt_amt as rdct_lmt_amt_day           -- 에누리한도금액 중 취소금액(일자별)
         , c.gs_slng_amt as gs_slng_amt                  -- 총매출금액
         , c.rdct_obj_amt as rdct_obj_amt                -- 에누리대상금액
         , c.rdct_amt as rdct_amt                        -- 에누리금액gks
         , c.pslng_amt as pslng_amt                      -- 순매출금액
    from lpciddw.TB_DWSL_NWRDCTCPNUSECANC_L c            -- DWSL_신에누리쿠폰사용취소_내역
    where 1 = 1
        and c.trns_dt between  '2023-01-01' and '2023-12-31'             -- 거래취소일자 범위: 설정년도 
        and c.cpn_evt_no = '999999990010'                                -- 우수고객 한도
        and c.use_rsn_cd  = '00'
        and c.otrns_dt >= concat(substring('2023-01-01',1,4),'-01-01')   -- 거래취소 시작일자가 속한 년도의 1월1일 이전값은 제외

), enuri_cstr_cd_used_cncl as (  --점별,고객번호별 사용취소금액 합산
    select cstr_cd
         , dpstr_cust_no
         , sum(rdct_lmt_amt_day) as rdct_lmt_amt_day
         , sum(gs_slng_amt) as gs_slng_amt 
         , sum(rdct_obj_amt) as rdct_obj_amt 
         , sum(rdct_amt) as rdct_amt
         , sum(pslng_amt) as pslng_amt
    from (      
   	select cstr_cd
         , dpstr_cust_no
         , rdct_lmt_amt_day
         , gs_slng_amt
         , rdct_obj_amt
         , rdct_amt
         , pslng_amt
    from enuri_used_log
    union all
    select cstr_cd
         , dpstr_cust_no
         , rdct_lmt_amt_day
         , gs_slng_amt
         , rdct_obj_amt
         , rdct_amt
         , pslng_amt
    from enuri_cncl_log
           )
    group by cstr_cd, dpstr_cust_no
), enuri_data as ( 
        select t1.dpstr_cust_no
             , t1.cstr_cd                                               -- 관리점코드
             , t4.str_nm
             , t1.excll_cust_grde_mdcls_cd
             , t1.excll_cust_grde_mdcls_nm
             , t1.max_rdcr                                              -- 최대에누리율
             , t1.lmt_ttl_amt                                           -- 지급에누리총액(한도 + 추가)
             , t1.lmt_bsc_amt                                           -- 한도기본금액(한도에누리)
             , t1.lmt_add_amt                                           -- 한도추가금액(추가에누리: 스타포인트를 한도에누리로 전환하여 기존 한도에 추가 or Upsell 프로모션으로 위한 추가 지급)
             , nvl(t1.use_amt,0) as rdct_lmt_amt_day                    -- 에누리사용금액
             , nvl(case when NVL(T1.lmt_ttl_amt,0) = 0 then null else (T1.use_amt*1.0) / (T1.lmt_ttl_amt*1.0) * 100 end,0.0) as enuri_usage_rt
             , nvl(t2.rdct_lmt_amt_day,0)  AS str_rdct_lmt_amt_day      -- 관리점에누리사용금액             
             , nvl(case when NVL(T1.lmt_ttl_amt, 0) = 0 then null else (T2.rdct_lmt_amt_day * 1.0) / (T1.lmt_ttl_amt * 1.0) * 100 end, 0.0) as str_enuri_usage_rt                     
               , nvl(t1.rmnd_amt,0) as rmnd_amt
        from enuri_standard t1
        left join enuri_cstr_cd_used_cncl t2         --관리점별 고객번호 조인
            on t1.cstr_cd = t2.cstr_cd and t1.dpstr_cust_no = T2.dpstr_cust_no
        join lpciddw.tb_dwbs_str_m t4
             on t1.cstr_cd = t4.cstr_cd            
)

select *
from enuri_data s
```
