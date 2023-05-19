```python

import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession().builder \
		      .master("yarn") \
		      .appName("enuri_report") \
		      .enableHiveSupport()

enuri = spark.sql(f"""
    with enuri_standard as (
        select t1.dpstr_cust_no
             , t3.cstr_cd
             , ifnull(t4.excll_cust_grde_mdcls_cd, 'Customer') as excll_cust_grde_mdcls_cd
             , ifnull(t4.excll_cust_grde_mdcls_nm, 'Customer') as excll_cust_grde_mdcls_nm
             , max(t1.max_rdcr) as max_rdcr                              -- 에누리율
             , sum(t1.lmt_bsc_amt) + sum(t1.lmt_add_amt) as lmt_ttl_amt  -- 지급에누리총액(한도 + 추가)
             , sum(t1.lmt_bsc_amt) as lmt_bsc_amt                        -- 한도기본금액(한도에누리)
             , sum(t1.lmt_add_amt) as lmt_add_amt                        -- 한도추가금액(추가에누리: AVENUEL POINT 전환 or 프로모션 한도 추가)
             , sum(use_amt) - sum(cncl_amt) as use_amt
             , sum(rmnd_amt) as rmnd_amt
            from lpciddw.TB_DWCU_NWRDCTLMTCUST_M t1                      -- DWCU_신에누리한도고객_마스터
            left join lpciddm.tb_dmcu_yrexcllcustgrdeinfo_i t2
                on (t1.dpstr_cust_no = t2.dpstr_cust_no) and (t2.excll_cust_slct_yy = date_format(date_sub(current_date(), 1), 'yyyy')) -- 어제일자 당해년도 등급만 확인
            join lpciddm.tb_dmct_excllcustsmcls_c t3
                on t2.now_excll_cust_grde_smcls_cd = t3.excll_cust_grde_smcls_cd
            join lpciddm.tb_dmct_excllcustmdcls_c t4
                on t3.excll_cust_grde_mdcls_cd = t4.excll_cust_grde_mdcls_cd
            where 1 = 1
                and t3.excll_cust_grde_mdcls_cd in ('1119', '1221', '122A', '1222', '122B', '1223', '1224', '155A', '1551')  -- 우수고객 필터링
                and t1.cpn_evt_no = '999999990010'                                                                           -- 우수고객 연간 한도에누리
                and t1.lmt_st_dt between '2023-01-01' and '2023-12-31'
            group by t1.dpstr_cust_no
                , t3.cstr_cd
                , ifnull(t4.excll_cust_grde_mdcls_cd, 'Customer')
                , ifnull(t4.excll_cust_grde_mdcls_nm, 'Customer')

        ), enuri_used_log as (
           select  t5.cstr_cd
                 , t5.dpstr_cust_no
                 , t5.rdct_lmt_amt as rdct_lmt_amt_day                -- 에누리한도금액(일자별)
                 , t5.gs_slng_amt as gs_slng_amt                      -- 총매출금액
                 , t5.rdct_obj_amt as rdct_obj_amt                    -- 에누리대상금액
                 , t5.rdct_amt as rdct_amt                            -- 에누리금액
                 , t5.pslng_amt as pslng_amt                          -- 순매출금액
            from lpciddw.TB_DWSL_NWRDCTCPNUSE_L t5                    -- DWSL_신에누리쿠폰사용_내역
            where 1 = 1
                and t5.trns_dt between '2023-01-01' and '2023-12-31'  -- 거래일자 범위 
                and t5.cpn_evt_no = '999999990010'                    -- 우수고객 연간 한도에누리
                and t5.use_rsn_cd  = '00'

        ), enuri_cncl_log as (
            select t6.cstr_cd
                 , t6.dpstr_cust_no
                 , -t6.rdct_lmt_amt as rdct_lmt_amt_day                     -- 에누리한도금액 중 취소금액(일자별)
                 , t6.gs_slng_amt as gs_slng_amt                            -- 총매출금액
                 , t6.rdct_obj_amt as rdct_obj_amt                          -- 에누리대상금액
                 , t6.rdct_amt as rdct_amt                                  -- 에누리금액
                 , t6.pslng_amt as pslng_amt                                -- 순매출금액
            from lpciddw.TB_DWSL_NWRDCTCPNUSECANC_L t6                      -- DWSL_신에누리쿠폰사용취소_내역
            where 1 = 1
                and t6.trns_dt between '2023-01-01' and '2023-12-31'        -- 거래취소일자 범위: 설정년도 
                and t6.cpn_evt_no = '999999990010'                          -- 우수고객 연간 한도에누리
                and t6.use_rsn_cd  = '00'
                and t6.trns_dt >= '2023-01-01'                              -- 거래취소시작일자가 속한 년도의 1월1일 이전값은 제외

        ), enuri_cstr_cd_used_cncl as (                                     -- 점/고객번호별 사용취소금액 합산
            select t9.cstr_cd
                 , t9.dpstr_cust_no
                 , sum(t9.rdct_lmt_amt_day) as rdct_lmt_amt_day
                 , sum(t9.gs_slng_amt) as gs_slng_amt 
                 , sum(t9.rdct_obj_amt) as rdct_obj_amt 
                 , sum(t9.rdct_amt) as rdct_amt
                 , sum(t9.pslng_amt) as pslng_amt
            from (
                select t7.cstr_cd
                     , t7.dpstr_cust_no
                     , t7.rdct_lmt_amt_day
                     , t7.gs_slng_amt
                     , t7.rdct_obj_amt
                     , t7.rdct_amt
                     , t7.pslng_amt
                from enuri_used_log t7 
                union all
                select t8.cstr_cd
                     , t8.dpstr_cust_no
                     , t8.rdct_lmt_amt_day
                     , t8.gs_slng_amt
                     , t8.rdct_obj_amt
                     , t8.rdct_amt
                     , t8.pslng_amt
               from enuri_cncl_log t8
               ) t9
            group by t9.cstr_cd
                   , t9.dpstr_cust_no
       ), enuri_data as ( 
            select t10.dpstr_cust_no
                 , t10.cstr_cd                                 -- 우수고객등급관리점코드
                 , t12.str_nm                                  -- 우수고객등급관리점명
                 , t10.excll_cust_grde_mdcls_cd                -- 우수고객등급중분류코드
                 , t10.excll_cust_grde_mdcls_nm                -- 우수고객등급중분류명
                 , t10.max_rdcr                                -- 최대에누리율
                 , t10.lmt_ttl_amt                             -- 지급에누리총액(한도 + 추가)
                 , t10.lmt_bsc_amt                             -- 한도기본금액(한도에누리)
                 , t10.lmt_add_amt                             -- 한도추가금액(추가에누리: AVENUEL POINT 전환 or 프로모션 한도 추가)
                 , ifnull(t10.use_amt, 0) as rdct_lmt_amt_day  -- 에누리사용금액
                 , ifnull(t10.rmnd_amt, 0) as rmnd_amt         -- 에누리잔여금액
		 , ifnull(case when ifnull(t10.lmt_ttl_amt,0) = 0 then null else (t10.use_amt / t10.lmt_ttl_amt * 100) end, 0) as enuri_usage_rt              -- 에누리소진율(%)
                 , ifnull(t11.rdct_lmt_amt_day, 0) as str_rdct_lmt_amt_day                                                                                    -- 관리점에누리사용금액 
                 , ifnull(case when ifnull(t10.lmt_ttl_amt,0) = 0 then null else (t11.rdct_lmt_amt_day / t10.lmt_ttl_amt * 100) end, 0) as str_enuri_usage_rt -- 관리점에누리소진율(%)                   

            from enuri_standard t10
            left join enuri_cstr_cd_used_cncl t11              -- 관리점별 고객번호 조인
                on t10.cstr_cd = t11.cstr_cd
                and t10.dpstr_cust_no = t11.dpstr_cust_no
            join lpciddw.tb_dwbs_str_m t12
                 on t10.cstr_cd = t12.cstr_cd            
    )
    select t13.*
    from enuri_data t13

    """)

enuri.createOrReplaceTempView("enuri")
enuri.cache()
enuri.show()
```

1. Query Result
![image](https://github.com/hjmthebest/SQL/assets/67835149/eec73e6d-501b-4e0e-8315-edf33ea431ac)

2. Result 
![enuri_report_landing_page](https://github.com/hjmthebest/SQL/assets/67835149/02d3399a-2b6b-45d2-9156-33a902803e67)

3. Report
![enuri_report_result](https://github.com/hjmthebest/SQL/assets/67835149/05f7cf4c-4e6a-49de-a21c-852f8fa8282d)
