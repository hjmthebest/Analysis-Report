```python

# -- #########################################################################################
# -- ################                                                         ################
# -- ################                 에누리 소진율 현황 리포트                 ################
# -- ################                                                         ################
# -- #########################################################################################

##############################################
###### 테스트용 리포트 필터 조건 임의 설정 ######
##############################################

# start_date = input("Please insert start date (yyyy-MM-dd): ")
# end_date = input("Please insert end date (yyyy-MM-dd): ")
# selstr_cd = input("Please insert store code (4 digits): ")
# sign = input("Sign: ")
# percentage = int(input("Percentage of enuri"))

start_date = '2023-01-01'
end_date = '2023-04-20'
selstr_cd = '0001'
sign = '>'
percentage = '90'
##############################################

enuri = spark.sql(f"""
    with enuri_standard as (
        select a.dpstr_cust_no                                          -- 고객번호
             , a.selstr_cd                                              -- 선정점코드
             , a.cust_grde_cd                                           -- 고객등급
             , a.max_rdcr                                               -- 최대에누리율
             , ifnull(a.lmt_bsc_amt + a.lmt_add_amt, 0) as lmt_ttl_amt  -- 지급에누리총액(한도+추가)
             , ifnull(a.lmt_bsc_amt, 0) as lmt_bsc_amt                  -- 한도기본금액(한도에누리)
             , ifnull(a.lmt_add_amt, 0) as lmt_add_amt                  -- 한도추가금액(추가에누리: 스타포인트를 전환하여 추가 or Upsell 프로모션 위한 추가 지급)
        from TB_DWCU_NWRDCTLMTCUST_M a                                  -- DWCU_신에누리한도고객_마스터
        where 1 = 1
            and a.lmt_slct_yy = '{start_date[:4]}'
            and ifnull(a.lmt_bsc_amt + a.lmt_add_amt, 0) != 0

    ), enuri_used_log as (
        select b.trns_dt                                                -- 거래일자
             , b.cstr_cd
             , b.pos_no
             , b.rcpt_no
             , b.cpn_evt_no
             , b.dpstr_cust_no
             , ifnull(b.rdct_lmt_amt, 0) as rdct_lmt_amt_day            -- 에누리한도금액(일자별)
             , ifnull(b.gs_slng_amt, 0) as gs_slng_amt                  -- 총매출금액
             , ifnull(b.rdct_obj_amt, 0) as rdct_obj_amt                -- 에누리대상금액
             , ifnull(b.rdct_amt, 0) as rdct_amt                        -- 에누리금액
             , ifnull(b.pslng_amt, 0) as pslng_amt                      -- 순매출금액
        from TB_DWSL_NWRDCTCPNUSE_L b                                   -- DWSL_신에누리쿠폰사용_내역
        where 1 = 1
            and b.trns_dt between '{start_date[:4]}-01-01' and '{start_date[:4]}-12-31'         -- 거래일자 범위: 설정년도 
            and b.cpn_evt_no like '9%'                                  -- 우수고객 한도/상시
            and b.rdct_lmt_amt != 0

    ), enuri_cncl_log as (
        select c.trns_dt
             , c.cstr_cd
             , c.pos_no
             , c.rcpt_no
             , c.cpn_evt_no
             , c.dpstr_cust_no
             , ifnull(-c.rdct_lmt_amt, 0) as rdct_lmt_amt_day           -- 에누리한도금액 중 취소금액(일자별)
             , ifnull(c.gs_slng_amt, 0) as gs_slng_amt                  -- 총매출금액
             , ifnull(c.rdct_obj_amt, 0) as rdct_obj_amt                -- 에누리대상금액
             , ifnull(c.rdct_amt, 0) as rdct_amt                        -- 에누리금액
             , ifnull(c.pslng_amt, 0) as pslng_amt                      -- 순매출금액
        from TB_DWSL_NWRDCTCPNUSECANC_L c                               -- DWSL_신에누리쿠폰사용취소_내역
        where 1 = 1
            and c.trns_dt between '{start_date[:4]}-01-01' and '{start_date[:4]}-12-31'         -- 거래취소일자 범위: 설정년도 
            and c.cpn_evt_no like '9%'                                  -- 우수고객 한도/상시
            and c.rdct_lmt_amt != 0

    ), tb_base as (
        select t1.dpstr_cust_no
             , t1.selstr_cd
             , case when t1.cust_grde_cd = '19' then 'AVE-BLACK'
                    when t1.cust_grde_cd = '21' then 'AVE-EMERALD'
                    when t1.cust_grde_cd = '2A' then 'AVE-PURPLE1'
                    when t1.cust_grde_cd = '22' then 'AVE-PURPLE2'
                    when t1.cust_grde_cd = '2B' then 'AVE-PURPLE3'
                    when t1.cust_grde_cd = '23' then 'AVE-PURPLE4'
                    when t1.cust_grde_cd in ('24', '25', '26') then 'AVE-ORANGE'
                    when t1.cust_grde_cd = '5A' then 'AVE-GREEN1'
                    when t1.cust_grde_cd = '51' then 'AVE-GREEN2'
                    when t1.cust_grde_cd = '52' then 'AVE-GREEN3'
                    when t1.cust_grde_cd = '53' then 'PRE-GREEN'
                    else 'Customer' end as cust_grde                    -- 고객등급 (조회시점 등급 기준, 연내 등급 변동되어도 현재등급기준)
             , t2.trns_dt                                               -- 거래일자
             , lpad(cast(t2.cstr_cd as string), "4", "0") as cstr_cd    -- 자점코드             
             , t1.max_rdcr                                              -- 최대에누리율
             , t1.lmt_ttl_amt                                           -- 지급에누리총액(한도+추가)
             , t1.lmt_bsc_amt                                           -- 한도기본금액(한도에누리)
             , t1.lmt_add_amt                                           -- 한도추가금액(추가에누리: 스타포인트를 전환하여 추가 or Upsell 프로모션 위한 추가 지급)

             , lpad(cast(t2.pos_no as string), "4", "0") as pos_no      -- POS번호
             , lpad(cast(t2.rcpt_no as string), "4", "0") as rcpt_no    -- 영수증번호
             , t2.cpn_evt_no                                            -- 쿠폰이벤트번호

             , t2.rdct_lmt_amt_day                                      -- 에누리한도금액(일자별)
             , t2.gs_slng_amt                                           -- 총매출금액(일자별)
             , t2.rdct_obj_amt                                          -- 에누리대상금액(일자별)
             , t2.rdct_amt                                              -- 에누리금액(일자별)
             , t2.pslng_amt                                             -- 순매출금액(일자별)

        from enuri_standard t1
        left join ((select b.* from enuri_used_log b) union (select c.* from enuri_cncl_log c)) t2
            on t1.dpstr_cust_no = t2.dpstr_cust_no
        where 1 = 1
    )
    select
    from (
        select t.dpstr_cust_no
             , t.selstr_cd
             , t.cust_grde
             , t.max_rdcr
             , t.lmt_ttl_amt
             , t.lmt_bsc_amt
             , t.lmt_add_amt
             , sum(t.rdct_lmt_amt_day) as enuri_used_amt
             , round((sum(t.rdct_lmt_amt_day) / t.lmt_ttl_amt) * 100, 1) as used_enuri_rt    -- 에누리소진율(%)
             , sum(t.gs_slng_amt) as pch_amt
             , sum(t.pslng_amt) as pslng_amt
        from tb_base t
        where 1 = 1
            and t.trns_dt between '{start_date}' and '{end_date}'           -- 에누리 거래일자 범위 지정: (필수 지정값)
            and t.selstr_cd = '{selstr_cd}'                                 -- 리포트 필터 내 여러 점포도 지정할 수 있도록 조치 필요 (DEFAULT: 전점)
        group by 1, 2, 3, 4, 5, 6, 7
        having used_enuri_rt {sign} {percentage}                            -- 리포트 필터 내 메트릭 범위 지정할 수 있도록 조치 필요 (DEFAULT: 전체)
        order by 1
        ) s
    """)

enuri.createOrReplaceTempView("enuri")
enuri.cache()
enuri.show()
```
