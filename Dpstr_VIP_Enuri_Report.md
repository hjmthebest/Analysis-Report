```python
# #########################################################################################
# ################                                                         ################
# ################                  에누리소진율 현황 리포트                 ################
# ################                                                         ################
# #########################################################################################

##############################################
##### 테스트를 위한 리포트 필터 임의 설정 #####
##############################################
start_date = '2023-03-01'
end_date = '2023-04-26'
selstr_cd = '0001'
##############################################

enuri = spark.sql(f"""
    with enuri_standard as (
        select a.dpstr_cust_no                                          -- 고객번호
             , a.selstr_cd                                              -- 선정점코드(우수고객관리점과 일치?)
             , a.cust_grde_cd                                           -- 고객등급
             , a.lmt_st_dt                                              -- 한도시작일자
             , a.max_rdcr                                               -- 최대에누리율
             , ifnull(a.lmt_bsc_amt + a.lmt_add_amt, 0) as lmt_ttl_amt  -- 지급에누리총액(한도 + 추가)
             , ifnull(a.lmt_bsc_amt, 0) as lmt_bsc_amt                  -- 한도기본금액(한도에누리)
             , ifnull(a.lmt_add_amt, 0) as lmt_add_amt                  -- 한도추가금액(추가에누리: 스타포인트를 한도에누리 전환하여 기존 한도에 추가 or Upsell 프로모션 위한 추가 지급)
        from TB_DWCU_NWRDCTLMTCUST_M a                                  -- DWCU_신에누리한도고객_마스터
        where 1 = 1
            and a.lmt_st_dt >= '{start_date[:4]}-01-01'                 -- 한도시작일자 (조회년도)
            and a.selstr_cd = '{selstr_cd}'                             -- 리포트 필터 내 여러 점포도 지정할 수 있도록 조치 필요 (DEFAULT: 전점)
            and a.cpn_evt_no = '999999990010'                           -- 우수고객 한도/상시
--            and a.cust_grde_cd in ('19', '21', '2A', '22', '2B', '23', '24', '5A', '51', '52')

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
            and b.trns_dt between '{start_date}' and '{end_date}'       -- 거래일자 범위: 설정년도 
            and b.cpn_evt_no = '999999990010'                           -- 우수고객 한도/상시

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
            and c.trns_dt between '{start_date}' and '{end_date}'       -- 거래취소일자 범위: 설정년도 
            and c.cpn_evt_no = '999999990010'                           -- 우수고객 한도/상시

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
                    else 'Customer' end as cust_grde                    -- 고객등급 (조회시점 등급 기준, 연내 등급 바뀌어도 현재 등급 기준)
             , t1.max_rdcr                                              -- 최대에누리율
             , t1.lmt_ttl_amt                                           -- 지급에누리총액(한도 + 추가)
             , t1.lmt_bsc_amt                                           -- 한도기본금액(한도에누리)
             , t1.lmt_add_amt                                           -- 한도추가금액(추가에누리: 스타포인트를 한도에누리로 전환하여 기존 한도에 추가 or Upsell 프로모션으로 위한 추가 지급)

             , t2.trns_dt                                               -- 거래일자
             , lpad(cast(t2.cstr_cd as string), "4", "0") as cstr_cd    -- 자점코드             
             , lpad(cast(t2.pos_no as string), "4", "0") as pos_no      -- POS번호
             , lpad(cast(t2.rcpt_no as string), "4", "0") as rcpt_no    -- 영수증번호
             , t2.cpn_evt_no                                            -- 쿠폰이벤트번호

             , t2.rdct_lmt_amt_day                                     -- 에누리한도금액(일자별)
             , t2.gs_slng_amt                                          -- 총매출금액(일자별)
             , t2.rdct_obj_amt                                         -- 에누리대상금액(일자별)
             , t2.rdct_amt                                             -- 에누리금액(일자별)
             , t2.pslng_amt                                            -- 순매출금액(일자별)
        from enuri_standard t1
        left join ((select b.* from enuri_used_log b) union (select c.* from enuri_cncl_log c)) t2
            on t1.dpstr_cust_no = t2.dpstr_cust_no
        where 1 = 1
--            and t1.cust_grde_cd in ('19', '21', '2A', '22', '2B', '23', '24', '5A', '51', '52')   
    ), tb_base_v2 as (
        select t.dpstr_cust_no
             , t.selstr_cd
             , case when t.selstr_cd = t.cstr_cd then 1 else 0 end as selstr_yn
             , t.cust_grde
             , t.max_rdcr
             , t.lmt_ttl_amt
             , t.lmt_bsc_amt
             , t.lmt_add_amt
             , count(t.dpstr_cust_no) over(partition by t.dpstr_cust_no) as count_cust
             , ifnull(sum(sum(t.rdct_lmt_amt_day)) over(partition by t.dpstr_cust_no), 0) as enuri_usage_amt                                            -- 에누리사용금액
             , ifnull(round(((sum(sum(t.rdct_lmt_amt_day)) over(partition by t.dpstr_cust_no)) / t.lmt_ttl_amt) * 100, 1), 0) as enuri_usage_rt         -- 에누리소진율(%)
             , ifnull(sum(case when t.selstr_cd = t.cstr_cd then t.rdct_lmt_amt_day end), 0) as str_enuri_usage_amt                                     -- 관리점에누리사용금액
             , ifnull(round((sum(case when t.selstr_cd = t.cstr_cd then t.rdct_lmt_amt_day end) / t.lmt_ttl_amt) * 100, 1), 0) as str_enuri_usage_rt    -- 관리점에누리소진율(%)
        from tb_base t
        where 1 = 1
            and t.selstr_cd != '0000'        -- 본사 제외
        group by 1, 2, 3, 4, 5, 6, 7, 8
        order by 1
    )
    select s.dpstr_cust_no
         , s.selstr_cd
         , s.cust_grde
         , s.max_rdcr
         , s.lmt_ttl_amt
         , s.lmt_bsc_amt
         , s.lmt_add_amt
         , s.enuri_usage_amt
         , s.enuri_usage_rt
         , s.str_enuri_usage_amt
         , s.str_enuri_usage_rt
    from tb_base_v2 s
    where 1 = 1
        and ((s.count_cust = 1) or (s.count_cust = 2 and s.selstr_yn = 1))
    """)

enuri.createOrReplaceTempView("enuri")
enuri.cache()
enuri.show()
```

Result
![image](https://user-images.githubusercontent.com/67835149/234806467-bbfd9197-c34e-4503-91fb-8ace238a9597.png)


시스템 배포 화면
![BI_Tool](https://user-images.githubusercontent.com/67835149/235053226-240e2cb6-4ef7-43f0-a760-6a3e4ee8b768.png)

