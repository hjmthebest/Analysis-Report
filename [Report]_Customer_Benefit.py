%pyspark

# /* Table 1. 사은할인 최종테이블 */
table1_fgft = spark.sql("""
    select t.*
    from (
        select a.dpstr_cust_no
             , case when substring(d.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119') then 'AVE' 
                    when substring(d.excll_cust_grde_smcls_cd, 1, 4) = '1221' then 'MVG_L'
                    when substring(d.excll_cust_grde_smcls_cd, 1, 4) = '1222' then 'MVG_P'
                    when substring(d.excll_cust_grde_smcls_cd, 1, 4) = '1223' then 'MVG_C'
                    when substring(d.excll_cust_grde_smcls_cd, 1, 4) = '1224' then 'MVG_A'
                    when substring(d.excll_cust_grde_smcls_cd, 1, 4) = '1551' then 'VIP+'
                    when substring(d.excll_cust_grde_smcls_cd, 1, 4) = '1552' then 'VIP'
                    else 'NON_EXCLL' end as cust_grde
             , a.std_dt
             , date_format(a.std_dt, 'yyyyMM') as std_ym
            --  , a.cstr_cd
             , case when b.fgft_clsf_cd in ('1') then '사은품증정'            -- 현물
                    when b.fgft_clsf_cd in ('2', '8') then '상품권증정'       -- 상품권, 모바일상품권 
                    when b.fgft_clsf_cd in ('11') then 'L.POINT_적립혜택'     -- L.POINT
               end as new_ctgr
             , c.fgft_clsf_cd
            --  , c.fgft_clsf_nm
            --  , b.fgft_nm
             , ifnull(b.ucst, 0) as ucst
             , e.thku_sys_evnt_nm as note
             , a.thku_evnt_no 
             , a.thku_sys_fgft_cd
             , a.cstr_cd
        from tb_dmcs_ddcustfgftppay_f a
        join lpciddw.tb_dwbs_thkuitem_m b
            on a.thku_sys_fgft_cd = b.thku_sys_fgft_cd
        join tb_dwct_fgftclsf_c c
            on b.fgft_clsf_cd = cast(c.fgft_clsf_cd as int)
        join lpciddm.tb_dmcs_mmcustinfo_f d
            on a.dpstr_cust_no = d.dpstr_cust_no
            and date_format(a.std_dt, 'yyyyMM') = d.std_ym
        join lpciddw.tb_dwbs_thkuevnt_m e
            on a.thku_evnt_no = e.thku_evnt_no 
            and a.cstr_cd = e.cstr_cd
        where 1 = 1
            and a.std_dt between cast('2021-01-01' as date) and cast('2021-12-31' as date)
        ) t
    where 1 = 1
        and t.new_ctgr is not null
    """)

table1_fgft.createOrReplaceTempView("table1_fgft")
table1_fgft.cache()
# table1_fgft.show()

# /* Table 2. 우수고객라운지 최종테이블 */
table2_excll_lounge = spark.sql("""
    select a.dpstr_cust_no
         , case when substring(d.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119') then 'AVE' 
                when substring(d.excll_cust_grde_smcls_cd, 1, 4) = '1221' then 'MVG_L'
                when substring(d.excll_cust_grde_smcls_cd, 1, 4) = '1222' then 'MVG_P'
                when substring(d.excll_cust_grde_smcls_cd, 1, 4) = '1223' then 'MVG_C'
                when substring(d.excll_cust_grde_smcls_cd, 1, 4) = '1224' then 'MVG_A'
                when substring(d.excll_cust_grde_smcls_cd, 1, 4) = '1551' then 'VIP+'
                when substring(d.excll_cust_grde_smcls_cd, 1, 4) = '1552' then 'VIP'
                else 'NON_EXCLL' end as cust_grde
     	 , a.utl_dt as std_dt
     	 , date_format(a.utl_dt, 'yyyyMM') as std_ym 
        --  , a.str_cd as cstr_cd 
         , case when substring(d.excll_cust_grde_smcls_cd, 1, 4) in ('1551', '1552') then "VIP_Bar_Service" 
                else "Lounge_Service" end as new_ctgr
         , count(*) * 5000 as ucst
     	 , max(c.drnk_ord_prdc_nm) as note
    from lpciddw.tb_dwcu_viplnguse_l a
    join tb_dwcu_viplngeordutl_l b
        on a.seqno_no = b.seqno_no and a.utl_dt = b.utl_dt
    join tb_dwbs_viplngeordmenu_l c
        on b.drnk_ord_no = c.drnk_ord_no
    join lpciddm.tb_dmcs_mmcustinfo_f d
        on a.dpstr_cust_no = d.dpstr_cust_no
        and date_format(a.utl_dt, 'yyyyMM') = d.std_ym
    where 1 = 1
        and a.utl_dt between cast('2021-01-01' as date) and cast('2021-12-31' as date) 
        and substring(d.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119', '1221', '1222', '1223', '1224', '1551', '1552')
    group by 1, 2, 3, 4, 5
    """)
    
table2_excll_lounge.createOrReplaceTempView("table2_excll_lounge")
table2_excll_lounge.cache()
# table2_excll_lounge.show()

# /* Table 3. 우수고객증정품  (srno 포함 시 중복없으나 srno를 제외후 distinct 적용해야 함) */
table3_excll_tgft = spark.sql("""
    select a.dpstr_cust_no
         , case when substring(c.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119') then 'AVE' 
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1221' then 'MVG_L'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1222' then 'MVG_P'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1223' then 'MVG_C'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1224' then 'MVG_A'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1551' then 'VIP+'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1552' then 'VIP'
                else 'NON_EXCLL' end as cust_grde
         , a.prst_dt as std_dt
         , date_format(a.prst_dt, 'yyyyMM') as std_ym
--         , a.mgm_str_cd as cstr_cd
--         , a.prst_str_cd
--         , b.excll_cust_tgft_cd
         , case when a.excll_cust_tgft_cl_cd = '10' then '무료주차서비스'              -- 주차권(10) 
--              when a.excll_cust_tgft_cl_cd = '20' then '선정감사품'
                when a.excll_cust_tgft_cl_cd = '30' then '기념일감사품증정'            -- 기념일감사품(30)
--              when a.excll_cust_tgft_cl_cd = '40' then '명절감사품(설)'
--              when a.excll_cust_tgft_cl_cd = '50' then '명절감사품(추석)'
                when a.excll_cust_tgft_cl_cd = '60' then '상품권증정'                  -- 리워드감사품(60) : L.O.V.E 마일리지
--              when a.excll_cust_tgft_cl_cd = '70' then '공항라운지바우처'
--              when a.excll_cust_tgft_cl_cd = '99' then '기타'
                end as new_ctgr
         , ifnull(b.excll_cust_tgft_ucst, 0) as ucst
         , case when a.excll_cust_tgft_cl_cd = '60' then 'L.O.V.E_마일리지_Gift증정'
                else b.excll_cust_tgft_nm end as note
         , a.srno                
    from tb_dwcu_vipexcllcusttgftprst_l a                                             -- DWCU_VIP우수고객증정품증정_내역
    join tb_dwcu_vipexcllcusttgft_i b                                                 -- DWCU_VIP우수고객증정품_정보
        on a.excll_cust_pgm_cd = b.excll_cust_pgm_cd
        and a.excll_cust_slct_yy = b.excll_cust_slct_yy
        and a.excll_cust_slct_ordr = b.excll_cust_slct_ordr
        and a.excll_cust_grde_cd = b.excll_cust_grde_cd
        and a.excll_cust_tgft_cl_cd = b.excll_cust_tgft_cl_cd
        and a.excll_cust_tgft_cd = b.excll_cust_tgft_cd
    join lpciddm.tb_dmcs_mmcustinfo_f c
        on a.dpstr_cust_no = c.dpstr_cust_no
        and date_format(a.prst_dt, 'yyyyMM') = c.std_ym
    where 1 = 1
        and a.prst_dt between cast('2021-01-01' as date) and cast('2021-12-31' as date)
        and a.excll_cust_tgft_cl_cd in ('10', '30', '60')
        and wdw_yn <> 'Y'
   """)
table3_excll_tgft.createOrReplaceTempView("table3_excll_tgft")
table3_excll_tgft.cache()
# table3_excll_tgft.show()

# Table 4. 스타멤버십 최종테이블
table4_star_membshp = spark.sql("""
    select a.dpstr_cust_no
         , case when substring(c.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119') then 'AVE' 
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1221' then 'MVG_L'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1222' then 'MVG_P'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1223' then 'MVG_C'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1224' then 'MVG_A'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1551' then 'VIP+'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1552' then 'VIP'
                else 'NON_EXCLL' end as cust_grde
         , a.app_dt as std_dt
         , date_format(a.app_dt, 'yyyyMM') as std_ym
--       , a.app_str_cd as cstr_cd
         , case when a.pnt_tgft_cl_cd in ('0001', '0002' ,'0003', '0004', '0005', '0006', '0009') then '프리미엄서비스' -- SPA&프리미엄서비스(0001), 호텔리조트(0002), 고메(0003), 쇼핑(0004), 엔터테인먼트(0005), 기프트(0006), P.EVENT(0009)
                when a.pnt_tgft_cl_cd = '0007' then '포인트에누리혜택'                                                  -- 에누리(0007)
--              when a.pnt_tgft_cl_cd = '0008' then '무료주차서비스'                                                    -- 주차권(0008) : 스타멤버십 주차권 제도 폐지 (사용 안 함)
                end as new_ctgr
         , a.use_pnt as ucst
         , b.pnt_tgft_nm as note
         , a.pnt_svc_app_no 
    from tb_dwcu_vipcustpntuse_l a
    join tb_dwbs_vippnttgft_i b
        on a.excll_cust_pgm_cd = b.excll_cust_pgm_cd
        and a.excll_cust_slct_yy = b.excll_cust_slct_yy
        and a.excll_cust_slct_ordr = b.excll_cust_slct_ordr
        and a.pnt_tgft_cl_cd = b.pnt_tgft_cl_cd
        and a.pnt_tgft_cd = b.pnt_tgft_cd
    join lpciddm.tb_dmcs_mmcustinfo_f c
        on a.dpstr_cust_no = c.dpstr_cust_no
        and date_format(a.app_dt, 'yyyyMM') = c.std_ym
    where 1 = 1
        and a.app_dt between cast('2021-01-01' as date) and cast('2021-12-31' as date)
    """)
table4_star_membshp.createOrReplaceTempView("table4_star_membshp")
table4_star_membshp.cache()
# table4_star_membshp.show()

# Table 5. 에누리 최종테이블 (일구매 팩트를 distinct 해서 구매할인내역에 조인)
table5_enuri = spark.sql("""
    select b.dpstr_cust_no
         , case when substring(c.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119') then 'AVE' 
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1221' then 'MVG_L'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1222' then 'MVG_P'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1223' then 'MVG_C'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1224' then 'MVG_A'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1551' then 'VIP+'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1552' then 'VIP'
                else 'NON_EXCLL' end as cust_grde
         , b.std_dt
         , b.std_ym
         , "에누리혜택" as new_ctgr
         , a.evnt_dc_amt as ucst
         , a.evnt_nm as note
         , a.CSTR_CD
         , a.POS_NO
         , a.RCPT_NO
         , a.DC_DTL_NO
--       , a.rcpt_dtl_no  -- 해당 컬럼값 데이터 정합성 맞지 않는 부분 수정할 것 
--       , a.rdct_amt
--       , a.dc_amt
    from tb_dwsl_trnsdc_l a   -- 거래할인내역 테이블
    join (select distinct STD_DT, STD_ym, DPSTR_CUST_NO, CSTR_CD, POS_NO, RCPT_NO from lpciddm.tb_dmcs_ddcustpchdtl_f) b -- DMCS_일고객구매상세_팩트
        on a.cstr_cd = b.cstr_cd
        and a.trns_dt between cast('2021-01-01' as date) and cast('2021-12-31' as date)
        and a.pos_no = b.pos_no
        and a.rcpt_no = b.rcpt_no
        and a.trns_dt = b.std_dt  
    join lpciddm.tb_dmcs_mmcustinfo_f c
        on b.dpstr_cust_no = c.dpstr_cust_no
        and b.STD_ym = c.std_ym
    """)
table5_enuri.createOrReplaceTempView("table5_enuri")
table5_enuri.cache()
# table5_enuri.show()

# Table 6. 고객접점서비스 최종테이블
table6_pocsvc = spark.sql("""
    select distinct a.dpstr_cust_no
         , case when substring(c.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119') then 'AVE' 
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1221' then 'MVG_L'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1222' then 'MVG_P'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1223' then 'MVG_C'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1224' then 'MVG_A'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1551' then 'VIP+'
                when substring(c.excll_cust_grde_smcls_cd, 1, 4) = '1552' then 'VIP'
                else 'NON_EXCLL' end as cust_grde
         , a.svc_rcept_dt as std_dt
         , date_format(a.svc_rcept_dt, 'yyyyMM') as std_ym
         , '대여보관서비스' as new_ctgr
--       , a.mgm_str_cd as cstr_cd
--       , case when a.prvd_svc_cd = 'SV001' then '유모차대여'
--              when a.prvd_svc_cd = 'SV002' then '보냉시트대여'
--              when a.prvd_svc_cd = 'SV003' then '보온시트대여'
--              when a.prvd_svc_cd = 'SV004' then '담요대여'
--              when a.prvd_svc_cd = 'SV005' then '컵홀더대여'
--              when a.prvd_svc_cd = 'SV006' then '우산대여'
--              when a.prvd_svc_cd = 'SV007' then '휠체어대여'
--              when a.prvd_svc_cd = 'SV008' then '보조배터리대여'
--              when a.prvd_svc_cd = 'SV009' then '휴대폰충전'
--              when a.prvd_svc_cd = 'SV010' then '반려동물용품대여'
--              when a.prvd_svc_cd = 'SV011' then '기타물품대여'
--              when a.prvd_svc_cd = 'SV012' then '일반보관'
--              when a.prvd_svc_cd = 'SV013' then '냉장보관'
--              when a.prvd_svc_cd = 'SV014' then '냉동보관'
--              when a.prvd_svc_cd = 'SV015' then '캐리어보관'
--              when a.prvd_svc_cd = 'SV016' then '외투보관'
--              when a.prvd_svc_cd = 'SV101' then '유모차푸쉬카대여'
--              when a.prvd_svc_cd = 'SV102' then '유모차대여_및_휴게실안내'
--              when a.prvd_svc_cd = 'SV103' then '유모차_쌍둥이대여'
--              when a.prvd_svc_cd = 'SV104' then '유모차_기타대여'
--              end as new_ctgr
         , 5000 as ucst -- 단가 미확정
         , b.prvd_svc_nm as note
         , a.svc_srno
         , a.mgm_str_cd
    from tb_dwcu_pocsvc_i a
    join (select distinct prvd_svc_cd, prvd_svc_nm from tb_dwbs_vipmbrsvctp_i) b
        on a.prvd_svc_cd = b.prvd_svc_cd
    join lpciddm.tb_dmcs_mmcustinfo_f c
        on a.dpstr_cust_no = c.dpstr_cust_no
        and date_format(a.svc_rcept_dt, 'yyyyMM') = c.std_ym
    where 1 = 1
        and a.svc_rcept_dt between cast('2021-01-01' as date) and cast('2021-12-31' as date) 
        and a.memb_cust_yn = 'Y' -- 회원만 불러 올 것
    """)
table6_pocsvc.createOrReplaceTempView("table6_pocsvc")
table6_pocsvc.cache()
# table6_pocsvc.show()

# 고객혜택리포트 (Customer Benefit Report) - Final
bnft_report_final = spark.sql("""    
    select t1.dpstr_cust_no, t1.cust_grde, t1.std_dt, t1.std_ym, t1.new_ctgr, t1.ucst, t1.note
    from table1_fgft t1         -- 사은
    union all 
    select t2.dpstr_cust_no, t2.cust_grde, t2.std_dt, t2.std_ym, t2.new_ctgr, t2.ucst, t2.note
    from table2_excll_lounge t2 -- 우수고객라운지
    union all 
    select distinct t3.dpstr_cust_no, t3.cust_grde, t3.std_dt, t3.std_ym, t3.new_ctgr, t3.ucst, t3.note
    from table3_excll_tgft t3   -- 우수고객증정품 
    union all 
    select t4.dpstr_cust_no, t4.cust_grde, t4.std_dt, t4.std_ym, t4.new_ctgr, t4.ucst, t4.note
    from table4_star_membshp t4 -- 스타멤버십
    union all 
    select t5.dpstr_cust_no, t5.cust_grde, t5.std_dt, t5.std_ym, t5.new_ctgr, t5.ucst, t5.note
    from table5_enuri t5        -- 에누리
    union all 
    select t6.dpstr_cust_no, t6.cust_grde, t6.std_dt, t6.std_ym, t6.new_ctgr, t6.ucst, t6.note
    from table6_pocsvc t6       -- 고객접점서비스(대여/보관)
    """)
bnft_report_final.createOrReplaceTempView("bnft_report_final")
bnft_report_final.cache()
bnft_report_final.show()

# Result
+-------------+---------+----------+------+----------+------+-----------------------------+
|dpstr_cust_no|cust_grde|    std_dt|std_ym|  new_ctgr|  ucst|                         note|
+-------------+---------+----------+------+----------+------+-----------------------------+
|1234593834061|NON_EXCLL|2021-03-05|202103|상품권증정|950000|[분당점]롯데롤라카드제휴 삼성...|
|1237824783657|NON_EXCLL|2021-03-06|202103|상품권증정|950000|[분당점]롯데롤라카드제휴 삼성...|
|1237824783657|NON_EXCLL|2021-03-13|202103|상품권증정|950000|[분당점]롯데롤라카드제휴 삼성...|
|1237824783657|NON_EXCLL|2021-03-06|202103|상품권증정|285000|[분당점]롯데롤라카드제휴 삼성...|
|1237824783657|NON_EXCLL|2021-03-13|202103|상품권증정|285000|[분당점]롯데롤라카드제휴 삼성...|
|1234465929516|NON_EXCLL|2021-03-05|202103|상품권증정| 95000|[분당점]롯데롤라카드제휴 삼성...|
|1234465929516|NON_EXCLL|2021-03-14|202103|상품권증정| 95000|[분당점]롯데롤라카드제휴 삼성...|
|1234862958001|    MVG_P|2021-03-06|202103|상품권증정|285000|[분당점]롯데롤라카드제휴 삼성...|
|1235173067597|     VIP+|2021-02-26|202102|상품권증정| 30000|     [창원점]남성 30/60/100...|
|1235379502445|NON_EXCLL|2021-02-26|202102|상품권증정| 30000|     [창원점]남성 30/60/100...|
|1235326746384|NON_EXCLL|2021-03-01|202103|상품권증정| 30000|     [창원점]남성 30/60/100...|
|1235336339112|    MVG_A|2021-02-26|202102|상품권증정| 30000|     [창원점]남성 30/60/100...|
|1234594841233|NON_EXCLL|2021-02-27|202102|상품권증정|100000|     [창원점]남성 30/60/100...|
|1234700811637|NON_EXCLL|2021-02-26|202102|상품권증정| 30000|     [창원점]남성 30/60/100...|
|1234574425475|    MVG_A|2021-02-28|202102|상품권증정| 30000|     [창원점]남성 30/60/100...|
|1234579880829|    MVG_P|2021-02-27|202102|상품권증정| 30000|     [창원점]남성 30/60/100...|
|1237748069415|NON_EXCLL|2021-03-01|202103|상품권증정| 30000|     [창원점]남성 30/60/100...|
|1234874686789|      VIP|2021-02-28|202102|상품권증정| 30000|     [창원점]남성 30/60/100...|
|1237327584061|    MVG_A|2021-02-27|202102|상품권증정| 30000|     [창원점]남성 30/60/100...|
|1234360038708|NON_EXCLL|2021-02-28|202102|상품권증정| 60000|     [창원점]남성 30/60/100...|
+-------------+---------+----------+------+----------+------+-----------------------------+
only showing top 20 rows
