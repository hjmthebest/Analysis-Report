```
SELECT N2.STR_NM
     , N3.CMPGN_ST_DT
     , N3.CMPGN_END_DT
     , N1.DT
     , N1.CMPGN_ID
     , N1.CMPGN_NM
     , N1.CGRP_ID
     , N1.CGRP_NM
     , N1.PG_CNT
     , N1.PG_SND_Y_CNT
     , N1.PG_PCH_CNT
     , N1.PG_SUM_PCH
     , N1.CG_CNT
     , N1.CG_PCH_CNT
     , N1.CG_SUM_PCH
    FROM (
        SELECT T1.RGST_STR_CD AS CSTR_CD
             , TO_CHAR(DTCT_DTM, 'YYYY-MM-DD') AS DT
             , T1.CMPGN_ID
             , T1.CGRP_ID
             , COUNT(DISTINCT CASE WHEN T1.CGRP_CL_CD IN ('T') THEN T1.DPSTR_CUST_NO END)                                                                                                                                                                AS PG_CNT
             , COUNT(DISTINCT CASE WHEN T1.CGRP_CL_CD IN ('T') AND CHNL_SND_SCES_YN IN ('Y') THEN T1.DPSTR_CUST_NO END)                                                                                                                                  AS PG_SND_Y_CNT
             , COUNT(DISTINCT CASE WHEN T1.CGRP_CL_CD IN ('T') AND CHNL_SND_SCES_YN IN ('Y') AND TO_CHAR(T1.DTCT_DTM, 'YYYYMMDD') = TO_CHAR(T1.SLNG_DTM, 'YYYYMMDD') AND T1.PCH_AMT > 0 AND T1.DTCT_DTM < T1.SLNG_DTM THEN T1.DPSTR_CUST_NO2 END)        AS PG_PCH_CNT
             , SUM(CASE WHEN T1.CGRP_CL_CD IN ('T') AND CHNL_SND_SCES_YN IN ('Y') AND TO_CHAR(t1.DTCT_DTM, 'YYYYMMDD') = TO_CHAR(T1.SLNG_DTM, 'YYYYMMDD') AND T1.PCH_AMT > 0 AND T1.DTCT_DTM < T1.SLNG_DTM THEN T1.PCH_AMT END)                          AS PG_SUM_PCH
             , COUNT(DISTINCT CASE WHEN T1.CGRP_CL_CD IN ('C') THEN T1.DPSTR_CUST_NO END)                                                                                                                                                                AS CG_CNT
             , COUNT(DISTINCT CASE WHEN T1.CGRP_CL_CD IN ('C') AND TO_CHAR(t1.DTCT_DTM, 'YYYYMMDD') = TO_CHAR(T1.SLNG_DTM, 'YYYYMMDD') AND T1.PCH_AMT > 0 AND T1.DTCT_DTM < T1.SLNG_DTM  THEN T1.DPSTR_CUST_NO2 END)                                     AS CG_PCH_CNT
             , SUM(CASE WHEN T1.CGRP_CL_CD IN ('C') AND TO_CHAR(t1.DTCT_DTM, 'YYYYMMDD') = TO_CHAR(T1.SLNG_DTM, 'YYYYMMDD') AND T1.PCH_AMT > 0 AND T1.DTCT_DTM < T1.SLNG_DTM THEN T1.PCH_AMT END)                                                        AS CG_SUM_PCH
        FROM (
            select A11.RGST_STR_CD
                 , A11.CMPGN_ID
                 , A11.CGRP_ID
                 , A11.CGRP_CL_CD
                 , A11.DPSTR_CUST_NO
                 , A11.DTCT_DTM
                 , A12.DPSTR_CUST_NO AS DPSTR_CUST_NO2
                 , A11.CHNL_SND_SCES_YN
                 , A12.PCH_AMT
                 , A12.SLNG_DTM
            from lpciddm.tb_dmcm_ebmcmpgnobjcustdtl_i a11
            left join lpciddm.tb_dmcm_ebmcmpgndtlpchresp_f a12
                on a11.cmpgn_id = a12.cmpgn_id
                and a11.cgrp_id = a12.cgrp_id
                and a11.cgrp_cl_cd = a12.cgrp_cl_cd
                and a11.dpstr_cust_no = a12.dpstr_cust_no
                AND A11.RGST_STR_CD = A12.CSTR_CD
            where 1 = 1 
                and a11.cgrp_cl_cd in ('T', 'C')
                and a11.std_ym in ('202106', '202107')
            ) T1 
        WHERE 1 = 1 
            AND T1.RGST_STR_CD <> '0000'
            AND TO_CHAR(DTCT_DTM, 'YYYY-MM-DD') BETWEEN '2021-06-01' AND '2021-07-31'
        GROUP BY T1.RGST_STR_CD
               , TO_CHAR(DTCT_DTM, 'YYYY-MM-DD')
               , T1.CMPGN_ID
               , T1.CGRP_ID
        ORDER BY 1,2,3,4
        ) N1
    JOIN LPCIDDW.TB_DWBS_STR_M N2 
        ON N1.CSTR_CD = N2.CSTR_CD
    JOIN LPCIDDW.TB_DWCM_EBMCMPGN_L N3
        ON N1.CMPGN_ID = N3.CMPGN_ID
    JOIN LPCIDDW.TB_DWCM_EBMCGRP_L N4 
        ON N1.CGRP_ID = N4.CGRP_ID
        AND N1.CMPGN_ID = N4.CMPGN_ID
    WHERE 1 = 1 
    ORDER BY N1.CSTR_CD, DT, N1.CMPGN_ID, N1.CGRP_ID

+--------+-----------+------------+----------+--------+---------------------------+----------------+--------------------------------+------+------------+----------+----------+------+----------+----------+
|  str_nm|cmpgn_st_dt|cmpgn_end_dt|        dt|cmpgn_id|                   cmpgn_nm|         cgrp_id|                         cgrp_nm|pg_cnt|pg_snd_y_cnt|pg_pch_cnt|pg_sum_pch|cg_cnt|cg_pch_cnt|cg_sum_pch|
+--------+-----------+------------+----------+--------+---------------------------+----------------+--------------------------------+------+------------+----------+----------+------+----------+----------+
|    본점| 2021-07-01|  2021-07-04|2021-07-01|C0000000|20210701_자동화캠페인_안...|ND20210701183657|                   영플라자 안내|    63|          61|        16|   3019430|     0|         0|      null|
|    본점| 2021-07-01|  2021-07-04|2021-07-02|C0000000|20210701_자동화캠페인_안...|ND20210701183657|                   영플라자 안내|   136|         127|        51|  19432000|     0|         0|      null|
|    본점| 2021-07-01|  2021-07-04|2021-07-03|C0000000|20210701_자동화캠페인_안...|ND20210701183657|                   영플라자 안내|   133|         127|        52|  17867160|     0|         0|      null|
|    본점| 2021-07-01|  2021-07-04|2021-07-04|C0000000|20210701_자동화캠페인_안...|ND20210701183657|                   영플라자 안내|    96|          95|        28|  31774960|     0|         0|      null|
|  잠실점| 2021-06-04|  2021-06-30|2021-06-04|C0000000| [자동화캠페인] 20210603...|ND10130603180243|     백_내점고객 Upsell_유도_...|   379|         373|       125|  29603960|    40|        22|   2762800|
|  잠실점| 2021-06-04|  2021-06-30|2021-06-04|C0000000| [자동화캠페인] 20210603...|ND10140603180244|     백_내점고객 Upsell_유도_...|   113|         112|        32|  18743470|    17|         8|   2177400|
|  잠실점| 2021-06-04|  2021-06-30|2021-06-04|C0000000| [자동화캠페인] 20210603...|ND10150603180245|     백_내점고객 Upsell_유도_...|   263|         259|        73|  48002980|    36|        12|   1796380|
