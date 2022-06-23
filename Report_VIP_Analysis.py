%pyspark

import numpy as np
import pandas as pd
import scipy as sp
import seaborn as sns
import matplotlib.pyplot as plt

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import math
import random

import json
import sys
import os
import datetime, timeit, time

spark = SparkSession.builder \
                    .master('yarn') \
                    .appName('report') \ 
                    .getOrCreate()

start = time.time()

def excll_cust_report():
    report = spark.sql(f"""
        with excll_crnt_yr as (
            select a.dpstr_cust_no
                 , 'current_yr' as yr
                 , case when substring(a.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119') then 'AVE' 
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1221' then 'MVG_L'
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1222' then 'MVG_P'
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1223' then 'MVG_C'
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1224' then 'MVG_A'
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1551' then 'VIP+'
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1552' then 'VIP'
                        end as excll_cust_grde_mdcls_cd
            from lpciddm.tb_dmcs_mmcustinfo_f a
            where 1 = 1
                and a.std_ym = date_format(add_months(current_date(), -1), 'yyyyMM')
                and substring(a.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119', '1221', '1222', '1223', '1224', '1551', '1552')
                and a.excll_cust_self_cl_cd = '1'
        ), excll_cmprd_yr as (
            select a.dpstr_cust_no
                 , 'compared_yr' as yr
                 , case when substring(a.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119') then 'AVE' 
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1221' then 'MVG_L' 
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1222' then 'MVG_P' 
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1223' then 'MVG_C' 
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1224' then 'MVG_A' 
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1551' then 'VIP+'
                        when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1552' then 'VIP'			
                        end as excll_cust_grde_mdcls_cd
            from lpciddm.tb_dmcs_mmcustinfo_f a
            where 1 = 1
                and a.std_ym = date_format(add_months(current_date(), -14), 'yyyyMM')
                and substring(a.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119', '1221', '1222', '1223', '1224', '1551', '1552')
                and a.excll_cust_self_cl_cd = '1'
        ), purchase_amount as (
                 -- 구매고객수 / 평균 객단가
            select a.dpstr_cust_no
                 -- 구매금액
                 , ifnull(sum(case when a.std_ym between date_format(add_months(current_date(), -13), 'yyyyMM') and date_format(add_months(current_date(),  -1), 'yyyyMM') then a.gs_slng_amt end), 0) as pch_amt_crnt_yr
                 , ifnull(sum(case when a.std_ym between date_format(add_months(current_date(), -26), 'yyyyMM') and date_format(add_months(current_date(), -14), 'yyyyMM') then a.gs_slng_amt end), 0) as pch_amt_cmprd_yr
                 -- 구매일수
                 , count(distinct case when a.std_ym between date_format(add_months(current_date(), -13), 'yyyyMM') and date_format(add_months(current_date(),  -1), 'yyyyMM') then a.std_dt end) as pch_dys_crnt_yr
                 , count(distinct case when a.std_ym between date_format(add_months(current_date(), -26), 'yyyyMM') and date_format(add_months(current_date(), -14), 'yyyyMM') then a.std_dt end) as pch_dys_cmprd_yr
            from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
            join lpciddm.tb_dmcs_mmcustinfo_f b
                on a.dpstr_cust_no = b.dpstr_cust_no
                and a.std_ym = b.std_ym
            where 1 = 1
                and a.std_ym between date_format(add_months(current_date(), -26), 'yyyyMM') and  date_format(add_months(current_date(), -1), 'yyyyMM')
                and a.typbu_dtl_cd = '01'
                and a.on_off_cl_cd = '1'
            group by 1
        ), purchase_cycle as (
                 -- 구매주기
            select s.dpstr_cust_no
                 , ifnull(round(case when s.cnt_dys_crnt_yr <= 2 then null else (s.diff_crnt_yr / (s.cnt_dys_crnt_yr - 1)) end, 2), 0) as pch_cyc_crnt_yr
                 , ifnull(round(case when s.cnt_dys_cmprd_yr <= 2 then null else (s.diff_cmprd_yr / (s.cnt_dys_cmprd_yr - 1)) end, 2), 0) as pch_cyc_cmprd_yr
            from (
                select t.*
                     , datediff(t.max_dys_crnt_yr, t.min_dys_crnt_yr) as diff_crnt_yr
                     , datediff(t.max_dys_cmprd_yr, t.min_dys_cmprd_yr) as diff_cmprd_yr
                from (
                    select a.dpstr_cust_no
                         , min(case when a.std_dt between date_format(add_months(current_date(), -13), 'yyyyMM') and date_format(add_months(current_date(), -1), 'yyyyMM') then a.std_dt end) as min_dys_crnt_yr
                         , max(case when a.std_dt between date_format(add_months(current_date(), -13), 'yyyyMM') and date_format(add_months(current_date(), -1), 'yyyyMM') then a.std_dt end) as max_dys_crnt_yr
                         , count(distinct case when a.std_dt between date_format(add_months(current_date(), -13), 'yyyyMM') and date_format(add_months(current_date(), -1), 'yyyyMM') then a.std_dt end) as cnt_dys_crnt_yr

                         , min(case when a.std_dt between date_format(add_months(current_date(), -26), 'yyyyMM') and date_format(add_months(current_date(), -14), 'yyyyMM') then a.std_dt end) as min_dys_cmprd_yr
                         , max(case when a.std_dt between date_format(add_months(current_date(), -26), 'yyyyMM') and date_format(add_months(current_date(), -14), 'yyyyMM') then a.std_dt end) as max_dys_cmprd_yr
                         , count(distinct case when a.std_dt between date_format(add_months(current_date(), -26), 'yyyyMM') and date_format(add_months(current_date(), -14), 'yyyyMM') then a.std_dt end) as cnt_dys_cmprd_yr
                         
                    from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
                    join lpciddm.tb_dmcs_mmcustinfo_f b
                        on a.dpstr_cust_no = b.dpstr_cust_no
                        and a.std_ym = b.std_ym
                    where 1 = 1
                        and a.std_ym between date_format(add_months(current_date(), -26), 'yyyyMM') and date_format(add_months(current_date(), -1), 'yyyyMM')
                        and a.typbu_dtl_cd = '01'
                        and a.on_off_cl_cd = '1'
                    group by 1
                    ) t
                ) s 
        ), customer_prfl as (select a.* from excll_crnt_yr a union all select b.* from excll_cmprd_yr b)
       select s1.excll_cust_grde_mdcls_cd                                                              -- 고객등급
            , s1.cust_cnt_crnt_yr_f                                                                    -- 금년 대상고객수
            , s1.pch_cust_cnt_crnt_yr_f                                                                -- 금년 구매고객수
            , round(s1.pch_cust_cnt_crnt_yr_f / s1.cust_cnt_crnt_yr_f * 100, 2) as pch_rt_crnt_yr      -- 금년 구매전환율

            , s1.cust_cnt_cmprd_yr_f                                                                   -- 전년 대상고객수	 
            , s1.pch_cust_cnt_cmprd_yr_f                                                               -- 전년 구매고객수
            , round(s1.pch_cust_cnt_cmprd_yr_f / s1.cust_cnt_cmprd_yr_f * 100, 2) as pch_rt_cmprd_yr   -- 전년 구매전환율
            
            , round(s1.pch_amt_crnt_yr_f, 2) as pch_amt_crnt_yr_f                                      -- 금년 구매금액
            , round(s1.pch_amt_cmprd_yr_f, 2) as pch_amt_cmprd_yr_f                                    -- 전년 구매금액
            
            , round(s1.pch_amt_crnt_yr_f / s1.pch_cust_cnt_crnt_yr_f, 2) as pch_pr_action_crnt_yr      -- 금년 평균객단가
            , round(s1.pch_amt_cmprd_yr_f / s1.pch_cust_cnt_cmprd_yr_f, 2) as pch_pr_action_cmprd_yr   -- 전년 평균객단가
            
            , round(s1.avg_pch_cyc_crnt_yr, 2) as avg_pch_cyc_crnt_yr                                  -- 금년 평균구매주기
            , round(s1.avg_pch_cyc_cmprd_yr, 2) as avg_pch_cyc_cmprd_yr                                -- 전년 평균구매주기
            
            , round(s1.avg_pch_dys_crnt_yr_f, 2) as avg_pch_dys_crnt_yr_f                              -- 금년 평균구매일수
            , round(s1.avg_pch_dys_cmprd_yr_f, 2) as avg_pch_dys_cmprd_yr_f                            -- 전년 평균구매일수
        from (
            select t1.excll_cust_grde_mdcls_cd
                 -- 대상고객수
                 , count(distinct case when t1.yr = 'current_yr' then t1.dpstr_cust_no end) as cust_cnt_crnt_yr_f
                 , count(distinct case when t1.yr = 'compared_yr' then t1.dpstr_cust_no end) as cust_cnt_cmprd_yr_f
                 -- 구매고객수
                 , count(distinct case when (t1.yr = 'current_yr' and t2.pch_amt_crnt_yr > 0) then t2.dpstr_cust_no end) as pch_cust_cnt_crnt_yr_f
                 , count(distinct case when (t1.yr = 'compared_yr' and t2.pch_amt_cmprd_yr > 0) then t2.dpstr_cust_no end) as pch_cust_cnt_cmprd_yr_f
                 -- 구매전환율 (상위 쿼리 참고)

                 -- 구매금액
                 , sum(case when t1.yr = 'current_yr' then t2.pch_amt_crnt_yr end) as pch_amt_crnt_yr_f
                 , sum(case when t1.yr = 'compared_yr' then t2.pch_amt_crnt_yr end) as pch_amt_cmprd_yr_f
                 -- 평균객단가 (상위 쿼리 참고)

                 -- 평균구매주기
                 , avg(case when t1.yr = 'current_yr' then t3.pch_cyc_crnt_yr end) as avg_pch_cyc_crnt_yr
                 , avg(case when t1.yr = 'compared_yr' then t3.pch_cyc_cmprd_yr end) as avg_pch_cyc_cmprd_yr	 
                 -- 평균구매일수
                 , avg(case when t1.yr = 'current_yr' then t2.pch_dys_crnt_yr end) as avg_pch_dys_crnt_yr_f
                 , avg(case when t1.yr = 'compared_yr' then t2.pch_dys_cmprd_yr end) as avg_pch_dys_cmprd_yr_f	 
            from customer_prfl t1
            left join purchase_amount t2
                 on t1.dpstr_cust_no = t2.dpstr_cust_no
            left join purchase_cycle t3
                 on t1.dpstr_cust_no = t3.dpstr_cust_no
            group by 1
            ) s1
    """)
    # pdf = report.toPandas()
    # pdf.sort_values(by = 'excll_cust_grde_mdcls_cd', ascending = True)
    # return pdf
    report = report.withColumn('pch_pr_action_crnt_yr', report['pch_pr_action_crnt_yr'].cast('bigint'))
    report = report.withColumn('pch_pr_action_cmprd_yr', report['pch_pr_action_cmprd_yr'].cast('bigint'))
    return report.orderBy(asc('excll_cust_grde_mdcls_cd')).show()

excll_cust_report()

end = time.time()
print(f"{end - start:.4f}")

# Result
+------------------------+------------------+----------------------+--------------+-------------------+-----------------------+---------------+-----------------+------------------+---------------------+----------------------+-------------------+--------------------+---------------------+----------------------+
|excll_cust_grde_mdcls_cd|cust_cnt_crnt_yr_f|pch_cust_cnt_crnt_yr_f|pch_rt_crnt_yr|cust_cnt_cmprd_yr_f|pch_cust_cnt_cmprd_yr_f|pch_rt_cmprd_yr|pch_amt_crnt_yr_f|pch_amt_cmprd_yr_f|pch_pr_action_crnt_yr|pch_pr_action_cmprd_yr|avg_pch_cyc_crnt_yr|avg_pch_cyc_cmprd_yr|avg_pch_dys_crnt_yr_f|avg_pch_dys_cmprd_yr_f|
+------------------------+------------------+----------------------+--------------+-------------------+-----------------------+---------------+-----------------+------------------+---------------------+----------------------+-------------------+--------------------+---------------------+----------------------+
|                     AVE|               758|                   744|         98.15|                587|                    573|          97.61|     240094302300|      151235811310|            322707395|             263936843|               6.75|                9.94|                75.88|                 75.31|
|                   MVG_A|             34855|                 34527|         99.06|              41606|                  41195|          99.01|     779078119640|      812110950020|             22564315|              19713823|               9.62|               15.36|                45.75|                 46.31|
|                   MVG_C|             10596|                 10524|         99.32|               8409|                   8348|          99.27|     450711303810|      323867052630|             42826995|              38795765|               8.59|               11.16|                58.01|                 61.67|
|                   MVG_L|              3223|                  3199|         99.26|               2022|                   2002|          99.01|     444602817090|      269582061130|            138981812|             134656374|               7.15|                9.43|                70.87|                 75.11|
|                   MVG_P|              6992|                  6948|         99.37|               5118|                   5077|           99.2|     432985175520|      324215024490|             62317958|              63859567|               7.84|                 9.9|                63.01|                 69.55|
|                     VIP|            130787|                118832|         90.86|             157349|                 147308|          93.62|     562965441180|      713660596710|              4737490|               4844683|              10.79|               23.22|                21.61|                 23.49|
|                    VIP+|             95128|                 92202|         96.92|              84148|                  82133|          97.61|     945577868840|      732917292730|             10255502|               8923542|              10.82|               20.44|                30.86|                 32.65|
+------------------------+------------------+----------------------+--------------+-------------------+-----------------------+---------------+-----------------+------------------+---------------------+----------------------+-------------------+--------------------+---------------------+----------------------+

