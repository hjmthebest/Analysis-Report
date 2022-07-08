```python
%pyspark

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

cohort = spark.sql("""
    with first_purchase as (
        select a.dpstr_cust_no
             , min(a.std_dt) as fst_pch
        from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
        join lpciddm.tb_dmcs_mmcustinfo_f b
            on a.dpstr_cust_no = b.dpstr_cust_no
            and a.std_ym = b.std_ym
        join lpciddw.tb_dwbs_stritmno_m c
            on a.cstr_cd = c.cstr_cd
            and a.itmno_cd = c.itmno_cd
        where 1 = 1
            and a.std_ym between date_format(add_months(current_date(), -17), 'yyyyMM') and date_format(current_date(), 'yyyyMM')
            and c.buy_fld_cd = '' 
            and c.buy_team_cd in ('', '')
            and a.typbu_dtl_cd = '01'
        group by 1
    ), chrt_idx as (
        select a.dpstr_cust_no
             , d.fst_pch
             , date_format(d.fst_pch, 'yyyyMM') as cohort_group
             , floor(datediff(a.std_dt, d.fst_pch)/30) as cohort_index
        from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
        join lpciddm.tb_dmcs_mmcustinfo_f b
            on a.dpstr_cust_no = b.dpstr_cust_no
            and a.std_ym = b.std_ym
        join lpciddw.tb_dwbs_stritmno_m c
            on a.cstr_cd = c.cstr_cd
            and a.itmno_cd = c.itmno_cd
        left join first_purchase d
            on a.dpstr_cust_no = d.dpstr_cust_no
        where 1 = 1
            and a.std_ym between date_format(add_months(current_date(), -17), 'yyyyMM') and date_format(current_date(), 'yyyyMM')
            and a.typbu_dtl_cd = '01'
        group by a.dpstr_cust_no, d.fst_pch, a.std_dt
    )
    select d.cohort_group
         , d.cohort_index
         , count(distinct d.dpstr_cust_no) as cnt
    from chrt_idx d
    group by 1, 2
    order by 1, 2
    """)

cohort_base = cohort.toPandas()

cohort_pvt_tb = cohort_base.groupby(['cohort_group', 'cohort_index'])['cnt'].agg('sum').reset_index().pivot(index='cohort_group', columns='cohort_index', values='cnt')
cohort_final = cohort_pvt_tb.div(cohort_pvt_tb[0], axis=0)
cohort_size = pd.DataFrame(cohort_pvt_tb.iloc[:, 0:1].rename(columns={0:'cohort_size'}))

plt.style.use('dark_background')
fig, ax = plt.subplots(1, 2, figsize=(15, 8), sharey=True, gridspec_kw={'width_ratios': [1, 11]})

# Cohort group size
sns.heatmap(cohort_size.iloc[6:, :], annot=True, fmt='.0f', ax=ax[0], cbar=False, cmap='YlGnBu')
ax[0].set_xlabel(" ", fontsize=8)
ax[0].set_ylabel("Number of Customer", fontsize=16)
ax[0].set_title(" ", fontsize=18)

# Cohort graph
sns.heatmap(cohort_final.iloc[6:, 1:12], annot=True, fmt='.2%', ax=ax[1], cmap='YlGnBu')
ax[1].set_xlabel("Cohort Group", fontsize=16)
ax[1].set_ylabel(" ", fontsize=16)
ax[1].set_title("Cohort: Retention Analysis", fontsize=20)
plt.tight_layout()
```
Result
![다운로드 (1)](https://user-images.githubusercontent.com/67835149/177941724-e1f46a7e-bb7b-4910-b588-36249eb94060.png)

```python
%pyspark

lux_cust = spark.sql("""
    with first_purchase as (
        select a.dpstr_cust_no
             , min(a.std_dt) as fst_pch
        from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
        join lpciddm.tb_dmcs_mmcustinfo_f b
            on a.dpstr_cust_no = b.dpstr_cust_no
            and a.std_ym = b.std_ym
        join lpciddw.tb_dwbs_stritmno_m c
            on a.cstr_cd = c.cstr_cd
            and a.itmno_cd = c.itmno_cd
        where 1 = 1
            and a.std_ym between date_format(add_months(current_date(), -17), 'yyyyMM') and date_format(current_date(), 'yyyyMM')
            and c.buy_fld_cd = '12' 
            and c.buy_team_cd in ('52', '53')
            and a.typbu_dtl_cd = '01'
        group by 1
    ), chrt_idx as (
        select a.dpstr_cust_no
             , d.fst_pch
             , date_format(d.fst_pch, 'yyyyMM') as cohort_group
             , floor(datediff(a.std_dt, d.fst_pch)/30) as cohort_index
        from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
        join lpciddm.tb_dmcs_mmcustinfo_f b
            on a.dpstr_cust_no = b.dpstr_cust_no
            and a.std_ym = b.std_ym
        join lpciddw.tb_dwbs_stritmno_m c
            on a.cstr_cd = c.cstr_cd
            and a.itmno_cd = c.itmno_cd
        left join first_purchase d
            on a.dpstr_cust_no = d.dpstr_cust_no
        where 1 = 1
            and a.std_ym between date_format(add_months(current_date(), -17), 'yyyyMM') and date_format(current_date(), 'yyyyMM')
            and a.typbu_dtl_cd = '01'
            and c.buy_fld_cd = '12' 
            and c.buy_team_cd in ('52', '53')
        group by a.dpstr_cust_no, d.fst_pch, a.std_dt
    ), cust_base_83502 as (
        select distinct a.dpstr_cust_no
        from chrt_idx a
        where 1 = 1
            and a.cohort_group = '202108'
            and a.cohort_index = 0
    )
    select a.dpstr_cust_no
         , b.std_dt
         , b.sex_cd
         , b.agrng_cd
         , b.cust_grde
         , b.pch_amt
         , b.pch_dys
    from cust_base_83502 a
    left join (select a.dpstr_cust_no
                    , a.std_dt
                    , max(case when a.memb_sex_cl_cd in ('#', '~', null) then '99' else a.memb_sex_cl_cd end) as sex_cd
                    , max(case when cast(a.age_cd as int) between  0 and 19 then '10s'
                               when cast(a.age_cd as int) between 20 and 29 then '20s'
                               when cast(a.age_cd as int) between 30 and 39 then '30s'
                               when cast(a.age_cd as int) between 40 and 49 then '40s'
                               when cast(a.age_cd as int) between 50 and 59 then '50s'
                               when cast(a.age_cd as int) between 60 and 69 then '60s'
                               when cast(a.age_cd as int) between 70 and 79 then '70s'
                               when cast(a.age_cd as int) between 80 and 89 then '80s'
                               when cast(a.age_cd as int) between 90 and 99 then '90s'
                               else '99s' end) as agrng_cd
                    , max(case when substring(a.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119') then 'AVE'
                               when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1221' then 'MVG_L'
                               when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1222' then 'MVG_P'
                               when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1223' then 'MVG_C'
                               when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1224' then 'MVG_A'
                               when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1551' then 'VIP+'
                               when substring(a.excll_cust_grde_smcls_cd, 1, 4) = '1552' then 'VIP'
                               else 'NON_EXCLL' end) as cust_grde
                    , sum(a.gs_slng_amt) as pch_amt
                    , count(distinct a.std_dt) as pch_dys
                from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
                join lpciddm.tb_dmcs_mmcustinfo_f b
                    on a.dpstr_cust_no = b.dpstr_cust_no
                    and a.std_ym = b.std_ym
                join lpciddw.tb_dwbs_stritmno_m c
                    on a.cstr_cd = c.cstr_cd
                    and a.itmno_cd = c.itmno_cd
                where 1 = 1
                    and a.std_ym between date_format(add_months(current_date(), -17), 'yyyyMM') and date_format(current_date(), 'yyyyMM')
                    and a.typbu_dtl_cd = '01'
                    and c.buy_fld_cd = '12' 
                    and c.buy_team_cd in ('52', '53')
                group by 1, 2
                ) b on a.dpstr_cust_no =  b.dpstr_cust_no
    
    """)
lux_cust.cache()

lux_cust.coalesce(1) \
        .write \
        .format("com.databricks.spark.csv") \
        .option("header","true") \
        .option("encoding", "utf-8") \
        .mode('Overwrite') \
        .save("s3://ldps-dev.data.anl.origin/bigdata_task/jungmin.ha/lux_cust.csv")
```
