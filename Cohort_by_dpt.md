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
sns.heatmap(cohort_final.iloc[6:, 1:12], annot=True, fmt='.0%', ax=ax[1], cmap='YlGnBu')
ax[1].set_xlabel("Cohort Group", fontsize=16)
ax[1].set_ylabel(" ", fontsize=16)
ax[1].set_title("Cohort: Retention Analysis", fontsize=20)
plt.tight_layout()
```
