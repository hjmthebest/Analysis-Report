```python 
%pyspark

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

cohort = spark.sql("""
    with fst_pch as (
        select a.dpstr_cust_no
             , min(a.std_dt) as fst_pch
        from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
        join lpciddm.tb_dmcs_mmcustinfo_f b on a.dpstr_cust_no = b.dpstr_cust_no and a.std_ym = b.std_ym
        where 1 = 1
            and a.std_ym between '202007' and '202206' 
            and a.cstr_cd = '0001'
            and a.on_off_cl_cd = '1'
        group by 1
    ), base as (
        select a.dpstr_cust_no
             , c.fst_pch
             , date_format(c.fst_pch, 'yyyyMM') as cohort_group
             , floor(datediff(a.std_dt, c.fst_pch) / 30) as cohort_index
        from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
        join lpciddm.tb_dmcs_mmcustinfo_f b on a.dpstr_cust_no = b.dpstr_cust_no and a.std_ym = b.std_ym
        left join fst_pch c on a.dpstr_cust_no = c.dpstr_cust_no
        where 1 = 1
            and a.std_ym between '202007' and '202206' 
            and a.cstr_cd = '0001'
            and a.on_off_cl_cd = '1'
        group by a.dpstr_cust_no, c.fst_pch, a.std_dt
    )
    select cohort_group
         , cohort_index
         , count(distinct dpstr_cust_no) as cnt
    from base
    group by 1, 2
    order by 1, 2
    """)
    
cohort.cache()
cohort_pd = cohort.toPandas()

cohort_final = cohort_pd.groupby(['cohort_group', 'cohort_index'])['cnt'].agg('sum').reset_index().pivot(index='cohort_group', columns='cohort_index', values='cnt')
cohort_final2 = cohort_final.div(cohort_final[0], axis=0)
cohort_final3 = cohort_final.iloc[:, 0:1]
cohort_size_df = pd.DataFrame(cohort_final3).rename(columns={0:'cohort_size'})

with sns.axes_style("white"):
    fig, ax = plt.subplots(1, 2, figsize=(15, 8), sharey=True, gridspec_kw={'width_ratios': [1, 11]})

# cohort size
sns.heatmap(cohort_size_df.iloc[6:, :], annot=True, ax=ax[0], fmt='g', cmap='YlGnBu', cbar=False)

# retention matrix
sns.heatmap(cohort_final2.iloc[6:, 1:], annot=True, ax=ax[1], fmt='.0%', cmap='YlGnBu')
ax[1].set_title("Cohort: Retention Analysis by percentage", fontsize=18)
ax[1].set_xlabel("Cohort_group", fontsize=15)
ax[1].set_ylabel("")
fig.tight_layout()
```

Result
![Image_2022-06-30 17_59_13](https://user-images.githubusercontent.com/67835149/176799012-84511d0c-de04-4274-8b2c-f15c1d1713ad.png)
