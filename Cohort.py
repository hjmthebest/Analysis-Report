```python
%pyspark

# import libraries
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark import SQLContext, SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

spark = SparkSession.builder \
                    .master("yarn") \
                    .appName("cohort_retention_analysis") \
                    .config("spark.sql.execution.arrow.enable","true") \
                    .config("spark.sql.crossJoin.enabled","true") \
	            .config("spark.executor.heartbeatInterval", "300s") \
                    .enableHiveSupport() \
                    .getOrCreate()
                    
hadoop_config = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_config.set('fs.s3.canned.acl', 'BucketOwnerFullControl')
sqlContext = SQLContext(spark.sparkContext)

# create cohort base table using Spark SQL
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
            and a.std_ym between date_format(add_months(current_date(), -23), 'yyyyMM') and date_format(current_date(), 'yyyyMM')
            and a.cstr_cd in ('0001', '0002', '0005')
            and a.on_off_cl_cd = '1'
            and c.buy_fld_cd in ('13', '14')
            and c.buy_team_cd in ('41', '45')
        group by 1
    ), cohort_base as (
        select d.dpstr_cust_no
             , d.fst_pch
             , date_format(d.fst_pch, 'yyyyMM') as cohort_grp
             , floor(datediff(t.std_dt, d.fst_pch)/30) as cohort_idx
        from first_purchase d
        join (select distinct e.dpstr_cust_no, e.std_dt
              from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f e
              join lpciddm.tb_dmcs_mmcustinfo_f f
                  on e.dpstr_cust_no = f.dpstr_cust_no
                  and e.std_ym = f.std_ym
              join lpciddw.tb_dwbs_stritmno_m g
                  on e.cstr_cd = g.cstr_cd
                  and e.itmno_cd = g.itmno_cd
              where 1 = 1
                  and e.std_ym between date_format(add_months(current_date(), -23), 'yyyyMM') and date_format(current_date(), 'yyyyMM')
                  and e.cstr_cd in ('0001', '0002', '0005')
                  and e.on_off_cl_cd = '1'
                  and g.buy_fld_cd in ('13', '14')
                  and g.buy_team_cd in ('41', '45')
             ) t
            on d.dpstr_cust_no = t.dpstr_cust_no
    )
    select s.cohort_grp
         , s.cohort_idx
         , count(distinct s.dpstr_cust_no) as cnt
    from cohort_base s
    group by 1, 2
    order by 1, 2
    """)

# cohort.createOrReplaceTempView("cohort")
# cohort.show()
cohort.cache()

# spark dataframe -> pandas dataframe
cohort_pd = cohort.toPandas()

# pandas
cohort_final = cohort_pd.groupby(['cohort_grp', 'cohort_idx'])['cnt'].agg('sum').reset_index().pivot(index='cohort_grp', columns='cohort_idx', values='cnt')

# number of customer by cohort group (1)
cohort_final_0 = cohort_final[0].iloc[12:].reset_index().rename(columns={0:'Customer number'}).drop('cohort_grp', axis=1)

# customer retention by percentage (2)
cohort_final_v2 = cohort_final.div(cohort_final[0], axis=0)

# definition of style and plots
plt.style.use('dark_background')
fig, ax = plt.subplots(nrows=1, ncols=2, sharey=True, figsize=(17,8), gridspec_kw={'width_ratios': [1, 12]})

# data visualization of (1)
sns.heatmap(cohort_final_0, annot=True, fmt='g', ax=ax[0], cmap='YlGnBu', cbar=False)
ax[0].set_ylabel("Cohort Group", fontsize=16)

# data visualization of (2)
sns.heatmap(cohort_final_v2.iloc[12:, 1:12], annot=True, fmt='.2%', ax=ax[1], cmap='YlGnBu')
ax[1].set_xlabel("Cohort Index", fontsize=16)
ax[1].set_ylabel(" ", fontsize=16)
ax[1].set_title("Cohort: Retention Analysis on Furniture & Electronic devices", fontsize=18)
plt.tight_layout()
```

![다운로드 (1)](https://user-images.githubusercontent.com/67835149/178654262-640f7b32-ce65-499c-aaf1-d62415153831.png)

```python
%pyspark

# import libraries
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# create cohort base table using Spark SQL
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
            and a.std_ym between date_format(add_months(current_date(), -23), 'yyyyMM') and date_format(current_date(), 'yyyyMM')
            and a.cstr_cd in ('0001', '0002', '0005')
            and a.on_off_cl_cd = '1'
            and c.buy_fld_cd in ('22')
            and c.buy_team_cd in ('34', '39', '40')
        group by 1
    ), cohort_base as (
        select d.dpstr_cust_no
             , d.fst_pch
             , date_format(d.fst_pch, 'yyyyMM') as cohort_grp
             , floor(datediff(t.std_dt, d.fst_pch)/30) as cohort_idx
        from first_purchase d
        join (select distinct e.dpstr_cust_no, e.std_dt
              from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f e
              join lpciddm.tb_dmcs_mmcustinfo_f f
                  on e.dpstr_cust_no = f.dpstr_cust_no
                  and e.std_ym = f.std_ym
              join lpciddw.tb_dwbs_stritmno_m g
                  on e.cstr_cd = g.cstr_cd
                  and e.itmno_cd = g.itmno_cd
              where 1 = 1
                  and e.std_ym between date_format(add_months(current_date(), -23), 'yyyyMM') and date_format(current_date(), 'yyyyMM')
                  and e.cstr_cd in ('0001', '0002', '0005')
                  and e.on_off_cl_cd = '1'
                  and g.buy_fld_cd in ('22')
                  and g.buy_team_cd in ('34', '39', '40')
             ) t
            on d.dpstr_cust_no = t.dpstr_cust_no
    )
    select s.cohort_grp
         , s.cohort_idx
         , count(distinct s.dpstr_cust_no) as cnt
    from cohort_base s
    group by 1, 2
    order by 1, 2
    """)

# cohort.createOrReplaceTempView("cohort")
# cohort.show()
cohort.cache()

# spark dataframe -> pandas dataframe
cohort_pd = cohort.toPandas()

# pandas
cohort_final = cohort_pd.groupby(['cohort_grp', 'cohort_idx'])['cnt'].agg('sum').reset_index().pivot(index='cohort_grp', columns='cohort_idx', values='cnt')

# number of customer by cohort group (1)
cohort_final_0 = cohort_final[0].iloc[12:].reset_index().rename(columns={0:'Customer number'}).drop('cohort_grp', axis=1)

# customer retention by percentage (2)
cohort_final_v2 = cohort_final.div(cohort_final[0], axis=0)

# definition of style and plots
plt.style.use('dark_background')
fig, ax = plt.subplots(nrows=1, ncols=2, sharey=True, figsize=(17,8), gridspec_kw={'width_ratios': [1, 12]})

# data visualization of (1)
sns.heatmap(cohort_final_0, annot=True, fmt='g', ax=ax[0], cmap='Greens', cbar=False)
ax[0].set_ylabel("Cohort Group", fontsize=16)

# data visualization of (2)
sns.heatmap(cohort_final_v2.iloc[12:, 1:12], annot=True, fmt='.2%', ax=ax[1], cmap='Greens')
ax[1].set_xlabel("Cohort Index", fontsize=16)
ax[1].set_ylabel(" ", fontsize=16)
ax[1].set_title("Cohort: Retention Analysis on F&B", fontsize=18)
plt.tight_layout()
```

![다운로드](https://user-images.githubusercontent.com/67835149/178654281-49ad2920-934f-4395-b171-af823e8c95b7.png)

