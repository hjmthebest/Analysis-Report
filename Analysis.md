Cohort analysis among VIPs (Women's fashion - Adult Character/Adult Contemporary)
```python
%pyspark
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

cohort_analysis = spark.sql("""
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
            and a.std_ym between date_format(add_months(current_date(), -24), 'yyyyMM') and date_format(add_months(current_date(), -1), 'yyyyMM')
            and substring(a.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119', '1221', '1222', '1223', '1224', '1551', '1552')
            and c.buy_fld_cd = '16'
            and ((c.buy_team_cd = '13' and c.buy_pc_cd = 'B') or (c.buy_team_cd = '14' and c.buy_pc_cd = 'A'))
        group by 1
    ), cohort_base as (
        select t1.dpstr_cust_no
             , t1.fst_pch
             , date_format(t1.fst_pch, 'yyyyMM') as cohort_group
             , floor(datediff(t2.std_dt, t1.fst_pch)/30) as cohort_index
        from first_purchase t1
        left join (select a.dpstr_cust_no
                        , a.std_dt
                   from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
                   join lpciddm.tb_dmcs_mmcustinfo_f b
                       on a.dpstr_cust_no = b.dpstr_cust_no
                       and a.std_ym = b.std_ym
                   join lpciddw.tb_dwbs_stritmno_m c
                       on a.cstr_cd = c.cstr_cd
                       and a.itmno_cd = c.itmno_cd
                   where 1 = 1
                       and a.std_ym between date_format(add_months(current_date(), -24), 'yyyyMM') and date_format(add_months(current_date(), -1), 'yyyyMM')
                       and substring(a.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119', '1221', '1222', '1223', '1224', '1551', '1552')
                       and c.buy_fld_cd = '16'
                       and ((c.buy_team_cd = '13' and c.buy_pc_cd = 'B') or (c.buy_team_cd = '14' and c.buy_pc_cd = 'A'))
                   ) t2 on t1.dpstr_cust_no = t2.dpstr_cust_no
    )
    select s.cohort_group
         , s.cohort_index
         , count(distinct s.dpstr_cust_no) as cnt
    from cohort_base s
    group by 1, 2
    """)

pdf = cohort_analysis.toPandas()    
ch = pdf.groupby(['cohort_group', 'cohort_index'])['cnt'].agg('sum').reset_index().pivot(index='cohort_group', columns='cohort_index', values='cnt')

# Population (Left)
ppl = ch[0].reset_index().rename(columns={0:'ppl'}).reindex(columns=['ppl'])

# Cohort (Right)
ch_div = ch.div(ch[0], axis=0)

plt.style.use('dark_background')
fig, ax = plt.subplots(nrows=1, ncols=2, sharey=True, figsize=(18,8), gridspec_kw={'width_ratios': [1, 12]})

sns.heatmap(ppl.iloc[12:], annot=True, fmt='g', cmap='YlGnBu', cbar=False, ax=ax[0])
ax[0].set_ylabel("Cohort Group", fontsize=16)

sns.heatmap(ch_div.iloc[12:, 1:13], annot=True, fmt='.2%', cmap='YlGnBu', ax=ax[1])
ax[1].set_title("Cohort: Retention analysis on VIP (Women's fashion)", fontsize=20)
ax[1].set_xlabel("Cohort Index", fontsize=16)
ax[1].set_ylabel(" ", fontsize=16)
plt.tight_layout()
```
![image](https://user-images.githubusercontent.com/67835149/182320271-bb64e21f-a291-458f-909f-ad5f23c537a3.png)

Among those 6,767 VIPs who have made their first purchase in January 2022
1,285 VIPs (18.99%) repurchased 3 months later (April 2022)

```sql
%sql
create or replace temporary view mvg2022_repch202203 as 
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
            and a.std_ym between date_format(add_months(current_date(), -24), 'yyyyMM') and date_format(add_months(current_date(), -1), 'yyyyMM')
            and substring(a.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119', '1221', '1222', '1223', '1224', '1551', '1552')
            and c.buy_fld_cd = '16'
            and ((c.buy_team_cd = '13' and c.buy_pc_cd = 'B') or (c.buy_team_cd = '14' and c.buy_pc_cd = 'A'))
        group by 1
    ), cohort_base as (
        select t1.dpstr_cust_no
             , t1.fst_pch
             , date_format(t1.fst_pch, 'yyyyMM') as cohort_group
             , floor(datediff(t2.std_dt, t1.fst_pch)/30) as cohort_index
        from first_purchase t1
        left join (select a.dpstr_cust_no
                        , a.std_dt
                   from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
                   join lpciddm.tb_dmcs_mmcustinfo_f b
                       on a.dpstr_cust_no = b.dpstr_cust_no
                       and a.std_ym = b.std_ym
                   join lpciddw.tb_dwbs_stritmno_m c
                       on a.cstr_cd = c.cstr_cd
                       and a.itmno_cd = c.itmno_cd
                   where 1 = 1
                       and a.std_ym between date_format(add_months(current_date(), -24), 'yyyyMM') and date_format(add_months(current_date(), -1), 'yyyyMM')
                       and substring(a.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119', '1221', '1222', '1223', '1224', '1551', '1552')
                       and c.buy_fld_cd = '16'
                       and ((c.buy_team_cd = '13' and c.buy_pc_cd = 'B') or (c.buy_team_cd = '14' and c.buy_pc_cd = 'A'))
                   ) t2 on t1.dpstr_cust_no = t2.dpstr_cust_no
    )
    select distinct s.dpstr_cust_no
    from cohort_base s
    where 1 = 1
        and s.cohort_group = '202201'
        and s.cohort_index = 3
```

Refer to the previous table to see gender/age/grade distribution of 1,285 VIPs:
```python
%pyspark
prfl_of_1285 = spark.sql("""
    with base as (
        select distinct a.dpstr_cust_no
             , b.memb_sex_cl_cd
             , case when cast(b.age_cd as int) between 0 and 19 then '10s'
                    when cast(b.age_cd as int) between 20 and 29 then '20s'
                    when cast(b.age_cd as int) between 30 and 39 then '30s'
                    when cast(b.age_cd as int) between 40 and 49 then '40s'
                    when cast(b.age_cd as int) between 50 and 59 then '50s'
                    when cast(b.age_cd as int) between 60 and 69 then '60s'
                    when cast(b.age_cd as int) between 70 and 79 then '70s'
                    when cast(b.age_cd as int) between 80 and 89 then '80s'
                    when cast(b.age_cd as int) between 90 and 99 then '90s'
                    else '99s' end as agrng_cd
             , case when substring(b.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119') then '1.AVE'
                    when substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1221' then '2.MVG_L'
                    when substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1222' then '3.MVG_P'
                    when substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1223' then '4.MVG_C'
                    when substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1224' then '5.MVG_A'
                    when substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1551' then '6.VIP+'
                    when substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1552' then '7.VIP'
                    else '8.NON_EXCLL' end as cust_grde
        from mvg2022_repch202203 a
        left join lpciddm.tb_dmcs_mmcustinfo_f b
            on a.dpstr_cust_no = b.dpstr_cust_no
        where 1 = 1
            and b.std_ym = '202201'
    )
    select a.agrng_cd
         , a.memb_sex_cl_cd
         , a.cust_grde
         , count(distinct a.dpstr_cust_no) as cnt
    from base a
    where 1 = 1
        and a.memb_sex_cl_cd in (1, 2)
    group by 1, 2, 3
    order by 1, 2, 3
    """)
 
prfl_of_1285.cache()
pdf = prfl_of_1285.toPandas()

plt.style.use('dark_background')
fig, ax = plt.subplots(nrows=1, ncols=3, figsize=(20,6))
sns.barplot(x='cust_grde', y='cnt', data=pdf, ax=ax[0])
ax[0].set_title("Grade distribution", fontsize=20)
ax[0].set_xlabel("Grade", fontsize=16)
ax[0].set_ylabel("cnt", fontsize=16)

sns.barplot(x='agrng_cd', y='cnt', data=pdf, ax=ax[1])
ax[1].set_title("Age distribution", fontsize=20)
ax[1].set_xlabel("Age", fontsize=16)
ax[1].set_ylabel("cnt", fontsize=16)

sns.barplot(x='memb_sex_cl_cd', y='cnt', data=pdf, ax=ax[2])
ax[2].set_title("Gender distribution", fontsize=20)
ax[2].set_xlabel("Gender (1:Male, 2:Female)", fontsize=16)
ax[2].set_ylabel("cnt", fontsize=16)
plt.tight_layout()
```
Result:
![image](https://user-images.githubusercontent.com/67835149/182333280-04a0a65f-97c6-455b-90db-bcf373262f5b.png)


```sql
%sql
select distinct a.dpstr_cust_no
     , b.memb_sex_cl_cd
     , case when cast(b.age_cd as int) between 0 and 19 then '10s'
            when cast(b.age_cd as int) between 20 and 29 then '20s'
            when cast(b.age_cd as int) between 30 and 39 then '30s'
            when cast(b.age_cd as int) between 40 and 49 then '40s'
            when cast(b.age_cd as int) between 50 and 59 then '50s'
            when cast(b.age_cd as int) between 60 and 69 then '60s'
            when cast(b.age_cd as int) between 70 and 79 then '70s'
            when cast(b.age_cd as int) between 80 and 89 then '80s'
            when cast(b.age_cd as int) between 90 and 99 then '90s'
            else '99s' end as agrng_cd
     , case when substring(b.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119') then '1.AVE'
            when substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1221' then '2.MVG_L'
            when substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1222' then '3.MVG_P'
            when substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1223' then '4.MVG_C'
            when substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1224' then '5.MVG_A'
            when substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1551' then '6.VIP+'
            when substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1552' then '7.VIP'
            else '8.NON_EXCLL' end as cust_grde
from mvg2022_repch202203 a
left join lpciddm.tb_dmcs_mmcustinfo_f b
    on a.dpstr_cust_no = b.dpstr_cust_no
where 1 = 1
    and b.std_ym = '202201'
```
Result:
![image](https://user-images.githubusercontent.com/67835149/182320514-231c8f06-41be-4230-aa77-76ef17ca0e09.png)
![image](https://user-images.githubusercontent.com/67835149/182325756-e79ae82b-ce52-49d3-bcdd-1a17f36c7fa5.png)
![image](https://user-images.githubusercontent.com/67835149/182325877-f9017e71-14c1-4d10-ba15-1155fe7966a4.png)
![image](https://user-images.githubusercontent.com/67835149/182325951-b238c31b-7768-489b-82d8-021c134100ca.png)
![image](https://user-images.githubusercontent.com/67835149/182325990-66e26e88-cfed-4af9-aabe-295ded369c9a.png)


2022년 1월 첫구매한 우수고객 6767명
```sql
%sql
create or replace temporary view mvg2022 as 
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
            and a.std_ym between date_format(add_months(current_date(), -24), 'yyyyMM') and date_format(add_months(current_date(), -1), 'yyyyMM')
            and substring(a.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119', '1221', '1222', '1223', '1224', '1551', '1552')
            and c.buy_fld_cd = '16'
            and ((c.buy_team_cd = '13' and c.buy_pc_cd = 'B') or (c.buy_team_cd = '14' and c.buy_pc_cd = 'A'))
        group by 1
    ), cohort_base as (
        select t1.dpstr_cust_no
             , t1.fst_pch
             , date_format(t1.fst_pch, 'yyyyMM') as cohort_group
             , floor(datediff(t2.std_dt, t1.fst_pch)/30) as cohort_index
        from first_purchase t1
        left join (select a.dpstr_cust_no
                        , a.std_dt
                   from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
                   join lpciddm.tb_dmcs_mmcustinfo_f b
                       on a.dpstr_cust_no = b.dpstr_cust_no
                       and a.std_ym = b.std_ym
                   join lpciddw.tb_dwbs_stritmno_m c
                       on a.cstr_cd = c.cstr_cd
                       and a.itmno_cd = c.itmno_cd
                   where 1 = 1
                       and a.std_ym between date_format(add_months(current_date(), -24), 'yyyyMM') and date_format(add_months(current_date(), -1), 'yyyyMM')
                       and substring(a.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119', '1221', '1222', '1223', '1224', '1551', '1552')
                       and c.buy_fld_cd = '16'
                       and ((c.buy_team_cd = '13' and c.buy_pc_cd = 'B') or (c.buy_team_cd = '14' and c.buy_pc_cd = 'A'))
                   ) t2 on t1.dpstr_cust_no = t2.dpstr_cust_no
    )
    select distinct s.dpstr_cust_no
    from cohort_base s
    where 1 = 1
        and s.cohort_group = '202201'

%sql
select a.dpstr_cust_no
     , min(case when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119'))) then '1.AVE'
            when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1221')) then '2.MVG_L'
            when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1222')) then '3.MVG_P'
            when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1223')) then '4.MVG_C'
            when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1224')) then '5.MVG_A'
            when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1551')) then '6.VIP+'
            when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1552')) then '7.VIP'
            else '8.NON_EXCLL' end) as cust_grde_2022
     , min(case when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119'))) then '1.AVE'
            when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1221')) then '2.MVG_L'
            when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1222')) then '3.MVG_P'
            when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1223')) then '4.MVG_C'
            when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1224')) then '5.MVG_A'
            when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1551')) then '6.VIP+'
            when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1552')) then '7.VIP'
            else '8.NON_EXCLL' end) as cust_grde_2021
from mvg2022 a
left join lpciddm.tb_dmcs_mmcustinfo_f b
    on a.dpstr_cust_no = b.dpstr_cust_no
group by 1
```
![image](https://user-images.githubusercontent.com/67835149/182320727-5c00aa0d-e867-41a3-af20-79b065ef5f36.png)

```sql
%sql

select sum(case when ((t.cust_grde_2022 != '8.NON_EXCLL') and (t.cust_grde_2021 != '8.NON_EXCLL')) then 1 else 0 end) as excll_2y
     , sum(case when ((t.cust_grde_2022 = '8.NON_EXCLL') or (t.cust_grde_2021 = '8.NON_EXCLL')) then 1 else 0 end) as new_excll
from (
    select a.dpstr_cust_no
         , min(case when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119'))) then '1.AVE'
                    when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1221')) then '2.MVG_L'
                    when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1222')) then '3.MVG_P'
                    when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1223')) then '4.MVG_C'
                    when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1224')) then '5.MVG_A'
                    when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1551')) then '6.VIP+'
                    when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1552')) then '7.VIP'
                    else '8.NON_EXCLL' end) as cust_grde_2022
         , min(case when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119'))) then '1.AVE'
                    when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1221')) then '2.MVG_L'
                    when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1222')) then '3.MVG_P'
                    when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1223')) then '4.MVG_C'
                    when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1224')) then '5.MVG_A'
                    when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1551')) then '6.VIP+'
                    when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1552')) then '7.VIP'
                    else '8.NON_EXCLL' end) as cust_grde_2021
    from mvg2022 a
    left join lpciddm.tb_dmcs_mmcustinfo_f b
        on a.dpstr_cust_no = b.dpstr_cust_no
    group by 1
    ) t
```
![image](https://user-images.githubusercontent.com/67835149/182320817-23f352c4-fd21-45ff-835b-8e173564d1d2.png)

```sql
%sql

with excll_cust as (
    select a.dpstr_cust_no
         , min(case when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119'))) then '1.AVE'
                    when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1221')) then '2.MVG_L'
                    when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1222')) then '3.MVG_P'
                    when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1223')) then '4.MVG_C'
                    when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1224')) then '5.MVG_A'
                    when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1551')) then '6.VIP+'
                    when ((b.std_ym = '202201') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1552')) then '7.VIP'
                    else '8.NON_EXCLL' end) as cust_grde_2022
         , min(case when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) in ('1111', '1112', '1113', '1119'))) then '1.AVE'
                    when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1221')) then '2.MVG_L'
                    when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1222')) then '3.MVG_P'
                    when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1223')) then '4.MVG_C'
                    when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1224')) then '5.MVG_A'
                    when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1551')) then '6.VIP+'
                    when ((b.std_ym = '202112') and (substring(b.excll_cust_grde_smcls_cd, 1, 4) = '1552')) then '7.VIP'
                    else '8.NON_EXCLL' end) as cust_grde_2021
    from mvg2022 a
    left join lpciddm.tb_dmcs_mmcustinfo_f b
        on a.dpstr_cust_no = b.dpstr_cust_no
    group by 1
), excll_cust2 as (
    select t1.dpstr_cust_no
         , min(t1.cust_grde_2022) as cust_grde_2022
         , min(t1.cust_grde_2021) as cust_grde_2021
         , max(case when ((t1.cust_grde_2022 != '8.NON_EXCLL') and (t1.cust_grde_2021 != '8.NON_EXCLL')) then 1 else 0 end) as excll_2y_yn
         , max(t2.memb_sex_cl_cd) as memb_sex_cl_cd
         , max(case when cast(t2.age_cd as int) between 0 and 19 then '10s'
                    when cast(t2.age_cd as int) between 20 and 29 then '20s'
                    when cast(t2.age_cd as int) between 30 and 39 then '30s'
                    when cast(t2.age_cd as int) between 40 and 49 then '40s'
                    when cast(t2.age_cd as int) between 50 and 59 then '50s'
                    when cast(t2.age_cd as int) between 60 and 69 then '60s'
                    when cast(t2.age_cd as int) between 70 and 79 then '70s'
                    when cast(t2.age_cd as int) between 80 and 89 then '80s'
                    when cast(t2.age_cd as int) between 90 and 99 then '90s'
                    else '99s' end) as agrng_cd
    from excll_cust t1
    left join lpciddm.tb_dmcs_mmcustinfo_f t2
        on t1.dpstr_cust_no = t2.dpstr_cust_no
    group by 1
)
select *
from excll_cust2
```

