%pyspark

base = spark.sql("""
    with base_v1 as (
        select a.dpstr_cust_no
             , min(a.std_dt) as cohort
        from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
        join lpciddm.tb_dmcs_mmcustinfo_f b
            on a.dpstr_cust_no = b.dpstr_cust_no
            and a.std_ym = b.std_ym
        join lpciddw.tb_dwbs_stritmno_m c
            on a.cstr_cd = c.cstr_cd
            and a.itmno_cd = c.itmno_cd
        where 1 = 1
            and a.std_ym between '202107' and '202206' 
            and a.typbu_dtl_cd = '01'
            and a.on_off_cl_cd = '1'
            and c.buy_team_cd in ('16', '17')
        group by 1
        having sum(a.gs_slng_amt) > 0
    ), joined as (
        select a.dpstr_cust_no
             , d.cohort
             , date_format(d.cohort, 'yyyyMM') as cohort_group
             , floor(datediff(a.std_dt, d.cohort) / 30) as cohort_index
        from lpciddm.tb_dmcs_dditmnocustitgslngdtl_f a
        join lpciddm.tb_dmcs_mmcustinfo_f b
            on a.dpstr_cust_no = b.dpstr_cust_no
            and a.std_ym = b.std_ym
        join lpciddw.tb_dwbs_stritmno_m c
            on a.cstr_cd = c.cstr_cd
            and a.itmno_cd = c.itmno_cd
        left join base_v1 d
            on a.dpstr_cust_no = d.dpstr_cust_no
        where 1 = 1
            and a.std_ym between '202107' and '202206' 
            and a.typbu_dtl_cd = '01'
            and a.on_off_cl_cd = '1'
            and c.buy_team_cd in ('16', '17')
        group by 1, 2, 3, 4
        having sum(a.gs_slng_amt) > 0
    )
    select cohort_group
         , cohort_index
         , count(distinct dpstr_cust_no) as cnt
    from joined
    group by 1, 2
    order by 1, 2
""")

base.createOrReplaceTempView("base")
base.cache()
base.show()

# Result
+------------+------------+------+
|cohort_group|cohort_index|   cnt|
+------------+------------+------+
|      202107|           0|403034|
|      202107|           1| 76453|
|      202107|           2| 89315|
|      202107|           3|103066|
|      202107|           4|100043|
|      202107|           5| 89426|
|      202107|           6| 75942|
|      202107|           7| 72568|
|      202107|           8| 80393|
|      202107|           9| 95761|
|      202107|          10|100303|
|      202107|          11| 53074|
|      202108|           0|214898|
|      202108|           1| 37200|
|      202108|           2| 41291|
|      202108|           3| 40770|
|      202108|           4| 35462|
|      202108|           5| 29829|
|      202108|           6| 29466|
|      202108|           7| 33725|
+------------+------------+------+


import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

base_pd = base.toPandas()
pdf = pd.DataFrame(base_pd)

pdf2 = pdf.groupby(['cohort_group', 'cohort_index'])['cnt'].agg('sum').reset_index().pivot(index='cohort_group', columns='cohort_index', values='cnt').fillna(0)
pdf_final = pdf2.div(pdf2[0], axis=0)
pdf_final

# Result
cohort_index   0         1         2   ...        9         10        11
cohort_group                           ...                              
202107        1.0  0.189694  0.221607  ...  0.237600  0.248870  0.131686
202108        1.0  0.173105  0.192142  ...  0.188773  0.085864  0.000000
202109        1.0  0.177596  0.170067  ...  0.065534  0.000000  0.000000
202110        1.0  0.146058  0.122709  ...  0.000000  0.000000  0.000000
202111        1.0  0.111528  0.087542  ...  0.000000  0.000000  0.000000
202112        1.0  0.082280  0.079893  ...  0.000000  0.000000  0.000000
202201        1.0  0.080856  0.087947  ...  0.000000  0.000000  0.000000
202202        1.0  0.088831  0.108426  ...  0.000000  0.000000  0.000000
202203        1.0  0.114284  0.113870  ...  0.000000  0.000000  0.000000
202204        1.0  0.108636  0.033535  ...  0.000000  0.000000  0.000000
202205        1.0  0.032447  0.000000  ...  0.000000  0.000000  0.000000
202206        1.0  0.000000  0.000000  ...  0.000000  0.000000  0.000000


fig, ax = plt.subplots(figsize=(12,8))
sns.heatmap(pdf_final, annot=True, cmap='YlGnBu', fmt='.0%', linewidths=.1, linecolor='black')

ax.set_title("Cohort Analysis", fontsize = 17)
ax.set_xlabel("Month", fontsize = 15)
ax.set_ylabel("Group", fontsize = 15)
plt.tight_layout()
