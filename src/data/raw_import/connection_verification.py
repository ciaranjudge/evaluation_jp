# %%
import pandas as pd
import sqlalchemy as sa

engine = sa.create_engine('sqlite:///\\\\cskma0294\\F\\Evaluations\\data\\wwld.db', echo=False)

# %%

query="""select * 
from ists_claims c
join ists_personal p
on c.personal_id=p.id
where lr_date = '2020-01-03' and occupation=10"""
sql_ists = pd.read_sql(query, con=engine)
sql_ists.head()
# %%

query="""select * from payments
where AMOUNT='500'"""
sql_pay = pd.read_sql(query, con=engine)
sql_pay.head()
# %%

query="""select * from payments
where QTR='2014Q1'"""
sql_pay = pd.read_sql(query, con=engine)
sql_pay.head()
#%%
query="""select * from earnings where 
CON_YEAR=2007 and 
EARNINGS_AMT=10000 """
sql_earnings = pd.read_sql(query, con=engine)
sql_earnings.head()
# %%
query="""select * from les"""
sql_les = pd.read_sql(query, con=engine)

# %%
query="""select * from Penalties"""
sql_penalties = pd.read_sql(query, con=engine)
# %%

# %%
select *                                              "+
                     "    from ists_claims c                                "+
                     "    join ists_personal p                              "+
                     "        on c.personal_id=p.id                         "+
                     "    where lr_date = '2020-01-03' and occupation=10    ",
                     con=engine
sql_ists = pd.read_sql(query, con=engine)

