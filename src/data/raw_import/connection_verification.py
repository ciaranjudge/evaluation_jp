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
# %%
select *                                              "+
                     "    from ists_claims c                                "+
                     "    join ists_personal p                              "+
                     "        on c.personal_id=p.id                         "+
                     "    where lr_date = '2020-01-03' and occupation=10    ",
                     con=engine
sql_ists = pd.read_sql(query, con=engine)

sql_ists = pd.read_sql("select *                                              "+
                     "    from ists_claims c                                "+
                     "    join ists_personal p                              "+
                     "        on c.personal_id=p.id                         "+
                     "    where lr_date = '2020-01-03' and occupation=10    ",
                     con=engine)
#print(sql_ists.head(3))

# %%

query="""select * from payments"""
sql_pay = pd.read_sql(query, con=engine)

sql_pay = pd.read_sql("select *                    "+
                     "    from payments           "+
                     "    where ppsn='7035482E'   ",
                     con=engine)
#print( sql_DF)
# %%

query="""select * from earnings"""
sql_earnings = pd.read_sql(query, con=engine)

sql_earnings = pd.read_sql("select *                          "+
                     "    from earnings                 "+
                     "    where RSI_NO='0062871I'       ",
                     con=engine)
#print( sql_DF)
# %%
query="""select * from les"""
sql_les = pd.read_sql(query, con=engine)


sql_les = pd.read_sql("select *                          "+
                     "    from les                      ",                    
                     con=engine)
#print( sql_DF)
# %%
query="""select * from Penalties"""
sql_penalties = pd.read_sql(query, con=engine)
#print( sql_DF)
# %%