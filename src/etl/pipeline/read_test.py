import pandas as pd
import sqlalchemy as sa

engine = sa.create_engine('sqlite:///\\\\cskma0294\\F\\Evaluations\\data\\wwld.db', echo=False)

sql_DF = pd.read_sql("select *                                              "+
                     "    from ists_claims c                                "+
                     "    join ists_personal p                              "+
                     "        on c.personal_id=p.id                         "+
                     "    where lr_date = '2020-01-03' and occupation=10    ",
                     con=engine)
print(sql_DF.head(3))

sql_DF = pd.read_sql("select *                    "+
                     "    from payments           "+
                     "    where ppsn='7035482E'   ",
                     con=engine)
print( sql_DF)

sql_DF = pd.read_sql("select *                          "+
                     "    from earnings                 "+
                     "    where RSI_NO='0062871I'       ",
                     con=engine)
print( sql_DF)

sql_DF = pd.read_sql("select *                          "+
                     "    from les                      "+
                     "    where ppsn='1257072FA'        ",
                     con=engine)
print( sql_DF)

sql_DF = pd.read_sql("select *                           "+
                     "    from Penalties                 "+
                     "    where ppsn='6922476U'          ",
                     con=engine)
print( sql_DF)


