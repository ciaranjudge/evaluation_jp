import datetime
import pandas as pd
import data_file
import sqlalchemy as sa
from sqlalchemy import text
import pyreadstat
from sas7bdat import SAS7BDAT


class PayOld_file(data_file.Data_file):

    def __init__(self, filename, settings):
        self.db = settings['db']
        self.filename = filename
        self.settings = settings

    def read(self):
        print('----  begin>' + str(datetime.datetime.now()))

        engine = sa.create_engine(self.db, echo=False)
        conn = engine.connect()

        print( '0>' + str(datetime.datetime.now()))
        try:
            t = text("""
                        drop table pay_old_tmp
                    """)
            conn.execute(t)
            t = text("""
                        CREATE TABLE pay_old_tmp (
                            CLMT_RSI_NO  TEXT,
                            CLM_SCH_CODE TEXT,
                            CLM_REG_DATE DATETIME,
                            ISSUE_DATE   DATETIME,
                            STAT_CODE    TEXT,
                            AMOUNT       FLOAT,
                            year         FLOAT
                    )
                    """)
            conn.execute(t)
            t = text("""
                    CREATE INDEX idx_po_1 ON pay_old_tmp (
                        CLMT_RSI_NO
                    )
                    """)
            conn.execute(t)
        except:
            pass

        i = self.filename.index( 'payee_pmt_line_' )
        self.year = self.filename[i + 15:i + 19]

        count = 0
        c = 0
        rows = []
        with SAS7BDAT(self.filename, skip_header=True) as reader:
            for row in reader:
                dict1 = {}
                dict1['CLMT_RSI_NO'] = row[0]
                dict1['CLM_SCH_CODE'] = row[1]
                dict1['CLM_REG_DATE'] = row[2]
                dict1['ISSUE_DATE'] = row[3]
                dict1['STAT_CODE'] = row[4]
                dict1['AMOUNT'] = float(row[5])
                rows.append(dict1)
                if c > int(self.settings['pay_old']['blocksize']):
                    df = pd.DataFrame(rows)
                    df['year'] = self.year
                    rows = []
                    df.to_sql("pay_old_tmp", con=engine, if_exists="append", index=False)
                    c = 0
                    print(count)
                    count += 1
                c += 1
            df = pd.DataFrame(rows)
            df['year'] = self.year
            df.to_sql("pay_old_tmp", con=engine, if_exists="append", index=False)

        print( '2>' + str(datetime.datetime.now()))
        t = text("""
                    insert into pay_old(CLMT_RSI_NO, CLM_SCH_CODE, CLM_REG_DATE, ISSUE_DATE, STAT_CODE, AMOUNT, year )
                    select te.CLMT_RSI_NO, te.CLM_SCH_CODE, te.CLM_REG_DATE, te.ISSUE_DATE, te.STAT_CODE, te.AMOUNT, te.year
                        from pay_old_tmp te
                        left join pay_old po
                            on te.CLMT_RSI_NO = po.CLMT_RSI_NO
                            and te.CLM_SCH_CODE = po.CLM_SCH_CODE
                            and te.CLM_REG_DATE = po.CLM_REG_DATE
                            and te.ISSUE_DATE = po.ISSUE_DATE
                            and te.STAT_CODE = po.STAT_CODE
                            and te.AMOUNT = po.AMOUNT
                            and te.year = po.year
                        where po.CLMT_RSI_NO is null       
        """)
        conn.execute(t)

        print( '3>' + str(datetime.datetime.now()))
#         t = text("""
# delete
#     from pay_old
#     where id in ( select po.id
#                       from pay_old po
#                           left join pay_old_tmp te
#                               on te.CLMT_RSI_NO = po.CLMT_RSI_NO
#                                   and te.CLM_SCH_CODE = po.CLM_SCH_CODE
#                                   and te.CLM_REG_DATE = po.CLM_REG_DATE
#                                   and te.ISSUE_DATE = po.ISSUE_DATE
#                                   and te.STAT_CODE = po.STAT_CODE
#                                   and te.AMOUNT = po.AMOUNT
#                                   and te.year = po.year
#                       where po.year="""+str(self.year)+""" and te.CLMT_RSI_NO is null  )
#                 """)
#         conn.execute(t)
#
        print( '4>' + str(datetime.datetime.now()))
        t = text("""
                    drop table pay_old_tmp
                """)
        conn.execute(t)

        print('------  end>' + str(datetime.datetime.now()))


