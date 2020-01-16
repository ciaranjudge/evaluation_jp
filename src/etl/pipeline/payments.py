import pandas as pd
import futil
import pyreadstat
import datetime
import os
import sqlalchemy as sa
from sqlalchemy import text
from sas7bdat import SAS7BDAT
import data_file


class Payments_file(data_file.Data_file):

    def __init__(self, db, filename):
        self.db = db
        self.filename = filename

    def read(self):
        engine = sa.create_engine("sqlite:///" + self.db)
        conn = engine.connect()
        print('----  begin >' + str(datetime.datetime.now()))

        count = 0
        print(str(count) + " " + str(datetime.datetime.now()))
        c = 0
        l = []
        with SAS7BDAT(self.filename, skip_header=True) as reader:
            for row in reader:
                l.append(row)
                c += 1
                if c > 2500000:
                    c = 0
                    df = pd.DataFrame(l, columns=['ppsn', 'Quarter', 'SCHEME_TYPE', 'AMOUNT', 'QTR', 'count'])
                    df.to_sql("payments_" + str(count) + "_tmp", con=engine, if_exists="replace")
                    l = []
                    count += 1
                    print(str(count) + " " + str(datetime.datetime.now()))
                    if count >= 1:
                        break
            df = pd.DataFrame(l, columns=['ppsn', 'Quarter', 'SCHEME_TYPE', 'AMOUNT', 'QTR', 'count'])
            df.to_sql("payments_" + str(count) + "_tmp", con=engine, if_exists="replace")
            count += 1
            print(str(count) + " " + str(datetime.datetime.now()))

        print('-----  read >' + str(datetime.datetime.now()))
        t = text("""
CREATE TABLE payments_tmp (
    ppsn        TEXT,
    Quarter     FLOAT,
    SCHEME_TYPE TEXT,
    AMOUNT      FLOAT,
    QTR         TEXT,
    count       FLOAT
)
        """)
        conn.execute(t)
        for n in range(0, count):
            print(str(n) + "    > " + str(datetime.datetime.now()))
            t = text( """
            insert into payments_tmp (ppsn, Quarter, SCHEME_TYPE, AMOUNT, QTR, count )
select ppsn, Quarter, SCHEME_TYPE, AMOUNT, QTR, count
from payments_"""+str(n)+"""_tmp
""")
            conn.execute(t)
            t = text( "drop table payments_"+str(n)+"_tmp")
            conn.execute(t)
            print(str(n) + "    > " + str(datetime.datetime.now()))
        print('----  merge >' + str(datetime.datetime.now()))
        t = text( """
insert into payments (ppsn, Quarter, SCHEME_TYPE, AMOUNT, QTR, count)
select te.ppsn, te.Quarter, te.SCHEME_TYPE, te.AMOUNT, te.QTR, te.count
    from payments_tmp te
    left join payments pa 
        on te.ppsn = pa.ppsn
            and te.Quarter = pa.Quarter
            and te.SCHEME_TYPE = pa.SCHEME_TYPE
            and te.AMOUNT = pa.AMOUNT
            and te.QTR = pa.QTR
            and te.count = pa.count
    where pa.ppsn is null
    """)
        conn.execute(t)
        print('------  add >' + str(datetime.datetime.now()))
        t = text("""
delete 
    from payments 
    where id in (select pa.id
                     from payments pa
                         left join payments_tmp te 
                             on te.ppsn = pa.ppsn
                                 and te.Quarter = pa.Quarter
                                 and te.SCHEME_TYPE = pa.SCHEME_TYPE
                                 and te.AMOUNT = pa.AMOUNT
                                 and te.QTR = pa.QTR
                                 and te.count = pa.count
                         where te.ppsn is null)      """)
        conn.execute(t)
        print('------  del >' + str(datetime.datetime.now()))
        t = text("drop table payments_tmp")
        conn.execute(t)
        print('-----  drop >' + str(datetime.datetime.now()))


if __name__ == '__main__':
    pay = Payments_file('d:\\data\\jp_pl.db', 'd:\\data\\con_year_payment_line.sas7bdat')
    # print( earn.processed() )
    # earn.do_process()
    # print( earn.processed() )
    pay.read()
