import pandas as pd
import datetime
import os
import futil
import sqlalchemy as sa
from sqlalchemy import text
import data_file

class Les_file(data_file.Data_file):

    def __init__(self, db, filename):
        self.db = db
        self.filename = filename

    # def processed(self):
    #     if os.path.isfile(self.filename):
    #         engine = sa.create_engine("sqlite:///" + self.db)
    #         conn = engine.connect()
    #         t = text("select count(*) from load_file where file='" + self.filename + "'")
    #         if next(conn.execute(t))[0] > 0:
    #             mtime = futil.modification_date(self.filename)
    #             t = text("select count(*) from load_file where file='" + self.filename +
    #                      "' and mod_date='" + mtime.strftime("%Y-%m-%d %H:%M:%S.%f") + "'")
    #             if next(conn.execute(t))[0] > 0:
    #                 return True
    #             else:
    #                 print("source file " + self.filename + " has been modified")
    #                 return False
    #         else:
    #             print("source file " + self.filename + " not loaded")
    #             return False
    #     else:
    #         print("source file " + self.filename + " not present")
    #         return True

    def do_process(self):
        mtime = futil.modification_date(self.filename)
        self.read()
        engine = sa.create_engine("sqlite:///" + self.db)
        conn = engine.connect()
        t = text("delete from load_file where file ='" +
                 self.filename + "'")
        conn.execute(t)
        t = text("insert into load_file (file, mod_date, load_time) values('" +
                 self.filename +
                 "', '" + mtime.strftime("%Y-%m-%d %H:%M:%S.%f") +
                 "', '" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + "')")
        conn.execute(t)
#
#
    def read(self):
        engine = sa.create_engine("sqlite:///" + self.db)
        conn = engine.connect()
        print( '----  begin >' + str(datetime.datetime.now()))

        print('-----  read >' + str(datetime.datetime.now()))

        df = pd.read_excel(self.filename)
        df.columns = ["client_group", "ppsn", "start_date"]
        days_to_friday = pd.to_timedelta((4 - df["start_date"].dt.dayofweek), unit="d")
        df["start_week"] = df["start_date"] + days_to_friday
        df.to_sql("les_tmp", con=engine, if_exists="replace")

        print('----  merge >' + str(datetime.datetime.now()))

        t = text( """
insert into les (client_group, ppsn, start_date, start_week)
select te.client_group, te.ppsn, te.start_date, te.start_week
    from les_tmp te
    left join les le
        on te.client_group = le.client_group
            and te.ppsn = le.ppsn
            and te.start_date = le.start_date
            and te.start_week = le.start_week
    where le.ppsn is null
    """)
        conn.execute(t)
        print('------  add >' + str(datetime.datetime.now()))
        t = text("""
delete
    from les
    where id in (select le.id
                     from les le
                         left join les_tmp te
                             on te.client_group = le.client_group
                                 and te.ppsn = le.ppsn
                                 and te.start_date = le.start_date
                                 and te.start_week = le.start_week
                         where te.ppsn is null)      """)
        conn.execute(t)
        print('------  del >' + str(datetime.datetime.now()))
        t = text("drop table les_tmp")
        conn.execute(t)
        print('-----  drop >' + str(datetime.datetime.now()))









if __name__ == '__main__':
    les = Les_file(  'd:\\data\\jp_pl.db', 'Z:\\Evaluations\\JobPath\\LES_exclusion\\LES_New_Registrations2016-2017.xlsx')
    # print( les.processed() )
    # les.do_process()
    # print( les.processed() )
    les.read()