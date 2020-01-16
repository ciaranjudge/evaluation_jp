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
                                               where te.ppsn is null)
                 """)
        conn.execute(t)
        print('------  del >' + str(datetime.datetime.now()))
        t = text("drop table les_tmp")
        conn.execute(t)
        print('-----  drop >' + str(datetime.datetime.now()))


