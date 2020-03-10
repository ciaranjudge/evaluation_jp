import pandas as pd
import datetime
import sqlalchemy as sa
from sqlalchemy import text

from evaluation_jp.data.etl.pipeline.data_file import Data_file


class Les_file(Data_file):
    def __init__(self, settings):
        self.db = settings["db"]
        self.filename = settings["les"]["file"]

    def read(self):
        engine = sa.create_engine(self.db)
        conn = engine.connect()
        print("----  begin >" + str(datetime.datetime.now()))
        print("-----  read >" + str(datetime.datetime.now()))
        df = pd.read_excel(self.filename)
        df.columns = ["client_group", "ppsn", "start_date"]
        df["ppsn"] = df["ppsn"].str.strip()
        df.to_sql("les_tmp", con=engine, if_exists="replace")
        print("----  merge >" + str(datetime.datetime.now()))
        t = text(
            """
                insert into les (client_group, ppsn, start_date)
                select te.client_group, te.ppsn, te.start_date
                    from les_tmp te
                    left join les le
                        on te.client_group = le.client_group
                            and te.ppsn = le.ppsn
                            and te.start_date = le.start_date
                    where le.ppsn is null
            """
        )
        conn.execute(t)
        print("------  add >" + str(datetime.datetime.now()))
        t = text(
            """delete
                    from les
                    where id in (select le.id
                                    from les le
                                        left join les_tmp te
                                            on te.client_group = le.client_group
                                                and te.ppsn = le.ppsn
                                                and te.start_date = le.start_date
                                        where te.ppsn is null)
            """
        )
        conn.execute(t)
        print("------  del >" + str(datetime.datetime.now()))
        t = text("drop table les_tmp")
        conn.execute(t)
        print("-----  drop >" + str(datetime.datetime.now()))

