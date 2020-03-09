import datetime
import pandas as pd
import data_file
import sqlalchemy as sa
from sqlalchemy import text


class Jp_file(data_file.Data_file):

    def __init__(self, filename, settings):
        self.db = settings['db']
        self.filename = filename

    def read(self):
        print('----  begin>' + str(datetime.datetime.now()))
        i = self.filename.index( 'flat' )
        self.q = self.filename[i+5:i+13]
        df = pd.read_csv( self.filename, usecols=[0,1])
        df['qtr'] = self.q
        engine = sa.create_engine(self.db, echo=False)
        conn = engine.connect()
        print( '1>' + str(datetime.datetime.now()))
        df.to_sql("jp_tmp", con=engine, if_exists="replace")

        print( '2>' + str(datetime.datetime.now()))
        t = text("""
                    CREATE INDEX idx1 ON jp_tmp (
                        ppsn
                    )
                """)
        conn.execute(t)

        print( '3>' + str(datetime.datetime.now()))
        t = text("""
                    insert into jobpath(ppsn, cluster,qtr )
                    select te.ppsn, te.cluster, te.qtr
                        from jp_tmp te
                        left join jobpath jp
                            on te.ppsn = jp.ppsn
                            and te.cluster = jp.cluster
                            and te.qtr = jp.qtr
                        where jp.qtr is null       
        """)
        conn.execute(t)

        print( '4>' + str(datetime.datetime.now()))
        t = text("""
                    delete
                       from jobpath
                       where id in (select jp.id
                            from jobpath jp
                               left join jp_tmp te
                                   on te.cluster = jp.cluster
                                   and te.ppsn = jp.ppsn
                               where jp.qtr = '"""+self.q+""""' and te.ppsn is null)
                """)
        conn.execute(t)

        print( '5>' + str(datetime.datetime.now()))
        t = text("""
                    drop table jp_tmp
                """)
        conn.execute(t)

        print('------  end>' + str(datetime.datetime.now()))


