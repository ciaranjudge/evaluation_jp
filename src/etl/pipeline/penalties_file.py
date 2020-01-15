import pandas as pd
import pyreadstat
import sqlalchemy as sa
from sqlalchemy import text

class Penalties_file:

    def __init__(self, filename):
        self.filename = filename

    def read(self):
        df, meta = pyreadstat.read_sas7bdat(self.filename)
        print( df.head(3));
        engine = sa.create_engine("sqlite:///d:\\data\\jp_pl_pen.db" )
        conn = engine.connect()
        df.to_sql("epenalties_tmp", con=engine, if_exists="replace")

if __name__ == '__main__':
    penalties = Penalties_file('Z:\\Management Board\\Penalty Rates\\penalties_00_latest.sas7bdat')
    penalties.read()