import os
import pandas as pd
import pyreadstat
import datetime
import csv
import sqlalchemy as sa
from sqlalchemy import text


path = '\\\\cskma0294\\F\\Evaluations\\JobPath\\Python\\Data\\payments_2006_2013'
engine = sa.create_engine("sqlite:///d:\\data\\read_pay.db" )
conn = engine.connect()


def getFiles(path):
    files = []
    for r, d, f in os.walk(path):
        for file in f:
            files.append(os.path.join(r, file))
    return files

for f in getFiles(path):
    print(f)
    data, meta = pyreadstat.read_sas7bdat(f, encoding='LATIN1')
    data.to_sql("oldpay_tmp", con=engine, if_exists="replace")
    print(data.shape)
    break

