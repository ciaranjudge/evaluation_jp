import pandas as pd
import pyreadstat
import datetime
import gc
import sqlalchemy as sa
from sqlalchemy import text


# ti = datetime.datetime.now()
# print('>>> ' + str(ti))
#
# reader = pyreadstat.read_file_in_chunks(pyreadstat.read_sas7bdat, 'd:\\data\\con_year_payment_line.sas7bdat', chunksize=10000000)
# count = 0
# for df, meta in reader:
#     ti1 = datetime.datetime.now()
#     print( str(count) + '    ' + str(ti1) +   '    ' + str(ti1-ti))
#     ti = ti1
#     count += 1
#
# engine = sa.create_engine("sqlite:///d:\\data\\read_test.db" )
# conn = engine.connect()

# chunk=1000000
# print('>>> ' + str(datetime.datetime.now()))
# df, meta = pyreadstat.read_sas7bdat( 'd:\\data\\parpall.sas7bdat', metadataonly=True )
# rows = meta.number_rows
# print( rows )
# print('>>> ' + str(datetime.datetime.now()))
#
# count = 0
# for i in range(chunk,rows,chunk):
#     df, meta = pyreadstat.read_sas7bdat( 'd:\\data\\parpall.sas7bdat', row_offset=i, row_limit=chunk )
#     df = None
#     gc.collect()
#     # df.to_sql("payments_" + str(count) + "_tmp", con=engine, if_exists="replace")
#     print(str(count)+' ' + str(datetime.datetime.now()))
#     count += 1

# reader = pyreadstat.read_file_in_chunks(pyreadstat.read_sas7bdat, 'd:\\data\\parpall.sas7bdat', chunksize=4000000)
#
# count = 0
# for df, meta in reader:
#     print(str(count) + " " + str(datetime.datetime.now()))
#     gc.collect()
#     count += 1

from sas7bdat import SAS7BDAT


engine = sa.create_engine("sqlite:///d:\\data\\read_test.db" )
conn = engine.connect()


count = 0
print(str(count) + " " + str(datetime.datetime.now()))
c=0
l=[]
with SAS7BDAT('d:\\data\\parpall.sas7bdat', skip_header=True) as reader:
    for row in reader:
        l.append(row)
        c+=1
        if c > 2500000:
            c=0
            df = pd.DataFrame(l, columns=['ppsn','Quarter','SCHEME_TYPE','AMOUNT','QTR','count'])
            df.to_sql("payments_" + str(count) + "_tmp", con=engine, if_exists="replace")
            l=[]
            count += 1
            print(str(count) + " " + str(datetime.datetime.now()))
    df = pd.DataFrame(l, columns=['ppsn','Quarter','SCHEME_TYPE','AMOUNT','QTR','count'])
    df.to_sql("payments_" + str(count) + "_tmp", con=engine, if_exists="replace")
    count += 1
    print(str(count) + " " + str(datetime.datetime.now()))
