import datetime
import pandas as pd
from sas7bdat import SAS7BDAT
import pyreadstat

print( " " + str(datetime.datetime.now()))
data, meta = pyreadstat.read_sas7bdat('\\\\cskma0294\\F\\Evaluations\\JobPath\\Python\\Data\\payments_2006_2013\\payee_pmt_line_2008.sas7bdat', encoding='LATIN1')
print( data.head(3) )
# with open( '\\\\cskma0294\\F\\Evaluations\\JobPath\\Python\\Data\\payments_2006_2013\\payments_2006_2013.txt','rt') as f:
#     content = f.readlines()
print( " " + str(datetime.datetime.now()))

