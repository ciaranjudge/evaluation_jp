import datetime
import pandas as pd
import src.etl.pipeline.data_file as ddf
import pyreadstat as pyreadstat

class Pay_old_file():
    def __init__(self, file):
        self.file = file

    def read(self):
        print('----  begin`>' + str(datetime.datetime.now()))
        data, meta = pyreadstat.read_sas7bdat(self.file, encoding='LATIN1')
        print(data.head(3))

        print('------  end`>' + str(datetime.datetime.now()))

p = Pay_old_file('Z:\\Evaluations\\JobPath\\Python\\Data\\payments_2006_2013\\payee_pmt_line_2007.sas7bdat')
p.read()