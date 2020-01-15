import pandas as pd
import pyreadstat


class Earnings_file:

    def __init__(self, filename, outprefix):
        self.filename = filename
        self.outprefix = outprefix

    def read(self):
        data, meta = pyreadstat.read_sas7bdat(self.filename, encoding='LATIN1', metadataonly=True)
        print(meta.column_names)
        print(meta.column_labels)
        print(meta.column_names_to_labels)
        print(meta.number_rows)
        print(meta.number_columns)
        print(meta.file_label)
        print(meta.file_encoding)

if __name__ == '__main__':
    earn = Earnings_file('d:\\data\\con_year_payment_line.sas7bdat', 'd:\\data\\earn')
    earn.read()