import pandas as pd
import datetime
import os
import sqlalchemy as sa
from sqlalchemy import text
from sas7bdat import SAS7BDAT
import data_file
import shutil
import luigi
import csv
from os import path


class Payments_file(data_file.Data_file):

    def __init__(self, settings):
        self.db = settings['db']
        self.filename = settings['payments']['file']
        self.dir = settings['payments']['workdir']
        if not(os.path.exists(str( self.dir))):
            os.makedirs(self.dir, exist_ok=True)

    def insertData(self, diff_file):
        headings = ['ppsn', 'Quarter', 'SCHEME_TYPE', 'AMOUNT', 'QTR', 'count']
        engine = sa.create_engine( self.db, echo=False)
        c = 0
        count = 0
        insert_buffer = []
        print(" start " + str(datetime.datetime.now()))
        with open(diff_file, 'rt') as diffin:
            for l in diffin:
                if c >= 1000000:
                    df = pd.DataFrame(insert_buffer, columns=headings)
                    insert_buffer = []
                    df.to_sql("payments", con=engine, if_exists="append", index=False)
                    print("     " + str(count) + "   " + str(datetime.datetime.now()))
                    count += 1
                    c = 0
                c += 1
                if l[0] == '>':
                    if l[1] == '"':
                        print("error   "+l)
                    else:
                        insert_buffer.append(l[1:].split(","))
            df = pd.DataFrame(insert_buffer, columns=headings)
            df.to_sql("payments", con=engine, if_exists="append", index=False)
            print("     " + str(count) + "   " + str(datetime.datetime.now()))
            count += 1
        print("   end " + str(datetime.datetime.now()))

    def deleteData(self, diff_file):
        headings = ['ppsn', 'Quarter', 'SCHEME_TYPE', 'AMOUNT', 'QTR', 'count']
        engine = sa.create_engine( self.db, echo=False)
        conn = engine.connect()
        t = text("""
        CREATE TABLE paymentst (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ppsn        TEXT,
            Quarter     TEXT,
            SCHEME_TYPE TEXT,
            AMOUNT      TEXT,
            QTR         TEXT,
            count       TEXT
            )
                """)
        conn.execute(t)
        t = text("""
        CREATE INDEX idx_payt_ppsn ON paymentst (
            ppsn
        )
        """)
        conn.execute(t)
        c = 0
        count = 0
        delete_buffer = []
        print(" start " + str(datetime.datetime.now()))
        with open(diff_file, 'rt') as diffin:
            for l in diffin:
                if c >= 1000000:
                    df = pd.DataFrame(delete_buffer, columns=headings)
                    delete_buffer = []
                    df.to_sql("paymentst", con=engine, if_exists="append", index=False)
                    print("     " + str(count) + "   " + str(datetime.datetime.now()))
                    count += 1
                    c = 0
                c += 1
                if l[0] == '<':
                    if l[1] == '"':
                        print("error   "+l)
                    else:
                        delete_buffer.append(l[1:].split(","))
            df = pd.DataFrame(delete_buffer, columns=headings)
            df.to_sql("paymentst", con=engine, if_exists="append", index=False)
            print("     " + str(count) + "   " + str(datetime.datetime.now()))
            count += 1
            t = text("""
            delete
        from payments
        where id in (select pa.id
                         from payments pa
                              left join paymentst te
                                 on te.ppsn = pa.ppsn
                                     and (te.Quarter = pa.Quarter OR (te.Quarter is null and pa.Quarter is null))
                                     and (te.SCHEME_TYPE = pa.SCHEME_TYPE OR (te.SCHEME_TYPE is null and pa.SCHEME_TYPE is null))
                                     and (te.AMOUNT = pa.AMOUNT OR (te.AMOUNT is null and pa.AMOUNT is null))
                                     and (te.QTR = pa.QTR OR (te.QTR is null and pa.QTR is null))
                                     and (te.count = pa.count OR (te.count is null and pa.count is null))
                             where  te.ppsn is not null)

            """)
        conn.execute(t)
        t = text("drop table paymentst")
        conn.execute(t)
        print("   end " + str(datetime.datetime.now()))


    def read(self):
        print('making inserts')
        self.insertData(self.dir+'diff-pay.txt')

        print('making deletes')
        self.deleteData( self.dir+'diff-pay.txt')
        os.remove( self.dir+'diff-pay.txt')
        shutil.move(self.dir + 'output-pay-sort.txt', self.dir + 'pay_current.txt')


class Payments_txt(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        return []

    def complete(self):
        self.dir = self.settings['payments']['workdir']
        return path.exists(self.dir+'output-pay.txt')

    def creatNewPay(self, source, filename):
        print(" start " + str(datetime.datetime.now()))
        c = 0
        count = 0
        result_file = open(filename, 'wt')
        wr = csv.writer(result_file, dialect='excel')
        with SAS7BDAT(source, skip_header=True) as reader:
            for row in reader:
                wr.writerow(row)
                c += 1
                if c >= int(self.settings['payments']['blocksize']):
                    print("     " + str(count) + "   " + str(datetime.datetime.now()))
                    count += 1
                    blocks = int(self.settings['payments']['blocks'])
                    if blocks>0 and count > blocks:
                        break
                    c = 0;
        print("     " + str(count) + "   " + str(datetime.datetime.now()))
        print("   end " + str(datetime.datetime.now()))
        result_file.close()

    def run(self):
        print('Creating new text')
        self.creatNewPay(self.settings['payments']['file'], self.dir+'\\output-pay.txt')


class Payments_sort(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        yield Payments_txt(self.settings)

    def complete(self):
        self.dir = self.settings['payments']['workdir']
        return path.exists(self.dir+'output-pay-sort.txt')

    def merge_sort(self, infile, outfile, dir):
        chunks = []
        print(" start " + str(datetime.datetime.now()))
        count = 0
        c = 0
        outtfile = open(dir + '\\temp_pay_' + str(count) + '.txt', 'wt')
        with open(infile, 'rt') as inf:
            for l in inf:
                if c >= int(self.settings['payments']['blocksize']):
                    outtfile.close()
                    os.system('sort < ' + self.dir + '\\temp_pay_' + str(count) + '.txt  > ' +  self.dir + '\\temp_pay_' + str( count) + '_sort.txt')
                    print("     " + str(count) + "   " + str(datetime.datetime.now()))
                    chunks.append(self.dir + '\\temp_pay_' + str(count) + '_sort.txt')
                    count += 1
                    outtfile = open(self.dir + '\\temp_pay_' + str(count) + '.txt', 'wt')
                    c = 0
                if (len(l) > 1):
                    outtfile.write(l.strip() + '\n')
                    c += 1
        outtfile.close()
        if c > 0:
            os.system( 'sort < ' +  self.dir + '\\temp_pay_' + str(count) + '.txt  > ' +  self.dir + '\\temp_pay_' + str(count) + '_sort.txt')
            chunks.append(self.dir +'temp_pay_' + str(count) + '_sort.txt')
        print("     " + str(count) + "   " + str(datetime.datetime.now()))
        print("   end " + str(datetime.datetime.now()))
        sfiles = []
        slines = []
        for ff in chunks:
            f = open(ff, 'rt')
            sfiles.append(f)
            li = f.readline()
            if li:
                slines.append(li)
            else:
                slines.append('zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz')
        fout = open(outfile, 'wt')
        while True:
            ind = slines.index(min(slines))
            if slines[ind] == 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz':
                break
            fout.write(slines[ind])
            slines[ind] = sfiles[ind].readline()
            if not (slines[ind]):
                slines[ind] = 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'
        fout.close()

    def run(self):
        print('Sorting new text')
        self.merge_sort(self.dir+'output-pay.txt', self.dir+'output-pay-sort.txt', self.dir)
        for root, dirs, files in os.walk(self.dir, topdown=False):
            for name in files:
                if 'temp' in name:
                    os.remove(os.path.join(root, name))
        os.remove(self.dir+'\\output-pay.txt')


class Payments_diff(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        yield Payments_sort(self.settings)

    def complete(self):
        self.dir = self.settings['payments']['workdir']
        return path.exists(self.dir+'diff-pay.txt')

    def createDiff(self, old_file, new_file, diff_file):
        if not(path.exists(old_file)):
            print( "No Payments current file. Creating empty file, Deleting earnings table data")
            f=open(old_file,'wt')
            f.close()
            engine = sa.create_engine(self.settings['db'], echo=False)
            conn=engine.connect()
            t=text("delete from earnings")
            conn.execute(t)
        c = 0
        count = 0
        print(" start " + str(datetime.datetime.now()))
        fout = open(diff_file, 'wt')
        fnew = open(new_file, 'rt')
        fold = open(old_file, 'rt')
        o = fold.readline()
        n = fnew.readline()
        while o or n:
            if c >= 1000000:
                print("     " + str(count) + "   " + str(datetime.datetime.now()))
                count += 1
                c = 0
            if not (o) or n < o:
                c += 1
                fout.write('>' + n)
                n = fnew.readline()
            elif not (n) or o < n:
                c += 1
                fout.write('<' + o)
                o = fold.readline()
            elif o == n:
                c += 2
                n = fnew.readline()
                o = fold.readline()
        fnew.close()
        fold.close()
        fout.close()
        print("     " + str(count) + "   " + str(datetime.datetime.now()))
        print("   end " + str(datetime.datetime.now()))

    def run(self):
        print('create load diff file')
        self.createDiff(self.dir+'pay_current.txt', self.dir+'output-pay-sort.txt', self.dir+'diff-pay.txt')
