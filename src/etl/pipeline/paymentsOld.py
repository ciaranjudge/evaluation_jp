import pandas as pd
import futil
import glob
import pyreadstat
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


class PaymentsOld_file(data_file.Data_file):

    def __init__(self, settings):
        self.settings = settings
        self.db = settings['db']
        self.location = settings['pay_old']['location']
        self.dir = settings['pay_old']['workdir']
        if not(os.path.exists(str( self.dir))):
            os.makedirs(self.dir, exist_ok=True)

    def check_file(self, filename, conn):
        if os.path.isfile(filename):
            t = text("select count(*) from load_file where file='" + filename + "'")
            if next(conn.execute(t))[0] > 0:
                mtime = futil.modification_date(filename)
                t = text("select count(*) from load_file where file='" + filename +
                         "' and mod_date='" + mtime.strftime("%Y-%m-%d %H:%M:%S.%f") + "'")
                if next(conn.execute(t))[0] > 0:
                    return True
                else:
                    return False
            else:
                return False
        else:
            return True

    def processed(self):
        engine = sa.create_engine(self.db)
        conn = engine.connect()
        self.processed_files = []
        result = True
        for f in glob.glob(self.location + self.settings['pay_old']['pattern']):
            self.processed_files.append( {'file': f ,
                                          'mod': futil.modification_date(f).strftime("%Y-%m-%d %H:%M:%S.%f"),
                                          'current': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") } )
            if not( self.check_file( f, conn) ):
                result = False
        return result

    def do_process(self):
        self.read()
        engine = sa.create_engine( self.db)
        conn = engine.connect()
        for d in self.processed_files:
            t = text("delete from load_file where file ='" +
                     d['file'] + "'")
            conn.execute(t)
            t = text("insert into load_file (file, mod_date, load_time) values('" +
                 d['file'] +
                 "', '" +d['mod'] +
                 "', '" + d['current'] + "')")
            conn.execute(t)

    def insertData(self, diff_file):
        headings = ['CLMT_RSI_NO', 'CLM_SCH_CODE', 'CLM_REG_DATE', 'ISSUE_DATE', 'STAT_CODE', 'AMOUNT','year']
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
                    df.to_sql("pay_old", con=engine, if_exists="append", index=False)
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
            df.to_sql("pay_old", con=engine, if_exists="append", index=False)
            print("     " + str(count) + "   " + str(datetime.datetime.now()))
            count += 1
        print("   end " + str(datetime.datetime.now()))

    def deleteData(self, diff_file):
        headings = ['CLMT_RSI_NO', 'CLM_SCH_CODE', 'CLM_REG_DATE', 'ISSUE_DATE', 'STAT_CODE', 'AMOUNT','year']
        engine = sa.create_engine( self.db, echo=False)
        conn = engine.connect()
        t = text("""
        CREATE TABLE pay_old_tmp (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            CLMT_RSI_NO  TEXT,
            CLM_SCH_CODE TEXT,
            CLM_REG_DATE DATETIME,
            ISSUE_DATE   DATETIME,
            STAT_CODE    TEXT,
            AMOUNT       FLOAT,
            year         FLOAT
            )
                """)
        conn.execute(t)
        t = text("""
        CREATE INDEX idx_payt_ppsn ON pay_old_tmp (
            CLMT_RSI_NO
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
                    df.to_sql("pay_old_tmp", con=engine, if_exists="append", index=False)
                    print("---- > " + len(df) )
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
            df.to_sql("pay_old_tmp", con=engine, if_exists="append", index=False)
            print("---- > " + len(df))
            print("     " + str(count) + "   " + str(datetime.datetime.now()))
            count += 1
            t = text("""
            delete
        from pay_old
        where id in (select pa.id
                         from pay_old pa
                              left join pay_old_tmp te
                                 on te.CLMT_RSI_NO = pa.CLMT_RSI_NO
                                     and (te.CLM_SCH_CODE = pa.CLM_SCH_CODE )
                                     and (te.CLM_REG_DATE = pa.CLM_REG_DATE )
                                     and (te.ISSUE_DATE = pa.ISSUE_DATE )
                                     and (te.STAT_CODE = pa.STAT_CODE )
                                     and (te.AMOUNT = pa.AMOUNT )
                                     and (te.year = pa.year )
                             where  te.CLMT_RSI_NO is not null)

            """)
        conn.execute(t)
        t = text("drop table pay_old_tmp")
        conn.execute(t)
        print("   end " + str(datetime.datetime.now()))

    def read(self):
        print('making inserts')
        self.insertData(self.dir+'diff-payold.txt')

        print('making deletes')
        self.deleteData( self.dir+'diff-payold.txt')
        os.remove( self.dir+'diff-payold.txt')
        shutil.move(self.dir + 'output-payold-sort.txt', self.dir + 'payold_current.txt')
        pass


class PaymentsOld_txt(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        return []

    def complete(self):
        self.dir = self.settings['pay_old']['workdir']
        return path.exists(self.dir+'output-payold.txt')

    def creatNewPay(self,  filename):
        print(" start " + str(datetime.datetime.now()))

        result_file = open(filename, 'wt')
        for source in glob.glob(self.settings['pay_old']['location'] + self.settings['pay_old']['pattern']):
            print("processing " + source )
            i = source.index('payee_pmt_line_')
            year = int(source[i + 15:i + 19])
            c = 0
            count = 0
            with SAS7BDAT(source, skip_header=True) as reader:
                for row in reader:
                    row.append( year )
                    result_file.write(", ".join(map(str, row))+'\n')
                    c += 1
                    if c >= int(self.settings['pay_old']['blocksize']):
                        print("     " + str(count) + "   " + str(datetime.datetime.now()))
                        count += 1
                        blocks = int(self.settings['pay_old']['blocks'])
                        if blocks>0 and count > blocks:
                            break
                        c = 0;
            print("     " + str(count) + "   " + str(datetime.datetime.now()))
        print("   end " + str(datetime.datetime.now()))
        result_file.close()

    def run(self):
        print('Creating new text')
        self.creatNewPay( self.dir+'\\output-payold.txt')


class PaymentsOld_sort(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        yield PaymentsOld_txt(self.settings)

    def complete(self):
        self.dir = self.settings['pay_old']['workdir']
        return path.exists(self.dir+'output-payold-sort.txt')

    def merge_sort(self, infile, outfile, dir):
        chunks = []
        print(" start " + str(datetime.datetime.now()))
        count = 0
        c = 0
        outtfile = open(dir + '\\temp_pay_' + str(count) + '.txt', 'wt')
        with open(infile, 'rt') as inf:
            for l in inf:
                if c >= int(self.settings['pay_old']['blocksize']):
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
        self.merge_sort(self.dir+'output-payold.txt', self.dir+'output-payold-sort.txt', self.dir)
        for root, dirs, files in os.walk(self.dir, topdown=False):
            for name in files:
                if 'temp' in name:
                    os.remove(os.path.join(root, name))
        os.remove(self.dir+'output-payold.txt')


class PaymentsOld_diff(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        yield PaymentsOld_sort(self.settings)

    def complete(self):
        self.dir = self.settings['pay_old']['workdir']
        return path.exists(self.dir+'diff-payold.txt')

    def createDiff(self, old_file, new_file, diff_file):
        if not(path.exists(old_file)):
            print( "No Payments current file. Creating empty file, Deleting earnings table data")
            f=open(old_file,'wt')
            f.close()
            engine = sa.create_engine(self.settings['db'], echo=False)
            conn=engine.connect()
            t=text("delete from earnings")
            conn.execute(t)
        delete_num = 0
        insert_num = 0
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
                insert_num += 1
                n = fnew.readline()
            elif not (n) or o < n:
                c += 1
                fout.write('<' + o)
                delete_num += 1
                o = fold.readline()
            elif o == n:
                c += 2
                n = fnew.readline()
                o = fold.readline()
        fnew.close()
        fold.close()
        fout.close()
        print("     " + str(count) + "   " + str(datetime.datetime.now()))
        print( "              Inserts  =  " + str(insert_num))
        print( "              Deletes  =  " + str(delete_num))
        print("   end " + str(datetime.datetime.now()))

    def run(self):
        print('create load diff file')
        self.createDiff(self.dir+'payold_current.txt', self.dir+'output-payold-sort.txt', self.dir+'diff-payold.txt')
