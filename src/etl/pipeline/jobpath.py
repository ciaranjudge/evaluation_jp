import luigi
from datetime import date
import datetime
import ists as ists
import earnings as earnings
import payments as payments
import les as les
import penalties as penalties
import pandas as pd
import json
import os
from os import path
import csv
from sas7bdat import SAS7BDAT

class Ists_load(luigi.Task):
    sunday = luigi.DateParameter()
    settings = luigi.Parameter()

    def requires(self):
        return []

    def complete(self):
        self.ists = ists.Ists_file(self.sunday,self.settings)
        return self.ists.processed()

    def run(self):
        self.ists.do_process()


class Les_load(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        return []

    def complete(self):
        self.les = les.Les_file(self.settings)
        return self.les.processed()

    def run(self):
        self.les.do_process()


class Penalties_load(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        return []

    def complete(self):
        self.penalties = penalties.Penalties_file(self.settings)
        return self.penalties.processed()

    def run(self):
        self.penalties.do_process()


class Payments_load(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        return []

    def complete(self):
        self.payments = payments.Payments_file(self.settings)
        return self.payments.processed()

    def run(self):
        self.payments.do_process()


class Earnings_txt(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        return []

    def complete(self):
        self.dir = self.settings['earnings']['workdir']
        return path.exists(self.dir+'output.txt')

    def creatNewEarn(self, source, filename):
        print(" start " + str(datetime.datetime.now()))
        c = 0
        count = 0
        result_file = open(filename, 'wt')
        wr = csv.writer(result_file, dialect='excel')
        with SAS7BDAT(source, skip_header=True) as reader:
            for row in reader:
                wr.writerow(row)
                c += 1
                if c >= int(self.settings['earnings']['blocksize']):
                    print("     " + str(count) + "   " + str(datetime.datetime.now()))
                    count += 1
                    blocks = int(self.settings['earnings']['blocks'])
                    if blocks>0 and count > blocks:
                        break
                    c = 0;
        print("     " + str(count) + "   " + str(datetime.datetime.now()))
        print("   end " + str(datetime.datetime.now()))
        result_file.close()

    def run(self):
        print('Creating new text')
        self.creatNewEarn(self.settings['earnings']['file'], self.dir+'\output.txt')


class Earnings_sort(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        yield Earnings_txt(self.settings)

    def complete(self):
        self.dir = self.settings['earnings']['workdir']
        return path.exists(self.dir+'output-sort.txt')

    def merge_sort(self, infile, outfile, dir):
        chunks = []
        print(" start " + str(datetime.datetime.now()))
        count = 0
        c = 0
        outtfile = open(dir + '\\temp_' + str(count) + '.txt', 'wt')
        with open(infile, 'rt') as inf:
            for l in inf:
                if c >= 1000000:
                    outtfile.close()
                    os.system('sort < ' + self.dir + '\\temp_' + str(count) + '.txt  > ' +  self.dir + '\\temp_' + str( count) + '_sort.txt')
                    print("     " + str(count) + "   " + str(datetime.datetime.now()))
                    chunks.append(self.dir + '\\temp_' + str(count) + '_sort.txt')
                    count += 1
                    outtfile = open(self.dir + '\\temp_' + str(count) + '.txt', 'wt')
                    c = 0
                if (len(l) > 1):
                    outtfile.write(l.strip() + '\n')
                    c += 1
        outtfile.close()
        if c > 0:
            os.system( 'sort < ' +  self.dir + '\\temp_' + str(count) + '.txt  > ' +  self.dir + '\\temp_' + str(count) + '_sort.txt')
            chunks.append(self.dir +'temp_' + str(count) + '_sort.txt')
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
        self.merge_sort(self.dir+'output.txt', self.dir+'output-sort.txt', self.dir)
        for root, dirs, files in os.walk("d:\\data_test\\load", topdown=False):
            for name in files:
                if 'temp' in name:
                    os.remove(os.path.join(root, name))
        os.remove('d:\\data_test\\load\\output.txt')


class Earnings_diff(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        yield Earnings_sort(self.settings)

    def complete(self):
        self.dir = self.settings['earnings']['workdir']
        return path.exists(self.dir+'diff.txt')

    def createDiff(self, old_file, new_file, diff_file):
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
        self.createDiff(self.dir+'earn_current.txt', self.dir+'output-sort.txt', self.dir+'diff.txt')


class Earnings_load(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        yield Earnings_diff(self.settings)

    def complete(self):
        self.earnings = earnings.Earnings_file(self.settings)
        return self.earnings.processed()

    def run(self):
        self.earnings.do_process()


class JobPathEtl(luigi.Task):
    config = luigi.Parameter()
    task_complete = False;

    def requires(self):
        with open(self.config, 'r') as f:
            settings = json.load(f)
        ists_end = date.today()
        sundays = pd.date_range(datetime.datetime.strptime(settings['ists']['startDate'],'%Y-%m-%d'), ists_end, freq="W-SUN")
        yield Earnings_load(settings)
        yield Penalties_load(settings)
        yield Les_load(settings)
        for sunday in sundays:
            yield Ists_load(sunday,settings)
        # yield Payments_load(settings)

    def complete(self):
        return self.task_complete

    def run(self):
        self.task_complete = True


if __name__ == '__main__':
    luigi.run()
