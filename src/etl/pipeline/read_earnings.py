import pandas as pd
import numpy as np
import datetime
import os
import csv
from sqlalchemy import create_engine
from sqlalchemy import text
from sas7bdat import SAS7BDAT
import shutil

def creatNewEarn( source, filename):
    print(" start " + str(datetime.datetime.now()))
    c=0
    count=0
    result_file = open(filename,'wt')
    wr = csv.writer(result_file, dialect='excel')
    with SAS7BDAT(source, skip_header=True) as reader:
        for row in reader:
            wr.writerow(row)
            c += 1
            if c >= 1000000:
                print("     " + str(count) + "   "+ str(datetime.datetime.now()))
                count += 1
                if count > 2:
                    break
                c = 0;
    print("     " + str(count) + "   " + str(datetime.datetime.now()))
    print( "   end " + str(datetime.datetime.now()))
    result_file.close()

def merge_sort( infile, outfile, dir ):
    chunks = []
    print(" start " + str(datetime.datetime.now()))
    count = 0
    c = 0
    outtfile = open(dir+'\\temp_' + str(count) + '.txt', 'wt')
    with open(infile,'rt') as inf:
        for l in inf:
            if c >= 1000000:
                outtfile.close()
                os.system('sort < '+dir+'\\temp_' + str(count) + '.txt  > '+dir+'\\temp_' + str(count) + '_sort.txt' )
                print("     " + str(count) + "   " + str(datetime.datetime.now()))
                chunks.append( dir+'\\temp_' + str(count) + '_sort.txt')
                count += 1
                outtfile = open(dir+'\\temp_' + str(count) + '.txt', 'wt')
                c=0
            if( len(l)>1 ):
                outtfile.write( l.strip() + '\n' )
                c += 1
    outtfile.close()
    if c > 0:
        os.system('sort < temp_' + str(count) + '.txt  > temp_' + str(count) + '_sort.txt')
        chunks.append('temp_' + str(count) + '_sort.txt')
    print("     " + str(count) + "   " + str(datetime.datetime.now()))
    print("   end " + str(datetime.datetime.now()))
    sfiles = []
    slines = []
    for ff in chunks:
        f= open(ff,'rt')
        sfiles.append(f)
        li = f.readline()
        if li:
            slines.append( li )
        else:
            slines.append('zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz')
    fout = open( outfile, 'wt' )
    while True:
        ind = slines.index(min(slines))
        if slines[ind] == 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz':
            break
        fout.write(slines[ind])
        slines[ind] = sfiles[ind].readline()
        if not(slines[ind]):
            slines[ind] = 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'
    fout.close()

def createDiff( old_file, new_file, diff_file ):
    c = 0
    count = 0
    print(" start " + str(datetime.datetime.now()))
    fout = open(diff_file,'wt')
    fnew = open(new_file,'rt')
    fold = open(old_file,'rt')
    o = fold.readline()
    n = fnew.readline()
    while o or n:
        if c >= 1000000:
            print("     " + str(count) + "   " + str(datetime.datetime.now()))
            count += 1
            c = 0
        if not(o) or n < o:
            c += 1
            fout.write( '>'+ n )
            n = fnew.readline()
        elif not(n) or o < n:
            c += 1
            fout.write( '<'+ o )
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

def insertData(dir, diff_file):
    headings = ['RSI_NO','CON_YEAR','PAYMENT_LINE_COUNT','CONS_SOURCE_CODE','CONS_SOURCE_SECTION_CODE','NO_OF_CONS','CONS_CLASS_CODE','CONS_FROM_DATE','CONS_TO_DATE','EARNINGS_AMT','TOT_PRSI_AMT','EMPLOYER_NO','EMPLT_COUNT','EMPLT_NO','EMPLOYEE_PRSI_AMT','EMPLT_SCH_ID_NO','EMPLT_SCH_FROM_DATE','NON_CONSOLIDATABLE_IND','PAY_ERR_IND','PRSI_ERR_IND','WIES_ERR_IND','CLASS_ERR_IND','PRSI_REFUND_IND','CANCELLED_IND','CRS_SEGMENT','LA_DATE_TIME','RECORD_VERS_NO','USER_ID_CODE','PROGRAM_ID_CODE']
    engine = create_engine('sqlite:///'+dir+'\\jobpath.db', echo = False)
    conn = engine.connect()
    c=0
    count =0
    insert_buffer = []
    print(" start " + str(datetime.datetime.now()))
    with open(diff_file,'rt') as diffin:
        for l in diffin:
            if c >= 1000000:
                df = pd.DataFrame(insert_buffer, columns=headings)
                insert_buffer = []
                df.to_sql("earnings", con=engine, if_exists="append", index=False)
                print("     " + str(count) + "   " + str(datetime.datetime.now()))
                count += 1
                c=0
            c += 1
            if l[0] == '>':
                insert_buffer.append( l[1:].split(","))
        df = pd.DataFrame(insert_buffer, columns=headings)
        df.to_sql("earnings", con=engine, if_exists="append", index=False)
        print("     " + str(count) + "   " + str(datetime.datetime.now()))
        count += 1
    print("   end " + str(datetime.datetime.now()))

def deleteData(dir, diff_file):
    headings = ['RSI_NO','CON_YEAR','PAYMENT_LINE_COUNT','CONS_SOURCE_CODE','CONS_SOURCE_SECTION_CODE','NO_OF_CONS','CONS_CLASS_CODE','CONS_FROM_DATE','CONS_TO_DATE','EARNINGS_AMT','TOT_PRSI_AMT','EMPLOYER_NO','EMPLT_COUNT','EMPLT_NO','EMPLOYEE_PRSI_AMT','EMPLT_SCH_ID_NO','EMPLT_SCH_FROM_DATE','NON_CONSOLIDATABLE_IND','PAY_ERR_IND','PRSI_ERR_IND','WIES_ERR_IND','CLASS_ERR_IND','PRSI_REFUND_IND','CANCELLED_IND','CRS_SEGMENT','LA_DATE_TIME','RECORD_VERS_NO','USER_ID_CODE','PROGRAM_ID_CODE']
    engine = create_engine('sqlite:///'+dir+'\\jobpath.db', echo = False)
    conn = engine.connect()
    t = text("""
    CREATE TABLE earningst (
        id                       INTEGER PRIMARY KEY AUTOINCREMENT,
        RSI_NO                   TEXT,
        CON_YEAR                 FLOAT,
        PAYMENT_LINE_COUNT       FLOAT,
        CONS_SOURCE_CODE         TEXT,
        CONS_SOURCE_SECTION_CODE TEXT,
        NO_OF_CONS               FLOAT,
        CONS_CLASS_CODE          TEXT,
        CONS_FROM_DATE           FLOAT,
        CONS_TO_DATE             FLOAT,
        EARNINGS_AMT             FLOAT,
        TOT_PRSI_AMT             FLOAT,
        EMPLOYER_NO              TEXT,
        EMPLT_COUNT              FLOAT,
        EMPLT_NO                 TEXT,
        EMPLOYEE_PRSI_AMT        FLOAT,
        EMPLT_SCH_ID_NO          TEXT,
        EMPLT_SCH_FROM_DATE      FLOAT,
        NON_CONSOLIDATABLE_IND   TEXT,
        PAY_ERR_IND              TEXT,
        PRSI_ERR_IND             TEXT,
        WIES_ERR_IND             TEXT,
        CLASS_ERR_IND            TEXT,
        PRSI_REFUND_IND          TEXT,
        CANCELLED_IND            TEXT,
        CRS_SEGMENT              FLOAT,
        LA_DATE_TIME             FLOAT,
        RECORD_VERS_NO           FLOAT,
        USER_ID_CODE             FLOAT,
        PROGRAM_ID_CODE          TEXT
    )
            """)
    conn.execute(t)
    t = text("""
    CREATE INDEX idx_earnt_ppsn ON earningst (
        RSI_NO
    )
    """)
    conn.execute(t)
    c=0
    count =0
    delete_buffer = []
    print(" start " + str(datetime.datetime.now()))
    with open(diff_file,'rt') as diffin:
        for l in diffin:
            if c >= 1000000:
                df = pd.DataFrame(delete_buffer, columns=headings)
                delete_buffer = []
                df.to_sql("earningst", con=engine, if_exists="append", index=False)
                print("     " + str(count) + "   " + str(datetime.datetime.now()))
                count += 1
                c=0
            c += 1
            if l[0] == '<':
                delete_buffer.append( l[1:].split(","))
        df = pd.DataFrame(delete_buffer, columns=headings)
        df.to_sql("earningst", con=engine, if_exists="append", index=False)
        print("     " + str(count) + "   " + str(datetime.datetime.now()))
        count += 1
        t = text("""
        delete
    from earnings
    where id in (select ea.id
                     from earnings ea
                          left join earningst te
                             on te.RSI_NO = ea.RSI_NO
                                 and te.CON_YEAR = ea.CON_YEAR
                                 and (te.PAYMENT_LINE_COUNT = ea.PAYMENT_LINE_COUNT OR (te.PAYMENT_LINE_COUNT is null and ea.PAYMENT_LINE_COUNT is null))
                                 and (te.CONS_SOURCE_CODE = ea.CONS_SOURCE_CODE OR (te.CONS_SOURCE_CODE is null and ea.CONS_SOURCE_CODE is null))
                                 and (te.CONS_SOURCE_SECTION_CODE = ea.CONS_SOURCE_SECTION_CODE OR (te.CONS_SOURCE_SECTION_CODE is null and ea.CONS_SOURCE_SECTION_CODE is null))
                                 and (te.NO_OF_CONS = ea.NO_OF_CONS OR (te.NO_OF_CONS is null and ea.NO_OF_CONS is null))
                                 and (te.CONS_CLASS_CODE = ea.CONS_CLASS_CODE OR (te.CONS_CLASS_CODE is null and ea.CONS_CLASS_CODE is null))
                                 and (te.CONS_FROM_DATE = ea.CONS_FROM_DATE OR (te.CONS_FROM_DATE is null and ea.CONS_FROM_DATE is null))
                                 and (te.CONS_TO_DATE = ea.CONS_TO_DATE OR (te.CONS_TO_DATE is null and ea.CONS_TO_DATE is null))
                                 and (te.EARNINGS_AMT = ea.EARNINGS_AMT OR (te.EARNINGS_AMT is null and ea.EARNINGS_AMT is null))
                                 and (te.TOT_PRSI_AMT = ea.TOT_PRSI_AMT OR (te.TOT_PRSI_AMT is null and ea.TOT_PRSI_AMT is null))
                                 and (te.EMPLOYER_NO = ea.EMPLOYER_NO OR (te.EMPLOYER_NO is null and ea.EMPLOYER_NO is null))
                                 and (te.EMPLT_COUNT = ea.EMPLT_COUNT OR (te.EMPLT_COUNT is null and ea.EMPLT_COUNT is null))
                                 and (te.EMPLT_NO = ea.EMPLT_NO OR (te.EMPLT_NO is null and ea.EMPLT_NO is null))
                                 and (te.EMPLOYEE_PRSI_AMT = ea.EMPLOYEE_PRSI_AMT OR (te.EMPLOYEE_PRSI_AMT is null and ea.EMPLOYEE_PRSI_AMT is null))
                                 and (te.EMPLT_SCH_ID_NO = ea.EMPLT_SCH_ID_NO OR (te.EMPLT_SCH_ID_NO is null and ea.EMPLT_SCH_ID_NO is null))
                                 and (te.EMPLT_SCH_FROM_DATE = ea.EMPLT_SCH_FROM_DATE OR (te.EMPLT_SCH_FROM_DATE is null and ea.EMPLT_SCH_FROM_DATE is null))
                                 and (te.NON_CONSOLIDATABLE_IND = ea.NON_CONSOLIDATABLE_IND OR (te.NON_CONSOLIDATABLE_IND is null and ea.NON_CONSOLIDATABLE_IND is null))
                                 and (te.PAY_ERR_IND = ea.PAY_ERR_IND OR (te.PAY_ERR_IND is null and ea.PAY_ERR_IND is null))
                                 and (te.PRSI_ERR_IND = ea.PRSI_ERR_IND OR (te.PRSI_ERR_IND is null and ea.PRSI_ERR_IND is null))
                                 and (te.WIES_ERR_IND = ea.WIES_ERR_IND OR (te.WIES_ERR_IND is null and ea.WIES_ERR_IND is null))
                                 and (te.CLASS_ERR_IND = ea.CLASS_ERR_IND OR (te.CLASS_ERR_IND is null and ea.CLASS_ERR_IND is null))
                                 and (te.PRSI_REFUND_IND = ea.PRSI_REFUND_IND OR (te.PRSI_REFUND_IND is null and ea.PRSI_REFUND_IND is null))
                                 and (te.CANCELLED_IND = ea.CANCELLED_IND OR (te.CANCELLED_IND is null and ea.CANCELLED_IND is null))
                                 and (te.CRS_SEGMENT = ea.CRS_SEGMENT OR (te.CRS_SEGMENT is null and ea.CRS_SEGMENT is null))
                                 and (te.LA_DATE_TIME = ea.LA_DATE_TIME OR (te.LA_DATE_TIME is null and ea.LA_DATE_TIME is null))
                                 and (te.RECORD_VERS_NO = ea.RECORD_VERS_NO OR (te.RECORD_VERS_NO is null and ea.RECORD_VERS_NO is null))
                                 and (te.USER_ID_CODE = ea.USER_ID_CODE OR (te.USER_ID_CODE is null and ea.USER_ID_CODE is null))
                                 and (te.PROGRAM_ID_CODE = ea.PROGRAM_ID_CODE OR (te.PROGRAM_ID_CODE is null and ea.PROGRAM_ID_CODE is null))
                         where  te.RSI_NO is not null)

        """)
    conn.execute(t)
    t = text("drop table earningst")
    conn.execute(t)
    print("   end " + str(datetime.datetime.now()))


print('Creating new text')
creatNewEarn('d:\\data\\con_year_payment_line.sas7bdat', 'd:\\data_test\\load\\output.txt')

print('Sorting new text')
merge_sort('d:\\data_test\\load\\output.txt','d:\\data_test\\load\\output-sort.txt', 'd:\\data_test\\load' )
for root, dirs, files in os.walk("d:\\data_test\\load", topdown=False):
    for name in files:
        if 'temp' in name:
            os.remove(os.path.join(root, name))
os.remove('d:\\data_test\\load\\output.txt')

print( 'create load diff file')
createDiff( 'd:\\data_test\\load\\earn_current.txt', 'd:\\data_test\\load\\output-sort.txt', 'd:\\data_test\\load\\diff.txt' )

print( 'making inserts')
insertData('d:\\data_test', 'd:\\data_test\\load\\diff.txt')

print( 'making deletes')
deleteData('d:\\data_test', 'd:\\data_test\\load\\diff.txt')
os.remove('d:\\data_test\\load\\diff.txt')
dest = shutil.move('d:\\data_test\\load\\output-sort.txt', 'd:\\data_test\\load\\earn_current.txt')
