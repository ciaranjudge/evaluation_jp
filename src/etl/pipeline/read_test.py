import pandas as pd
import pyreadstat
import datetime
import csv
import sqlalchemy as sa
from sqlalchemy import text
from sas7bdat import SAS7BDAT



def load_files(files):
    for k in files.keys():
        files[k].close()
    for k in files.keys():
        df = pd.read_csv("d:\\data\\earnings_" + str(k) + ".txt", dtype=types)
        df.to_sql("earnings_" + str(k) + "_tmp", con=engine, if_exists="replace")
        t = text("""
    CREATE INDEX idx_earnt_""" + str(k) + """_ppsn ON earnings_""" + str(k) + """_tmp (
        RSI_NO
    )
    """)
        conn.execute(t)
        t = text("""
    CREATE INDEX idx_earnt_""" + str(k) + """_con_y ON earnings_""" + str(k) + """_tmp (
       CON_YEAR
    )
    """)
        conn.execute(t)
        t = text("""
    CREATE INDEX idx_earnt_""" + str(k) + """_ppsn_con ON earnings_""" + str(k) + """_tmp (
        RSI_NO,
        CON_YEAR
    )
    """)
        conn.execute(t)
        t = text("""
insert into earnings (RSI_NO,CON_YEAR,PAYMENT_LINE_COUNT,CONS_SOURCE_CODE,CONS_SOURCE_SECTION_CODE,NO_OF_CONS,CONS_CLASS_CODE,CONS_FROM_DATE,
                      CONS_TO_DATE,EARNINGS_AMT,TOT_PRSI_AMT,EMPLOYER_NO,EMPLT_COUNT,EMPLT_NO,EMPLOYEE_PRSI_AMT,EMPLT_SCH_ID_NO,EMPLT_SCH_FROM_DATE,
                      NON_CONSOLIDATABLE_IND,PAY_ERR_IND,PRSI_ERR_IND,WIES_ERR_IND,CLASS_ERR_IND,PRSI_REFUND_IND,CANCELLED_IND,CRS_SEGMENT,
                      LA_DATE_TIME,RECORD_VERS_NO,USER_ID_CODE,PROGRAM_ID_CODE)
select te.RSI_NO, te.CON_YEAR, te.PAYMENT_LINE_COUNT, te.CONS_SOURCE_CODE, te.CONS_SOURCE_SECTION_CODE, te.NO_OF_CONS, te.CONS_CLASS_CODE,
       te.CONS_FROM_DATE, te.CONS_TO_DATE, te.EARNINGS_AMT, te.TOT_PRSI_AMT, te.EMPLOYER_NO, te.EMPLT_COUNT, te.EMPLT_NO, te.EMPLOYEE_PRSI_AMT,
       te.EMPLT_SCH_ID_NO, te.EMPLT_SCH_FROM_DATE, te.NON_CONSOLIDATABLE_IND, te.PAY_ERR_IND, te.PRSI_ERR_IND, te. WIES_ERR_IND, te.CLASS_ERR_IND,
       te.PRSI_REFUND_IND, te.CANCELLED_IND, te.CRS_SEGMENT, te.LA_DATE_TIME, te.RECORD_VERS_NO, te.USER_ID_CODE, te.PROGRAM_ID_CODE
    from earnings_""" + str(k) + """_tmp te
    left join earnings ea 
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
    where ea.rsi_no is null
    """)
        conn.execute(t)
        t = text("""
delete
    from earnings
    where id in (select ea.id
                     from earnings ea
                          left join earnings_""" + str(k) + """_tmp te
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
                         where ea.CON_YEAR="""  + str(k) + """ and te.RSI_NO is null
                         )
                         """)
        conn.execute(t)
        t = text("""
        drop table earnings_""" + str(k) + """_tmp
        """)
        conn.execute(t)
    files = {}
    return files


engine = sa.create_engine("sqlite:///d:\\data\\read_test.db" )
conn = engine.connect()

# t = text("""
# CREATE TABLE earnings (
#     id                       INTEGER PRIMARY KEY AUTOINCREMENT,
#     RSI_NO                   TEXT,
#     CON_YEAR                 FLOAT,
#     PAYMENT_LINE_COUNT       FLOAT,
#     CONS_SOURCE_CODE         TEXT,
#     CONS_SOURCE_SECTION_CODE TEXT,
#     NO_OF_CONS               FLOAT,
#     CONS_CLASS_CODE          TEXT,
#     CONS_FROM_DATE           FLOAT,
#     CONS_TO_DATE             FLOAT,
#     EARNINGS_AMT             FLOAT,
#     TOT_PRSI_AMT             FLOAT,
#     EMPLOYER_NO              TEXT,
#     EMPLT_COUNT              FLOAT,
#     EMPLT_NO                 TEXT,
#     EMPLOYEE_PRSI_AMT        FLOAT,
#     EMPLT_SCH_ID_NO          TEXT,
#     EMPLT_SCH_FROM_DATE      FLOAT,
#     NON_CONSOLIDATABLE_IND   TEXT,
#     PAY_ERR_IND              TEXT,
#     PRSI_ERR_IND             TEXT,
#     WIES_ERR_IND             TEXT,
#     CLASS_ERR_IND            TEXT,
#     PRSI_REFUND_IND          TEXT,
#     CANCELLED_IND            TEXT,
#     CRS_SEGMENT              FLOAT,
#     LA_DATE_TIME             FLOAT,
#     RECORD_VERS_NO           FLOAT,
#     USER_ID_CODE             FLOAT,
#     PROGRAM_ID_CODE          TEXT
# )
# """)
# conn.execute(t)
# t = text("""
# CREATE INDEX idx_earn_ppsn ON earnings (
#     RSI_NO
# )
# """)
# conn.execute(t)
# t = text("""
# CREATE INDEX idx_earnt_con_y ON earnings (
#    CON_YEAR
# )
# """)
# conn.execute(t)
# t = text("""
# CREATE INDEX idx_earnt_ppsn_con ON earnings (
#     RSI_NO,
#     CON_YEAR
# )
# """)
# conn.execute(t)


#
# headings = ['RSI_NO','CON_YEAR','PAYMENT_LINE_COUNT','CONS_SOURCE_CODE','CONS_SOURCE_SECTION_CODE','NO_OF_CONS','CONS_CLASS_CODE','CONS_FROM_DATE','CONS_TO_DATE','EARNINGS_AMT','TOT_PRSI_AMT','EMPLOYER_NO','EMPLT_COUNT','EMPLT_NO','EMPLOYEE_PRSI_AMT','EMPLT_SCH_ID_NO','EMPLT_SCH_FROM_DATE','NON_CONSOLIDATABLE_IND','PAY_ERR_IND','PRSI_ERR_IND','WIES_ERR_IND','CLASS_ERR_IND','PRSI_REFUND_IND','CANCELLED_IND','CRS_SEGMENT','LA_DATE_TIME','RECORD_VERS_NO','USER_ID_CODE','PROGRAM_ID_CODE']
# types= {'RSI_NO': 'str',        'CON_YEAR':'float',
#         'PAYMENT_LINE_COUNT':'float',       'CONS_SOURCE_CODE': 'str',       'CONS_SOURCE_SECTION_CODE': 'str',
#         'NO_OF_CONS':'float',       'CONS_CLASS_CODE':'str',       'CONS_FROM_DATE':'float',
#         'CONS_TO_DATE':'float',       'EARNINGS_AMT':'float',       'TOT_PRSI_AMT':'float',
#         'EMPLOYER_NO': 'str',       'EMPLT_COUNT':'float',       'EMPLT_NO': 'str',
#         'EMPLOYEE_PRSI_AMT':'float',       'EMPLT_SCH_ID_NO': 'str',       'EMPLT_SCH_FROM_DATE':'float',
#         'NON_CONSOLIDATABLE_IND': 'str',       'PAY_ERR_IND': 'str',       'PRSI_ERR_IND': 'str',
#         'WIES_ERR_IND': 'str',       'CLASS_ERR_IND': 'str',       'PRSI_REFUND_IND': 'str',
#         'CANCELLED_IND': 'str',       'CRS_SEGMENT':'float',       'LA_DATE_TIME':'float',
#         'RECORD_VERS_NO':'float',       'USER_ID_CODE':'float',       'PROGRAM_ID_CODE': 'str'}
#
# print(" start " + str(datetime.datetime.now()))
# c=0
# count=0
# files={}
# with SAS7BDAT('d:\\data\\con_year_payment_line.sas7bdat', skip_header=True) as reader:
#     for row in reader:
#         if not( int(row[1]) in files.keys()):
#             files[int(row[1])] = open("d:\\data\\earnings_"+str(int(row[1]))+".txt", "wt")
#             files[int(row[1])].write(','.join(map(str, headings)) + "\n")
#         files[int(row[1])].write(','.join(map(str ,row))+"\n")
#         c += 1
#         if c >= 1000000:
#             count += 1
#             c = 0;
#             print("     " + str(count) + "   "+ str(datetime.datetime.now()))
#             # if count >= 3:
#             #     break
#     print("     " + str(count) + "   " + str(datetime.datetime.now()))
#     files = load_files(files)
#     print( "   end " + str(datetime.datetime.now()))
#



print(" start " + str(datetime.datetime.now()))
c=0
count=0


result_file = open("output.csv",'wb')
wr = csv.writer(result_file, dialect='excel')
with SAS7BDAT('d:\\data\\con_year_payment_line.sas7bdat', skip_header=True) as reader:
    for row in reader:
            fout.
        c += 1
        if c >= 1000000:
            count += 1
            c = 0;
            print("     " + str(count) + "   "+ str(datetime.datetime.now()))
    print("     " + str(count) + "   " + str(datetime.datetime.now()))
    print( "   end " + str(datetime.datetime.now()))



