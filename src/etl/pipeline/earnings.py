from abc import ABCMeta, abstractmethod

import pandas as pd
import futil
import data_file
import datetime
import os
import sqlalchemy as sa
from sqlalchemy import text
from sas7bdat import SAS7BDAT
import data_file




class Earnings_file(data_file.Data_file):

    def __init__(self, settings):
        self.db = settings['db']
        self.filename = settings['earnings']['file']

    def read(self):
        engine = sa.create_engine("sqlite:///" + self.db)
        conn = engine.connect()
        print('----  begin >' + str(datetime.datetime.now()))


        count = 0
        print(str(count) + " " + str(datetime.datetime.now()))
        c = 0
        l = []
        with SAS7BDAT(self.filename, skip_header=True) as reader:
            for row in reader:
                l.append(row)
                c += 1
                if c > 2500000:
                    c = 0
                    df = pd.DataFrame(l, columns=['RSI_NO','CON_YEAR','PAYMENT_LINE_COUNT','CONS_SOURCE_CODE','CONS_SOURCE_SECTION_CODE','NO_OF_CONS','CONS_CLASS_CODE','CONS_FROM_DATE','CONS_TO_DATE','EARNINGS_AMT','TOT_PRSI_AMT','EMPLOYER_NO','EMPLT_COUNT','EMPLT_NO','EMPLOYEE_PRSI_AMT','EMPLT_SCH_ID_NO','EMPLT_SCH_FROM_DATE','NON_CONSOLIDATABLE_IND','PAY_ERR_IND','PRSI_ERR_IND','WIES_ERR_IND','CLASS_ERR_IND','PRSI_REFUND_IND','CANCELLED_IND','CRS_SEGMENT','LA_DATE_TIME','RECORD_VERS_NO','USER_ID_CODE','PROGRAM_ID_CODE'])
                    df.to_sql("earnings_" + str(count) + "_tmp", con=engine, if_exists="replace")
                    l = []
                    count += 1
                    print(str(count) + " " + str(datetime.datetime.now()))
                    if count >= 1:
                        break
            df = pd.DataFrame(l, columns=['RSI_NO','CON_YEAR','PAYMENT_LINE_COUNT','CONS_SOURCE_CODE','CONS_SOURCE_SECTION_CODE','NO_OF_CONS','CONS_CLASS_CODE','CONS_FROM_DATE','CONS_TO_DATE','EARNINGS_AMT','TOT_PRSI_AMT','EMPLOYER_NO','EMPLT_COUNT','EMPLT_NO','EMPLOYEE_PRSI_AMT','EMPLT_SCH_ID_NO','EMPLT_SCH_FROM_DATE','NON_CONSOLIDATABLE_IND','PAY_ERR_IND','PRSI_ERR_IND','WIES_ERR_IND','CLASS_ERR_IND','PRSI_REFUND_IND','CANCELLED_IND','CRS_SEGMENT','LA_DATE_TIME','RECORD_VERS_NO','USER_ID_CODE','PROGRAM_ID_CODE'])
            df.to_sql("earnings_" + str(count) + "_tmp", con=engine, if_exists="replace")
            count += 1
            print(str(count) + " " + str(datetime.datetime.now()))

        print('-----  read >' + str(datetime.datetime.now()))
        t = text("""
CREATE TABLE earnings_tmp (
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
        for n in range(0, count):
            print(str(n) + "    > " + str(datetime.datetime.now()))
            t = text( """
insert into earnings_tmp (RSI_NO, CON_YEAR, PAYMENT_LINE_COUNT, CONS_SOURCE_CODE, CONS_SOURCE_SECTION_CODE, NO_OF_CONS,CONS_CLASS_CODE, CONS_FROM_DATE, CONS_TO_DATE, EARNINGS_AMT, TOT_PRSI_AMT, EMPLOYER_NO,
EMPLT_COUNT, EMPLT_NO, EMPLOYEE_PRSI_AMT, EMPLT_SCH_ID_NO, EMPLT_SCH_FROM_DATE, NON_CONSOLIDATABLE_IND,PAY_ERR_IND, PRSI_ERR_IND, WIES_ERR_IND, CLASS_ERR_IND, PRSI_REFUND_IND, CANCELLED_IND, CRS_SEGMENT,
LA_DATE_TIME, RECORD_VERS_NO, USER_ID_CODE, PROGRAM_ID_CODE)
select RSI_NO, CON_YEAR, PAYMENT_LINE_COUNT, CONS_SOURCE_CODE, CONS_SOURCE_SECTION_CODE, NO_OF_CONS,CONS_CLASS_CODE, CONS_FROM_DATE, CONS_TO_DATE, EARNINGS_AMT, TOT_PRSI_AMT, EMPLOYER_NO,
EMPLT_COUNT, EMPLT_NO, EMPLOYEE_PRSI_AMT, EMPLT_SCH_ID_NO, EMPLT_SCH_FROM_DATE, NON_CONSOLIDATABLE_IND,PAY_ERR_IND, PRSI_ERR_IND, WIES_ERR_IND, CLASS_ERR_IND, PRSI_REFUND_IND, CANCELLED_IND, CRS_SEGMENT,
LA_DATE_TIME, RECORD_VERS_NO, USER_ID_CODE, PROGRAM_ID_CODE
from earnings_"""+str(n)+"""_tmp
""")
            conn.execute(t)
            t = text( "drop table earnings_"+str(n)+"_tmp")
            conn.execute(t)
            print(str(n) + "    > " + str(datetime.datetime.now()))
        print('----  merge >' + str(datetime.datetime.now()))
        t = text( """
insert into earnings (RSI_NO,CON_YEAR,PAYMENT_LINE_COUNT,CONS_SOURCE_CODE,CONS_SOURCE_SECTION_CODE,NO_OF_CONS,CONS_CLASS_CODE,CONS_FROM_DATE,
                      CONS_TO_DATE,EARNINGS_AMT,TOT_PRSI_AMT,EMPLOYER_NO,EMPLT_COUNT,EMPLT_NO,EMPLOYEE_PRSI_AMT,EMPLT_SCH_ID_NO,EMPLT_SCH_FROM_DATE,
                      NON_CONSOLIDATABLE_IND,PAY_ERR_IND,PRSI_ERR_IND,WIES_ERR_IND,CLASS_ERR_IND,PRSI_REFUND_IND,CANCELLED_IND,CRS_SEGMENT,
                      LA_DATE_TIME,RECORD_VERS_NO,USER_ID_CODE,PROGRAM_ID_CODE)
select te.RSI_NO, te.CON_YEAR, te.PAYMENT_LINE_COUNT, te.CONS_SOURCE_CODE, te.CONS_SOURCE_SECTION_CODE, te.NO_OF_CONS, te.CONS_CLASS_CODE,
       te.CONS_FROM_DATE, te.CONS_TO_DATE, te.EARNINGS_AMT, te.TOT_PRSI_AMT, te.EMPLOYER_NO, te.EMPLT_COUNT, te.EMPLT_NO, te.EMPLOYEE_PRSI_AMT,
       te.EMPLT_SCH_ID_NO, te.EMPLT_SCH_FROM_DATE, te.NON_CONSOLIDATABLE_IND, te.PAY_ERR_IND, te.PRSI_ERR_IND, te. WIES_ERR_IND, te.CLASS_ERR_IND,
       te.PRSI_REFUND_IND, te.CANCELLED_IND, te.CRS_SEGMENT, te.LA_DATE_TIME, te.RECORD_VERS_NO, te.USER_ID_CODE, te.PROGRAM_ID_CODE
    from earnings_tmp te
    left join earnings ea 
        on te.RSI_NO = ea.RSI_NO
            and te.CON_YEAR = ea.CON_YEAR
            and te.PAYMENT_LINE_COUNT = ea.PAYMENT_LINE_COUNT
            and te.CONS_SOURCE_CODE = ea.CONS_SOURCE_CODE
            and te.CONS_SOURCE_SECTION_CODE = ea.CONS_SOURCE_SECTION_CODE
            and te.NO_OF_CONS = ea.NO_OF_CONS
            and te.CONS_CLASS_CODE = ea.CONS_CLASS_CODE
            and te.CONS_FROM_DATE = ea.CONS_FROM_DATE
            and te.CONS_TO_DATE = ea.CONS_TO_DATE
            and te.EARNINGS_AMT = ea.EARNINGS_AMT
            and te.TOT_PRSI_AMT = ea.TOT_PRSI_AMT
            and te.EMPLOYER_NO = ea.EMPLOYER_NO
            and te.EMPLT_COUNT = ea.EMPLT_COUNT
            and te.EMPLT_NO = ea.EMPLT_NO
            and te.EMPLOYEE_PRSI_AMT = ea.EMPLOYEE_PRSI_AMT
            and te.EMPLT_SCH_ID_NO = ea.EMPLT_SCH_ID_NO
            and te.EMPLT_SCH_FROM_DATE = ea.EMPLT_SCH_FROM_DATE
            and te.NON_CONSOLIDATABLE_IND = ea.NON_CONSOLIDATABLE_IND
            and te.PAY_ERR_IND = ea.PAY_ERR_IND
            and te.PRSI_ERR_IND = ea.PRSI_ERR_IND
            and te.WIES_ERR_IND = ea.WIES_ERR_IND
            and te.CLASS_ERR_IND = ea.CLASS_ERR_IND
            and te.PRSI_REFUND_IND = ea.PRSI_REFUND_IND
            and te.CANCELLED_IND = ea.CANCELLED_IND
            and te.CRS_SEGMENT = ea.CRS_SEGMENT
            and te.LA_DATE_TIME = ea.LA_DATE_TIME
            and te.RECORD_VERS_NO = ea.RECORD_VERS_NO
            and te.USER_ID_CODE = ea.USER_ID_CODE
            and te.PROGRAM_ID_CODE = ea.PROGRAM_ID_CODE
    where ea.rsi_no is null
    """)
        conn.execute(t)
        print('------  add >' + str(datetime.datetime.now()))
        t = text("""
delete 
    from earnings 
    where id in (select ea.id
                     from earnings ea
                         left join earnings_tmp te 
                             on te.RSI_NO = ea.RSI_NO
                                 and te.CON_YEAR = ea.CON_YEAR
                                 and te.PAYMENT_LINE_COUNT = ea.PAYMENT_LINE_COUNT
                                 and te.CONS_SOURCE_CODE = ea.CONS_SOURCE_CODE
                                 and te.CONS_SOURCE_SECTION_CODE = ea.CONS_SOURCE_SECTION_CODE
                                 and te.NO_OF_CONS = ea.NO_OF_CONS
                                 and te.CONS_CLASS_CODE = ea.CONS_CLASS_CODE
                                 and te.CONS_FROM_DATE = ea.CONS_FROM_DATE
                                 and te.CONS_TO_DATE = ea.CONS_TO_DATE
                                 and te.EARNINGS_AMT = ea.EARNINGS_AMT
                                 and te.TOT_PRSI_AMT = ea.TOT_PRSI_AMT
                                 and te.EMPLOYER_NO = ea.EMPLOYER_NO
                                 and te.EMPLT_COUNT = ea.EMPLT_COUNT
                                 and te.EMPLT_NO = ea.EMPLT_NO
                                 and te.EMPLOYEE_PRSI_AMT = ea.EMPLOYEE_PRSI_AMT
                                 and te.EMPLT_SCH_ID_NO = ea.EMPLT_SCH_ID_NO
                                 and te.EMPLT_SCH_FROM_DATE = ea.EMPLT_SCH_FROM_DATE
                                 and te.NON_CONSOLIDATABLE_IND = ea.NON_CONSOLIDATABLE_IND
                                 and te.PAY_ERR_IND = ea.PAY_ERR_IND
                                 and te.PRSI_ERR_IND = ea.PRSI_ERR_IND
                                 and te.WIES_ERR_IND = ea.WIES_ERR_IND
                                 and te.CLASS_ERR_IND = ea.CLASS_ERR_IND
                                 and te.PRSI_REFUND_IND = ea.PRSI_REFUND_IND
                                 and te.CANCELLED_IND = ea.CANCELLED_IND
                                 and te.CRS_SEGMENT = ea.CRS_SEGMENT
                                 and te.LA_DATE_TIME = ea.LA_DATE_TIME
                                 and te.RECORD_VERS_NO = ea.RECORD_VERS_NO
                                 and te.USER_ID_CODE = ea.USER_ID_CODE
                                 and te.PROGRAM_ID_CODE = ea.PROGRAM_ID_CODE
                         where te.rsi_no is null)      """)
        conn.execute(t)
        print('------  del >' + str(datetime.datetime.now()))
        t = text("drop table earnings_tmp")
        conn.execute(t)
        print('-----  drop >' + str(datetime.datetime.now()))


if __name__ == '__main__':
    earn = Earnings_file('d:\\data\\jp_pl.db', 'd:\\data\\con_year_payment_line.sas7bdat')
    # print( earn.processed() )
    # earn.do_process()
    # print( earn.processed() )
    earn.read()
