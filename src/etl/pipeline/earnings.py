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
import shutil


class Earnings_file(data_file.Data_file):

    def __init__(self, settings):
        self.db = settings['db']
        self.filename = settings['earnings']['file']
        self.dir = settings['earnings']['workdir']

    def insertData(self, diff_file):
        headings = ['RSI_NO', 'CON_YEAR', 'PAYMENT_LINE_COUNT', 'CONS_SOURCE_CODE', 'CONS_SOURCE_SECTION_CODE',
                    'NO_OF_CONS', 'CONS_CLASS_CODE', 'CONS_FROM_DATE', 'CONS_TO_DATE', 'EARNINGS_AMT', 'TOT_PRSI_AMT',
                    'EMPLOYER_NO', 'EMPLT_COUNT', 'EMPLT_NO', 'EMPLOYEE_PRSI_AMT', 'EMPLT_SCH_ID_NO',
                    'EMPLT_SCH_FROM_DATE', 'NON_CONSOLIDATABLE_IND', 'PAY_ERR_IND', 'PRSI_ERR_IND', 'WIES_ERR_IND',
                    'CLASS_ERR_IND', 'PRSI_REFUND_IND', 'CANCELLED_IND', 'CRS_SEGMENT', 'LA_DATE_TIME',
                    'RECORD_VERS_NO', 'USER_ID_CODE', 'PROGRAM_ID_CODE']
        engine = sa.create_engine('sqlite:///' + self.db, echo=False)
        conn = engine.connect()
        c = 0
        count = 0
        insert_buffer = []
        print(" start " + str(datetime.datetime.now()))
        with open(diff_file, 'rt') as diffin:
            for l in diffin:
                if c >= 1000000:
                    df = pd.DataFrame(insert_buffer, columns=headings)
                    insert_buffer = []
                    df.to_sql("earnings", con=engine, if_exists="append", index=False)
                    print("     " + str(count) + "   " + str(datetime.datetime.now()))
                    count += 1
                    c = 0
                c += 1
                if l[0] == '>':
                    insert_buffer.append(l[1:].split(","))
            df = pd.DataFrame(insert_buffer, columns=headings)
            df.to_sql("earnings", con=engine, if_exists="append", index=False)
            print("     " + str(count) + "   " + str(datetime.datetime.now()))
            count += 1
        print("   end " + str(datetime.datetime.now()))

    def deleteData(self, diff_file):
        headings = ['RSI_NO', 'CON_YEAR', 'PAYMENT_LINE_COUNT', 'CONS_SOURCE_CODE', 'CONS_SOURCE_SECTION_CODE',
                    'NO_OF_CONS', 'CONS_CLASS_CODE', 'CONS_FROM_DATE', 'CONS_TO_DATE', 'EARNINGS_AMT', 'TOT_PRSI_AMT',
                    'EMPLOYER_NO', 'EMPLT_COUNT', 'EMPLT_NO', 'EMPLOYEE_PRSI_AMT', 'EMPLT_SCH_ID_NO',
                    'EMPLT_SCH_FROM_DATE', 'NON_CONSOLIDATABLE_IND', 'PAY_ERR_IND', 'PRSI_ERR_IND', 'WIES_ERR_IND',
                    'CLASS_ERR_IND', 'PRSI_REFUND_IND', 'CANCELLED_IND', 'CRS_SEGMENT', 'LA_DATE_TIME',
                    'RECORD_VERS_NO', 'USER_ID_CODE', 'PROGRAM_ID_CODE']
        engine = sa.create_engine('sqlite:///' + self.db, echo=False)
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
        c = 0
        count = 0
        delete_buffer = []
        print(" start " + str(datetime.datetime.now()))
        with open(diff_file, 'rt') as diffin:
            for l in diffin:
                if c >= 1000000:
                    df = pd.DataFrame(delete_buffer, columns=headings)
                    delete_buffer = []
                    df.to_sql("earningst", con=engine, if_exists="append", index=False)
                    print("     " + str(count) + "   " + str(datetime.datetime.now()))
                    count += 1
                    c = 0
                c += 1
                if l[0] == '<':
                    delete_buffer.append(l[1:].split(","))
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

    def read(self):
        print('making inserts')
        self.insertData(self.dir+'diff.txt')

        print('making deletes')
        self.deleteData( self.dir+'diff.txt')
        os.remove( self.dir+'diff.txt')
        shutil.move(self.dir+'output-sort.txt', self.dir+'earn_current.txt')

