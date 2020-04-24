import luigi
from datetime import date
import datetime
import ists as ists
import jp as jp
import earnings as earnings
import paymentsOld as paymentsOld
import payments as payments
import les as les
import penalties as penalties
import plss as plss
import pandas as pd
import json
import glob
import os
import sqlalchemy as sa
from sqlalchemy import text

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


class Plss_load(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        return []

    def complete(self):
        self.plss = plss.Plss_file(self.settings)
        return self.plss.processed()

    def run(self):
        self.plss.do_process()


class Payold_load(luigi.Task):
    filename = luigi.Parameter()
    settings = luigi.Parameter()

    def requires(self):
        return []

    def complete(self):
        self.pay_old = pay_old.PayOld_file(self.filename,self.settings)
        return self.pay_old.processed()

    def run(self):
        self.pay_old.do_process()


class Payments_load(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        yield payments.Payments_diff(self.settings)

    def complete(self):
        self.payments = payments.Payments_file(self.settings)
        return self.payments.processed()

    def run(self):
        self.payments.do_process()


class PaymentsOld_load(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        return []
        # yield paymentsOld.PaymentsOld_diff(self.settings)

    def complete(self):
        self.paymentsOld = paymentsOld.PaymentsOld_file(self.settings)
        return self.paymentsOld.processed()

    def run(self):
        self.paymentsOld.do_process()
        # pass


class Earnings_load(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        yield earnings.Earnings_diff(self.settings)

    def complete(self):
        self.earnings = earnings.Earnings_file(self.settings)
        return self.earnings.processed()

    def run(self):
        self.earnings.do_process()


class Jobpath_load(luigi.Task):
    filename = luigi.Parameter()
    settings = luigi.Parameter()

    def requires(self):
        []

    def complete(self):
        self.jp = jp.Jp_file(self.filename,self.settings)
        return self.jp.processed()

    def run(self):
        self.jp.do_process()


class WWLDEtl(luigi.Task):
    config = luigi.Parameter()
    task_complete = False;

    def create_db(self,db):
        engine = sa.create_engine(db, echo=False)
        conn = engine.connect()
        try:
            t = text("""CREATE TABLE load_file (
                     id         INTEGER PRIMARY KEY AUTOINCREMENT,
                     file       TEXT,
                     mod_date   DATETIME,
                     load_time  DATETIME)
                     """)
            conn.execute(t)
        except:
            print("table 'load_file' already exists")

        self.create_plss_sql(conn)
        self.create_pay_old_sql(conn)
        self.create_jobpath_sql(conn)
        self.create_earnings_sql(conn)
        self.create_payments_sql(conn)
        self.create_les_sql(conn)
        self.create_penalties_sql(conn)
        self.create_ists_sql(conn)

    def create_plss_sql(self, conn):
        try:
            t = text("""
                        CREATE TABLE plss (
                            id                               INTEGER PRIMARY KEY AUTOINCREMENT,
                            CourseParticipationId            BIGINT,
                            CourseCalendarId                 BIGINT,
                            ApplicationId                    BIGINT,
                            DateStartCourse                  FLOAT,
                            DateActualFinishCourse           TEXT,
                            OutcomeId                        FLOAT,
                            DateDeleted                      TEXT,
                            OutcomeStatusId                  FLOAT,
                            OutcomeCertificationId           FLOAT,
                            OutcomeCertificationAwardId      FLOAT,
                            OutcomeEarlyFinishReasonId       TEXT,
                            OriginalOutcomeId                TEXT,
                            ApplicationOriginId              BIGINT,
                            ApplicationStatusId              BIGINT,
                            FETProviderId                    BIGINT,
                            DeliveryFETProviderId            BIGINT,
                            ProgrammeId                      BIGINT,
                            TargetAwardId                    TEXT,
                            PublishedCourseTitle             TEXT,
                            ProgrammeCategoryId              BIGINT,
                            DateActualStart                  TEXT,
                            DateActualFinish                 TEXT,
                            DeliveryModeId                   TEXT,
                            Title                            TEXT,
                            Cluster                          TEXT,
                            ProgrammeAwardLevel              TEXT,
                            ProgrammeCertified               TEXT,
                            TargetAward                      TEXT,
                            AwardAchievable                  BIGINT,
                            AwardSummary                     TEXT,
                            ISCEDBroadUID                    BIGINT,
                            ISCEDDetailedFieldID             TEXT,
                            learnerfinishdate                DATE,
                            learnerstartyear                 FLOAT,
                            finishmonth                      FLOAT,
                            finishyear                       FLOAT,
                            Gender                           TEXT,
                            ParentFETProviderId              TEXT,
                            Parent                           TEXT,
                            ParentDivisionProviderId         TEXT,
                            FETProviderTypeId                BIGINT,
                            FETProviderName                  TEXT,
                            IsETB                            BIGINT,
                            CountyId                         BIGINT,
                            programmecategorydescription     TEXT,
                            hasemployers                     BIGINT,
                            LearnerCountyId                  TEXT,
                            NationalityId                    TEXT,
                            outcomestatusdescription         TEXT,
                            outcomecertificationdescription  TEXT,
                            isoutcomecertified               FLOAT,
                            outcomecertificationawarddescrip TEXT,
                            outcomedescription               TEXT,
                            hash_ppsn                        TEXT,
                            age                              FLOAT
                    );
                """)
            conn.execute(t)
            t = text("""
                CREATE INDEX idx_plss_ppsn ON plss (
                    hash_ppsn
                )
            """)
            conn.execute(t)
        except:
            print("table 'plss' already exists")

    def create_jobpath_sql(self, conn):
        try:
            t = text("""
                CREATE TABLE jobpath (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    cluster    BIGINT,
                    ppsn       TEXT,
                    qtr        TEXT
                )
                """)
            conn.execute(t)
            t = text("""
                CREATE INDEX idx_jp_ppsn ON jobpath (
                    ppsn
                )
            """)
            conn.execute(t)
            # t = text("""
            #     CREATE INDEX idx_jp_cluster ON jobpath (
            #         cluster
            #     )
            # """)
            # conn.execute(t)
            # t = text("""
            #     CREATE INDEX idx_jp_qtr ON jobpath (
            #         qtr
            #     )
            # """)
            # conn.execute(t)
            t = text("""
                CREATE INDEX idx_jp_pcq ON jobpath (
                    ppsn,cluster,qtr
                )
            """)
            conn.execute(t)
        except:
            print("table 'jobpath' already exists")

    def create_earnings_sql(self, conn):
        try:
            t = text("""
                        CREATE TABLE earnings (
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
                        CREATE INDEX idx_earn_ppsn ON earnings (
                            RSI_NO
                        )
            """)
            conn.execute(t)

            t = text("""
                        CREATE INDEX idx_earn_cons_class_code ON earnings (
                            CONS_CLASS_CODE
                        )            
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_earn_cons_source_code ON earnings (
                            CONS_SOURCE_CODE
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_earn_cons_year ON earnings (
                            CON_YEAR
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_earn_earnings_amt ON earnings (
                            EARNINGS_AMT
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_earn_employee_prsi_amt ON earnings (
                            EMPLOYEE_PRSI_AMT
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_earn_employer_no ON earnings (
                            EMPLOYER_NO
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_earn_no_cons ON earnings (
                            NO_OF_CONS
                        )
            """)
            t = text("""
                        CREATE INDEX idx_earn_tot_prsi_amt ON earnings (
                            TOT_PRSI_AMT
                        )            
            """)
            conn.execute(t)
        except:
            print("table 'earnings' already exists")

    def create_les_sql(self, conn):
        try:
            t = text("""
            CREATE TABLE les (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                client_group TEXT,
                ppsn         TEXT,
                start_date   DATETIME,
                start_week   DATETIME
            )
            """)
            conn.execute(t)
            t = text("""
            CREATE INDEX idx_les_ppsn ON les (
                ppsn
            )
            """)
            conn.execute(t)
            t = text("""
            CREATE INDEX idx_les_group ON les (
                client_group
            )
            """)
            conn.execute(t)
            t = text("""
            CREATE INDEX idx_les_st_d ON les (
                start_date
            )
            """)
            conn.execute(t)
            t = text("""
            CREATE INDEX idx_les_st_w ON les (
                start_week
            )            """)
            conn.execute(t)
        except:
            print("table 'les' already exists")

    def create_penalties_sql(self, conn):
        try:
            t = text("""
                        CREATE TABLE penalties (
                            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
                            ppsn                   TEXT,
                            sex                    TEXT,
                            age_penstart           FLOAT,
                            life_event_date        DATE,
                            location               TEXT,
                            loc_div                TEXT,
                            marital_status         TEXT,
                            marital_group          TEXT,
                            nat_code               TEXT,
                            nat_detail             TEXT,
                            nat_group              TEXT,
                            occ_group              TEXT,
                            occupation             FLOAT,
                            ada_code               TEXT,
                            spouse                 TEXT,
                            cda_number             FLOAT,
                            cdas                   TEXT,
                            startdate              DATE,
                            Extractdate            DATE,
                            pendur                 FLOAT,
                            duration               TEXT,
                            status                 TEXT,
                            clm_code               TEXT,
                            clm_comm_date          DATE,
                            clm_end_date           DATE,
                            rra                    TEXT,
                            LO_office              TEXT,
                            RRB                    FLOAT,
                            CLM_SUSP_DTL_REAS_CODE TEXT
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_penalties_ppsn ON penalties (
                            ppsn
                        )
            """)
            conn.execute(t)
            t = text("""
                         CREATE INDEX idx_penalties_Extractdate ON penalties (
                            Extractdate
                        )           
            """)
            conn.execute(t)
            t = text("""
                         CREATE INDEX idx_penalties_location ON penalties (
                            location
                        )           
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_penalties_startdate ON penalties (
                            startdate
                        )            
            """)
            conn.execute(t)
        except:
            print("table 'penalties' already exists")

    def create_ists_sql(self, conn):
        try:
            t = text("""
                        CREATE TABLE ists_personal (
                            id             INTEGER PRIMARY KEY AUTOINCREMENT,
                            date_of_birth  DATETIME,
                            sex            TEXT,
                            nat_code       TEXT,
                            occupation     TEXT,
                            ppsn           TEXT,
                            related_rsi_no TEXT
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_ists_p_por ON ists_personal (
                            ppsn, occupation, related_rsi_no
                        )
            """)
            conn.execute(t)
        except:
            print("table 'ists_personal' already exists")
        try:
            t = text("""CREATE TABLE ists_claims (
                            id             INTEGER PRIMARY KEY AUTOINCREMENT,
                            lr_code                TEXT,
                            lr_flag                BIGINT,
                            lls_code               TEXT,
                            clm_reg_date           DATETIME,
                            clm_comm_date          DATETIME,
                            location               TEXT,
                            CLM_STATUS             TEXT,
                            CLM_SUSP_DTL_REAS_CODE TEXT,
                            CDAS                   FLOAT,
                            ada_code               TEXT,
                            JobPath_Flag           BIGINT,
                            JobPathHold            BIGINT,
                            PERS_RATE              FLOAT,
                            ADA_AMT                FLOAT,
                            CDA_AMT                FLOAT,
                            MEANS                  FLOAT,
                            EMEANS                 FLOAT,
                            NEMEANS                FLOAT,
                            NET_FLAT               FLOAT,
                            FUEL                   FLOAT,
                            RRA                    FLOAT,
                            WEEKLY_RATE            FLOAT,
                            Recip_flag             BIGINT,
                            lr_date                DATE,
                            personal_id            INTEGER,
                            foreign key (personal_id) references ists_personal(id)
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_isits_cl_pid ON ists_claims (
                            personal_id
                        )           
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_ists_cl_lr_d ON ists_claims (
                            lr_date
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_ists_c_clm_comm_date ON ists_claims (
                            clm_comm_date
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_ists_c_location ON ists_claims (
                            location
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_ists_c_lr_code ON ists_claims (
                            lr_code
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_ists_c_lr_flag ON ists_claims (
                            lr_flag
                        )           
            """)

            conn.execute(t)
        except:
            print("table 'ists_claims' already exists")

    def create_pay_old_sql(self, conn):
        try:
            t = text("""
                        CREATE TABLE pay_old (
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
                        CREATE INDEX idx_pay_old_ppsn ON pay_old (
                            CLMT_RSI_NO
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_pay_old_year ON pay_old (
                            year
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_pay_old_AMOUNT ON pay_old (
                            AMOUNT
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_pay_old_CLM_REG_DATE ON pay_old (
                            CLM_REG_DATE
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_pay_old_CLM_SCH_CODE ON pay_old (
                            CLM_SCH_CODE
                        )
            """)
            conn.execute(t)
        except:
            print("table 'pay_old' already exists")

    def create_payments_sql(self, conn):
        try:
            t = text("""
                        CREATE TABLE payments (
                            id          INTEGER PRIMARY KEY AUTOINCREMENT,
                            ppsn        TEXT,
                            Quarter     FLOAT,
                            SCHEME_TYPE TEXT,
                            AMOUNT      FLOAT,
                            QTR         TEXT,
                            count       FLOAT
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_pay_ppsn ON payments (
                            ppsn
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_payments_full1 ON payments (
                            ppsn,
                            QTR,
                            count
                        )
            """)
            conn.execute(t)
            t = text("""
                        CREATE INDEX idx_payments_Quarter ON payments (
                            Quarter
                        )
            """)
            conn.execute(t)

            t = text("""
                        CREATE INDEX idx_payments_QTR ON payments (
                            QTR
                        )
            """)
            conn.execute(t)

            t = text("""
                        CREATE INDEX idx_payments_SCHEME_TYPE ON payments (
                            SCHEME_TYPE
                        )
            """)
            conn.execute(t)

            t = text("""
                        CREATE INDEX idx_payments_AMOUNT ON payments (
                            AMOUNT
                        )
            """)
            conn.execute(t)

            t = text("""
                        CREATE INDEX idx_payments_count ON payments (
                            count
                        )
            """)
            conn.execute(t)
        except:
            print("table 'payments' already exists")

    def requires(self):
        if 'ists' in self.settings:
            ists_end = date.today()
            sundays = pd.date_range(datetime.datetime.strptime(self.settings['ists']['startDate'], '%Y-%m-%d'),
                                    ists_end, freq="W-SUN")
            for sunday in sundays:
                yield Ists_load(sunday,self.settings)
        if 'jobpath' in self.settings:
            for f in glob.glob(self.settings['jobpath']['location'] + self.settings['jobpath']['pattern']):
                yield Jobpath_load(f, self.settings)
        if 'earnings' in self.settings:
            yield Earnings_load(self.settings)
        if 'penalties' in self.settings:
            yield Penalties_load(self.settings)
        if 'les' in self.settings:
            yield Les_load(self.settings)
        if 'payments' in self.settings:
            yield Payments_load(self.settings)
        if 'pay_old' in self.settings:
            yield PaymentsOld_load(self.settings)
        if 'plss' in self.settings:
            yield Plss_load(self.settings)

    def complete(self):
        with open(self.config, 'r') as f:
            self.settings = json.load(f)
        self.create_db(self.settings['db'])
        return self.task_complete

    def run(self):
        self.task_complete = True


if __name__ == '__main__':
    luigi.run()
