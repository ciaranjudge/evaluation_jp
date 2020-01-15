from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd


# engine = create_engine('sqlite:///\\\\cskma0294\\F\\Evaluations\data\\jobpath.db', echo = False)
engine = create_engine('sqlite:///d:\\data\\jp_pl.db', echo = False)
conn = engine.connect()

# sql_DF = pd.read_sql(" select *                                            "+
#                      "    from ists_claims c                               "+
#                      "    join ists_personal p                             "+
#                      "       on c.personal_id=p.id                         "+
#                      "    where lr_date = '2020-01-03' and occupation=10   ",
#                      con=engine)
# print(sql_DF.head(3))

# t = text("""CREATE TABLE load_ists (
#                 id         INTEGER PRIMARY KEY AUTOINCREMENT,
#                 file_date  DATETIME,
#                 mod_date   DATETIME,
#                 load_time  DATETIME
#             )"""
# )
# conn.execute(t)


# t = text("""CREATE TABLE load_earnings (
#                 id         INTEGER PRIMARY KEY AUTOINCREMENT,
#                 file       TEXT,
#                 mod_date   DATETIME,
#                 load_time  DATETIME
#             )"""
# )
# conn.execute(t)
#
#
# t = text("""CREATE TABLE load_payments (
#                 id         INTEGER PRIMARY KEY AUTOINCREMENT,
#                 file       TEXT,
#                 mod_date   DATETIME,
#                 load_time  DATETIME
#             )"""
# )
# conn.execute(t)
#
#
# t = text("""CREATE TABLE load_penalties (
#                 id         INTEGER PRIMARY KEY AUTOINCREMENT,
#                 file       TEXT,
#                 mod_date   DATETIME,
#                 load_time  DATETIME
#             )"""
# )
# conn.execute(t)
#
#
# t = text("""CREATE TABLE load_les (
#                 id         INTEGER PRIMARY KEY AUTOINCREMENT,
#                 file       TEXT,
#                 mod_date   DATETIME,
#                 load_time  DATETIME
#             )"""
# )
# conn.execute(t)
#
#
# t = text("""CREATE TABLE load_jobpath (
#                 id         INTEGER PRIMARY KEY AUTOINCREMENT,
#                 file       TEXT,
#                 mod_date   DATETIME,
#                 load_time  DATETIME
#             )"""
# )
# conn.execute(t)

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
#         """)
# conn.execute(t)

# t = text("""
# CREATE TABLE payments (
#     id          INTEGER PRIMARY KEY AUTOINCREMENT,
#     ppsn        TEXT,
#     Quarter     FLOAT,
#     SCHEME_TYPE TEXT,
#     AMOUNT      FLOAT,
#     QTR         TEXT,
#     count       FLOAT
# )
#         """)
# conn.execute(t)


# t = text("""CREATE TABLE ists_personal (
#                 id             INTEGER PRIMARY KEY AUTOINCREMENT,
#                 date_of_birth  DATETIME,
#                 sex            TEXT,
#                 nat_code       TEXT,
#                 occupation     TEXT,
#                 ppsn           TEXT,
#                 related_rsi_no TEXT
#             )"""
# )
# conn.execute(t)
#
#

# t= text("""
# CREATE TABLE les (
#     id           INTEGER PRIMARY KEY AUTOINCREMENT,
#     client_group TEXT,
#     ppsn         TEXT,
#     start_date   DATETIME,
#     start_week   DATETIME
# )
# """)
# conn.execute(t)



t= text("""
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







# t = text("""CREATE TABLE ists_claims (
#                     id             INTEGER PRIMARY KEY AUTOINCREMENT,
#                     lr_code                TEXT,
#                     lr_flag                BIGINT,
#                     lls_code               TEXT,
#                     clm_reg_date           DATETIME,
#                     clm_comm_date          DATETIME,
#                     location               TEXT,
#                     CLM_STATUS             TEXT,
#                     CLM_SUSP_DTL_REAS_CODE TEXT,
#                     CDAS                   FLOAT,
#                     ada_code               TEXT,
#                     JobPath_Flag           BIGINT,
#                     JobPathHold            BIGINT,
#                     PERS_RATE              FLOAT,
#                     ADA_AMT                FLOAT,
#                     CDA_AMT                FLOAT,
#                     MEANS                  FLOAT,
#                     EMEANS                 FLOAT,
#                     NEMEANS                FLOAT,
#                     NET_FLAT               FLOAT,
#                     FUEL                   FLOAT,
#                     RRA                    FLOAT,
#                     WEEKLY_RATE            FLOAT,
#                     Recip_flag             BIGINT,
#                     lr_date                DATE,
#                     personal_id            INTEGER
#                 )"""
#           )
# conn.execute(t)


