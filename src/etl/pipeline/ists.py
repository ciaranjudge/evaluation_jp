from datetime import date, timedelta
import datetime
import os
import sqlalchemy as sa
from sqlalchemy import text
import pyreadstat
import futil as futil




class Ists_file:

    def __init__(self, db, location, file_date):
        self.db = db
        self.location = location
        self.file_date = file_date
        self.claim_cols = [
            "lr_code", "lr_flag", "lls_code", "clm_reg_date", "clm_comm_date",
            "location", "CLM_STATUS", "CLM_SUSP_DTL_REAS_CODE",
            "CDAS", "ada_code", "JobPath_Flag", "JobPathHold",
            "PERS_RATE", "ADA_AMT", "CDA_AMT", "MEANS", "EMEANS", "NEMEANS", "NET_FLAT",
            "FUEL", "RRA", "WEEKLY_RATE", "Recip_flag"
        ]
        self.personal_cols = [
            'date_of_birth', "sex", "nat_code", "occupation", "ppsn", "RELATED_RSI_NO"
        ]
        self.byte_cols = ["sex", "lr_code", "lls_code", "ppsn"]
        self.categorical_cols = ["sex", "lr_code", "lls_code"]
        self.to_int_cols = ["Recip_flag", "lr_flag", "JobPath_Flag", "JobPathHold"]
        self.date_cols = ["clm_reg_date", "clm_comm_date", 'date_of_birth']

    def processed(self):
        ists_file = self.location + "\\ists_ext_" + \
                    self.file_date.strftime("%d%b%Y").lower() + ".sas7bdat"
        if os.path.isfile(ists_file):
            engine = sa.create_engine("sqlite:///" + self.db)
            conn = engine.connect()
            t = text("select count(*) from load_ists where file_date='" + self.file_date.strftime(
                "%Y-%m-%d %H:%M:%S.%f") + "'")
            if next(conn.execute(t))[0] > 0:
                mtime = futil.modification_date(ists_file)
                t = text("select count(*) from load_ists where file_date='" + self.file_date.strftime(
                    "%Y-%m-%d %H:%M:%S.%f") +
                         "' and mod_date='" + mtime.strftime("%Y-%m-%d %H:%M:%S.%f") + "'")
                if next(conn.execute(t))[0] > 0:
                    return True
                else:
                    print("source file " + ists_file + " has been modified")
                    return False
            else:
                print("source file " + ists_file + " not loaded")
                return False
        else:
            print("source file " + ists_file + " not present")
            return True

    def do_process(self):
        print( '----  begin`>' + str(datetime.datetime.now()))
        engine = sa.create_engine("sqlite:///" + self.db)
        conn = engine.connect()
        ists_file = self.location + "\\ists_ext_" + \
                    self.file_date.strftime("%d%b%Y").lower() + ".sas7bdat"
        if os.path.isfile(ists_file):
            lr_date = self.file_date - timedelta(days=2)
            cols_fl = self.personal_cols.copy()
            cols_fl.extend(self.claim_cols)
            try:
                data = self.load(ists_file, cols_fl)
                self.processData(data, lr_date, self.claim_cols, self.personal_cols, self.to_int_cols)
                data.to_sql("ists_data_tmp", con=engine, if_exists="replace")
                self.run_sql(conn, ists_file)
            except:
                print( 'ERROR - ' + str(ists_file) + ' failed to load')
        print( '------  end`>' + str(datetime.datetime.now()))

    def run_sql(self, conn, ists_file):
        print( '1>' + str(datetime.datetime.now()))
        t = text("""  delete from ists_claims 
            where lr_date in ( select lr_date from ists_data_tmp group by lr_date ); """)
        conn.execute(t)
        print( '2>' + str(datetime.datetime.now()))
        t = text("""  INSERT INTO ists_personal (date_of_birth, sex, nat_code, occupation, ppsn, RELATED_RSI_NO)      
              select te.date_of_birth, te.sex, te.nat_code, te.occupation, te.ppsn, te.RELATED_RSI_NO
                  from ists_data_tmp te
                  left join ists_personal pd 
                      on te.date_of_birth = pd.date_of_birth 
                          and te.sex = pd.sex
                          and te.nat_code = pd.nat_code
                          and te.occupation = pd.occupation
                          and te.ppsn = pd.ppsn
                          and ((te.RELATED_RSI_NO IS NULL AND pd.RELATED_RSI_NO IS NULL) OR (te.RELATED_RSI_NO = pd.RELATED_RSI_NO))
                where pd.ppsn IS NULL
                group by te.date_of_birth, te.sex, te.nat_code, te.occupation, te.ppsn, te.RELATED_RSI_NO;
            """)
        conn.execute(t)
        print( '3>' + str(datetime.datetime.now()))
        t = text("""  insert into ists_claims ( lr_code, lr_flag, lls_code, clm_reg_date, clm_comm_date, 
                                                     location, CLM_STATUS, CLM_SUSP_DTL_REAS_CODE, CDAS, ada_code, 
                                                     JobPath_Flag, JobPathHold, PERS_RATE, ADA_AMT, CDA_AMT, MEANS, 
                                                     EMEANS, NEMEANS, NET_FLAT, FUEL, RRA, WEEKLY_RATE, Recip_flag, 
                                                     lr_date, personal_id )         
    select te.lr_code, te.lr_flag, te.lls_code, te.clm_reg_date, te.clm_comm_date, te.location, te.CLM_STATUS,
            te.CLM_SUSP_DTL_REAS_CODE, te.CDAS, te.ada_code, te.JobPath_Flag, te.JobPathHold, te.PERS_RATE, 
            te.ADA_AMT, te.CDA_AMT, te.MEANS, te.EMEANS, te.NEMEANS, te.NET_FLAT, te.FUEL, te.RRA, te.WEEKLY_RATE,
             te.Recip_flag, te.lr_date, pd.id
        from ists_data_tmp te
        join ists_personal pd 
            on te.date_of_birth = pd.date_of_birth 
                and te.sex = pd.sex
                and te.nat_code = pd.nat_code
                and te.occupation = pd.occupation
                and te.ppsn = pd.ppsn
                and ((te.RELATED_RSI_NO IS NULL AND pd.RELATED_RSI_NO IS NULL) OR (te.RELATED_RSI_NO = pd.RELATED_RSI_NO)) """)
        conn.execute(t)
        print( '4>' + str(datetime.datetime.now()))
        t = text("""  drop table ists_data_tmp """)
        conn.execute(t)
        mtime = futil.modification_date(ists_file)
        print( '5>' + str(datetime.datetime.now()))
        t = text("delete from load_ists where file_date ='" +
                 self.file_date.strftime("%Y-%m-%d %H:%M:%S.%f") + "'")
        conn.execute(t)
        print( '6>' + str(datetime.datetime.now()))
        t = text("insert into load_ists (file_date, mod_date, load_time) values('" +
                 self.file_date.strftime("%Y-%m-%d %H:%M:%S.%f") +
                 "', '" + mtime.strftime("%Y-%m-%d %H:%M:%S.%f") +
                 "', '" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + "')")
        conn.execute(t)
        print( '7>' + str(datetime.datetime.now()))

    def load(self, filepath, cols):
        data, meta = pyreadstat.read_sas7bdat(filepath, encoding='LATIN1')
        data = data[cols]
        return data

    def processData(self, data, lr_date, claim_cols, personal_cols, to_int_cols):
        data[to_int_cols] = data[to_int_cols].fillna(0).astype("int8")
        if "JobPath_Flag" not in data.columns.to_list():
            data["JobPath_Flag"] = 0
        if "JobPathHold" not in data.columns.to_list():
            data["JobPathHold"] = 0
        data["lr_date"] = lr_date

