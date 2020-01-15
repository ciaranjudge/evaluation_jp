import pyreadstat
import datetime
import os
import futil
import sqlalchemy as sa
from sqlalchemy import text

class Penalties_file:

    def __init__(self, db, filename):
        self.db = db
        self.filename = filename


    def processed(self):
        if os.path.isfile(self.filename):
            engine = sa.create_engine("sqlite:///" + self.db)
            conn = engine.connect()
            t = text("select count(*) from load_penalties where file='" + self.filename + "'")
            if next(conn.execute(t))[0] > 0:
                mtime = futil.modification_date(self.filename)
                t = text("select count(*) from load_penalties where file='" + self.filename +
                         "' and mod_date='" + mtime.strftime("%Y-%m-%d %H:%M:%S.%f") + "'")
                if next(conn.execute(t))[0] > 0:
                    return True
                else:
                    print("source file " + self.filename + " has been modified")
                    return False
            else:
                print("source file " + self.filename + " not loaded")
                return False
        else:
            print("source file " + self.filename + " not present")
            return True

    def do_process(self):
        mtime = futil.modification_date(self.filename)
        self.read()
        engine = sa.create_engine("sqlite:///" + self.db)
        conn = engine.connect()
        t = text("insert into load_penalties (file, mod_date, load_time) values('" +
                 self.filename +
                 "', '" + mtime.strftime("%Y-%m-%d %H:%M:%S.%f") +
                 "', '" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + "')")
        conn.execute(t)



    def read(self):
        engine = sa.create_engine("sqlite:///" + self.db)
        conn = engine.connect()
        print( '----  begin >' + str(datetime.datetime.now()))

        print('-----  read >' + str(datetime.datetime.now()))

        df, meta = pyreadstat.read_sas7bdat( self.filename)
        df.to_sql("penalties_tmp", con=engine, if_exists="replace")

        print('----  merge >' + str(datetime.datetime.now()))
        t = text( """
insert into penalties (    ppsn,sex,age_penstart,life_event_date,location,loc_div,marital_status,marital_group,nat_code ,nat_detail,nat_group,occ_group, occupation,ada_code,spouse,cda_number,cdas,startdate,Extractdate,pendur,duration,status,clm_code,clm_comm_date,clm_end_date,rra,LO_office,RRB,CLM_SUSP_DTL_REAS_CODE)
select te.ppsn, te.sex,te.age_penstart,te.life_event_date,te.location,te.loc_div,te.marital_status,te.marital_group,te.nat_code ,te.nat_detail,te.nat_group,te.occ_group,
	te.occupation,te.ada_code,te.spouse,te.cda_number,te.cdas,te.startdate,te.Extractdate,te.pendur,te.duration,te.status,te.clm_code,te.clm_comm_date,te.clm_end_date,te.rra,
	te.LO_office,te.RRB,te.CLM_SUSP_DTL_REAS_CODE
    from penalties_tmp te
    left join penalties pe
        on te.ppsn = pe.ppsn
            and te.sex = pe.sex
            and te.age_penstart = pe.age_penstart
            and te.location = pe.location
            and te.loc_div = pe.loc_div
            and te.marital_status = pe.marital_status
            and te.marital_group = pe.marital_group
            and te.nat_code = pe.nat_code
            and te.nat_detail = pe.nat_detail
            and te.nat_group = pe.nat_group
            and te.occ_group = pe.occ_group
            and te.occupation = pe.occupation
            and te.ada_code = pe.ada_code
            and te.spouse = pe.spouse
            and te.cda_number = pe.cda_number
            and te.cdas = pe.cdas
            and te.startdate = pe.startdate
            and te.Extractdate = pe.Extractdate
            and te.pendur = pe.pendur
            and te.duration = pe.duration
            and te.status = pe.status
            and te.clm_code = pe.clm_code
            and te.clm_comm_date = pe.clm_comm_date
            and te.clm_end_date = pe.clm_end_date
            and te.rra = pe.rra
            and te.LO_office = pe.LO_office
            and te.RRB = pe.RRB
            and te.CLM_SUSP_DTL_REAS_CODE = pe.CLM_SUSP_DTL_REAS_CODE
    where pe.ppsn is null
    """)
        conn.execute(t)
        print('------  add >' + str(datetime.datetime.now()))
        t = text("""
delete
    from penalties
    where id in (select pe.id
                     from penalties pe
                         left join penalties_tmp te
                             on te.ppsn = pe.ppsn
                                  and te.sex = pe.sex
                                  and te.age_penstart = pe.age_penstart
                                  and te.location = pe.location
                                  and te.loc_div = pe.loc_div
                                  and te.marital_status = pe.marital_status
                                  and te.marital_group = pe.marital_group
                                  and te.nat_code = pe.nat_code
                                  and te.nat_detail = pe.nat_detail
                                  and te.nat_group = pe.nat_group
                                  and te.occ_group = pe.occ_group
                                  and te.occupation = pe.occupation
                                  and te.ada_code = pe.ada_code
                                  and te.spouse = pe.spouse
                                  and te.cda_number = pe.cda_number
                                  and te.cdas = pe.cdas
                                  and te.startdate = pe.startdate
                                  and te.Extractdate = pe.Extractdate
                                  and te.pendur = pe.pendur
                                  and te.duration = pe.duration
                                  and te.status = pe.status
                                  and te.clm_code = pe.clm_code
                                  and te.clm_comm_date = pe.clm_comm_date
                                  and te.clm_end_date = pe.clm_end_date
                                  and te.rra = pe.rra
                                  and te.LO_office = pe.LO_office
                                  and te.RRB = pe.RRB
                                  and te.CLM_SUSP_DTL_REAS_CODE = pe.CLM_SUSP_DTL_REAS_CODE
                         where te.ppsn is null)      """)
        conn.execute(t)
        print('------  del >' + str(datetime.datetime.now()))
        t = text("drop table penalties_tmp")
        conn.execute(t)
        print('-----  drop >' + str(datetime.datetime.now()))


if __name__ == '__main__':
    penalties = Penalties_file(  'd:\\data\\jp_pl.db', '\\\\cskma0294\\F\\Management Board\\Penalty Rates\\penalties_00_latest.sas7bdat')
    # print( penalties.processed() )
    # penalties.do_process()
    # print( penalties.processed() )
    penalties.read()