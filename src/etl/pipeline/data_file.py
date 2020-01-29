from abc import ABCMeta, abstractmethod

import pandas as pd
import futil as futil
import datetime
import os
import sqlalchemy as sa
from sqlalchemy import text


class Data_file(metaclass=ABCMeta):

    def processed(self):
        if os.path.isfile(self.filename):
            engine = sa.create_engine( self.db)
            conn = engine.connect()
            t = text("select count(*) from load_file where file='" + self.filename + "'")
            if next(conn.execute(t))[0] > 0:
                mtime = futil.modification_date(self.filename)
                t = text("select count(*) from load_file where file='" + self.filename +
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
        engine = sa.create_engine( self.db)
        conn = engine.connect()
        t = text("delete from load_file where file ='" +
                 self.filename + "'")
        conn.execute(t)
        t = text("insert into load_file (file, mod_date, load_time) values('" +
                 self.filename +
                 "', '" + mtime.strftime("%Y-%m-%d %H:%M:%S.%f") +
                 "', '" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + "')")
        conn.execute(t)

    @abstractmethod
    def read(self):
        pass


