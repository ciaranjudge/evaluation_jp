import luigi
from datetime import date
import ists as ists
import earnings as earnings
import payments as payments
import les as les
import penalties as penalties
import pandas as pd


class Ists_load(luigi.Task):
    ists_sunday = luigi.DateParameter()
    db = luigi.Parameter()
    location = luigi.Parameter()

    def requires(self):
        return []

    def complete(self):
        self.ists = ists.Ists_file(self.db, self.location, self.ists_sunday)
        return self.ists.processed()

    def run(self):
        self.ists.do_process()

class Les_load(luigi.Task):
    db = luigi.Parameter()
    filename = luigi.Parameter()

    def requires(self):
        return []

    def complete(self):
        self.les = les.Les_file(self.db, self.filename)
        return self.les.processed()

    def run(self):
        self.les.do_process()


class Penalties_load(luigi.Task):
    db = luigi.Parameter()
    filename = luigi.Parameter()

    def requires(self):
        return []

    def complete(self):
        self.penalties = penalties.Penalties_file(self.db, self.filename)
        return self.penalties.processed()

    def run(self):
        self.penalties.do_process()


class Payments_load(luigi.Task):
    db = luigi.Parameter()
    filename = luigi.Parameter()

    def requires(self):
        return []

    def complete(self):
        self.payments = payments.Payments_file(self.db, self.filename)
        return self.payments.processed()

    def run(self):
        self.payments.do_process()


class Earnings_load(luigi.Task):
    db = luigi.Parameter()
    filename = luigi.Parameter()

    def requires(self):
        return []

    def complete(self):
        self.earnings = earnings.Earnings_file(self.db, self.filename)
        return self.earnings.processed()

    def run(self):
        self.earnings.do_process()


class JobPathEtl(luigi.Task):
    st = luigi.DateParameter(default=date.today())
    db = luigi.Parameter()
    il = luigi.Parameter()
    ea = luigi.Parameter()
    pay = luigi.Parameter()
    pen = luigi.Parameter()
    les = luigi.Parameter()
    task_complete = False;

    def requires(self):
        ists_end = date.today()
        sundays = pd.date_range(self.st, ists_end, freq="W-SUN")
        for sunday in sundays:
            yield Ists_load(sunday, self.db, self.il)
        yield Earnings_load(self.db, self.ea)
        yield Payments_load(self.db, self.pay)
        yield Penalties_load(self.db, self.pen)
        yield Les_load(self.db, self.les)

    def complete(self):
        return self.task_complete

    def run(self):
        self.task_complete = True


if __name__ == '__main__':
    luigi.run()
