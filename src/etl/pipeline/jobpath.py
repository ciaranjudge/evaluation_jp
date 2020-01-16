import luigi
from datetime import date
import datetime
import ists as ists
import earnings as earnings
import payments as payments
import les as les
import penalties as penalties
import pandas as pd
import json


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


class Payments_load(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        return []

    def complete(self):
        self.payments = payments.Payments_file(self.settings)
        return self.payments.processed()

    def run(self):
        self.payments.do_process()


class Earnings_load(luigi.Task):
    settings = luigi.Parameter()

    def requires(self):
        return []

    def complete(self):
        self.earnings = earnings.Earnings_file(self.settings)
        return self.earnings.processed()

    def run(self):
        self.earnings.do_process()


class JobPathEtl(luigi.Task):
    config = luigi.Parameter()
    task_complete = False;

    def requires(self):
        with open(self.config, 'r') as f:
            settings = json.load(f)
        ists_end = date.today()
        sundays = pd.date_range(datetime.datetime.strptime(settings['ists']['startDate'],'%Y-%m-%d'), ists_end, freq="W-SUN")
        for sunday in sundays:
            yield Ists_load(sunday,settings)
        yield Earnings_load(settings)
        yield Payments_load(settings)
        yield Penalties_load(settings)
        yield Les_load(settings)

    def complete(self):
        return self.task_complete

    def run(self):
        self.task_complete = True


if __name__ == '__main__':
    luigi.run()
