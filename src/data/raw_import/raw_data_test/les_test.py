import pandas as pd
import sqlalchemy as sa


def les_sql(les, db):
    engine = sa.create_engine("sqlite:///"+db)
    metadata = sa.MetaData()

    df = pd.read_excel(les)
    df.columns = ["client_group", "ppsn", "start_date"]

    days_to_friday = pd.to_timedelta((4 - df["start_date"].dt.dayofweek), unit="d")
    df["start_week"] = df["start_date"] + days_to_friday

    df.to_sql("les_starts", con=engine, if_exists="replace")


if __name__ == '__main__':
    les_sql( "//cskma0294/F\\Evaluations\\JobPath\\LES_exclusion\\LES_New_Registrations2016-2017.xlsx", "C:\\Users\\MarioRomera\\repos\\data1\\jobpath.db" )


