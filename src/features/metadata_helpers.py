# %%
import calendar
import datetime as dt

import dateutil.relativedelta as rd

import pandas as pd

# %%
def nearest_lr_date(date: pd.Timestamp, how: str="previous") -> pd.Timestamp:
    """
    Given any date, returns the date of the nearest LR (Friday) date.

    Can specify "previous" or "next" LR Friday. "previous" is default.

    Parameters
    ----------
    date: pd.Timestamp
        Any date, within reason. 
    how: str
        Tell the function how to find the nearest LR date:
            "this_week" -> LR Friday of the week in which date falls (Mon to Sun)
            "previous" -> most recent LR Friday before date
            "next" -> next LR Friday after date

    Returns
    -------
    pd.Timestamp
        Date of nearest LR Friday to date
    """
    # Looking for the previous LR Friday
    # dateutil.relativedelta (aliased as 'rd') has a list of 2-letter daycodes so FR
    # rd.FR(-1) means the last Friday before now. 
    # Does nothing if date is already a Friday.
    if how == "previous":
        lr_date = date + rd.relativedelta(weekday=rd.FR(-1))
    
    # Looking for the next LR Friday
    # rd.FR(-1) means the next Friday after now.
    # Does nothing if date is already a Friday.
    if how == "next":
        lr_date = date + rd.relativedelta(weekday=rd.FR(+1))

    return lr_date


def last_dayname_date_in_month(date: pd.Timestamp, dayname: str) -> pd.Timestamp:
    """
    Given a date and a dayname, returns the last date that weekday occurs in same month

    Parameters
    ----------
    date: pd.Timestamp
        Any date in the month of interest
    dayname: str
        Format is like "Monday".

    Returns
    -------
    pd.Timestamp
        Date of last specified dayofweek of given month

    References
    ----------
    Adapted from here: https://stackoverflow.com/a/52721988
    """
    dayname_daynumbers = {d: i for i, d in enumerate(calendar.day_name)}
    cal = calendar.monthcalendar(date.year, date.month)
    daynumber = dayname_daynumbers[dayname]

    # Calendar weeks are indexed from 0 == first week of month
    # Calendar days are indexed from 0 == Monday, so Thursday is at index 3
    # ...so if day [4][3] exists that means it's the last (5th) Thursday in the month
    # ...otherwise day [3][3] must be the last Thursday.
    if cal[4][daynumber]:
        last_dayofweek_monthday = cal[4][daynumber]
    else:
        last_dayofweek_monthday = cal[3][daynumber]

    return pd.Timestamp(date.year, date.month, last_dayofweek_monthday)


def lr_reporting_date(date: pd.Timestamp) -> dt.datetime:
    """
    Given a date, returns the LR reporting date for the month that date falls in

    Parameters
    ----------
    date: pd.Timestamp
        Any date in the month of interest

    Returns
    -------
    pd.Timestamp
        Live Register reporting date for given month

    Notes
    -----
    From May 2015, LR reporting date is the Friday 1 day after last Thursday of the month.
    Previously, the reporting date was the last Friday in the month.

    References
    ----------
    https://www.cso.ie/en/releasesandpublications/er/lr/liveregisterjanuary2019/
    """

    # Last Friday of month for months before May 2015
    if date < pd.Timestamp("2015-05-01"):
        reporting_date = last_dayname_date_in_month(date, "Friday")
    # Friday 1 day after last Thursday of month for May 2015 and later months
    else:
        reporting_date = last_dayname_date_in_month(date, "Thursday") + dt.timedelta(
            days=1
        )

    return reporting_date

