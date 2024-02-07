import datetime
import pandas as pd
import requests


def retrieve_load_dates(
    start_date: datetime,
    end_date: datetime = datetime.datetime.today().replace(day=1)
):
    """
    Generate a list of dates and times of active yellow taxi parquet files from the NYC Taxi & Limousine Commission
    (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

    :param start_date: Start date to retrieve yellow taxi parquet files
    :param end_date: End date to retrieve yellow taxi parquet files
    :return: list
    """
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    date_range = pd.date_range(start_date, end_date, freq="MS").tolist()
    file_range = []

    for date in date_range:
        date_str = date.strftime("%Y-%m")
        file = "yellow_tripdata_%s.parquet" % date_str

        request = requests.get(url+file, stream=True)

        if request.status_code < 400:
            file_range.append(date)

    return file_range


if __name__ == "__main__":
    start_date_ = datetime.datetime(year=2022,month=1,day=1)
    end_date_ = datetime.datetime.today().replace(day=1)
    print(retrieve_load_dates(start_date_,end_date_))
