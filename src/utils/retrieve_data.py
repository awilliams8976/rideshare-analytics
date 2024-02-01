import datetime
import os
import pandas as pd
import requests

def retrieve_data(
        download_path: str,
        year: int,
        month: int
):
    """
    Downloads both yellow and green rideshare parquet files for a specific year and month from the NYC Taxi & Limousine
    Commission (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) to a local directory.

    :param download_path: Local file path to download rideshare parquet files to
    :param year: Year from which the files is to be downloaded
    :param month: Month (number) from which the files is to be downloaded
    :return: Success string
    """
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    date_str = datetime.date(year,month,1).strftime("%Y-%m")
    green_file = f"green_tripdata_{date_str}.parquet"
    yellow_file = f"yellow_tripdata_{date_str}.parquet"
    urls = [url+green_file,url+yellow_file]
    paths = [os.path.join(download_path,green_file),os.path.join(download_path,yellow_file)]

    for path,url in zip(paths,urls):
        try:
            request = requests.get(url, stream=True)
            request.raise_for_status()
            content = request.content

            with open(path,"wb") as file:
                file.write(content)

        except requests.exceptions.HTTPError as err:
            raise SystemExit(err)

    return f"SUCCESS: {green_file} & {yellow_file} written to {download_path}."

if __name__ == "__main__":

    path = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","..","data/raw/"))
    date_list = pd.date_range("2022-01-01","2022-12-31",freq="MS").tolist()

    for date in date_list:
        retrieve_data(download_path=path,year=date.year,month=date.month)