import boto3
import datetime as dt
import pandas as pd
import oracles_elixir as oe

from dotenv import dotenv_values

config = dotenv_values(r"..\.env")


def main():
    # Define time frame for analytics
    current_year = dt.date.today().year
    years = [str(current_year), str(current_year - 1), str(current_year - 2)]

    # Download Data
    s3_session = boto3.Session(aws_access_key_id=config.get("ACCESS_ID"),
                               aws_secret_access_key=config.get("ACCESS_SECRET"))

    oracle = oe.OraclesElixir(session=s3_session,
                              bucket='oracles-elixir')
    data = oracle.ingest_data(years=years)

    print(len(data))
    print(data.head())


if __name__ == '__main__':
    main()