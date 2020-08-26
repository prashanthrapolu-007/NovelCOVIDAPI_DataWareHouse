import requests
import csv
import pandas as pd


def fetch_historical_api_data(output_file_path, history_param, row):
    country_code = row[0]
    region_code = row[1]
    sub_region_code = row[2]
    country_name = row[3]
    url = 'https://corona.lmao.ninja/v2/historical/' + country_name + '?lastdays=' + history_param
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        dates = list(data['timeline']['cases'].keys())
        cases = list(data['timeline']['cases'].values())
        deaths = list(data['timeline']['deaths'].values())
        recovered = list(data['timeline']['recovered'].values())

        country_code = [country_code for i in range(len(recovered))]
        region_code = [region_code for i in range(len(recovered))]
        sub_region_code = [sub_region_code for i in range(len(recovered))]
        with open(output_file_path, 'a+') as f:
            writer = csv.writer(f)
            writer.writerows(zip(country_code, region_code, sub_region_code, dates, cases, deaths, recovered))
        f.close()


def fetch_yesterday_data_from_api(output_file_path, url):
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        with open(output_file_path, 'w') as file:
            writer = csv.writer(file, delimiter=",")
            for message in data:
                if 'country' in message:
                    country = message['country']
                    record_date = list(message['timeline']['cases'].keys())[0]
                    cases = list(message['timeline']['cases'].values())[0]
                    deaths = list(message['timeline']['deaths'].values())[0]
                    recovered = list(message['timeline']['recovered'].values())[0]
                    writer.writerow([country, record_date, cases, deaths, recovered])
        file.close()


def get_data_from_api(output_file_path, countries_csv_file, history_param):
    if history_param == 'all':
        with open(countries_csv_file, 'r') as file:
            try:
                read_csv = csv.reader(file, delimiter=',')
                next(file)
                for row in read_csv:
                    fetch_historical_api_data(output_file_path, history_param, row)

            except Exception as e:
                raise ValueError(e)
            finally:
                file.close()

    elif history_param == 1:
        try:
            with open(countries_csv_file, 'r') as file:
                csv_reader = csv.reader(file, delimiter=",")
                next(file)
                countries = list(csv_reader)
            file.close()

            all_countries = ",".join([countries[ind][0] for ind in range(len(countries))])
            url = 'https://corona.lmao.ninja/v2/historical/' + all_countries + '?lastdays=1'
            fetch_yesterday_data_from_api(output_file_path, url)
        except Exception as e:
            raise ValueError(e)


def get_specific_columns_and_store_csv(destination_file, source_file, columns):
    source_df = pd.read_csv(source_file, usecols=columns)
    source_df = source_df[columns]
    source_df = source_df.drop_duplicates().dropna()
    source_df.to_csv(destination_file, index=False)

