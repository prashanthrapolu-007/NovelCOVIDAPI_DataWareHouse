import requests
import csv


def fetch_api_data_and_store_in_csv(output_file_path, url, country):
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        dates = list(data['timeline']['cases'].keys())
        cases = list(data['timeline']['cases'].values())
        deaths = list(data['timeline']['deaths'].values())
        recovered = list(data['timeline']['recovered'].values())
        country_name = [country for i in range(len(recovered))]
        with open(output_file_path, 'a+') as f:
            writer = csv.writer(f)
            writer.writerows(zip(country_name, dates, cases, deaths, recovered))
        f.close()


def get_data_from_api(output_file_path, countries_csv_file, history_param):

    with open(countries_csv_file, 'r') as file:
        try:
            read_csv = csv.reader(file, delimiter=',')
            next(file)
            for row in read_csv:
                country = row[0]
                url = 'https://corona.lmao.ninja/v2/historical/' + country + '?lastdays=' + history_param
                fetch_api_data_and_store_in_csv(output_file_path, url=url, country=country)

        except Exception as e:
            raise ValueError(e)
        finally:
            file.close()
