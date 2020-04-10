# Query all companies websites from singapore-companies-directory.com

from bs4 import BeautifulSoup
import pandas as pd
import re
import requests
from string import ascii_lowercase
from time import sleep

from utils import (
    assign_category,
    correct_columns_dtype
)


def parse_page(page, d, e):
    soup = BeautifulSoup(page, 'html.parser')

    try:
        table = (soup.find(id="table116") or
                 soup.find(id="table119") or
                 soup.find(id="table24"))
        rows = table.find_all("tr")[1:][:-1]  # [7:][:-5]

        for row in rows:
            # read content from page, select company name, profile and url
            cells = row.find_all("td")
            name = cells[0].get_text().replace(
                '\r\n\t\t\t\t\t', '').strip()
            profile = " ".join(cells[1].get_text().split()).strip()
            profile = re.split(r'\t+', profile)[0]
            try:
                url = cells[0].find_all("a", href=True)[0]['href'].replace(
                    '../', base_url)
                row = [name, profile, url]
                row.extend([''] * 8)
                d.append(row)
            except IndexError:
                # no url, so we'll save this and search for details later
                row = [name, profile]
                e.append(row)
                print("IndexError: url not found for '{}'.".format(name))
            print('\n\n{}\n\n'.format(row))

    except AttributeError:
        pass


def build_url_list(sub_url, build=True):
    categories_url = 'Categories/'
    sub_args = '_{}'
    end_url = '.htm'

    if not build:
        return [base_url + categories_url + sub_url + end_url]

    pagination = list(ascii_lowercase)
    pagination.pop(0)  # remove a, there is no 'a' in the urls

    url_list = [base_url + categories_url + sub_url + end_url]
    url_list.extend(
        [base_url + categories_url + sub_url + sub_args.format(l) + end_url for
         l in pagination])

    return url_list


def fetch_companies_from_directories(directories):
    companies_data = []
    unsearched_directory = []
    # failed_tries = 0
    # url_max_tries = 4

    for url in directories:
        try:
            req = requests.get(url)
            if req.status_code == 404:
                # failed_tries += 1
                print('\n\n404: {}\n\n'.format(url))
                # if failed_tries % url_max_tries == 0:
                #     failed_tries = 0
                #     break
                sleep(2)
                continue
            parse_page(req.content, companies_data, unsearched_directory)
            print(len(companies_data))
            sleep(2)
        except requests.exceptions.Timeout:
            print('\n\nTimeout for: {}\n\n'.format(url))
            sleep(2)
        except requests.exceptions.ConnectionError:
            print('\n\nConnectionError for: {}\n\n'.format(url))
            sleep(2)
    print(unsearched_directory)
    return (companies_data, unsearched_directory)


if __name__ == "__main__":
    global base_url
    base_url = 'http://singapore-companies-directory.com/'
    data_source = 'singapore-companies-directory.com'
    sub_url_list = ['singapore_nurseries']
    build_urls = False
    category = 'Education'
    subcategory = 'Nursery'
    csv_out_filename = '../data/{}/{}.csv'.format(
        data_source, '_'.join(sub_url_list))

    directories = []

    for sub_url in sub_url_list:
        directories.extend(build_url_list(sub_url, build_urls))

    data, data_extra = fetch_companies_from_directories(directories)

    df = pd.DataFrame(data, columns=[
        'name',
        'profile',
        'url',
        'categories',
        'mobile',
        'fax',
        'contact',
        'email',
        'website',
        'address',
        'data_source'])

    correct_columns_dtype(df)
    df = assign_category(df, category, subcategory)

    df.to_csv(csv_out_filename, sep=';', index=False)
    if len(data_extra):
        df_extra = pd.DataFrame(data_extra, columns=['name', 'profile'])
        df_extra.to_csv(csv_out_filename.replace(
            '.csv', '_names_only.csv'), sep=';', index=False)
