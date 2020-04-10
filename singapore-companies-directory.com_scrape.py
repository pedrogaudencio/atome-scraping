# Query all companies websites from singapore-companies-directory.com

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import re
import requests
from string import ascii_lowercase
from time import sleep

from utils import (
    assign_category,
    correct_columns_dtype,
    extract_designation,
    extract_name
)


def clean_data(df):
    df = df.assign(designation=extract_designation(df.contact.values.tolist()))
    df = df.assign(poc=extract_name(df.contact.values.tolist()))
    df.fillna('', inplace=True)
    del df['contact']

    return df


def parse_page(page, d):
    soup = BeautifulSoup(page, 'html.parser')

    try:
        rows = soup.find(id="table116").find_all("tr")[1:][:-1]  # [7:][:-5]

        for row in rows:
            # read content from page, select company name, profile and url
            cells = row.find_all("td")
            name = cells[0].get_text().replace(
                '\r\n\t\t\t\t\t', '').strip()
            url = cells[0].find_all("a", href=True)[0]['href'].replace(
                '../', base_url)
            profile = " ".join(cells[1].get_text().split()).strip()
            profile = re.split(r'\t+', profile)[0]

            row = [name, profile, url]
            print('\n\n{}\n\n'.format(row))
            row.extend([''] * 8)
            d.append(row)
    except AttributeError:
        pass


def build_url_list(sub_url):
    pagination = list(ascii_lowercase)
    pagination.pop(0)  # remove a, there is no 'a' in the urls

    categories_url = 'Categories/'
    sub_args = '_{}'
    end_url = '.htm'

    url_list = [base_url + categories_url + sub_url + end_url]
    url_list.extend(
        [base_url + categories_url + sub_url + sub_args.format(l) + end_url for
         l in pagination])

    return url_list


def fetch_companies_from_directories(directories):
    companies_data = []
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
            parse_page(req.content, companies_data)
            print(len(companies_data))
            sleep(2)
        except requests.exceptions.Timeout:
            print('\n\nTimeout for: {}\n\n'.format(url))
            sleep(2)
        except requests.exceptions.ConnectionError:
            print('\n\nConnectionError for: {}\n\n'.format(url))
            sleep(2)
    return companies_data


if __name__ == "__main__":
    global base_url
    base_url = 'http://singapore-companies-directory.com/'
    data_source = 'singapore-companies-directory.com'
    sub_url_list = ['singapore_furniture_list',
                    'singapore_furnishings_list',
                    'singapore_furnishings']
    category = 'Furniture'
    directories = []

    for sub_url in sub_url_list:
        directories.extend(build_url_list(sub_url))

    data = fetch_companies_from_directories(directories)

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
    df = clean_data(df)
    df = assign_category(df, category)

    df.to_csv('{}_a-z.csv'.format(
        '_'.join([data_source] + sub_url_list), sep=';'), index=False)
