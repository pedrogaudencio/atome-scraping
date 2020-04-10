# Query all companies websites from singapore-companies-directory.com

from bs4 import BeautifulSoup
import pandas as pd
import re
import requests
from string import ascii_lowercase
from time import sleep

from utils import (
    correct_columns_dtype
)


def parse_page(page, d):
    soup = BeautifulSoup(page, 'html.parser')

    try:
        rows = soup.find(id="table116").find_all("tr")[1:][:-1]  # [7:][:-5]

        for row in rows:
            # read content from page, select company name, profile and url
            cells = row.find_all("td")
            name = cells[0].get_text().strip()
            url = cells[0].find_all("a", href=True)[0]['href'].replace(
                '../', base_url)
            profile = " ".join(cells[1].get_text().split()).strip()
            profile = re.split(r'\t+', profile)[0]

            row = [name, url, profile]
            row.extend([''] * 9)
            d.append(row)
    except AttributeError:
        pass


def build_url_list(sub_url):
    pagination = list(ascii_lowercase)

    categories_url = 'Database/'
    sub_args = '_{}.htm'

    url_list = [base_url + categories_url + sub_url + sub_args.format(l) for
                l in pagination]
    print(url_list)

    url_list_paginated = [
        url.replace('.htm', '{}.htm'.format(i)) for i in range(1, 101)
        for url in url_list]

    return url_list_paginated


def fetch_companies_from_directories(directories):
    companies_data = []
    url_tries_failed = 0
    url_max_tries = 4

    for url in directories:
        try:
            req = requests.get(url, timeout=7)
            if req.status_code == 404:
                url_tries_failed += 1
                if url_tries_failed % url_max_tries != 0:
                    url_tries_failed = 0
                    break
            parse_page(req.content, companies_data)
            print(len(companies_data))
            sleep(2)
        except requests.exceptions.Timeout:
            print('\n\nTimeout for: {}\n\n'.format(url))

    return companies_data


if __name__ == "__main__":
    global base_url
    base_url = 'http://singapore-companies-directory.com/'
    data_source = 'singapore-companies-directory.com'
    sub_url = 'singapore_companies_directory'
    directories = build_url_list(sub_url)

    data = fetch_companies_from_directories(directories)

    df = pd.DataFrame(data, columns=[
        'name',
        'profile',
        'url',
        'categories',
        'description',
        'tel',
        'fax',
        'contact',
        'email',
        'website',
        'address',
        'data_source'])

    correct_columns_dtype(df)

    df.to_csv('{}_{}_a-z.csv'.format(
        data_source, sub_url_list[0], sep=';'), index=False)
