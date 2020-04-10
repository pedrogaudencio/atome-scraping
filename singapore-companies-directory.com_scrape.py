# Query all companies websites from singapore-companies-directory.com

from bs4 import BeautifulSoup
import pandas as pd
import re
import requests
from string import ascii_lowercase
from time import sleep


def parse_page(page, d):
    soup = BeautifulSoup(page.content, 'html.parser')

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


base_url = 'http://singapore-companies-directory.com/'
categories_url = base_url + 'Categories/singapore_furniture_list_{}.htm'
data_source = 'singapore-companies-directory.com'
# there is no 'a', only the link without the '_'
directories = list(ascii_lowercase)
directories.pop(0)  # remove a
companies_data = []

# first category
first_category_url = 'http://singapore-companies-directory.com/Categories/singapore_furniture_list.htm'
page = requests.get(first_category_url)
parse_page(page, companies_data)
sleep(2)

for p in directories:
    q_url = categories_url.format(p)
    page = requests.get(q_url)
    parse_page(page, companies_data)

    print(len(companies_data))
    sleep(2)

data = pd.DataFrame(companies_data, columns=[
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

data.to_csv('{}_furniture_a-z.csv'.format(data_source, sep=';'), index=False)
