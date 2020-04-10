# Fetch details for all companies from singapore-companies-directory.com

from bs4 import BeautifulSoup
import pandas as pd
import re
import requests
from time import sleep

from utils import (
    correct_columns_dtype,
    parse_online_status
)


regex_dict = {
    'categories': r'((?<=Categories: )(.*)?(?=,Company Profile:)|(?<=Categories: )(.*)?(?=\s))',
    'contact': r'((?<=Contact: )(.*)?(?=Address:))|((?<=Contact: )(.*)?(?=Tel:)|((?<=Contact: )(.*)?(?=Fax:)))',
    'mobile': r'((?<=Tel: )(.*)?(?=Fax:))|((?<=Tel: )(.*)?(?=e-mail:)|((?<=Tel: )(.*)?(?=Website:)))',
    'fax': r'(((?<=Fax: )(.*)?(?=e-mail:))|((?<=Fax: )(.*)?(?=Website:)))',
    'email': r'[\w\.-]+@[\w\.-]+',
    'website': r'(http[s]?:\/\/)?(www)(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+(?=,)',
    'address': r'(Address: )(?(1)((?<=Address: )(.+)(?= Tel:))|((?<=Contact: )(.+)(?= Tel:)))'
}


def extract_content(soup):
    try:
        company_content = soup.find(id="table12")
        cells = company_content.find_all("td")
        content = cells[0].get_text().strip().replace(
            '\r\n\t\t\t\t\t', ' ').replace('\n', ',')

        return content
    except AttributeError:
        return ''


def find_t(exp, content):
    m = re.search(regex_dict[exp], content)
    return m.group().strip() if m else ''


def clean_content(content):
    content_clean = {}

    for param in regex_dict.keys():
        if param not in content_clean:
            res = find_t(param, content)
            content_clean[param] = res

    content_clean['address'] = content_clean['address'].replace(
        'Address: ', '')
    content_clean['mobile'] = content_clean['mobile'].replace('65', '+65')
    content_clean['fax'] = content_clean['fax'].replace('65', '+65')
    content_clean['website'] = content_clean['website'].replace(',', '')

    return content_clean


def assign_source(df, i):
    j = df.columns.get_loc('data_source')
    df.iat[i, j] = 'singapore-companies-directory.com'


def assign_data(df, row_idx, data):
    for column, v in data.items():
        column_idx = df.columns.get_loc(column)
        df.iat[row_idx, column_idx] = v

    assign_source(df, row_idx)


def populate_companies(df):
    for index, row in df.iterrows():
        try:
            req = requests.get(row['url'], timeout=7)
            if req.status_code == requests.codes.ok:
                soup = BeautifulSoup(req.content, 'html.parser')

                content = extract_content(soup)
                if len(content):
                    cleaned = clean_content(content)
                    assign_data(companies_df, index, cleaned)
                    print('\n\n')
                    print(companies_df.iloc[index])
                    print('\n\n')
                else:
                    print('\n\nNo content for: {}\n\n'.format(row['url']))

            sleep(2)
        except requests.exceptions.Timeout:
            print('\n\nTimeout for: {}\n\n'.format(row['url']))


if __name__ == "__main__":
    # csv_filename = 'singapore-companies-directory.com_furniture_b-z_clean.csv'
    csv_filename = 'singapore-companies-directory.com_singapore_furniture_list_singapore_furnishings_list_singapore_furnishings_a-z.csv'
    companies_df = pd.read_csv(csv_filename)
    correct_columns_dtype(companies_df)

    populate_companies(companies_df)
    companies_df = parse_online_status(companies_df)

    companies_df.to_csv(
        # 'singapore-companies-directory.com_furniture_b-z_clean_full.csv',
        'singapore-companies-directory.com_singapore_furniture_list_singapore_furnishings_list_singapore_furnishings_a-z_clean.csv',
        index=False)
