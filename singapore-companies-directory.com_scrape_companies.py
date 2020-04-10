# Fetch details for all companies from singapore-companies-directory.com

from bs4 import BeautifulSoup
import pandas as pd
import re
import requests
from time import sleep


regex_dict = {
    'categories': r'((?<=Categories: )(.*)?(?=,Company Profile:)|(?<=Categories: )(.*)?(?=\s))',
    'contact': r'((?<=Contact: )(.*)?(?=Address:))|((?<=Contact: )(.*)?(?=Tel:)|((?<=Contact: )(.*)?(?=Fax:)))',
    'tel': r'((?<=Tel: )(.*)?(?=Fax:))|((?<=Tel: )(.*)?(?=e-mail:)|((?<=Tel: )(.*)?(?=Website:)))',
    'fax': r'(((?<=Fax: )(.*)?(?=e-mail:))|((?<=Fax: )(.*)?(?=Website:)))',
    'email': r'[\w\.-]+@[\w\.-]+',
    'website': r'(http[s]?:\/\/)?(www)(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+(?=,)',
    'address': r'(Address: )(?(1)((?<=Address: )(.+)(?= Tel:))|((?<=Contact: )(.+)(?= Tel:)))'
}

csv_filename = 'singapore-companies-directory.com_furniture_b-z_clean.csv'
companies_df = pd.read_csv(csv_filename)


def correct_columns_dtype(df):
    for column in regex_dict.keys():
        df[column] = df[column].astype(str)


def extract_content(soup):
    company_content = soup.find(id="table12")
    cells = company_content.find_all("td")
    content = cells[0].get_text().strip().replace(
        '\r\n\t\t\t\t\t', ' ').replace('\n', ',')

    return content


def find_t(exp, content):
    m = re.search(regex_dict[exp], content)
    return m.group().strip() if m else ''


def clean_content(content):
    content_clean = {}

    for param in regex_dict.keys():
        if param not in content_clean:
            res = find_t(param, content)
            content_clean[param] = res

    return content_clean


def assign_source(df, i, j):
    df.iat[i, j] = 'singapore-companies-directory.com'


def assign_data(df, row_idx, data):
    for column, v in data.items():
        column_idx = df.columns.get_loc(column)
        df.iat[row_idx, column_idx] = v

    assign_source(df, row_idx, column_idx)


correct_columns_dtype(companies_df)


for index, row in companies_df.iterrows():
    req = requests.get(row['url'])
    if req.status_code == requests.codes.ok:
        soup = BeautifulSoup(req.content, 'html.parser')

        content = extract_content(soup)
        cleaned = clean_content(content)
        assign_data(companies_df, index, cleaned)
        print(companies_df.iloc[index])

    sleep(2)

companies_df.to_csv(
    'singapore-companies-directory.com_furniture_b-z_clean_full.csv',
    index=False)
