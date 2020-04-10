# Fetch details for all companies from singapore-companies-directory.com

from bs4 import BeautifulSoup
import pandas as pd
import re
import requests
from time import sleep

from utils import (
    clean_data,
    correct_columns_dtype,
    parse_online_status
)


regex_dict = {
    'categories': r'((?<=Categories: )(.*)?(?=,Company Profile:)|(?<=Categories: )(.*)?(?=\s))',
    'contact': r'((?<=Contact: )(.*)?(?=Address:))|((?<=Contact: )(.*)?(?=Tel:)|((?<=Contact: )(.*)?(?=Fax:)))',
    'mobile': r'((?<=Tel: )(.*)?(?=Fax:))|((?<=Tel: )(.*)?(?=e-mail:)|((?<=Tel: )(.*)?(?=Website:)))',
    'fax': r'(((?<=Fax: )(.*)?(?=e-mail:))|((?<=Fax: )(.*)?(?=E-mail:)))',
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


def clean_content(content, row):
    content_clean = {}

    for param in regex_dict.keys():
        if param not in content_clean:
            res = find_t(param, content)
            content_clean[param] = res

    if (content_clean['contact'] is not None and
            'Singapore' in content_clean['contact'] and
            content_clean['address'] == ''):
        content_clean['address'] = content_clean['contact']
        content_clean['address'].replace(row['name'], '')
        if not content_clean['contact'][0].isalpha():
            content_clean['contact'] = ''
        else:
            # test if we can split the name and the address
            contact_parts = content_clean['contact'].split(' ')
            for part in contact_parts:
                if part.isdigit() or any(
                        map(lambda x: x in part.lower(),
                            ['blk', 'rd', '#', 'lot', 'block'])):
                    content_clean['contact'] = ' '.join(
                        contact_parts[:contact_parts.index(part)])
                    break

    content_clean['address'] = content_clean['address'].replace(
        'Address: ', '')
    content_clean['mobile'] = content_clean['mobile'].replace('(65)', '(+65)')
    content_clean['fax'] = content_clean['fax'].replace('(65)', '(+65)')
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
                    cleaned = clean_content(content, row)
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
    dir_name = '../data/singapore-companies-directory.com/'
    filename = 'singapore_nurseries'
    csv_filename = filename + '.csv'
    csv_filepath = dir_name + csv_filename
    csv_out_filepath = dir_name + filename + '_clean.csv'
    companies_df = pd.read_csv(csv_filepath, sep=';')

    correct_columns_dtype(companies_df)

    populate_companies(companies_df)
    companies_df = parse_online_status(companies_df)
    companies_df = clean_data(companies_df)

    companies_df.to_csv(csv_out_filepath, sep=';', index=False)

    # export to spreadsheet
    csv_filename_export = dir_name + 'export/export_' + csv_filename
    df_out = companies_df[['name',
                           'poc',
                           'designation',
                           'email',
                           'website',
                           'mobile',
                           'fax',
                           'address',
                           'online',
                           'category',
                           'sub-category',
                           'categories',
                           'profile']]
    df_out.to_csv(csv_filename_export, index=False, header=False)
