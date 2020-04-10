import numpy as np
import pandas as pd


designation_list = ['Regional Managing Director',
                    'Managing Director',
                    'Business Development Director',
                    'Executive Director',
                    'Director',
                    'Admin Manager',
                    'General Manager',
                    'Manager',
                    'Proprietor',
                    'Founder',
                    'Co-Founder']


def extract_designation(contacts):
    designations = []
    for contact in contacts:
        for d in designation_list:
            designation = ''
            if not pd.isnull(contact) and d.lower() in contact.lower():
                designation = d
                break
        designations.append(designation)

    return pd.Series(designations)


def extract_name(contacts):
    new_names = []
    for contact in contacts:
        for d in designation_list:
            name = contact
            if not pd.isnull(contact) and d.lower() in contact.lower():
                name = name.replace(d, '').strip()
                break
        new_names.append(name)

    return pd.Series(new_names)


def parse_online_status(df):
    df['online'] = np.where(
        ((df['email'].notnull() | df['email'] != '') |
         (df['website'].notnull()) | df['website'] != ''),
        'Online', 'Offline')

    return df


def assign_category(df, category):
    df['category'] = category

    return df


def correct_columns_dtype(df):
    nan_columns = df.columns[df.isna().any()].tolist()
    for column in nan_columns:
        df[column] = df[column].astype(str)
    df.fillna('', inplace=True)
