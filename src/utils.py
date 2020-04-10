import numpy as np
import pandas as pd


designation_list = ['Regional Managing Director',
                    'Managing Director',
                    'Business Development Director',
                    'Executive Director',
                    'Director of Sales & Marketing',
                    'Director of Sales and Marketing',
                    'Director',
                    'Admin Manager',
                    'General Manager',
                    'Manager',
                    'Proprietor',
                    'Founder',
                    'Co-Founder',
                    'President',
                    'Secretary']


def extract_designation(contacts):
    """Extracts the designation of the POC from the contact
    (name + designation)."""
    designations = []
    for contact in contacts:
        for d in designation_list:
            designation = ''
            if not pd.isnull(contact) and d.lower() in contact.lower():
                designation = d
                break
        designations.append(designation)

    return pd.Series(designations)


def extract_poc(contacts):
    """Extracts the name of the POC from the contact (name + designation)."""
    new_pocs = []
    for contact in contacts:
        for d in designation_list:
            poc = contact
            if not pd.isnull(contact) and d.lower() in contact.lower():
                poc = poc.replace(d, '').strip()
                break
        new_pocs.append(poc)

    return pd.Series(new_pocs)


def parse_online_status(df):
    """Adds Online/Offline values depending on existance of email or
    website."""
    df['online'] = np.where(
        (((df.email != '') & (df.email != ' ') & (df.email != 'nan')) |
         ((df.website != '') & (df.website != ' ') & (df.website != 'nan'))),
        'Online', 'Offline')

    return df


def assign_category(df, category, subcategory=None):
    """Adds category and sub-category columns."""
    df['category'] = category
    if subcategory:
        df['sub-category'] = subcategory

    return df


def clean_data(df):
    """Final clean up."""
    contacts = df.contact.values.tolist()
    df = df.assign(designation=extract_designation(contacts))
    df = df.assign(poc=extract_poc(contacts))
    df.fillna('', inplace=True)
    del df['contact']

    return df


def correct_columns_dtype(df):
    """Initial clean up."""
    nan_columns = df.columns[df.isna().any()].tolist()
    for column in nan_columns:
        df[column] = df[column].astype(str)
    df.fillna('', inplace=True)
