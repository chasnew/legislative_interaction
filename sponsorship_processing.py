# from pyDataverse.api import NativeApi
# from pyDataverse.api import DataAccessApi
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import requests
import yaml

with open("config.yaml") as f:
    config_param = yaml.load(f, Loader=yaml.SafeLoader)

legis_int_path = config_param['legis_int_path']

def name_split(representative):
    split_list = representative.split(', ')
    merged_lname = '-'.join(split_list[0].split(' '))

    if len(split_list) > 1:
        # merged_fname = ' '.join(split_list[1:]) # middle names, titles, and nicknames get merged
        if split_list[1].endswith(')'):
            atom_fname = split_list[1].split('(')[-1]
            atom_fname = atom_fname.replace(')', '')
        else:
            atom_fname = split_list[1].split(' ')[0]
        return [merged_lname, atom_fname]
    else:
        return [merged_lname, None]

party_map = {200: 'R', 100: 'D', 328: 'I'}

congress_df = pd.read_csv(os.path.join(legis_int_path, 'processed_data',
                                           'target_congress_nominate.csv'))

hr09_12_df = congress_df.loc[(congress_df['chamber'] == 'House') &
                             ((congress_df['congress'] >= 109) | (congress_df['congress'] <= 112)),
                             ['congress', 'bioname', 'bioguide_id', 'state_abbrev',
                              'party_code', 'district_code']]
# hr09_12_df = hr09_12_df.drop_duplicates(subset='bioguide_id', keep='last').reset_index(drop=True)

lowercase_names = hr09_12_df['bioname'].str.lower()
hr09_12_df[['lastname', 'firstname']] = pd.DataFrame(lowercase_names.map(name_split).to_list(),
                                                     index=hr09_12_df.index)

hr09_12_df.columns = ['congress', 'bioname', 'bioguide_id', 'state', 'party_code',
                      'district_code', 'lastname', 'firstname']

# 2005 = PIERLUISI, Pedro (Independent), 374 = HALL, Ralph Moody (as Democrat)
# 1647 = GRIFFITH, Parker (as Republican), 1956 = SABLAN, Gregorio Kilili Camacho (as Independent)
hr09_12_df = hr09_12_df.drop([374, 2005, 1647, 1956])
# tmp[(tmp['lastname'] == 'hall')][['congress', 'district_code', 'party_code',
#                                   'bioname', 'bioguide_id']]

edit_inds = hr09_12_df[(hr09_12_df['bioname'].str.contains("Nydia"))].index
hr09_12_df.loc[edit_inds, 'lastname'] = 'velazquez'

edit_inds = hr09_12_df[(hr09_12_df['bioname'].str.contains("GUTIÉRREZ"))].index
hr09_12_df.loc[edit_inds, 'lastname'] = 'gutierrez'

edit_inds = hr09_12_df[(hr09_12_df['bioname'].str.contains("Ben Ray"))].index
hr09_12_df.loc[edit_inds, 'lastname'] = 'lujan'

edit_inds = hr09_12_df[(hr09_12_df['bioname'].str.contains("Linda"))].index
hr09_12_df.loc[edit_inds, 'lastname'] = 'sanchez'

edit_inds = hr09_12_df[(hr09_12_df['bioname'].str.contains("BONO"))].index
hr09_12_df.loc[edit_inds, 'lastname'] = 'bono-mack'

edit_inds = hr09_12_df[(hr09_12_df['bioname'].str.contains("FORTUÑO"))].index
hr09_12_df.loc[edit_inds, 'lastname'] = 'fortuno'

hr09_12_main = hr09_12_df[['congress', 'state', 'lastname', 'party_code', 'district_code', 'bioguide_id']]

# Delegate representatives data not exist before congress 110 and after 111 for some reason
delegate109 = {'congress': [109]*5, 'state': ['PR', 'GU', 'VI', 'AS', 'DC'],
               'lastname': ['fortuno', 'bordallo', 'christensen', 'faleomavaega', 'norton'],
               'party_code': [200, 100, 100, 100, 100], 'district_code': [1, 1, 1, 1, 0],
               'bioguide_id': ['F000452', 'B001245', 'C000380', 'F000010', 'N000147']}
delegate109 = pd.DataFrame(delegate109)

delegate112 = {'congress': [112]*6, 'state': ['PR', 'GU', 'VI', 'AS', 'MP', 'DC'],
               'lastname': ['pierluisi', 'bordallo', 'christensen', 'faleomavaega', 'sablan', 'norton'],
               'party_code': [100, 100, 100, 100, 100, 100], 'district_code': [1, 1, 1, 1, 0, 0],
               'bioguide_id': ['P000596', 'B001245', 'C000380', 'F000010', 'S001177', 'N000147']}
delegate112 = pd.DataFrame(delegate112)

hr09_12_main = pd.concat([delegate109, hr09_12_main, delegate112]).reset_index(drop=True)
# hr09_12_df[(hr09_12_df['lastname'].str.contains("fortuno"))][['bioname', 'party_code', 'district_code', 'bioguide_id']]



# sponsorship data

# Julia Carson in 2007 data (died December 15th 2007)
# Andre Carson in 2008+ data

# HALL, Ralph Moody is Republican from 2004+ (party code = 200)
# GRIFFITH, Parker switched party Dem -> Rep in Dec 22nd 2009
# PIERLUISI, Pedro both Dem and Independent (New Progressive)


# Congress 109 bills

congress_id = 109

hr_sponsor_df = pd.read_csv(os.path.join(legis_int_path, 'processed_data',
                                         'congress{}_hr_bill.csv'.format(congress_id)))
# hr_sponsor_df.iloc[0]
lowercase_names = hr_sponsor_df['name'].str.lower()
hr_sponsor_df[['lastname', 'firstname']] = pd.DataFrame(lowercase_names.map(name_split).to_list(),
                                                        index=hr_sponsor_df.index)

# merging dataframes to get partisan data for cosponsor data
hr_sponsor_df.info()
bill_cols = hr_sponsor_df.columns

# hr09_12_df[(hr09_12_df['lastname'].str.contains("carson"))][['congress', 'state', 'bioname',
#                                                              'bioguide_id', 'party_code', 'district_code']]

hr_sponsor_df = hr_sponsor_df.merge(hr09_12_main,
                                    how='left', left_on=['congress', 'state', 'lastname', 'district'],
                                    right_on=['congress', 'state', 'lastname', 'district_code'])

# split dataframe to patch in rows that don't have district code to merge on
null_districts = hr_sponsor_df.loc[(pd.isna(hr_sponsor_df['district_code'])), bill_cols]
hr_sponsor_df = hr_sponsor_df.loc[(~pd.isna(hr_sponsor_df['district_code']))]

# null_districts.drop_duplicates(subset='name')[['congress', 'state', 'name', 'lastname']]
# hr09_12_df[(hr09_12_df['lastname'].str.contains("fortuno"))][['congress', 'state', 'bioname', 'lastname', 'party_code']]

# lastname + state unique enough
# norton, lummis, herseth-sandlin, welch, young (AK), bordallo, christensen, pierluisi, castle
# rehberg, faleomavaega, pomeroy, cubin, fortuno

# All rows has their names as "Herseth" instead of "Herseth Sandlin"
null_districts.loc[null_districts['name'].str.contains('Herseth'), 'lastname'] = 'herseth-sandlin'

null_districts = null_districts.merge(hr09_12_main, how='left', left_on=['congress', 'state', 'lastname'],
                                      right_on=['congress', 'state', 'lastname'])

hr_sponsor_df = pd.concat([hr_sponsor_df, null_districts]).drop(columns=['district'])
hr_sponsor_df = hr_sponsor_df.sort_values(by=['intro_date', 'bill_id']).reset_index(drop=True)

hr_sponsor_df.info()
needed_cols = ['bioguide_id', 'congress', 'intro_date', 'bill_id', 'bill_type',
               'subject', 'name', 'party_code', 'sponsor', 'cosponsor']

hr109_cosponsor_df = hr_sponsor_df[needed_cols]
hr109_cosponsor_df.to_csv(os.path.join(legis_int_path, 'processed_data', 'hr109_cosponsor.csv'),
                          index=False)



# Congress 110 bills

congress_id = 110

hr_sponsor_df = pd.read_csv(os.path.join(legis_int_path, 'processed_data',
                                         'congress{}_hr_bill.csv'.format(congress_id)))
# hr_sponsor_df.iloc[0]
lowercase_names = hr_sponsor_df['name'].str.lower()
hr_sponsor_df[['lastname', 'firstname']] = pd.DataFrame(lowercase_names.map(name_split).to_list(),
                                                        index=hr_sponsor_df.index)

# merging dataframes to get partisan data for cosponsor data
hr_sponsor_df.info()
bill_cols = hr_sponsor_df.columns

# hr09_12_df[(hr09_12_df['lastname'].str.contains("carson"))][['congress', 'state', 'bioname',
#                                                              'bioguide_id', 'party_code', 'district_code']]

# extract rows for Carson Julia and Carson Andre because they overlap on dates
car_jul_bills = hr_sponsor_df.loc[(hr_sponsor_df['lastname'].str.contains("carson")) &
                                  (hr_sponsor_df['firstname'] == 'julia')]

car_andr_bills = hr_sponsor_df.loc[(hr_sponsor_df['lastname'].str.contains("carson")) &
                                   (hr_sponsor_df['firstname'] == 'andre')]

# add data to Carson Julia rows
car_jul_bills['party_code'] = 100
car_jul_bills['district_code'] = 7
car_jul_bills['bioguide_id'] = 'C000191'

# add data to Carson Andre rows
car_andr_bills['party_code'] = 100
car_andr_bills['district_code'] = 7
car_andr_bills['bioguide_id'] = 'C001072'

carson_inds = car_jul_bills.index.to_list() + car_andr_bills.index.to_list()

hr_sponsor_df = hr_sponsor_df.drop(index=carson_inds)

hr_sponsor_df = hr_sponsor_df.merge(hr09_12_main,
                                    how='left', left_on=['congress', 'state', 'lastname', 'district'],
                                    right_on=['congress', 'state', 'lastname', 'district_code'])

# split dataframe to patch in rows that don't have district code to merge on
null_districts = hr_sponsor_df.loc[(pd.isna(hr_sponsor_df['district_code'])), bill_cols]
hr_sponsor_df = hr_sponsor_df.loc[(~pd.isna(hr_sponsor_df['district_code']))]

# null_districts.drop_duplicates(subset='name')[['congress', 'state', 'name', 'lastname']]
# hr09_12_df[(hr09_12_df['lastname'].str.contains("cubin"))][['congress', 'state', 'bioname', 'lastname', 'party_code']]

# lastname + state unique enough
# norton, lummis, herseth-sandlin, welch, young (AK), bordallo, christensen, pierluisi, castle
# rehberg, faleomavaega, pomeroy, cubin, fortuno

# some rows has their names as "Herseth" instead of "Herseth Sandlin"
null_districts.loc[null_districts['name'].str.contains('Herseth'), 'lastname'] = 'herseth-sandlin'

null_districts = null_districts.merge(hr09_12_main, how='left', left_on=['congress', 'state', 'lastname'],
                                      right_on=['congress', 'state', 'lastname'])

hr_sponsor_df = pd.concat([hr_sponsor_df, null_districts,
                           car_jul_bills, car_andr_bills]).drop(columns=['district'])
hr_sponsor_df = hr_sponsor_df.sort_values(by=['intro_date', 'bill_id']).reset_index(drop=True)

hr_sponsor_df.info()
needed_cols = ['bioguide_id', 'congress', 'intro_date', 'bill_id', 'bill_type',
               'subject', 'name', 'party_code', 'sponsor', 'cosponsor']

hr110_cosponsor_df = hr_sponsor_df[needed_cols]
hr110_cosponsor_df.to_csv(os.path.join(legis_int_path, 'processed_data', 'hr110_cosponsor.csv'),
                          index=False)



# Congress 111 bills

congress_id = 111

hr_sponsor_df = pd.read_csv(os.path.join(legis_int_path, 'processed_data',
                                         'congress{}_hr_bill.csv'.format(congress_id)))
# hr_sponsor_df.iloc[0]
lowercase_names = hr_sponsor_df['name'].str.lower()
hr_sponsor_df[['lastname', 'firstname']] = pd.DataFrame(lowercase_names.map(name_split).to_list(),
                                                        index=hr_sponsor_df.index)

# merging dataframes to get partisan data for cosponsor data
hr_sponsor_df.info()
bill_cols = hr_sponsor_df.columns

hr_sponsor_df = hr_sponsor_df.merge(hr09_12_main,
                                    how='left', left_on=['congress', 'state', 'lastname', 'district'],
                                    right_on=['congress', 'state', 'lastname', 'district_code'])

# split dataframe to patch in rows that don't have district code to merge on
null_districts = hr_sponsor_df.loc[(pd.isna(hr_sponsor_df['district_code'])), bill_cols]
hr_sponsor_df = hr_sponsor_df.loc[(~pd.isna(hr_sponsor_df['district_code']))]

# lastname + state unique enough
# norton, lummis, herseth-sandlin, sablan, welch, young (AK), bordallo, christensen, pierluisi, castle
# rehberg, faleomavaega, pomeroy
null_districts = null_districts.merge(hr09_12_main, how='left', left_on=['congress', 'state', 'lastname'],
                                      right_on=['congress', 'state', 'lastname'])

hr_sponsor_df = pd.concat([hr_sponsor_df, null_districts]).drop(columns=['district'])
hr_sponsor_df = hr_sponsor_df.sort_values(by=['intro_date', 'bill_id']).reset_index(drop=True)

# GRIFFITH, Parker switched party Dem -> Rep in Dec 22nd 2009
hr_sponsor_df.loc[(hr_sponsor_df['bioguide_id'] == "G000557") &
                  (hr_sponsor_df['intro_date'] > '2009-12-22'), 'party_code'] = 200

hr_sponsor_df.info()
needed_cols = ['bioguide_id', 'congress', 'intro_date', 'bill_id', 'bill_type',
               'subject', 'name', 'party_code', 'sponsor', 'cosponsor']
# hr_sponsor_df.drop_duplicates(subset=['bill_id'])
hr111_cosponsor_df = hr_sponsor_df[needed_cols]
hr111_cosponsor_df.to_csv(os.path.join(legis_int_path, 'processed_data', 'hr111_cosponsor.csv'),
                          index=False)

# mismatch_names = tmp.loc[(~pd.isna(tmp['district'])) &
#                          (pd.isna(tmp['district_code'])),
#                          ['congress', 'name', 'state', 'lastname', 'district']]
# mismatch_names.drop_duplicates(inplace=True)

# hr09_12_df[(hr09_12_df['lastname'].str.contains("pomeroy"))][['congress', 'state', 'bioname', 'lastname', 'party_code']]


# check "duplicate" representatives (candidates have the same last name in the same district)
# hr09_12_df[hr09_12_df.duplicated(['congress', 'state',
#                                   'lastname', 'district_code'])][['congress', 'state',
#                                                                   'lastname', 'district_code']]
# hr09_12_df[(hr09_12_df['congress'] == 112) & (hr09_12_df['state'] == 'NJ') &
#            (hr09_12_df['lastname'] == 'payne')][['district_code', 'party_code', 'bioname', 'bioguide_id']]





# Congress 112 bills

congress_id = 112

hr_sponsor_df = pd.read_csv(os.path.join(legis_int_path, 'processed_data',
                                         'congress{}_hr_bill.csv'.format(congress_id)))
# hr_sponsor_df.iloc[0]
lowercase_names = hr_sponsor_df['name'].str.lower()
hr_sponsor_df[['lastname', 'firstname']] = pd.DataFrame(lowercase_names.map(name_split).to_list(),
                                                        index=hr_sponsor_df.index)

# merging dataframes to get partisan data for cosponsor data
hr_sponsor_df.info()
bill_cols = hr_sponsor_df.columns

# PAYNE, Donald Milford (died March 6th 2012) vs PAYNE, Donald, Jr.
# extract rows for Payne Donald M. and Payne Donald Jr. because they overlap on years
paynem_bills = hr_sponsor_df.loc[(hr_sponsor_df['name'].str.contains("Payne")) &
                                 (hr_sponsor_df['intro_date'] < '2012-03-31')]

paynejr_bills = hr_sponsor_df.loc[(hr_sponsor_df['name'].str.contains("Payne")) &
                                  (hr_sponsor_df['intro_date'] > '2012-03-31')]

# hr09_12_df[(hr09_12_df['lastname'].str.contains("payne"))][['congress', 'bioname',
#                                                              'bioguide_id', 'party_code', 'district_code']]

# add data to PAYNE, Donald Milford rows
paynem_bills['party_code'] = 100
paynem_bills['district_code'] = 10
paynem_bills['bioguide_id'] = 'P000149'

# add data to PAYNE, Donald, Jr. rows
paynejr_bills['party_code'] = 100
paynejr_bills['district_code'] = 10
paynejr_bills['bioguide_id'] = 'P000604'

payne_inds = paynem_bills.index.to_list() + paynejr_bills.index.to_list()
hr_sponsor_df = hr_sponsor_df.drop(index=payne_inds)

hr_sponsor_df = hr_sponsor_df.merge(hr09_12_main,
                                    how='left', left_on=['congress', 'state', 'lastname', 'district'],
                                    right_on=['congress', 'state', 'lastname', 'district_code'])

# split dataframe to patch in rows that don't have district code to merge on
null_districts = hr_sponsor_df.loc[(pd.isna(hr_sponsor_df['district_code'])), bill_cols]
hr_sponsor_df = hr_sponsor_df.loc[(~pd.isna(hr_sponsor_df['district_code']))]

# null_districts.drop_duplicates(subset='name')[['congress', 'state', 'name', 'lastname']]
# hr09_12_df[(hr09_12_df['lastname'].str.contains("faleomavaega"))][['congress', 'state', 'bioname', 'lastname', 'party_code']]

# lastname + state unique enough
# norton, lummis, herseth-sandlin, sablan, welch, young (AK), bordallo, christensen, pierluisi, berg
# rehberg, faleomavaega, carney, noem
null_districts = null_districts.merge(hr09_12_main, how='left', left_on=['congress', 'state', 'lastname'],
                                      right_on=['congress', 'state', 'lastname'])

hr_sponsor_df = pd.concat([hr_sponsor_df, null_districts, paynem_bills, paynejr_bills]).drop(columns=['district'])
hr_sponsor_df = hr_sponsor_df.sort_values(by=['intro_date', 'bill_id']).reset_index(drop=True)

hr_sponsor_df.info()
needed_cols = ['bioguide_id', 'congress', 'intro_date', 'bill_id', 'bill_type',
               'subject', 'name', 'party_code', 'sponsor', 'cosponsor']
# hr_sponsor_df.drop_duplicates(subset=['bill_id'])
hr112_cosponsor_df = hr_sponsor_df[needed_cols]
hr112_cosponsor_df.to_csv(os.path.join(legis_int_path, 'processed_data', 'hr112_cosponsor.csv'),
                          index=False)



# combining (co)sponsorship data from congress 109 - 112
complete_sponsor = pd.concat([hr109_cosponsor_df, hr110_cosponsor_df,
                              hr111_cosponsor_df, hr112_cosponsor_df]).reset_index(drop=True)
complete_sponsor.to_csv(os.path.join(legis_int_path, 'processed_data', 'hr109_112_cosponsor.csv'),
                        index=False)