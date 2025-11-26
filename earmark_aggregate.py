import os
import numpy as np
import pandas as pd
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


earmark_cols = ['solo_amount', 'solo_num', 'solo_other_amount', 'solo_other_num']

# load congress members from target years
congress_df = pd.read_csv(os.path.join(legis_int_path, 'processed_data', 'target_congress_nominate.csv'))
congress_df.info()
hr10_11_df = congress_df.loc[(congress_df['chamber'] == 'House') &
                             ((congress_df['congress'] == 111) | (congress_df['congress'] == 110)),
                             ['bioname', 'bioguide_id', 'state_abbrev',
                              'party_code', 'district_code']]
hr10_11_df = hr10_11_df.drop_duplicates(subset='bioguide_id', keep='last').reset_index(drop=True)

lowercase_names = hr10_11_df['bioname'].str.lower()
hr10_11_df[['lastname', 'firstname']] = pd.DataFrame(lowercase_names.map(name_split).to_list(),
                                                     index=hr10_11_df.index)

hr10_11_df.columns = ['bioname', 'bioguide_id', 'state', 'party_code',
                      'district_code', 'lastname', 'firstname']
# hr10_11_df = hr10_11_df.drop(21)
# hr10_11_df[hr10_11_df['lastname'] == 'carson']
# Julia Carson in 2008 data
# Andre Carson in 2009 and 2010 data

# Young Don = Young Donal Edwin
# Scott Bobby = Scott, Robert Cortez
# Rogers Harold = Rogers Hal
# Davis Tom = Davis, Thomas M., III
# Ryan Tim = Ryan, Timothy
# Thompson Mike = Thompson Michael
# Sanchez Linda = Sánchez Linda
# Fortuno = Fortuño
# Bono Mary = Bono-mack
# Velazquez = Velázquez
# Gutierrez = Gutiérrez

# Duncan Lee Hunter (CA) (1993–2009)
# Duncan Duane Hunter (CA) (2009-2013)

hr10_11_df.at[3, 'firstname'] = 'duncan-l'
hr10_11_df.at[66, 'firstname'] = 'tom'
hr10_11_df.at[76, 'firstname'] = 'don'
hr10_11_df.at[120, 'firstname'] = 'duncan-d'
hr10_11_df.at[141, 'firstname'] = 'mike'
hr10_11_df.at[243, 'firstname'] = 'harold'
hr10_11_df.at[387, 'firstname'] = 'tim'
hr10_11_df.at[497, 'firstname'] = 'bobby'
hr10_11_df.at[60, 'lastname'] = 'fortuno'
hr10_11_df.at[109, 'lastname'] = 'sanchez'
hr10_11_df.at[139, 'lastname'] = 'bono-mack'
hr10_11_df.at[217, 'lastname'] = 'gutierrez'
hr10_11_df.at[361, 'lastname'] = 'velazquez'





# load earmark data by representatives (2008), which is similar to 2010
state_maps = {'Rl': 'RI', 'Ml': 'MI', 'Wl': 'WI'}

earmark_reps_df = pd.read_excel(os.path.join(legis_int_path, 'FY2008_Earmarks.xlsx'),
                                sheet_name='Reps.', header=3)

earmark_reps_df.drop(columns=['Unnamed: 3', 'Unnamed: 6', 'Unnamed: 9'], inplace=True)
earmark_reps_df.info()

rename_cols = ['representative', 'solo_amount', 'solo_num',
               'solo_other_amount', 'solo_other_num',
               'solo_other_pres_amount', 'solo_other_pres_num']
earmark_reps_df.columns = rename_cols

ps_series = earmark_reps_df['representative'].str.split('(').str[-1].str.replace(')', '', regex=False)
earmark_reps_df[['party', 'state']] = ps_series.str.split('-', expand=True)[[0, 1]]

# correcting state abbreviation
earmark_reps_df['state'] = earmark_reps_df['state'].map(lambda state: state_maps[state] if state in state_maps else state)

repname_series = earmark_reps_df['representative'].str.replace('\s+(\(.*?\))', '', regex=True).str.lower()

earmark_reps_df[['lastname', 'firstname']] = pd.DataFrame(repname_series.map(name_split).to_list(),
                                                          index=earmark_reps_df.index)
earmark_reps_df['lastname'] = earmark_reps_df['lastname'].str.replace('*', '')


earmark2008 = earmark_reps_df[['representative', 'state', 'party', 'lastname',
                               'firstname'] + earmark_cols]

# Marilyn Musgrave (CO)
earmark2008.at[65, 'party'] = 'R'
earmark2008.at[65, 'state'] = 'CO'

# Virgil Hamlin Goode (VA)
earmark2008.at[178, 'party'] = 'R'
earmark2008.at[178, 'state'] = 'VA'

# manually edit firstnames to match the other dataset
earmark2008.at[56, 'firstname'] = 'mike'
earmark2008.at[161, 'firstname'] = 'timothy'
earmark2008.at[227, 'firstname'] = 'edward'
earmark2008.at[253, 'firstname'] = 'mike'
earmark2008.at[314, 'firstname'] = 'hank'
earmark2008.at[362, 'firstname'] = 'julia'
earmark2008.at[218, 'firstname'] = 'duncan-l'

# merge earmark with complete congress file
# split into 2 sets (last + first + state, last + state)
earmark2008_firstreq = earmark2008[~pd.isna(earmark2008['firstname'])]
earmark2008_laststate = earmark2008[(pd.isna(earmark2008['firstname']))]

earmark2008_firstreq = earmark2008_firstreq[['lastname', 'firstname',
                                             'state'] + earmark_cols].merge(hr10_11_df, how='left',
                                                                            on=['lastname', 'firstname', 'state'])

earmark2008_laststate = earmark2008_laststate[['lastname', 'state'] + earmark_cols].merge(hr10_11_df, how='left',
                                                                                          on=['lastname', 'state'])

earmark2008_firstreq = earmark2008_firstreq[['bioname', 'bioguide_id', 'state',
                                             'party_code'] + earmark_cols]
earmark2008_laststate = earmark2008_laststate[['bioname', 'bioguide_id', 'state',
                                               'party_code'] + earmark_cols]

earmark2008 = pd.concat([earmark2008_firstreq, earmark2008_laststate])
earmark2008['year'] = 2008






# load earmark data by representatives (2009)
earmark_reps_df = pd.read_excel(os.path.join(legis_int_path, 'FY2009_Earmarks.xls'),
                                sheet_name='Reps.', header=0)
earmark_reps_df.drop(columns=['Unnamed: 3', 'Unnamed: 6'], inplace=True)
earmark_reps_df.info()

rename_cols = ['representative', 'solo_amount', 'solo_num',
               'solo_other_amount', 'solo_other_num',
               'solo_other_pres_amount', 'solo_other_pres_num']
earmark_reps_df.columns = rename_cols

# filter out 2 extra rows at the bottom
earmark_reps_df = earmark_reps_df.iloc[:452]
earmark_reps_df['representative'] = earmark_reps_df['representative'].str.replace('*', '')

# extract states from parentheses (2 rows)
earmark_reps_df['state'] = earmark_reps_df['representative'].str.extract('.*\((.*)\).*')
earmark_reps_df[~pd.isna(earmark_reps_df['state'])]


repname_series = earmark_reps_df['representative'].str.replace('\s+(\(.*?\))', '', regex=True).str.lower()
earmark_reps_df[['lastname', 'firstname']] = pd.DataFrame(repname_series.map(name_split).to_list(),
                                                          index=earmark_reps_df.index)

earmark2009 = earmark_reps_df.copy()
earmark2009.loc[150, 'firstname'] = 'duncan-l'
earmark2009.loc[266, 'firstname'] = 'samuel'
earmark2009.at[269, 'firstname'] = 'timothy'
earmark2009.at[351, 'firstname'] = 'andré'

# merge earmark with complete congress file
# split into 3 sets (last + first + state, last + first, last only)
earmark2009_statereq = earmark2009[~pd.isna(earmark2009['state'])]
earmark2009_lastfirst = earmark2009[pd.isna(earmark2009['state']) & (~pd.isna(earmark2009['firstname']))]
earmark2009_lastonly = earmark2009[pd.isna(earmark2009['state']) & (pd.isna(earmark2009['firstname']))]

earmark2009_statereq = hr10_11_df.merge(earmark2009_statereq[['lastname', 'firstname', 'state'] + earmark_cols],
                                        how='inner', on=['lastname', 'firstname', 'state'])

earmark2009_lastfirst = earmark2009_lastfirst[['lastname',
                                               'firstname'] + earmark_cols].merge(hr10_11_df, how='left',
                                                                                  on=['lastname', 'firstname'])

earmark2009_lastonly = earmark2009_lastonly[['lastname'] + earmark_cols].merge(hr10_11_df, how='left',
                                                                               on='lastname')

# Young Don = Young Donal Edwin
# Scott Bobby = Robert Cortez Scott
# Rogers Harold = Rogers Hal
# Davis Tom = Davis, Thomas M., III
# Ryan Tim = Ryan, Timothy
# Thompson Mike = Thompson Michael
# Murphy Tim = Murphy Timothy
# Sanchez Linda = Sánchez Linda
# Graves Samuel (MO)
# Carson André (2008) Preceded by Carson Julia May

earmark2009_statereq = earmark2009_statereq[['bioname', 'bioguide_id', 'state',
                                             'party_code'] + earmark_cols]
earmark2009_lastfirst = earmark2009_lastfirst[['bioname', 'bioguide_id', 'state',
                                               'party_code'] + earmark_cols]
earmark2009_lastonly = earmark2009_lastonly[['bioname', 'bioguide_id', 'state',
                                             'party_code'] + earmark_cols]

earmark2009 = pd.concat([earmark2009_statereq, earmark2009_lastfirst, earmark2009_lastonly])
earmark2009['year'] = 2009






# load earmark data by representatives (2010)
state_maps = {'Rl': 'RI', 'Ml': 'MI', 'Wl': 'WI'}

earmark_reps_df = pd.read_excel(os.path.join(legis_int_path, 'FY2010_Earmarks.xls'),
                                sheet_name='Reps.', header=3)
earmark_reps_df.drop(columns=['Unnamed: 3', 'Unnamed: 6'], inplace=True)
earmark_reps_df.info()

rename_cols = ['representative', 'solo_amount', 'solo_num',
               'solo_other_amount', 'solo_other_num',
               'solo_other_pres_amount', 'solo_other_pres_num']
earmark_reps_df.columns = rename_cols

# filter out 2 extra rows at the bottom
earmark_reps_df = earmark_reps_df.iloc[:443]

ps_series = earmark_reps_df['representative'].str.split('(').str[-1].str.replace(')', '', regex=False)
earmark_reps_df[['party', 'state']] = ps_series.str.split('-', expand=True)[[0, 1]]

# correcting state abbreviation
earmark_reps_df['state'] = earmark_reps_df['state'].map(lambda state: state_maps[state] if state in state_maps else state)

repname_series = earmark_reps_df['representative'].str.replace('\s+(\(.*?\))', '', regex=True).str.lower()

earmark_reps_df[['lastname', 'firstname']] = pd.DataFrame(repname_series.map(name_split).to_list(),
                                                          index=earmark_reps_df.index)
earmark_reps_df['lastname'] = earmark_reps_df['lastname'].str.replace('*', '')

earmark2010 = earmark_reps_df[['representative', 'state', 'party', 'lastname',
                               'firstname'] + earmark_cols]

# manually edit firstnames to match the other dataset
earmark2010.at[31, 'firstname'] = 'edward'
earmark2010.at[57, 'firstname'] = 'mike'
earmark2010.at[131, 'firstname'] = 'mike'
earmark2010.at[177, 'firstname'] = 'hank'
earmark2010.at[243, 'firstname'] = 'timothy'
earmark2010.at[314, 'firstname'] = 'andré'
earmark2010.at[336, 'firstname'] = 'duncan-d'

earmark2010.at[303, 'lastname'] = 'luján'

# merge earmark with complete congress file
# split into 2 sets (last + first + state, last + state)
earmark2010_firstreq = earmark2010[~pd.isna(earmark2010['firstname'])]
earmark2010_laststate = earmark2010[(pd.isna(earmark2010['firstname']))]

earmark2010_firstreq = earmark2010_firstreq[['lastname', 'firstname',
                                             'state'] + earmark_cols].merge(hr10_11_df, how='left',
                                                                            on=['lastname', 'firstname', 'state'])

earmark2010_laststate = earmark2010_laststate[['lastname', 'state'] + earmark_cols].merge(hr10_11_df, how='left',
                                                                                          on=['lastname', 'state'])

# Young Don = Young Donal Edwin
# Scott Bobby = Robert Cortez Scott
# Rogers Harold = Rogers Hal
# Ryan Tim = Ryan, Timothy
# Thompson Mike = Thompson Michael
# Sanchez Linda = Sánchez Linda
# Kennedy Patrick (*Rl -> RI)
# Markey Ed = Markey Edward
# Rogers Michale (AL & Ml) = Rogers Mike (*Ml -> MI)
# Johnson Henry = Johnson Hank
# Moore Gwen (*Wl -> WI)
# Levin Sander (*Ml -> MI)
# Bishop Tim = Bishop Timothy
# Miller Candice (*Ml -> MI)
# Ryan Paul (*Wl -> WI)
# Luhan = luján

earmark2010_firstreq = earmark2010_firstreq[['bioname', 'bioguide_id', 'state',
                                             'party_code'] + earmark_cols]
earmark2010_laststate = earmark2010_laststate[['bioname', 'bioguide_id', 'state',
                                               'party_code'] + earmark_cols]

earmark2010 = pd.concat([earmark2010_firstreq, earmark2010_laststate])
earmark2010['year'] = 2010




# combine earmark data from 2008-2010 and save the data
complete_earmark = pd.concat([earmark2008, earmark2009, earmark2010])
complete_earmark['other_num'] = complete_earmark['solo_other_num'] - complete_earmark['solo_num']
complete_earmark['other_amount'] = complete_earmark['solo_other_amount'] - complete_earmark['solo_amount']
complete_earmark = complete_earmark[['bioname', 'bioguide_id', 'state', 'party_code',
                                     'year', 'solo_num', 'other_num', 'solo_amount', 'other_amount']]

complete_earmark.to_csv(os.path.join(legis_int_path, 'processed_data', 'earmark_2008_2010.csv'),
                        index=False)