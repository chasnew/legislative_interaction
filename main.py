# Data retrieved using scripts from https://github.com/unitedstates/congress
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
import numpy as np
import pandas as pd
from itertools import product
import matplotlib.pyplot as plt
import seaborn as sns
import yaml

import datetime
import pytz

# 114th (2015-2016)
# 113th (2013-2014)
# 112th (2011-2012)
# 111th (2009-2010)
# 110th (2007-2008)
# 109th (2005-2006)
# 108th (2003-2004)

# home_path = '/Users/chanuwasaswamenakul/'
# box_path = os.path.join(home_path, 'Library', 'CloudStorage', 'Box-Box')

with open("config.yaml") as f:
    config_param = yaml.load(f, Loader=yaml.SafeLoader)

legis_int_path = config_param['legis_int_path']

def process_congress():
    congress_df = pd.read_csv(os.path.join(legis_int_path, 'HSall_members.csv'))
    congress_df = congress_df[(congress_df['congress'] >= 108) & (congress_df['congress'] < 115)]

    select_cols = ['congress', 'chamber', 'district_code', 'state_abbrev', 'party_code',
                   'bioname', 'bioguide_id', 'nominate_dim1', 'nominate_dim2']
    congress_df[select_cols].to_csv(os.path.join(legis_int_path, 'processed_data',
                                                 'target_congress_nominate.csv'),
                                    index=False)

# process_congress()

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

# rep_maps = {'Diaz-Balart, L.': 'Diaz-Balart, Lincoln',
#             'Diaz-Balart, M.': 'Diaz-Balart, Mario',
#             'Murphy, Tim': 'Murphy, Timothy',
#             'Sánchez, Linda T.': 'Sanchez, Linda'}

congress_list = [109, 110, 111, 112]
major_party = {109: 'R', 110: 'D', 111: 'D', 112: 'R'}

party_map = {200: 'R', 100: 'D', 328: 'I'}

# hr111_sponsor_df = pd.read_csv(os.path.join(legis_int_path, 'processed_data', 'hr111_cosponsor.csv'))
complete_cosponsor = pd.read_csv(os.path.join(legis_int_path, 'processed_data', 'hr109_112_cosponsor.csv'))

spon_reduced_cols = ['congress', 'bill_id', 'bioguide_id', 'name', 'party_code']
unique_sponsor = complete_cosponsor.loc[complete_cosponsor['sponsor'], spon_reduced_cols]

congress_df_list = []
for i in range(len(congress_list)):
    congress_id = congress_list[i]
    congress_vote_df = pd.read_csv(os.path.join(legis_int_path, 'processed_data',
                                                'congress{}_vote.csv'.format(congress_id)))
    congress_df_list.append(congress_vote_df)

congress_vote_df = pd.concat(congress_df_list)

# congress_id = 110
# congress_vote_df = pd.read_csv(os.path.join(legis_int_path, 'processed_data',
#                                             'congress{}_vote.csv'.format(congress_id)))

bill_hr_vote_df = congress_vote_df[(congress_vote_df['chamber'] == 'h') &
                                   (congress_vote_df['bill_type'] == 'hr') &
                                   ((congress_vote_df['vote'] != 'Present'))]
bill_hr_vote_df.info()
bill_hr_vote_df['datetime'] = pd.to_datetime(bill_hr_vote_df['date'])
bill_hr_vote_df['date'] = pd.to_datetime(bill_hr_vote_df['date'].str.replace('T(.*)', '', regex=True))
bill_hr_vote_df['year'] = bill_hr_vote_df['date'].dt.year

# Remove whitespace followed by "(state)"
bill_hr_vote_df['name'] = bill_hr_vote_df['name'].str.replace('\s+(\(.*?\))', '', regex=True)

# bill_hr_vote_df['category'].unique()
# bill_hr_vote_df['vote'].unique()

# parse name columns to extract lastname and first name (if available)
bill_hr_vote_df[['lastname', 'firstname']] = pd.DataFrame(bill_hr_vote_df['name'].map(name_split).to_list(),
                                                          index=bill_hr_vote_df.index)

# check number of latest vote on a bill in different categories
# tmp = bill_hr_vote_df.sort_values('datetime').groupby(['bill_number', 'congress'])[['datetime', 'category']].last()
# tmp['category'].value_counts()

# filter for passage and passage suspension bills (convert to numpy to avoid recurring warning)
bill_pass_mask = ((bill_hr_vote_df['category'] == 'passage') |
                  (bill_hr_vote_df['category'] == 'passage-suspension')).to_numpy()

passage_bills = bill_hr_vote_df[bill_pass_mask]

# checking participation rate of each legislator
# tmp = passage_bills.groupby(['vote_id', 'vote'])[['vote']].count()
# tmp.columns = ['vcount']
# tmp.reset_index(inplace=True)
# not_vote = tmp[tmp['vote'] == "Not Voting"]
#
# no_presence = passage_bills.groupby(['vote_id'])[['vote']].count()
# (no_presence['vote'] >= 430).sum()


# bill_hr_vote_df[bill_hr_vote_df['vote_id'] == 'h294-113.2013']['category']

# bill_vote_count = passage_bills.groupby(['congress', 'bill_number'])['bill_number'].count()
# dup_masks = bill_vote_count > 435
# dup_bills = bill_vote_count.index[dup_masks]
# list(dup_bills)
#
# multi_dup_masks = bill_vote_count > 869
# multi_dup_bills = bill_vote_count.index[multi_dup_masks]
# list(multi_dup_bills)
#
# passage_bills[(passage_bills['congress'] == 109) &
#               (passage_bills['bill_number'] == 3010)].groupby(['vote_id', 'vote'])['vote'].count()

# keep only last vote session of a bill
unique_vote_ids = passage_bills.sort_values('datetime').drop_duplicates(['congress', 'bill_number'],
                                                                        keep='last')[['vote_id']]
last_bill_vote_df = unique_vote_ids.merge(passage_bills, how='left', on='vote_id')
last_bill_vote_df = last_bill_vote_df[last_bill_vote_df['year'] < 2013]
last_bill_vote_df['bill_id'] = 'hr' + last_bill_vote_df['bill_number'].astype(int).astype(str) +\
                               '-' + last_bill_vote_df['congress'].astype(str)


# voted_bills = last_bill_vote_df.drop_duplicates(subset=['congress', 'bill_number'])[['congress', 'bill_number']]
# voted_bills['bill_id'] = 'hr' + voted_bills['bill_number'].astype(int).astype(str) +\
#                          '-' + voted_bills['congress'].astype(str)
#
# tmp = voted_bills.merge(unique_sponsor[['bill_id', 'party_code']], how='left', on='bill_id')
# tmp['party_code'].value_counts() # 308 Demcrat bills + 67 Republican bills
# tmp.groupby(['congress', 'party_code'])[['bill_id']].count()

# check unique legislators that hold seats pre- and post- earmark ban
short_bvote_df = last_bill_vote_df[last_bill_vote_df['year'] > 2008]

# 2005/2007 - 2012
unique_legis = last_bill_vote_df.groupby(['year', 'l_id'])[['name']].first().reset_index()
year_count = unique_legis.groupby('l_id')[['year']].count()
(year_count == 6).sum() # 292
(year_count == 8).sum() # 256

# 2009 - 2012
unique_short_ls = short_bvote_df.groupby(['year', 'l_id'])[['name']].first().reset_index()
(unique_short_ls.groupby('l_id')[['year']].count() == 4).sum() # 331



# count the number of votes for each bill from each party
agg_bill_vote = last_bill_vote_df.groupby(['vote_id', 'party', 'vote']).agg({'congress': 'first',
                                                                             'bill_id': 'first',
                                                                             'date': 'first',
                                                                             'vote': 'count'})
agg_bill_vote.columns = ['congress', 'bill_id', 'date', 'vcount']
vote_sum = agg_bill_vote.groupby(['vote_id', 'party'])['vcount'].transform('sum') # create repeated warnings
agg_bill_vote['prop'] = agg_bill_vote['vcount'] / vote_sum
agg_bill_vote = agg_bill_vote.reset_index()

last_bill_vote_df
last_bill_vote_df[(last_bill_vote_df['date'] < '2005-09-01')]
agg_bill_vote[(agg_bill_vote['date'] > '2005-07-31') & (agg_bill_vote['date'] < '2005-09-01')]

# identify majority vote response of each party
max_ids = agg_bill_vote.groupby(['vote_id', 'party'])['vcount'].idxmax()
major_votes = agg_bill_vote.loc[max_ids, ['congress', 'bill_id', 'date',
                                          'vote_id', 'party', 'vote', 'vcount']]
major_votes.columns = ['congress', 'bill_id', 'date', 'vote_id', 'party', 'major_vote', 'vcount']
major_votes['major_party'] = major_votes['congress'].map(major_party)

major_votes = major_votes.merge(unique_sponsor[['bill_id', 'party_code']], how='left', on='bill_id')
major_votes['party_code'] = major_votes['party_code'].map(party_map)

major_votes.columns = ['congress', 'bill_id', 'date', 'vote_id', 'party',
                       'major_vote', 'vcount', 'major_party', 'bill_party']

# actually not needed now
# major_votes.to_csv(os.path.join(legis_int_path, 'processed_data', 'party_major_vote.csv'),
#                    index=False)


# check whether the majority votes of Dem and Rep agree with each other
piv_major_vote = major_votes.pivot(index=['vote_id', 'bill_id', 'date'], columns='party', values='major_vote')
piv_major_vote['dr_agree'] = (piv_major_vote['D'] == piv_major_vote['R'])
piv_major_vote['dr_approve'] = ((piv_major_vote['D'] == 'Yea') & (piv_major_vote['R'] == 'Yea'))

# Proportion of both parties agreeing on a bill by week / month / year
piv_major_vote = piv_major_vote.reset_index()
dr_magree_df = piv_major_vote.groupby(pd.Grouper(key='date', freq='M'))[['dr_agree', 'dr_approve']].mean()
dr_magree_df = dr_magree_df.reset_index()

plt.figure(figsize=(12,6))
ax = sns.lineplot(x='date', y='dr_approve', alpha=0.7, data=dr_magree_df)
ax.set(ylabel='Dem-Rep agreement')
plt.savefig('img/monthly_yea_prop_congress{}.png'.format(congress_id))


# The party supports bills sponsored by the other party
xpartisan_supprt = major_votes[major_votes['party'] != major_votes['bill_party']]
xpartisan_supprt['support'] = (xpartisan_supprt['major_vote'] == 'Yea')
xpartisan_supprt = xpartisan_supprt.groupby(pd.Grouper(key='date', freq='M'))[['support']].mean().reset_index()
xpartisan_supprt.columns = ['date', 'xparty_support']

plt.figure(figsize=(12,6))
ax = sns.lineplot(x='date', y='support', alpha=0.7, data=xpartisan_supprt)
ax.set(ylabel='Cross-partisan support rate')
plt.show()


# minority cooperation with majority house
maj_pow_votes = major_votes[['vote_id', 'party', 'major_party', 'major_vote']]
maj_pow_votes = maj_pow_votes[maj_pow_votes['party'] == maj_pow_votes['major_party']]

maj_bill_vote = agg_bill_vote.merge(maj_pow_votes[['vote_id', 'major_party', 'major_vote']],
                                    how='left', on=['vote_id'])
minor_agree = maj_bill_vote[(maj_bill_vote['party'] != maj_bill_vote['major_party']) &
                            (maj_bill_vote['vote'] == maj_bill_vote['major_vote'])]
minor_agree = minor_agree.groupby(pd.Grouper(key='date', freq='M'))[['prop']].mean().reset_index()
minor_agree.columns = ['date', 'minor_house_prop']

plt.figure(figsize=(12,6))
ax = sns.lineplot(x='date', y='minor_house_prop', alpha=0.7, data=minor_agree)
ax.set(ylabel='Minority cooperation rate')
plt.show()

# identify majority cluster party of each bill (party that can come together and out-vote the other)
# most likely will correlate with majority party of the house
max_ids = agg_bill_vote.groupby(['vote_id'])['vcount'].idxmax()
major_clust = agg_bill_vote.loc[max_ids, ['bill_id', 'date', 'vote_id', 'party', 'vote']]
major_clust.columns = ['bill_id', 'date', 'vote_id', 'major_party', 'major_vote']

maj_clust_vote = agg_bill_vote.merge(major_clust[['vote_id', 'major_party', 'major_vote']],
                                     how='left', on=['vote_id'])
minorc_agree = maj_bill_vote[(maj_clust_vote['party'] != maj_clust_vote['major_party']) &
                            (maj_clust_vote['vote'] == maj_clust_vote['major_vote'])]
minorc_agree = minorc_agree.groupby(pd.Grouper(key='date', freq='M'))[['prop']].mean().reset_index()
minorc_agree.columns = ['date', 'minor_cluster_prop']

plt.figure(figsize=(12,6))
ax = sns.lineplot(x='date', y='minor_cluster_prop', alpha=0.7, data=minorc_agree)
ax.set(ylabel='Minority cooperation rate')
plt.show()


# monthly cooperation w/ various measures
month_coop_df = minor_agree.merge(minorc_agree, how='left', on=['date'])
month_coop_df = month_coop_df.merge(dr_magree_df, how='left', on=['date'])
month_coop_df = month_coop_df.merge(xpartisan_supprt, how='left', on=['date'])
month_coop_df = month_coop_df.melt(id_vars=['date'], value_vars=['minor_house_prop', 'minor_cluster_prop',
                                                                 'xparty_support', 'dr_agree'],
                                   var_name='measure', value_name='prop')

plt.figure(figsize=(12,6))
ax = sns.lineplot(x='date', y='prop', hue='measure', alpha=0.5, data=month_coop_df)
ax.set(ylabel='Minority cooperation rate')
# plt.show()
plt.savefig('img/monthly_coop_congress110_112.png')

month_coop_df.to_csv(os.path.join(legis_int_path, 'processed_data', 'monthly_coop.csv'),
                     index=False)


# yearly cooperation w/ various measures
## cross-partisan agreement
dr_yagree_df = piv_major_vote.groupby(pd.Grouper(key='date', freq='Y'))[['dr_agree']].mean()
dr_yagree_df = dr_yagree_df.reset_index()

## cross-partisan support
xpartisan_supprt = major_votes[major_votes['party'] != major_votes['bill_party']]
xpartisan_supprt['support'] = (xpartisan_supprt['major_vote'] == 'Yea')
xpartisan_supprt = xpartisan_supprt.groupby(pd.Grouper(key='date', freq='Y'))[['support']].mean().reset_index()
xpartisan_supprt.columns = ['date', 'xparty_support']

## minority cooperation
maj_pow_votes = major_votes[['vote_id', 'party', 'major_party', 'major_vote']]
maj_pow_votes = maj_pow_votes[maj_pow_votes['party'] == maj_pow_votes['major_party']]

maj_bill_vote = agg_bill_vote.merge(maj_pow_votes[['vote_id', 'major_party', 'major_vote']],
                                    how='left', on=['vote_id'])
minor_agree = maj_bill_vote[(maj_bill_vote['party'] != maj_bill_vote['major_party']) &
                            (maj_bill_vote['vote'] == maj_bill_vote['major_vote'])]
minor_agree = minor_agree.groupby(pd.Grouper(key='date', freq='Y'))[['prop']].mean().reset_index()
minor_agree.columns = ['date', 'minor_house_prop']

year_coop_df = minor_agree.merge(dr_yagree_df, how='left', on=['date'])
year_coop_df = year_coop_df.merge(xpartisan_supprt, how='left', on=['date'])
year_coop_df = year_coop_df.melt(id_vars=['date'], value_vars=['minor_house_prop', 'xparty_support', 'dr_agree'],
                                 var_name='measure', value_name='prop')

year_coop_df.to_csv(os.path.join(legis_int_path, 'processed_data', 'yearly_coop.csv'),
                    index=False)



# vote x earmarking analysis

# load earmark data
complete_earmark = pd.read_csv(os.path.join(legis_int_path, 'processed_data',
                                            'earmark_2008_2010.csv'))
complete_earmark.info()
complete_earmark['shift_year'] = complete_earmark['year'] - 1

# FUDGE, Marcia L. joined congress in November 2008
fm_row_tmp = complete_earmark.loc[1222]
fm_row_tmp['shift_year'] = 2008
fm_row_tmp['year'] = 2009
fm_row_tmp['solo_num'] = 0
fm_row_tmp['other_num'] = 0
complete_earmark = pd.concat([complete_earmark, fm_row_tmp.to_frame().T], ignore_index=True)

def fiscal_year(date):
    cur_year = date.year
    if date >= pd.to_datetime('-'.join([str(cur_year), '10', '01'])):
        return cur_year+1
    else:
        return cur_year

# last_bill_vote_df['fiscal_year'] = last_bill_vote_df['date'].map(fiscal_year)

# sns.scatterplot(x='solo_num', y='other_num', data=complete_earmark)
# plt.show()

# keep legislators that exists for 6 / 8 years (ideally 2005 / 2007 - 2012)
unique_legis = last_bill_vote_df.groupby(['year', 'l_id'])[['name']].first().reset_index()
stable_legis = unique_legis.groupby('l_id')[['year']].count().reset_index()
stable_legis = stable_legis[stable_legis['year'] >= 8] # 256 / 292
stable_legis.drop(columns=['year'], inplace=True)

# merge long-stay legislators with earmark data
stable_legis = stable_legis.merge(complete_earmark[['bioguide_id', 'solo_num', 'other_num', 'shift_year']],
                                  how='left', left_on='l_id', right_on='bioguide_id').drop(columns=['bioguide_id'])

stable_legis['cat_year'] = stable_legis['shift_year'].astype(str)
# stable_legis[['l_id', 'solo_num', 'other_num', 'cat_year']].to_csv(os.path.join(legis_int_path, 'processed_data',
#                                                                                 'house_yearmark_data2005_2012.csv'),
#                                                                    index=False)

# plt.figure(figsize=(12,6))
# ax = sns.histplot(data=stable_legis, x='solo_num', hue='cat_year', bins=50, alpha=0.5)
# ax.set(xlabel='Number of solo earmarks')
# plt.show()
# plt.savefig('img/earmark_dist_2007_2009.png')

# tmp = stable_legis.groupby('l_id')[['solo_num']].count()
# short_l = list(tmp[tmp['solo_num'] < 3].index)
# stable_legis[stable_legis['l_id'].isin(short_l)]


def classify_earmarkers(earmark_pct):
    if earmark_pct < 0.2:
        return '1'
    elif earmark_pct >= 0.2 and earmark_pct < 0.4:
        return '2'
    elif earmark_pct >= 0.4 and earmark_pct < 0.6:
        return '3'
    elif earmark_pct >= 0.6 and earmark_pct < 0.8:
        return '4'
    else:
        return '5'

def median_earmarkers(earmark_pct):
    if earmark_pct < 0.5:
        return 'low'
    else:
        return 'high'


stable_legis.info()
last_bill_vote_df.info()

# earmark_num_array = stable_legis['solo_num'].to_numpy()
stable_legis['total_num'] = stable_legis['solo_num'] + stable_legis['other_num']
avg_earmark = stable_legis.groupby('l_id')[['solo_num', 'total_num']].sum().reset_index()
avg_earmark['solo_pct'] = avg_earmark['solo_num'].rank(pct=True)
avg_earmark['total_pct'] = avg_earmark['total_num'].rank(pct=True)
avg_earmark['earmark_class'] = avg_earmark['total_pct'].map(classify_earmarkers)
avg_earmark['earmark_use'] = avg_earmark['total_pct'].map(median_earmarkers)

# sns.scatterplot(x='solo_num', y='earmark_pct', data=stable_legis)
# plt.show()

# unique_legis = stable_legis['l_id'].unique()
# pre_year_list = list(range(2005, 2007))
# first_year_list = list(range(2007, 2010))
# second_year_list = list(range(2010, 2013))
# year_list = list(range(2005, 2013))

# merge data between 2005-2006 that has more complete earmark data
# pre_le_df = pd.DataFrame(list(product(unique_legis, pre_year_list)), columns=['l_id', 'year'])
# legis_earm2007 = stable_legis[stable_legis['shift_year'] == 2007][['l_id', 'solo_num', 'earmark_class']]
# pre_le_df = pre_le_df.merge(legis_earm2007, how='left', on='l_id')

# merge data between 2007-2009 that has more complete earmark data
# first_le_df = pd.DataFrame(list(product(unique_legis, first_year_list)), columns=['l_id', 'year'])
# first_le_df = first_le_df.merge(stable_legis, how='left', left_on=['l_id', 'year'],
#                                 right_on=['l_id', 'shift_year']).drop(columns=['shift_year'])
# first_le_df.drop(columns=['cat_year', 'earmark_pct'], inplace=True)

# imputate earmark data in 2010-2013 with average earmark numbers in 2008-2009
# second_le_df = pd.DataFrame(list(product(unique_legis, second_year_list)), columns=['l_id', 'year'])
# legis_earm2009 = stable_legis[stable_legis['shift_year'] == 2009][['l_id', 'solo_num', 'earmark_class']]
# second_le_df = second_le_df.merge(legis_earm2009, how='left', on='l_id')

# complete_le_df = pd.DataFrame(list(product(unique_legis, year_list)), columns=['l_id', 'year'])
# complete_le_df = complete_le_df.merge(avg_earmark, how='left', on='l_id')

# merge earmark data with vote data
earmark_bill_vote = last_bill_vote_df.merge(avg_earmark, how='inner', on='l_id')
earmark_bill_vote = earmark_bill_vote[['vote_id', 'date', 'l_id', 'party', 'bill_id',
                                       'vote', 'year', 'solo_num', 'total_num',
                                       'solo_pct', 'total_pct']]

# NOW measuring cooperation on whether they support bills sponsored by the other party
# originally used major power vote data to anchor cooperation (merged with maj_pow_votes or major_clust)
earmark_bill_vote = earmark_bill_vote.merge(unique_sponsor[['bill_id', 'party_code']],
                                            how='left', on='bill_id')
earmark_bill_vote['party_code'] = earmark_bill_vote['party_code'].map(party_map)
earmark_bill_vote.rename(columns={'party_code': 'bill_party'}, inplace=True)

# earmark_bill_vote = earmark_bill_vote.merge(maj_pow_votes[['vote_id', 'major_party', 'major_vote']],
#                                             how='left', on='vote_id')

earmark_bill_vote.to_csv(os.path.join(legis_int_path, 'processed_data',
                                      'indiv_vote-x-earmark_data2005_2012.csv'),
                         index=False)

def earmark_period(year):
    if year < 2007:
        return 'soft'
    elif year >= 2007 and year < 2011:
        return 'hard'
    else:
        return 'ban'

# Focus on a specified party when they're minority
xpartisan_vote = earmark_bill_vote[(earmark_bill_vote['party'] != earmark_bill_vote['bill_party'])].reset_index(drop=True)

earmark_bill_vote.info()
# tmp = dem_minor_vote.groupby(['year', 'l_id'])[['vote_id']].count().reset_index()
# tmp.groupby('year')['vote_id'].max() # 6 + 6 + 1 = 13 sessions from 2007-2010

xpartisan_vote['coop'] = (xpartisan_vote['vote'] == 'Yea')
xpartisan_vote['earmark_period'] = xpartisan_vote['year'].map(earmark_period)

# Focus specifically on the minority
# minor_earmark_vote = earmark_bill_vote[(earmark_bill_vote['party'] != earmark_bill_vote['major_party'])].reset_index(drop=True)
# minor_earmark_vote['coop'] = (minor_earmark_vote['vote'] == minor_earmark_vote['major_vote'])
# minor_earmark_vote['earmark_period'] = minor_earmark_vote['year'].map(earmark_period)


# scatter plot between earmark numbers and cooperation rate (at the year level)
# This only focuses on the minor power cluster
tmp = complete_earmark[['bioguide_id', 'shift_year', 'solo_num', 'other_num']]
tmp.columns = ['l_id', 'year', 'solo_num', 'other_num']

real_earmark_coop = last_bill_vote_df.merge(tmp, how='left', on=['l_id', 'year'])
# tmp = tmp.groupby('l_id')[['solo_num', 'other_num']].sum()
# real_earmark_coop = last_bill_vote_df.merge(tmp, how='left', on=['l_id'])

# pre-ban (2007 - 2009) >= 2007 & < 2010
# post-ban (2011 - 2012) >= 2011 & < 2013
real_earmark_coop = real_earmark_coop[(real_earmark_coop['year'] >= 2007)
                                       & (real_earmark_coop['year'] < 2010)]
real_earmark_coop = real_earmark_coop[['vote_id', 'bill_id', 'date', 'l_id',
                                       'party', 'vote', 'year', 'solo_num', 'other_num']]

# classifying earmark use
real_earmark_coop['total_num'] = real_earmark_coop['solo_num'] + real_earmark_coop['other_num']
real_earmark_coop['solo_pct'] = real_earmark_coop['solo_num'].rank(pct=True)
real_earmark_coop['total_pct'] = real_earmark_coop['total_num'].rank(pct=True)
real_earmark_coop['earmark_class'] = real_earmark_coop['total_pct'].map(classify_earmarkers)
real_earmark_coop['earmark_use'] = real_earmark_coop['total_pct'].map(median_earmarkers)

# get which party sponsor the bills
real_earmark_coop = real_earmark_coop.merge(unique_sponsor[['bill_id', 'party_code']],
                                            how='left', on='bill_id')
real_earmark_coop['party_code'] = real_earmark_coop['party_code'].map(party_map)
real_earmark_coop.rename(columns={'party_code': 'bill_party'}, inplace=True)

# calculate cooperation rate
real_earmark_coop = real_earmark_coop[(real_earmark_coop['party'] != real_earmark_coop['bill_party'])].reset_index(drop=True)
real_earmark_coop['coop'] = (real_earmark_coop['vote'] == 'Yea')
real_earmark_coop['earmark_period'] = real_earmark_coop['year'].map(earmark_period)
year_earmark_coop = real_earmark_coop.groupby(['l_id', 'year']).agg({'solo_num': 'mean',
                                                                     'total_num': 'mean',
                                                                     'earmark_class': 'first',
                                                                     'coop': 'mean'}).reset_index()
year_earmark_coop['year'] = year_earmark_coop['year'].astype(str)

year_earmark_coop.to_csv(os.path.join(legis_int_path, 'processed_data',
                                      'indiv_yearmark_coop_preban.csv'),
                         index=False)

# year_earmark_coop.to_csv(os.path.join(legis_int_path, 'processed_data',
#                                       'indiv_yearmark_coop_postban.csv'),
#                          index=False)

plt.figure(figsize=(12,6))
ax = sns.scatterplot(x='total_num', y='coop', hue='year',
                     data=year_earmark_coop)
ax.set(xlabel='Number of solo earmarks')
plt.show()


# trend change plot showing changes in cooperation rate in each earmark group after the ban (diff-in-diff)
# first plot it by year, and then contrasting pre- and post- periods
# changes by year
agg_yearmark_coop = xpartisan_vote.groupby(['earmark_class', 'year'])[['coop']].mean().reset_index()

plt.figure(figsize=(12,6))
sns.pointplot(x='year', y='coop', hue='earmark_class', data=agg_yearmark_coop)
plt.show()

# Pre- vs Post- cooperation rate
# agg_prepost_coop = minor_earmark_vote.groupby(['earmark_class', 'earmark_period'])[['coop']].mean().reset_index()
xpartisan_prepost_coop = xpartisan_vote.groupby(['earmark_class', 'earmark_period'])[['coop']].mean().reset_index()


plt.figure(figsize=(12,6))
sns.pointplot(x='earmark_period', y='coop', hue='earmark_class', data=xpartisan_prepost_coop)
plt.show()



# check associations between how long legislators have been in congress (stability) and their behavior
congress_age = pd.DataFrame({'bioguide_id': complete_earmark['bioguide_id'].unique()})

congress_df = pd.read_csv(os.path.join(legis_int_path, 'HSall_members.csv'))
congress_df = congress_df[congress_df['congress'] < 112]
congress_age = congress_age.merge(congress_df, how='left', on='bioguide_id')

# average nominate score 2007-2009
tmp = congress_age[congress_age['congress'] >= 110]
tmp = tmp.groupby('bioguide_id')[['nominate_dim1', 'nominate_dim2']].mean().reset_index()

congress_age = congress_age.groupby('bioguide_id')[['congress']].count().reset_index() # derive congress age
congress_age = congress_age.merge(tmp, how='left', on='bioguide_id')
congress_age = congress_age.merge(complete_earmark, how='left', on='bioguide_id')

# keep last earmark numbers and party code of each legislator
congress_age = congress_age.groupby('bioguide_id').agg({'congress': 'mean', 'party_code': 'last',
                                                        'nominate_dim1': 'mean', 'nominate_dim2': 'mean',
                                                        'solo_num': 'last'}).reset_index()
congress_age.columns = ['bioguide_id', 'position_age', 'party_code',
                        'nominate_dim1', 'nominate_dim2', 'solo_num']
congress_age.to_csv(os.path.join(legis_int_path, 'processed_data',
                                                 'congress_age_behav_data.csv'),
                    index=False)