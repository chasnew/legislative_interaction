import os
import pandas as pd
import json
import yaml

with open("config.yaml") as f:
    config_param = yaml.load(f, Loader=yaml.SafeLoader)

legis_int_path = config_param['legis_int_path']

congress_id = 113

congress_vote_path = os.path.join(legis_int_path, 'votes_{}'.format(congress_id))
year_list = os.listdir(congress_vote_path)
if '.DS_Store' in year_list:
    year_list.remove('.DS_Store')

results = {'congress': [], 'date': [], 'category': [], 'chamber': [],
           'bill_number': [], 'bill_type': [], 'vote_id': [], 'result': [],
           'name': [], 'l_id': [], 'party': [], 'state': [], 'vote': []}

# iterate over each of 2 years of the congress
for year in year_list:
    print('Year ', year)
    congress_year_path = os.path.join(os.path.join(congress_vote_path, year))
    vote_session_list = os.listdir(congress_year_path)

    # stupid OSx file directory thing
    if '.DS_Store' in vote_session_list:
        vote_session_list.remove('.DS_Store')

    # iterate over each voting session in a year
    for vote_session in sorted(vote_session_list):
        print('vote session: ', vote_session)
        json_path = os.path.join(congress_year_path, vote_session, 'data.json')

        # load a voting session data
        with open(json_path) as file:
            session_data = json.load(file)

        vote_data = session_data['votes']
        vote_responses = vote_data.keys()
        num_vote = 0

        # iterate over different vote responses (Aye, Nay, Not voting, etc.)
        for response in vote_responses:
            vote_list = vote_data[response]
            num_vote += len(vote_list) # sum up number of votes
            if (response == 'No') or (response == 'no') or ((response == 'nay')):
                response = 'Nay'

            if (response == 'Aye'):
                response = 'Yea'

            for vote in vote_list:
                if vote == 'VP':
                    results['name'].append('VP')
                    results['l_id'].append('VP')
                    results['party'].append(None)
                    results['state'].append(None)
                else:
                    results['name'].append(vote['display_name'])
                    results['l_id'].append(vote['id'])
                    results['party'].append(vote['party'])
                    results['state'].append(vote['state'])

            results['vote'].extend([response] * len(vote_list))

        results['congress'].extend([congress_id] * num_vote)
        results['date'].extend([session_data['date']] * num_vote)
        results['category'].extend([session_data['category']] * num_vote)
        results['chamber'].extend([session_data['chamber']] * num_vote)
        results['vote_id'].extend([session_data['vote_id']] * num_vote)
        results['result'].extend([session_data['result']] * num_vote)

        data_keys = session_data.keys()
        # check if the voting session is regarding a bill
        if 'bill' in data_keys:
            bill_number = session_data['bill']['number']
            bill_type = session_data['bill']['type']
        else:
            bill_number = None
            bill_type = None

        results['bill_number'].extend([bill_number] * num_vote)
        results['bill_type'].extend([bill_type] * num_vote)

print('complete processing json files for congress ', congress_id)

congress_vote_df = pd.DataFrame(results)
congress_vote_df.to_csv(os.path.join(legis_int_path, 'processed_data',
                                     'congress{}_vote.csv'.format(congress_id)),
                        index=False)