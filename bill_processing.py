import os
import pandas as pd
import json

# hr: "H.R. 1234". It stands for House of Representatives, but it is the prefix used for bills introduced in the House.
# hres: "H.Res. 1234". It stands for House Simple Resolution.
# hconres: "H.Con.Res. 1234". It stands for House Concurrent Resolution.
# hjres: "H.J.Res. 1234". It stands for House Joint Resolution.
# s: "S. 1234". It stands for Senate and it is the prefix used for bills introduced in the Senate.
# sres: "S.Res. 1234". It stands for Senate Simple Resolution.
# sconres: "S.Con.Res. 1234". It stands for Senate Concurrent Resolution.
# sjres: "S.J.Res. 1234". It stands for Senate Joint Resolution.

# Simple resolutions only get a vote in their originating chamber. Concurrent resolutions get a vote in both chambers
# but do not go to the President. Neither has the force of law. Joint resolutions can be used either to propose
# an amendment to the constitution or to propose a law. When used to propose a law,
# they have exactly the same procedural steps as bills.

import yaml

with open("config.yaml") as f:
    config_param = yaml.load(f, Loader=yaml.SafeLoader)

legis_int_path = config_param['legis_int_path']

congress_id = 109
house_bill_types = ['hres', 'hr', 'hconres', 'hjres']

bill_type = 'hr'

congress_bill_path = os.path.join(legis_int_path, 'bills_{}'.format(congress_id), bill_type)
bill_list = os.listdir(congress_bill_path)
if '.DS_Store' in bill_list:
    bill_list.remove('.DS_Store')

results = {'congress': [], 'intro_date': [], 'bill_id': [], 'bill_type': [], 'subject': [],
           'name': [], 'thomas_id': [], 'withdrawn_at': [],
           'state': [], 'district': [], 'sponsor': [], 'cosponsor': []}

# iterate over each bill
for bill in sorted(bill_list):
    print('Bill: ', bill)
    bill_path = os.path.join(os.path.join(congress_bill_path, bill))
    json_path = os.path.join(bill_path, 'data.json')

    # load a bill data
    with open(json_path) as file:
        bill_data = json.load(file)

        # adding data of the sponsor
        sponsor = bill_data['sponsor']
        results['name'].append(sponsor['name'])
        results['district'].append(sponsor['district'])
        results['state'].append(sponsor['state'])
        results['thomas_id'].append(sponsor['thomas_id'])
        results['withdrawn_at'].append(None)
        results['sponsor'].append(True)
        results['cosponsor'].append(False)

        cosponsors = bill_data['cosponsors']
        num_cosponsor = len(cosponsors)

        # iterate over cosponsors
        for cosponsor in cosponsors:
            results['name'].append(cosponsor['name'])
            results['district'].append(cosponsor['district'])
            results['state'].append(cosponsor['state'])
            results['thomas_id'].append(cosponsor['thomas_id'])
            results['withdrawn_at'].append(cosponsor['withdrawn_at'])
            results['sponsor'].append(False)
            results['cosponsor'].append(True)

        results['congress'].extend([congress_id] * (num_cosponsor + 1))
        results['intro_date'].extend([bill_data['introduced_at']] * (num_cosponsor + 1))
        results['bill_id'].extend([bill_data['bill_id']] * (num_cosponsor + 1))
        results['bill_type'].extend([bill_data['bill_type']] * (num_cosponsor + 1))
        results['subject'].extend([bill_data['subjects_top_term']] * (num_cosponsor + 1))

print('complete processing bill json files for congress ', congress_id)

congress_bill_df = pd.DataFrame(results)
congress_bill_df.to_csv(os.path.join(legis_int_path, 'processed_data',
                                     'congress{}_{}_bill.csv'.format(congress_id, bill_type)),
                        index=False)