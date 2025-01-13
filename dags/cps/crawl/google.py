import json
import os
import pathlib
import shutil

import requests

DIR = pathlib.Path(__file__).parent.resolve()
CONFIG_PATH = os.path.join(DIR, 'config.json')
TEMP_PATH = os.path.join(DIR, 'temp')
ADVERTISERS = {
    'AR01022311892732870657': 'CPS',
    'AR16310080066803466241': 'FPT',
    'AR07573700799545475073': 'TGDD',
    'AR02614177543162429441': 'CL'
}
VARIABLES = {
    '2': 40,
    '3': {'8': [2704],
          '12': {'1': '',
                 '2': True},
          '13': {'1': []}},
    '7': {'1': 1,
          '2': 29,
          '3': 2704}
}


def fetch_google_ads():
    shutil.rmtree(TEMP_PATH, ignore_errors=True)
    os.mkdir(TEMP_PATH)

    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
        url = config.get('GOOGLE_ADS_URL')
        headers = config.get('GOOGLE_ADS_HEADER')

    for advertiser in ADVERTISERS.keys():
        if VARIABLES.get('4'):
            VARIABLES.pop('4')

        VARIABLES['3']['13']['1'] = [advertiser]
        next_token = True

        i = 0
        while next_token:
            response = requests.post(
                url,
                headers=headers,
                data={'f.req': json.dumps(VARIABLES).replace(' ', '')},
            )

            i += 1
            file_path = os.path.join(TEMP_PATH, f'{advertiser}_{i:03d}.json')
            root = response.json()

            with open(file_path, 'w') as fp:
                json.dump(root, fp)

            next_token = root.get('2')
            VARIABLES.update({'4': next_token})

        print(f'Crawled {advertiser} done!')
