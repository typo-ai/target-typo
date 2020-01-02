# Copyright 2019 Typo. All Rights Reserved.
#
#
#
# Licensed under the Apache License, Version 2.0 (the "License");
#
# you may not use this file except in compliance with the
#
# License.
#
#
#
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
#
#
# Unless required by applicable law or agreed to in writing, software
#
# distributed under the License is distributed on an "AS IS" BASIS,
#
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#
# implied. See the License for the specific language governing
#
# permissions and limitations under the License.
#
#
#
# This product includes software developed at
#
# or by Typo (https://www.typo.ai/).

import sys
import json
import requests

import singer

# Singer Logger
logger = singer.get_logger()


class TypoTarget():
    '''
    TypoTarget Module Constructor
    '''

    def __init__(self, api_key, api_secret, cluster_api_endpoint, repository,
                 send_threshold):
        logger.debug('__init__ - self=[%s], api_key=[%s], api_secret=[%s], cluster_api_endpoint=[%s], repository=[%s], send_threshold=[%s]',
                        self, api_key, api_secret, cluster_api_endpoint, repository, send_threshold)
        self.base_url = cluster_api_endpoint
        self.api_key = api_key
        self.api_secret = api_secret
        self.repository = repository
        self.retry_bool = False
        self.token = ''
        self.data_out = []
        self.send_threshold = int(send_threshold)

    def post_request(self, url, headers, payload):
        '''
        Generic POST request
        '''

        logger.debug('post_request - self=[%s], url=[%s], headers=[%s], payload=[%s]', self, url, headers, payload)

        try:
            r = requests.post(url, headers=headers, data=json.dumps(payload))
            logger.debug('post_request - r.text=[%s], data=[%s]', r.text, json.dumps(payload))
        except Exception as e:
            logger.error('post_request - Request failed.')
            logger.error(e)
            sys.exit(1)

        logger.debug('post_request - url=[%s], request.status_code=[%s]', url, r.status_code)
        status = r.status_code
        if status == 200:
            data = r.json()
            return status, data
        else:
            logger.error('post_request - url=[%s], request.status_code=[%s], response.text=[%s]', url, r.status_code, r.text)
            raise Exception('url {} returned status code {}. Please check that you are using the correct url.'.format(url, r.status_code))

    def request_token(self):
        '''
        Token Request for other requests
        '''
        logger.debug('request_token - self=[%s]', self)

        # Required parameters
        url = self.base_url.rstrip('/') + '/token'
        headers = {
            'Content-Type': 'application/json'
            # ,'Authorization': 'Bearer {}'.format(self.access_token)
        }
        payload = {
            'apikey': self.api_key,
            'secret': self.api_secret
        }

        # POST request
        try:
            status, data = self.post_request(url, headers, payload)
        except Exception:
            logger.error('request_token - Please validate your configuration inputs.', exc_info=True)
            sys.exit(1)
            
        # Check Status
        if status != 200:
            logger.error(
                'request_token - Token Request Failed. Please check your credentials. Details: \
                {}'.format(data))
            sys.exit(1)

        return data['token']

    def queue_to_dataset(self, dataset, line):
        '''
        Constructing dataset for POST Request
        '''

        logger.debug('queue_to_dataset - self=[%s], dataset=[%s], line=[%s]', self, dataset, line)
        
        data = {
            'repository': self.repository,
            'dataset': dataset,
            'data': line
        }
        self.data_out.append(data)

        # submitting a post request every configured number of records in dataset
        if len(self.data_out) == self.send_threshold:
            self.import_dataset(self.data_out)

        return data

    def import_dataset(self, datasets):
        '''
        Push Dataset to Typo via POST Request
        '''

        logger.debug('import_dataset - self=[%s], datasets=[%s]', self, datasets)

        # Required parameters
        url = self.base_url.rstrip('/') + '/import'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + self.token  # self.access_token
        }

        # POST Request
        payload = datasets
        logger.debug('import_dataset - POST payload: {}'.format(payload))
        status, data = self.post_request(url, headers, payload)

        logger.debug(data)
        # Expired token
        if status == 401:
            logger.debug('import_dataset - Token expired. Requesting new token.')
            self.token = self.request_token()
            # Retry post_request with new token
            status, data = self.post_request(url, headers, payload)
        # Check Status
        good_status = [200, 201, 202]
        if status not in good_status:
            logger.error(
                'import_dataset - Request failed. Please try again later. {}\
                    '.format(data['message']))
            sys.exit(1)

        # Reset data_out
        self.data_out = []
