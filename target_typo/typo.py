# Copyright 2019-2020 Typo. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# This product includes software developed at or by Typo (https://www.typo.ai/).

import sys
import json
import requests

import backoff

from target_typo.default_config import DEFAULTS
from target_typo.logging import log_backoff, log_critical, log_debug, log_info
from target_typo.utils import emit_state


# pylint: disable=unused-argument
def backoff_giveup(exception):
    '''
    Called when backoff exhausts max tries
    '''
    log_critical('Unable to make network requests. Please check your internet connection.')
    sys.exit(1)


class TypoTarget():
    '''
    TypoTarget Module Constructor
    '''

    def __init__(self, config):
        # api_key, api_secret, cluster_api_endpoint, repository, send_threshold
        self.base_url = config['cluster_api_endpoint']
        self.api_key = config['api_key']
        self.api_secret = config['api_secret']
        self.repository = config['repository']
        self.send_threshold = config['send_threshold'] if 'send_threshold' in config else DEFAULTS['send_threshold']
        self.retry_bool = False
        self.token = ''
        self.data_out = []
        self.batch_number = 0
        self.state = None

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.Timeout, requests.exceptions.ConnectionError),
        max_tries=8,
        on_backoff=log_backoff,
        on_giveup=backoff_giveup,
        logger=None,
        factor=3
    )
    def post_request(self, url, headers, payload):
        '''
        Generic POST request
        '''
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        status = response.status_code

        if status == 200:
            data = response.json()
            return status, data

        log_critical('Request to URL %s returned status code %s. Please check your configuration and try again later.',
                     url, status)
        sys.exit(1)

    def request_token(self):
        '''
        Token Request for other requests
        '''
        # Required parameters
        url = self.base_url.rstrip('/') + '/token'
        headers = {
            'Content-Type': 'application/json'
        }
        payload = {
            'apikey': self.api_key,
            'secret': self.api_secret
        }

        error_message = 'Token Request Failed. Please check your credentials and cluster_api_endpoint config.'
        # POST request
        try:
            status, data = self.post_request(url, headers, payload)
        except Exception:  # pylint: disable=W0703
            log_critical(error_message, exc_info=True)
            sys.exit(1)

        # Check Status
        if status != 200:
            log_critical('%s Details: %s', error_message, data)
            sys.exit(1)

        return data['token']

    def enqueue_to_dataset(self, dataset, line):
        '''
        Constructing dataset for POST Request
        '''
        data = {
            'repository': self.repository,
            'dataset': dataset,
            'data': line
        }
        self.data_out.append(data)

        # Submitting a post request every configured number of records in dataset
        if len(self.data_out) == self.send_threshold:
            self.import_dataset(self.data_out)

        return data

    def import_dataset(self, datasets):
        '''
        Push Dataset to Typo via POST Request
        '''
        self.batch_number += 1

        # Required parameters
        url = self.base_url.rstrip('/') + '/import'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + self.token  # self.access_token
        }

        log_info('Batch %s: Sending %s records to Typo.', self.batch_number, len(datasets))

        # POST Request
        status, data = self.post_request(url, headers, datasets)

        # Expired token
        if status == 401:
            log_debug('Token expired. Requesting new token.')
            self.token = self.request_token()
            # Retry post_request with new token
            status, data = self.post_request(url, headers, datasets)

        # Check Status
        good_status = [200, 201, 202]
        if status not in good_status:
            log_critical('Request failed. Please try again later. %s', data['message'])
            sys.exit(1)

        # Reset data_out
        self.data_out = []
        self.emit_state()

    def emit_state(self):
        if self.state is not None:
            emit_state(self.state)
            self.state = None

    def set_state(self, state):
        self.state = state
