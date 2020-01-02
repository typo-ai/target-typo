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
# !/usr/bin/env python3
# 11, 20, 29

import argparse
import io
import os  # noqa
import sys
import json
import threading
import http.client
import urllib
from datetime import datetime
import collections
import pkg_resources
from jsonschema.validators import Draft4Validator
import singer

from target_typo.typo import TypoTarget


logger = singer.get_logger()

# Type Constants
TYPE_RECORD = 'RECORD'
TYPE_STATE = 'STATE'
TYPE_SCHEMA = 'SCHEMA'


def emit_state(state):
    logger.debug('emit_state - state=[%s]', state)
    if state is not None:
        line = json.dumps(state)
        logger.debug('emit_stat - Emitting state %s', line)
        sys.stdout.write('{}\n'.format(line))
        sys.stdout.flush()


def flatten(data_json, parent_key='', sep='__'):
    '''
    Flattening JSON nested file
    *Singer default template function
    '''
    logger.debug('flatten - data_json=[%s], parent_key=[%s], sep=[%s]', data_json, parent_key, sep)
    items = []
    for json_object, json_value in data_json.items():
        new_key = parent_key + sep + json_object if parent_key else json_object
        if isinstance(json_value, collections.MutableMapping):
            items.extend(flatten(json_value, new_key, sep=sep).items())
        else:
            items.append((new_key, str(json_value) if type(
                json_value) is list else json_value))
    return dict(items)


def persist_lines(config, records):
    logger.debug('persist_lines - config=[%s], records=[%s]', config, records)
    state = None
    schemas = {}
    key_properties = {}
    validators = {}
    imported_key_properties = []
    processed_streams = set()

    now = datetime.now().strftime('%Y%m%dT%H%M%S')  # noqa

    # Typo Class
    typo = TypoTarget(
        api_key=config['api_key'],
        api_secret=config['api_secret'],
        cluster_api_endpoint=config['cluster_api_endpoint'],
        repository=config['repository'],
        send_threshold=config['send_threshold']
    )

    typo.token = typo.request_token()
    # Test Account token
    # typo.token = config['access_token']

    # Loop over records from stdin
    for record in records:
        try:
            input_record = json.loads(record)
        except json.decoder.JSONDecodeError:
            logger.error('Unable to parse:\n{}'.format(record))
            raise

        if 'type' not in input_record:
            raise Exception(
                'Line is missing required key "type": {}'.format(record))
        input_type = input_record['type']

        if input_type == TYPE_RECORD:
            # Validate record
            if input_record['stream'] in validators:
                try:
                    validators[input_record['stream']].validate(
                        input_record['record'])
                except Exception as err:
                    logger.error(err)
                    sys.exit(1)

            # If the record needs to be flattened, uncomment this line
            flattened_record = flatten(input_record['record'])

            # TODO: Process Record message here..
            dataset = typo.queue_to_dataset(  # noqa
                dataset=input_record['stream'],
                line=flattened_record
            )

            # Outputting state for imported key_properties
            if input_record['stream'] in key_properties:
                if len(key_properties[input_record['stream']]) != 0:
                    key_json = {}
                    for key in key_properties[input_record['stream']]:
                        key_json[key] = input_record['record'][key]
                    imported_key_properties.append(key_json)

            # Adding processed streams
            processed_streams.add(input_record['stream'])

        elif input_type == TYPE_STATE:
            logger.debug('Setting state to {}'.format(input_record['value']))
            state = input_record['value']
            state['imported_records'] = imported_key_properties

        elif input_type == TYPE_SCHEMA:
            if 'stream' not in input_record:
                raise Exception(
                    'Line is missing required key "stream": {}'.format(record))
            stream = input_record['stream']

            # Validate if stream is processed
            if stream in processed_streams:
                logger.error(
                    'Tap error. SCHEMA record should be specified before \
                        RECORDS.')
                sys.exit(1)

            # Validate schema
            try:
                schemas[stream] = input_record['schema']
            except Exception:
                logger.error('Tap error: Schema is missing.')
                sys.exit(1)

            validators[stream] = Draft4Validator(input_record['schema'])
            if 'key_properties' not in input_record:
                raise Exception('key_properties field is required')
            key_properties[stream] = input_record['key_properties']
        else:
            raise Exception('Unknown message type {} in message {}'
                            .format(input_record['type'], input_record))

    if len(typo.data_out) != 0:
        typo.import_dataset(typo.data_out)

    return state


def send_usage_stats():
    logger.debug('send_usage_stats')
    try:
        version = pkg_resources.get_distribution('target-typo').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-typo',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        response = conn.getresponse()  # noqa        
    except Exception:
        logger.debug('Collection request failed', exc_info=True)
    finally:
        conn.close()


def validate_config(config, config_loc):
    logger.debug('validate_config - config=[%s], config_loc=[%s]', config, config_loc)
    logger.info('Input Configuration Parameters: {}'.format(config))
    missing_parameters = []
    if 'api_key' not in config:
        missing_parameters.append('api_key')
    if 'api_secret' not in config:
        missing_parameters.append('api_secret')
    if 'cluster_api_endpoint' not in config:
        missing_parameters.append('cluster_api_endpoint')
    if 'repository' not in config:
        missing_parameters.append('repository')
    if 'send_threshold' not in config:
        missing_parameters.append('send_threshold')

    # Output error message is there are missing parameters
    if len(missing_parameters) != 0:
        sep = ','
        logger.error('Configuration parameter missing. Please',
                     'set the [{0}] in the configuration file "{1}"'.format(
                         sep.join(missing_parameters, config_loc)))


def main():
    logger.debug('main')
    logger.info('\'target-typo:{}\' Starting...'.format(
        pkg_resources.get_distribution('target_typo').version))
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
            logger.info(
                'Target configuration file {} loaded.'.format(args.config))
    else:
        logger.error('Please specify configuration file.')
        sys.exit(1)

    # Validate configuration for required parameters
    validate_config(config, args.config)

    if not config.get('disable_collection', False):
        # logger.info('Sending version information to singer.io.',
        #            'To disable sending anonymous usage data, set',
        #            'the config parameter 'disable_collection' to true')
        threading.Thread(target=send_usage_stats).start()

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    state = persist_lines(config, input)

    emit_state(state)
    logger.info('Target exiting normally')


if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        logger.error('Target-typo cannot get executed at the moment. \
            Please try again later. Details: {}'.format(err))
        sys.exit(1)
