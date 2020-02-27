#!/usr/bin/env python3

'''
'''
# Copyright 2019-2020 Typo. All Rights Reserved.
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

import argparse
import io
import os  # noqa
import sys
import json
import threading
import http.client
import numbers
import urllib
import pkg_resources
from jsonschema.exceptions import ValidationError, SchemaError
from jsonschema.validators import Draft4Validator

from target_typo.constants import TYPE_RECORD, TYPE_SCHEMA, TYPE_STATE
from target_typo.logging import log_critical, log_debug, log_info
from target_typo.typo import TypoTarget
from target_typo.utils import flatten


def persist_lines(config, records):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}
    processed_streams = set()

    # Typo Class
    typo = TypoTarget(config)
    typo.token = typo.request_token()

    # Loop over records from stdin
    for record in records:
        try:
            input_record = json.loads(record)
        except json.decoder.JSONDecodeError:
            log_critical('Unable to parse line: %s', record)
            sys.exit(1)

        if 'type' not in input_record:
            log_critical('Line is missing required key "type": %s', record)
            sys.exit(1)

        input_type = input_record['type']

        if input_type == TYPE_RECORD:
            # Validate record
            if input_record['stream'] in validators:
                try:
                    validators[input_record['stream']].validate(
                        input_record['record'])
                except ValidationError as err:
                    log_critical(err)
                    sys.exit(1)
                except SchemaError as err:
                    log_critical('Invalid schema: %s', err)
                    sys.exit(1)

            # If the record has properties with JSON sub-properties, they will
            # be flattened like "a": {"b": 1, "c": 2} -> {"a__b": 1, "a__c": 2}
            flattened_record = flatten(input_record['record'])

            typo.queue_to_dataset(
                dataset=input_record['stream'],
                line=flattened_record
            )

            # Adding processed streams
            processed_streams.add(input_record['stream'])

        elif input_type == TYPE_STATE:
            log_debug('Setting state to {}'.format(input_record['value']))
            state = input_record['value']

        elif input_type == TYPE_SCHEMA:
            if 'stream' not in input_record:
                raise Exception(
                    'Line is missing required key "stream": {}'.format(record))
            stream = input_record['stream']

            # Validate if stream is processed
            if stream in processed_streams:
                log_critical(
                    'Tap error. SCHEMA record should be specified before \
                        RECORDS.')
                sys.exit(1)

            # Validate schema
            try:
                schemas[stream] = input_record['schema']
            except KeyError:
                log_critical('SCHEMA record is missing \'schema\' property: %s', input_record)
                sys.exit(1)

            validators[stream] = Draft4Validator(input_record['schema'])

    if len(typo.data_out) != 0:
        typo.import_dataset(typo.data_out)

    return state


def send_usage_stats():
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
        conn.getresponse()
    except Exception:  # pylint: disable=W0703
        log_debug('Collection request failed', exc_info=True)
    finally:
        conn.close()


def validate_number_value(parameter_name, value, min_value, max_value, validate_int=False):
    if not isinstance(value, numbers.Number):
        log_critical('Configuration file parameter "%s" must be a number.', parameter_name)
        return False

    if validate_int and not isinstance(value, int):
        log_critical('Configuration file parameter "%s" must be an integer number.', parameter_name)
        return False

    if value < min_value:
        log_critical('Configuration file parameter "%s" must be higher than %s.', parameter_name, min_value)
        return False

    if value > max_value:
        log_critical('Configuration file parameter "%s" must be lower than or equal to %s.', parameter_name, max_value)
        return False

    return True


def validate_config(config, config_file):
    missing_parameters = []
    if 'api_key' not in config:
        missing_parameters.append('api_key')
    if 'api_secret' not in config:
        missing_parameters.append('api_secret')
    if 'cluster_api_endpoint' not in config:
        missing_parameters.append('cluster_api_endpoint')
    if 'repository' not in config:
        missing_parameters.append('repository')

    if 'send_threshold' in config:
        if not validate_number_value('send_threshold', config['send_threshold'], 0, 200, True):
            return False

    # Output error message is there are missing parameters
    if len(missing_parameters) != 0:
        sep = ','
        log_critical('Configuration parameter missing. Please set the [%s] parameter in the configuration file "%s"',
                     sep.join(missing_parameters), config_file)
        return False

    return True


def main():
    log_info('Starting...')
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    config_file = args.config

    if config_file:
        with open(config_file) as config_input:
            config = json.load(config_input)
    else:
        log_critical('Please specify configuration file.')
        sys.exit(1)

    # Validate configuration for required parameters
    if not validate_config(config, config_file):
        sys.exit(1)

    if not config.get('disable_collection', False):
        log_info('Sending version information to singer.io. To disable sending anonymous usage data, set',
                 'the config parameter \'disable_collection'' to true.')
        threading.Thread(target=send_usage_stats).start()

    stdin_input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    persist_lines(config, stdin_input)

    log_info('Target exiting normally')


if __name__ == '__main__':
    try:
        main()
    except Exception as err:  # pylint: disable=W0703
        log_critical('Target-typo cannot get executed at the moment. Please try again later. Details: %s', err)
        sys.exit(1)
