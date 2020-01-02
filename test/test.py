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

import json
import unittest
from unittest.mock import patch
import target_typo.__init__ as init
from target_typo.typo import TypoTarget


typo_1 = TypoTarget(
    cluster_api_endpoint='https://www.mock.com',
    api_key='typo_key',
    api_secret='typo_secret',
    repository='test_typo',
    send_threshold=5
)
typo_1.token = '123'

dataset = "dataset"
data = {
    "date": "2019-06-23",
    "user": "leo"
}


class TestTypo(unittest.TestCase):

    def test_request_token(self):
        print("Test: When API key and API secret are provided, a token",
              "property should be returned in the API return.")
        with patch('target_typo.typo.requests.post') as mocked_post:
            mocked_post.return_value.status_code = 200
            mocked_post.return_value.json.return_value = {"token": "test"}

            expected_headers = {
                "Content-Type": "application/json"
            }
            expected_payload = {
                "apikey": "typo_key",
                "secret": "typo_secret"
            }

            token = typo_1.request_token()
            mocked_post.assert_called_with('https://www.mock.com/token',
                                           data=json.dumps(expected_payload),
                                           headers=expected_headers)
            self.assertEqual(token, 'test')

    def test_queue_to_dataset(self):
        print("Test: When dataset and dataset name are provided,",
              "expected JSON object should be created.")
        dataset = "dataset"
        data = {
            "date": "2019-06-23",
            "user": "leo"
        }

        data_expected = {
            "repository": "test_typo",
            "dataset": dataset,
            "data": data
        }

        result = typo_1.queue_to_dataset(dataset, data)

        self.assertEqual(result, data_expected)

    @patch('target_typo.typo.requests.post')
    def test_import_dataset(self, mock):
        print("Test: With all the required information, import_dataset",
              "should match expected request call.")
        mock.return_value.status_code = 200

        expected_url = "https://www.mock.com/import"
        expected_headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer "+typo_1.token
        }
        expected_payload = {
            "repository": "test_typo",
            "dataset": dataset,
            "data": data
        }

        typo_1.import_dataset(expected_payload)

        mock.assert_called_with(expected_url,
                                data=json.dumps(expected_payload),
                                headers=expected_headers)

    @patch('target_typo.typo.TypoTarget.import_dataset')
    def test_with_4_records_post_will_not_be_called(self, mock):
        print("Test: When there are less than 5 records in the queue, a POST",
              "request should not be created.")

        typo_2 = TypoTarget(
            cluster_api_endpoint='https://www.mock.com',
            api_key='typo_key',
            api_secret='typo_secret',
            repository='test_typo',
            send_threshold=5
        )
        for i in range(4):
            typo_2.queue_to_dataset(dataset, data)

        self.assertFalse(mock.called)

    @patch('target_typo.typo.TypoTarget.import_dataset')
    def test_with_5_records_post_request_will_be_made(self, mock):
        print("Test: When there are 5 records in the queue, a POST",
              "request should be called.")

        typo_3 = TypoTarget(
            cluster_api_endpoint='https://www.mock.com',
            api_key='typo_key',
            api_secret='typo_secret',
            repository='test_typo',
            send_threshold=5
        )
        payload = {
            "repository": "test_typo",
            "dataset": dataset,
            "data": data
        }
        expected_payload = []

        for i in range(5):
            typo_3.queue_to_dataset(dataset, data)
            expected_payload.append(payload)

        self.assertTrue(mock.called)
        self.assertEqual(typo_3.data_out, expected_payload)

    @patch('target_typo.typo.requests.post')
    def test_with_6_records(self, mock_post):
        print("Test: When there are 6 records in the queue, a",
              "POST request is expected with 1 record remaining.")

        mock_post.return_value.status_code = 200

        typo_4 = TypoTarget(
            cluster_api_endpoint='https://www.mock.com',
            api_key='typo_key',
            api_secret='typo_secret',
            repository='test_typo',
            send_threshold=5
        )
        payload = {
            "repository": "test_typo",
            "dataset": dataset,
            "data": data
        }
        data2 = {
            "date": "2019-06-26",
            "user": "leo_is_full"
        }
        payload_2 = {
            "repository": "test_typo",
            "dataset": dataset,
            "data": data2
        }
        expected_payload = []
        expected_url = "https://www.mock.com/import"
        expected_headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer "
        }

        for i in range(6):
            if i < 5:
                typo_4.queue_to_dataset(dataset, data)
                expected_payload.append(payload)
            else:
                typo_4.queue_to_dataset(dataset, data2)

        mock_post.assert_called_with(expected_url,
                                     data=json.dumps(expected_payload),
                                     headers=expected_headers)
        self.assertEqual(typo_4.data_out, [payload_2])

    @patch('target_typo.typo.requests.post')
    def test_stdin_ends_post_request_in_queue(self, mock_post):
        print("Test: When STDIN ends before POST request thredhold,",
              "records in queue should be sent.")

        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"token": ""}

        config = {
            "cluster_api_endpoint": "https://mock.com",
            "api_key": "1",
            "api_secret": "2",
            "repository": "test_typo",
            "send_threshold": 5
        }
        schema = json.dumps({
            "type": "SCHEMA",
            "stream": "mock",
            "schema": {},
            "key_properties": ["date"]
        })
        record = json.dumps({
            "type": "RECORD",
            "stream": "mock",
            "record": {
                "date": "today",
                "subj": "mock"
            }
        })
        state = json.dumps({
            "type": "STATE",
            "value": {
                "start_date": "today"
            }
        })
        records = [schema, record, state]

        expected_url = "https://mock.com/import"
        expected_headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer "
        }
        expected_payload = [
            {
                "repository": "test_typo",
                "dataset": "mock",
                "data": {
                    "date": "today",
                    "subj": "mock"
                }
            }
        ]

        init.persist_lines(config, records)
        mock_post.assert_called_with(expected_url,
                                     data=json.dumps(expected_payload),
                                     headers=expected_headers)

    @patch('jsonschema.validators.Draft4Validator.validate')
    @patch('target_typo.typo.requests.post')
    def test_no_schema(self, mock_post, mock_validate):
        print("Test: When SCHEMA is not provided, the code should",
              "exit with an error.")

        mock_post.return_value.status_code = 200
        config = {
            "cluster_api_endpoint": "https://mock.com",
            "api_key": "1",
            "api_secret": "2",
            "repository": "mock",
            "send_threshold": 5
        }
        record = json.dumps({
            "type": "RECORD",
            "stream": "mock",
            "record": {
                "date": "today",
                "subj": "mock"
            }
        })
        state = json.dumps({
            "type": "STATE",
            "value": {
                "start_date": "today"
            }
        })
        records = [record, state]

        init.persist_lines(config, records)
        self.assertTrue(mock_validate)

    @patch('target_typo.typo.requests.post')
    def test_record_with_invalid_schema(self, mock_post):
        print("Test: When RECORD has invalid SCHEMA, it is expected",
              "to output a validationError message.")

        mock_post.return_value.status_code = 200
        config = {
            "cluster_api_endpoint": "https://mock.com",
            "api_key": "1",
            "api_secret": "2",
            "repository": "mock",
            "send_threshold": 5
        }
        schema = json.dumps({
            "type": "SCHEMA",
            "stream": "mock",
            "schema": {
                "type": "object",
                "properties": {
                    "number": {
                        "type": "number"
                    }
                }
            },
            "key_properties": []
        })
        record = json.dumps({
            "type": "RECORD",
            "stream": "mock",
            "record": {
                "number": "one"
            }
        })
        state = json.dumps({
            "type": "STATE",
            "value": {
                "start_date": "today"
            }
        })
        records = [schema, record, state]

        with self.assertRaises(SystemExit) as ts:
            init.persist_lines(config, records)

        self.assertEqual(ts.exception.code, 1)

    @patch('target_typo.typo.requests.post')
    def test_output_state_of_persisted_records(self, mock_post):
        print("Test: After parsing the datasets, it is expected to",
              "output a STATE of persisted records."
              )

        mock_post.return_value.status_code = 200
        config = {
            "cluster_api_endpoint": "https://mock.com",
            "api_key": "1",
            "api_secret": "2",
            "repository": "mock",
            "send_threshold": 5
        }
        schema = json.dumps({
            "type": "SCHEMA",
            "stream": "mock",
            "schema": {
                "type": "object",
                "properties": {
                    "number": {
                        "type": "string"
                    }
                }
            },
            "key_properties": ["number"]
        })
        record = json.dumps({
            "type": "RECORD",
            "stream": "mock",
            "record": {
                "number": "one",
                "data": "typo"
            }
        })
        state = json.dumps({
            "type": "STATE",
            "value": {
                "start_date": "today"
            }
        })
        expected_state_msg = {
            "start_date": "today",
            "imported_records": [
                {"number": "one"}
            ]
        }

        records = [schema, record, state]
        returned_state_msg = init.persist_lines(config, records)

        self.assertEqual(returned_state_msg, expected_state_msg)

    @patch('target_typo.typo.requests.post')
    def test_schema_specified_after_record(self, mock_post):
        print("Test: When SCHEMA is specified after RECORD,",
              "Tap process should exit with error")

        mock_post.return_value.status_code = 200
        config = {
            "cluster_api_endpoint": "https://mock.com",
            "api_key": "1",
            "api_secret": "2",
            "repository": "mock",
            "send_threshold": 5
        }
        record = json.dumps({
            "type": "RECORD",
            "stream": "mock",
            "record": {
                "date": "today",
                "subj": "mock"
            }
        })
        schema = json.dumps({
            "type": "SCHEMA",
            "stream": "mock",
            "schema": {
                "type": "object",
                "properties": {
                    "number": {
                        "type": "string"
                    }
                }
            },
            "key_properties": ["number"]
        })
        records = [record, schema]

        with self.assertRaises(SystemExit) as ts:
            init.persist_lines(config, records)
        self.assertEqual(ts.exception.code, 1)


if __name__ == '__main__':
    unittest.main()
