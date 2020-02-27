# target-typo

[Singer](https://singer.io) target to load data into [Typo](https://www.typo.ai/). 

- [Usage](#usage)
  - [Installation](#installation)
  - [Create a configuration file](#create-a-configuration-file)
  - [Run target-typo](#run-target-typo)
  - [Saving state](#saving-state)
- [Typo registration and setup](#typo-registration-and-setup)
- [Development](#development)
- [Support](#support)

## Usage

This section describes the basic usage of **target-typo**. It assumes that you already have a Typo account, with an existing repository and a dataset. If you do not meet these prerequisites, please go to [Typo Registration and Setup](#typo-registration-and-setup).



### Installation

Python 3 is required. It is recommended to create a separate virtual environment for each tap or target as their may be incompatibilities between dependency versions.

```bash
> pip install tap-typo
```



### Create a configuration file

The config file (usually config.json) is a JSON file describing the target's settings.

The following sample configuration can be used as a starting point:

```json
{
  "api_key": "my_apikey",
  "api_secret": "my_apisecret",
  "cluster_api_endpoint": "https://cluster.typo.ai/management/api/v1",
  "repository": "my_repository"
}
```

- **api_key**, **api_secret** and **cluster_api_endpoint** can be obtained by logging into the [Typo Console](https://console.typo.ai/?utm_source=github&utm_medium=target-typo), clicking on your username, and then on **My Account**.
- **repository** corresponds to the target Typo Repository where the data will be stored. If not found, a Typo Dataset with the same name as the input stream name will be created in this Repository.
- Additionally, some optional parameters can be provided:
  - **send_threshold**: determines how many records will be sent to Typo in one batch. Default: `100`. Maximum value: `200`.
  - **disable_collection**: boolean property that prevents target-typo-proxy from sending anonymous usage data to Singer.io. Default: `false`.



### Run target-typo

```bash
> example-tap -c example_tap_config.json | target-typo -c config.json
```



## Typo registration and setup

In order to create a Typo account, visit [https://www.typo.ai/signup](https://www.typo.ai/signup?utm_source=github&utm_medium=target-typo) and follow the instructions.

Once registered you can log in to the Typo Console ([https://console.typo.ai/](https://console.typo.ai/?utm_source=github&utm_medium=target-typo)) and go to the Repositories section to create a new Repository.

Next, you can start uploading data by using [target-typo](https://github.com/typo-ai/target-typo). A new dataset will be created automatically when data is submitted.



## Development

To work on development of tap-typo, clone the repository, create and activate a new virtual environment, go into the cloned folder and install tap-typo in editable mode.

```bash
git clone https://github.com/typo-ai/tap-typo.git
cd tap-typo
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```



## Support

You may reach Typo Support at the email address support@ followed by the typo domain or see the full contact information at [https://www.typo.ai](https://www.typo.ai?utm_source=github&utm_medium=target-typo)



---

Copyright 2019-2020 Typo. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions and limitations under the License.

This product includes software developed at or by Typo ([https://www.typo.ai](https://www.typo.ai?utm_source=github&utm_medium=target-typo)).