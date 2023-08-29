# Copyright 2022 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import os

sys.path.insert(0, ".")
cwd: str = os.getcwd()
rtdip_root: str = os.path.join(cwd, "..", "..", "..", "..", "..")
sys.path.insert(0, rtdip_root)

from src.sdk.python.rtdip_sdk.data_models.utils import CreateTimeSeriesObject
from src.sdk.python.rtdip_sdk.data_models.meters.utils import CreateUsageObject
from src.sdk.python.rtdip_sdk.data_models.timeseries import SeriesType
from src.sdk.python.rtdip_sdk.data_models.timeseries import MetaData
from src.sdk.python.rtdip_sdk.data_models.timeseries import ValueType
from src.sdk.python.rtdip_sdk.data_models.timeseries import ModelType
from src.sdk.python.rtdip_sdk.data_models.timeseries import Uom
from src.sdk.python.rtdip_sdk.data_models.utils import timeseries_utils
from fastapi.encoders import jsonable_encoder
from uuid import uuid4
import pandas as pd
import datetime
import logging

import random

import pytz

import argparse


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def generate_timeseries_instance(series_id: str, series_parent_id: str, timezone: str):
    #
    meter_uid: str = str(uuid4())
    description: str = "description_" + str(uuid4())

    version: str = "Version_" + str(uuid4())

    timestamp_start: int = int(timeseries_utils.get_utc_timestamp())
    timestamp_end: int = timestamp_start
    name: str = "name_" + str(uuid4())
    uom = Uom.KWH

    series_type = SeriesType.Minutes10
    model_type = ModelType.AMI_USAGE
    value_type = ValueType.Usage

    properties: dict = dict()
    key: str = "key_" + str(uuid4())
    value: str = "value_" + str(uuid4())
    properties[key] = value

    metadata_instance: MetaData = CreateTimeSeriesObject.create_timeseries_vo(
        meter_uid,
        series_id,
        series_parent_id,
        name,
        uom,
        description,
        timestamp_start,
        timestamp_end,
        timezone,
        version,
        series_type,
        model_type,
        value_type,
        properties,
    )
    return metadata_instance


def generate_usage_instance(
    meter_id: str, series_id: str, timestamp: int, timestamp_interval: int, value: float
):
    return CreateUsageObject.create_usage_VO(
        meter_id, series_id, timestamp, timestamp_interval, value
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--number_of_samples", required=True, help="e.g.: 100")

    parser.add_argument("--output_dir", required=True, help="/tmp/")

    scenarios: list = ["SINGLE_TIMEZONE", "MULTIPLE_TIMEZONE"]
    parser.add_argument("--scenario", required=True, help=f"{str(scenarios)}")

    args = parser.parse_args()

    try:
        number_of_samples: int = int(args.number_of_samples)
    except Exception as ex:
        logger.error("Error parsing number of samples: %s", args.number_of_samples)
        logger.error(ex)
        sys.exit()

    output_dir: str = args.output_dir
    scenario: str = args.scenario

    database_name = "samples"
    table_name_metadata = "metadata"
    table_name_usage = "usage"

    if not os.path.exists(output_dir):
        logger.error("Output directory does not exist: %s", output_dir)
        sys.exit()

    if scenario not in scenarios:
        logger.error("Scenario not supported: %s", scenario)
        logger.error("\tValid scenarios: %s", str(scenarios))
        sys.exit()

    database_path: str = os.path.join(output_dir, database_name)
    if not os.path.exists(database_path):
        os.makedirs(database_path)

    if scenario == scenarios[0]:
        # Scenario 1: SINGLE_TIMEZONE
        usage_list: list = []
        timeseries_metadata_list: list = []
        timezone: str = str(
            datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
        )
        series_id: str = str(uuid4())
        series_parent_id: str = "parent_id_" + str(uuid4())
        timeseries_metadata_instance = generate_timeseries_instance(
            series_id, series_parent_id, timezone
        )
        for i in range(number_of_samples):
            usage_timestamp: int = timeseries_metadata_instance.TimestampStart
            usage_value: float = (
                timeseries_utils.generate_random_int_number(0, 1000) * 0.1
            )
            usage_instance = generate_usage_instance(
                str(uuid4()),
                timeseries_metadata_instance.SeriesId,
                usage_timestamp,
                usage_timestamp,
                usage_value,
            )
            usage_df = pd.DataFrame(jsonable_encoder(usage_instance), index=[0])
            usage_list.append(usage_df)
        timeseries_metadata_instance_df = pd.DataFrame(
            jsonable_encoder(timeseries_metadata_instance), index=[0]
        )
        timeseries_metadata_list.append(timeseries_metadata_instance_df)
        logger.debug(timeseries_metadata_instance_df)
        usage_df = pd.concat(usage_list)
        usage_df = pd.concat(usage_list)
        usage_df.to_csv(
            os.path.join(database_path, table_name_usage + ".csv"),
            index=False)
        timeseries_metadata_df = pd.concat(timeseries_metadata_list)
        timeseries_metadata_df.to_csv(
            os.path.join(database_path, table_name_metadata + ".csv"),
            index=False)

    elif scenario == scenarios[1]:
        # Scenario 2. MULTIPLE_TIMEZONE
        usage_list: list = []
        timeseries_metadata_list: list = []
        timezones: list = pytz.all_timezones
        timezone_1: str = random.choice(timezones)
        timezone_2: str = random.choice(timezones)
        logger.debug("TimeZone 1: %s", timezone_1)
        logger.debug("TimeZone 2: %s", timezone_2)
        today_date: str = datetime.datetime.now().strftime("%m/%d/%Y")
        timezone_1_series_id: str = f"{today_date}.{ValueType.Forecast}.{SeriesType.Hour}.{ModelType.AMI_USAGE}.{timezone_1}"
        timezone_2_series_id: str = f"{today_date}.{ValueType.Forecast}.{SeriesType.Hour}.{ModelType.AMI_USAGE}.{timezone_2}"

        series_parent_id: str = "parent_id_" + str(uuid4())
        timezone_1_timeseries_metadata_instance = generate_timeseries_instance(
            timezone_1_series_id, series_parent_id, timezone_1
        )
        timezone_2_timeseries_metadata_instance = generate_timeseries_instance(
            timezone_2_series_id, series_parent_id, timezone_2
        )

        for timeseries_metadata_instance in [
            timezone_1_timeseries_metadata_instance,
            timezone_2_timeseries_metadata_instance,
        ]:
            for i in range(number_of_samples):
                usage_timestamp: int = timeseries_metadata_instance.TimestampStart
                usage_value: float = (
                    timeseries_utils.generate_random_int_number(0, 1000) * 0.1
                )
                usage_instance = generate_usage_instance(
                    str(uuid4()),
                    timeseries_metadata_instance.SeriesId,
                    usage_timestamp,     
                    usage_timestamp,
                    usage_value,
                )
                usage_df = pd.DataFrame(jsonable_encoder(usage_instance), index=[0])
                usage_list.append(usage_df)
            timeseries_metadata_instance_df = pd.DataFrame(
                jsonable_encoder(timeseries_metadata_instance), index=[0]
            )

            timeseries_metadata_list.append(timeseries_metadata_instance_df)
            logger.debug(timeseries_metadata_instance_df)
        usage_df = pd.concat(usage_list)
        usage_df.to_csv(
            os.path.join(database_path, table_name_usage + ".csv"),
            index=False,
        )
        timeseries_metadata_df = pd.concat(timeseries_metadata_list)
        timeseries_metadata_df.to_csv(
            os.path.join(database_path, table_name_metadata + ".csv"),
            index=False,
        )
