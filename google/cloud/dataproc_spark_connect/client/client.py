# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime
import logging
import random
import string
import uuid

from pyspark.sql.connect.client import SparkConnectClient

logger = logging.getLogger(__name__)


class DataprocSparkConnectClient(SparkConnectClient):
    """
    TODO: add docstring
    """

    def __init__(self, *args, **kwargs):
        logger.debug("! Initiating DataprocSparkConnectClient")
        print("Initiating DataprocSparkConnectClient")
        super().__init__(*args, **kwargs)

    # Handle must be a UUID string of the format
    # '00112233-4455-6677-8899-aabbccddeeff'
    @staticmethod
    def generate_dataproc_operation_id():
        my_uuid = uuid.uuid4()
        return str(my_uuid)
        # timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M")
        # suffix_length = 4
        # random_suffix = "".join(
        #     random.choices(
        #         string.ascii_lowercase + string.digits, k=suffix_length
        #     )
        # )
        # return f"{timestamp}-{random_suffix}-{random_suffix}-{random_suffix*3}"

    def _execute_plan_request_with_metadata(self):
        req = super()._execute_plan_request_with_metadata()
        print("Checking if operation_id exists: " + req.operation_id)
        if not req.operation_id:
            dataproc_operation_id = self.generate_dataproc_operation_id()
            print("Generated operation_id: " + dataproc_operation_id)
            req.operation_id = dataproc_operation_id

        return req
    @property
    def session_id(self):
        return self._session_id
