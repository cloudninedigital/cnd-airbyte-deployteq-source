#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
import base64
import requests
import requests.auth
from abc import ABC
import datetime
import re
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import logging
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import BasicHttpAuthenticator, TokenAuthenticator

logging.getLogger().setLevel(logging.INFO)

_SLICE_RANGE = 1
_OUTGOING_DATETIME_FORMAT = "%Y-%m-%d"
_INCOMING_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
_LIMIT= 1000
"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


# Basic full refresh stream
class DeployteqStream(HttpStream, ABC):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class DeployteqStream(HttpStream, ABC)` which is the current class
    `class Customers(DeployteqStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(DeployteqStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalDeployteqStream((DeployteqStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """
    def __init__(self, cursor_field: str, start_date, **kwargs):
        super().__init__(**kwargs)
        self.metric = 'clicks'
        self._cursor_field = cursor_field
        self.start_date = datetime.datetime.strptime(start_date, _OUTGOING_DATETIME_FORMAT).timestamp()

        # Here's where we set the variable from our input to pass it down to the source.


    @property
    def cursor_field(self) -> Optional[str]:
        return self._cursor_field

    url_base = "https://api.canopydeploy.net/analytics-events/"
    primary_key = "uuid"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        if response.status_code == 200 and response.json().get('_links') and response.json()['_links'].get('next'):
            return {"offset":re.search('.*offset\=(\d+)', response.json()['_links'].get('next')).group(1)}
        return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        logging.info(f"response given with status {response.status_code}")
        if response.status_code != 200:
            raise Exception(f"error response given: {response.text}")
        if not response.json or not response.json().get("data"):
            logging.warn(f"no data present in response: {response.text}")
        data = response.json().get("data") if response.json().get("data") is not None else []
        for d in data:
            yield d

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        cf = latest_record.get(self._cursor_field, "")
        latest_record_time = 0
        if cf != "":
            latest_record_time = datetime.datetime.strptime(cf, _INCOMING_DATETIME_FORMAT).timestamp()
        state_value = max(current_stream_state.get(self.cursor_field, 0), latest_record_time)
        return {self._cursor_field: state_value}

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        start_ts = stream_state.get(self._cursor_field, self.start_date) if stream_state else self.start_date
        now_ts = datetime.datetime.now().timestamp()
        if start_ts >= now_ts:
            yield from []
            return
        for start, end in self.chunk_dates(start_ts, now_ts):
            yield {"start_date": start, "end_date": end}

    def chunk_dates(self, start_date_ts: int, end_date_ts: int) -> Iterable[Tuple[int, int]]:
        step = int(_SLICE_RANGE * 24 * 60 * 60)
        after_ts = start_date_ts
        while after_ts < end_date_ts:
            before_ts = min(end_date_ts, after_ts + step)
            yield after_ts, before_ts
            after_ts = before_ts

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        offset = next_page_token.get('offset') if next_page_token else 0
        return {"filter[date_from]":datetime.datetime.strftime(datetime.datetime.fromtimestamp(stream_slice["start_date"]), _OUTGOING_DATETIME_FORMAT), 
                "filter[date_to]":datetime.datetime.strftime(datetime.datetime.fromtimestamp(stream_slice["end_date"]), _OUTGOING_DATETIME_FORMAT), 
                "filter[metric]": self.metric, 
                "limit": _LIMIT, 
                "offset": offset}

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "events"

class ClickEvents(DeployteqStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.metric = 'clicks'


class SendEvents(DeployteqStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.metric = 'sends'



class OpenEvents(DeployteqStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.metric = 'opens'



# Source
class SourceDeployteq(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.
        auth = TokenAuthenticator(token=self.authenticate(config['client_id'], config['client_secret']))  # Oauth2Authenticator is also available if you need oauth support
        return [ClickEvents(authenticator=auth, cursor_field="metric_timestamp", start_date=config['start_date']), 
                SendEvents(authenticator=auth, cursor_field="metric_timestamp", start_date=config['start_date']),
                OpenEvents(authenticator=auth, cursor_field="metric_timestamp", start_date=config['start_date'])]
    
    def authenticate(self, client_id, client_secret):
        auth_string = base64.b64encode(f'{client_id}:{client_secret}'.encode("ascii"))
        res = requests.post(
                "https://auth.clang.cloud/oauth2/token",
                data={"grant_type": "client_credentials",
                        "scope": "analytics.events:read"},
                headers={
                "Authorization": f"Basic {auth_string.decode('ascii')}"
                }
            )
        logging.info(f"made request to status code: {res.status_code}")
        logging.info(f"request response: {res.text}")
        return res.json()['access_token']