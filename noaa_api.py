import functools
import logging
import requests
import urllib

from datetime import date

import config


BASE_URL = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/'


class RetryLimitExceeded(Exception):
  pass


def retry_wrapper(func):
  # This enables non-200 responses to be retried

  # functools.wraps enables exceptions to properly propogate
  @functools.wraps(func)
  def wrapper(*args, **kwargs):
    for i in range(config.retry_limit):
      request = func(*args, **kwargs)

      # If the request was ok then return the request object
      if request.status_code == requests.codes.ok:
        return request

    # If the retry atttempts are surpassed then raise a RetryLimit
    # exception
    raise RetryLimitExceeded()
  return wrapper


class NOAADataset(object):
  def __init__(self, datasetid):
    self.datasetid = datasetid

  def stream_date_data(self, date_to_fetch):
    """Stream the data from the NOAA API for the date in chunks.

    :param date_to_fetch: The date to fetch data for
    :returns: A generator for fetching the chunks of data for the date
    """

    return self.stream_date_range_data(date_to_fetch, date_to_fetch)

  def stream_date_range_data(self, start_date, end_date):
    """Stream the data from the NOAA API for the days from the
    start_date to the end_date in chunks.

    :param date_to_fetch: The date to fetch data for
    :returns: A generator for fetching the chunks of data in the date range
    """

    params = {'startdate': str(start_date), 'enddate': str(end_date)}
    return self._stream_dataset(**params)

  def _stream_dataset(self, **kwargs):
    # Request the first set of data to get the metadata on the size of
    # the entire dataset
    request_data = self._make_request(**kwargs, offset=1).json()

    # Some dates return no data. If this is the case return an empty list.
    if not request_data:
      return

    # Parse the total count for the entire dataset
    data_count = request_data['metadata']['resultset']['count']

    # Yield the first set of data
    yield request_data['results']

    # Determine how many subsequent requests we will have to make
    total_requests = int(data_count/config.request_limit) + 1

    for i in range(1, total_requests):
      # Make the request with the correct offset and then yield the result
      request_data = self._make_request(**kwargs, offset=1 + i * config.request_limit)
      yield request_data.json()['results']
  
  @retry_wrapper
  def _make_request(self, **kwargs):
    # Set the datasetid and limit 
    kwargs.update({
      'datasetid': self.datasetid,
      'limit': config.request_limit,
    })

    # Generate the url for the request
    url = "{}/{}".format(config.base_url, config.data_endpoint)

    # Add the auth token to the request headers
    headers = {'token': config.auth_token}

    # return the request object
    return requests.get(url, params=kwargs, headers=headers)