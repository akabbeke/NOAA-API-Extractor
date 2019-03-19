import json
import logging

from datetime import date
from datetime import timedelta
from os import path

import config

from noaa_api import NOAADataset
from noaa_api import RetryLimitExceeded

class Extractor(object):
  def __init__(self, datesetid):
    self.dataset = NOAADataset(datesetid)

  def extract_date_range(self, start_date, end_date):
    """ Extracts NOAA dataset for each day in an inclusive range of dates and
    writes the data to JSON files.

    :param start_date: The first date to be extracted
    :param end_date: The last date to be extracted
    """

    # For each day in the range extract the data for that date
    for i in range((end_date - start_date).days + 1):
      try:
        self._extract_date(start_date + timedelta(days=i))
      except RetryLimitExceeded:
        logging.error("Could not fetch data for {}".format(start_date + timedelta(days=i)))


  def _extract_date(self, date_to_fetch):
    data = []
    file_count = 0

    # Stream the data in chunks
    for data_chunk in self.dataset.stream_date_data(date_to_fetch):
      # Add the current chunk of data to the batch of data
      data += data_chunk

      # If the current batch of data is larger than our file_line_limit
      # then write it out
      if len(data) >= config.file_line_limit:
        for i in range(len(data)//config.file_line_limit):
          self._write_file(date_to_fetch, file_count, data[:config.file_line_limit])
          file_count += 1

          # remove the data written out from the data list
          data = data[config.file_line_limit:]

    # write out any remainder data
    self._write_file(date_to_fetch, file_count, data)

  def _write_file(self, date_to_fetch, count, date_data):
    # Writes out a file to the output_dir configured in the configs
    # Filename is of the format "YYYY-MM-DD-0.json"

    # Format the filename and path
    filename = "{}-{}.json".format(date_to_fetch, count)
    filepath = path.join(config.output_dir, filename)

    # Write out the file in json format
    with open(filepath, 'w') as output_file:
      json.dump(date_data, output_file, indent=4)
