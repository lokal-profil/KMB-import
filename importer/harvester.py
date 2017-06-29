#!/usr/bin/python
# -*- coding: utf-8  -*-
"""
Download and process KMB data.

Download xml metadata about KMB images, and preprocess
it as json files.
Requires a settings.json file containing an API key
and a list of keywords. Generates one json file per
keywords.
"""
import urllib.parse
import urllib.request
from xml.dom.minidom import parseString
from collections import OrderedDict
import time

import batchupload.common as common
from kmb_massload import parser

SETTINGS = "settings.json"
THROTTLE = 0.5
LOGFILE = 'kmb_massloading.log'


def load_settings(filename=SETTINGS):
    """Load settings from file."""
    return common.open_and_read_file(filename, as_json=True)


def save_data(data, filename='kmb_data.json'):
    """Dump data as json blob."""
    common.open_and_write_file(filename, data, as_json=True)
    print("Saved file: {}.".format(filename))


def create_url(keyword, hits_limit, start_record, api_key):
    """
    Create url from which to download image metadata.

    :param keyword: keyword to search for
    :param hits_limit: how many hits per page
    :param start_record: from which item to start
    :param api_key: key to access API
    """
    keyword = urllib.parse.quote(keyword)
    url_base = ("http://kulturarvsdata.se/ksamsok/api?x-api={api_key}"
                "&method=search&hitsPerPage={hits_limit}"
                "&startRecord={start_record}"
                "&query=serviceOrganization=RA%C3%84%20"
                "and%20serviceName=KMB%20"
                "and%20itemType=foto%20and%20mediaLicense=*%20"
                "and%20text={keyword}")
    return url_base.format(api_key=api_key,
                           hits_limit=hits_limit,
                           start_record=start_record,
                           keyword=keyword)


def split_records(dom):
    """Split xml data into separate entry objects."""
    return dom.getElementsByTagName("record")


def parse_record(dom, record_dict, log):
    """Parse and process the xml metadata into a dict."""
    record_dict = parser(dom, record_dict, log)
    return OrderedDict(sorted(record_dict.items()))


def get_records_from_url(url):
    """Download xml metadata from url."""
    with urllib.request.urlopen(url) as response:
        source = response.read()
    return parseString(source)


def get_records_from_file(filename):
    """Get xml metadata from file, used for testing."""
    with open(filename) as f:
        data = "".join(line.rstrip() for line in f)
    return parseString(data)


def get_total_hits(records_blob):
    """Extract total number of hits from xml metadata."""
    hits_tag = records_blob.getElementsByTagName('totalHits')[0]
    return int(hits_tag.firstChild.nodeValue)


def extract_id_number(records_blob):
    """Get ID number from unprocessed xml record."""
    id_tag = records_blob.getElementsByTagName('pres:id')[0]
    return id_tag.firstChild.nodeValue


def get_data():
    """Get parsed data for given keywords and store as json files."""
    log = common.LogFile('', LOGFILE)
    settings = load_settings()
    keywords = settings["keywords"]
    api_key = settings["api_key"]
    for keyword in keywords:
        print("[{}] : fetching data.".format(keyword))
        filename = "results_" + keyword + ".json"
        results = {}
        stop = False
        hits_limit = 500
        start_at = 1
        counter = 0
        while stop is False:
            url = create_url(keyword, hits_limit, start_at, api_key)
            records = get_records_from_url(url)
            total_results = get_total_hits(records)
            records = split_records(records)
            records_on_page = len(records)
            if records_on_page == 0:
                stop = True
            else:
                for record in records:
                    counter = counter + 1
                    id_no = extract_id_number(record)
                    processed_dict = {'ID': id_no, 'problem': []}
                    processed_record = parse_record(
                        record, processed_dict, log)
                    if id_no not in results:
                        results[id_no] = processed_record
                    if counter % 100 == 0:
                        print("Processed {} out of {}".format(
                            counter, total_results))
                start_at = start_at + hits_limit
                time.sleep(THROTTLE)
        print("[{}] : fetched {} records to {}.".format(
            keyword, len(results), filename))
        save_data(results, filename)


if __name__ == "__main__":
    get_data()
