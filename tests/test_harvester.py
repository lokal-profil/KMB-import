#!/usr/bin/python
# -*- coding: utf-8  -*-
import os
import unittest

import batchupload.common as common
import importer.harvester as harvester


class TestUrl(unittest.TestCase):

    def test_create_url_encoded(self):
        keyword = "bruksmiljö"
        hits_limit = 50
        start_at = 1
        api_key = "test"
        result = ("http://kulturarvsdata.se/ksamsok/api?x-api=test"
                  "&method=search&hitsPerPage=50"
                  "&startRecord=1"
                  "&query=serviceOrganization=RA%C3%84%20"
                  "and%20serviceName=KMB%20"
                  "and%20itemType=foto%20and%20mediaLicense=*%20"
                  "and%20text=bruksmilj%C3%B6")
        self.assertEqual(harvester.create_url(
            keyword, hits_limit, start_at, api_key), result)


class TestParser(unittest.TestCase):

    def setUp(self):
        data_dir = os.path.join(os.path.split(__file__)[0], 'data')
        self.cat_file = os.path.join(data_dir, "test_katt.xml")

    def test_get_total_hits(self):
        records = harvester.get_records_from_file(self.cat_file)
        result = 14
        self.assertEqual(harvester.get_total_hits(records), result)

    def test_split_records(self):
        records = harvester.get_records_from_file(self.cat_file)
        records = harvester.split_records(records)
        self.assertEqual(len(records), 14)

    def extract_id_number(self):
        records = harvester.get_records_from_file(self.cat_file)
        record = harvester.split_records(records)[0]
        id_no = "16000300028666"
        self.assertEqual(harvester.extract_id_number(record), id_no)

    def test_parse_entry(self):
        self.maxDiff = None
        records = harvester.get_records_from_file(self.cat_file)
        record = harvester.split_records(records)[4]
        result = {
            "ID": "16000300035205",
            "bbr": [],
            "beskrivning": "Nyfiken katt i området Lindalen.",
            "bildbeteckning": "fd925430",
            "byline": "Bengt A Lundberg",
            "copyright": "RAÄ",
            "date": "1992-06-01",
            "dateFrom": "1992-06-01",
            "dateTo": "1992-06-01",
            "fmis": [],
            "item_classes": [
                "Förortsmiljö",
                "Miljöer",
                "Villastad/villasamhälle"
            ],
            "item_keywords": [],
            "kommun": "0138",
            "kommunName": "Tyresö",
            "lan": "Stockholm",
            "land": "SE",
            "landskap": "Södermanland",
            "license": "by",
            "license_text": "{{CC-BY-2.5|Bengt A Lundberg / Riksantikvarieämbetet}}",
            "motiv": "Tyresö",
            "namn": "Tyresö",
            "problem": [],
            "socken": "0103",
            "sockenName": "Tyresö",
            "source": "http://kmb.raa.se/cocoon/bild/raa-image/16000300035205/normal/1.jpg",
            "thumbnail": "http://kmb.raa.se/cocoon/bild/raa-image/16000300035205/thumbnail/1.jpg"
        }
        record_dict = {}
        log = common.LogFile('', "test_logfile.log")
        self.assertEqual(harvester.parse_record(
            record, record_dict, log), result)


if __name__ == '__main__':
    unittest.main()
