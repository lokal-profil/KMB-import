#!/usr/bin/python
# -*- coding: utf-8  -*-
import os
import unittest

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

    def test_process_byline_unknown(self):
        byline = "Okänd"
        result = "{{unknown}}"
        self.assertEqual(harvester.process_byline(byline), result)

    def test_process_byline_flip(self):
        byline = "Svensson, Jan A"
        result = "Jan A Svensson"
        self.assertEqual(harvester.process_byline(byline), result)

    def test_process_byline_none(self):
        byline = None
        result = "{{not provided}}"
        self.assertEqual(harvester.process_byline(byline), result)

    def test_parse_entry(self):
        records = harvester.get_records_from_file(self.cat_file)
        record = harvester.split_records(records)[0]
        result = {
            "bbr": [],
            "byline": "Bengt A Lundberg",
            "copyright": "RAÄ",
            "country": "SE",
            "county": "Skåne",
            "date": "2000-08-23",
            "date_from": "2000-08-23",
            "date_to": "2000-08-23",
            "description": "Katt på trappan.",
            "fmis": [],
            "id_label": "f0009418",
            "id_no": "16000300028666",
            "item_classes": [
                "Bondgård",
                "Byggnadsverk",
                "Jordbruk"
            ],
            "item_keywords": [
                "Byggnadsminnen",
                "Riksintressen"
            ],
            "label": "Kumlatofta",
            "license": "by",
            "license_text": "{{CC-BY-2.5|Bengt A Lundberg / Riksantikvarieämbetet}}",
            "motif": "Kumlatofta",
            "municipality": "1265",
            "municipality_name": "Sjöbo",
            "parish": "1215",
            "parish_name": "Everlöv",
            "problem": [],
            "province": "Skåne",
            "source": "http://kmb.raa.se/cocoon/bild/raa-image/16000300028666/normal/1.jpg",
            "thumbnail": "http://kmb.raa.se/cocoon/bild/raa-image/16000300028666/thumbnail/1.jpg",
            "uri": "http://kulturarvsdata.se/raa/kmb/16000300028666"
        }
        self.assertEqual(harvester.parse_record(record), result)


if __name__ == '__main__':
    unittest.main()
