#!/usr/bin/python
# -*- coding: utf-8  -*-
import os
import pywikibot
import batchupload.common as common
import batchupload.postUpload as postupload

DIR_PATH = os.path.dirname(os.path.realpath(__file__))


def load_churches():
    church_commonscat_file = os.path.join(DIR_PATH, "church_commonscat.json")
    raw_church = common.open_and_read_file(
        church_commonscat_file, as_json=True)

    # use commonscat as keys
    commonscat_churches = {}
    for k, v in raw_church.items():
        if not v.get("commonscat"):
            continue
        commonscat = v.pop("commonscat")
        if commonscat not in commonscat_churches:
            commonscat_churches[commonscat] = v
        else:
            commonscat_churches[commonscat].update(v)

    return commonscat_churches


def main():
    commonscat_churches = load_churches()
    main_counter = 0
    muni_counter = 0
    for commonscat, churches in commonscat_churches.items():
        muni_counter = 0
        for church_name, church_cat in churches.items():
            muni_counter += postupload.trim_second_category(
                church_cat, commonscat, in_filename="- KMB -")
        main_counter += muni_counter
        pywikibot.output(
            'Removed {num} pages from {commonscat}'.format(
                num=muni_counter, commonscat=commonscat))

    pywikibot.output('Removed {num} pages in total'.format(num=main_counter))


if __name__ == '__main__':
    main()
