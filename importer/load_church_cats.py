#!/usr/bin/python
# -*- coding: utf-8  -*-
"""
Maintenance script for creating a church categories per municipality mapping.

This takes too long to run to be worth doing on the fly as part of
KMBInfo.load_mappings().
"""
from __future__ import unicode_literals
import os
import pywikibot as pwb
import batchupload.common as common
MAPPINGS_DIR = 'mappings'


def main():
    """Request church categories and output to json."""
    church_cats = get_all_church_cats()
    church_file = os.path.join(MAPPINGS_DIR, 'churches.json')
    common.open_and_write_file(
        church_file, church_cats, as_json=True)


def get_all_church_cats():
    """Extract all church categories, per municipality."""
    site = pwb.Site('commons', 'commons')
    top_cat = pwb.Category(site, 'Category:Churches in Sweden by municipality')
    church_cats_per_muni = {}
    for sub_cat in top_cat.subcategories():
        # municipal level
        sub_cat_name = sub_cat.title(withNamespace=False)
        if not sub_cat_name.startswith('Churches in '):
            raise pwb.Error(
                'Basic assumption failed: "{}" does not start with '
                '"Churches in"'.format(sub_cat_name))
        sub_cat_name_end = sub_cat_name[len('Churches in '):]
        muni_name = sub_cat_name_end.partition(',')[0]
        church_dict = {}
        loop_over_candidates(sub_cat, church_dict, sub_cat_name_end, depth=0)
        church_cats_per_muni[muni_name] = church_dict
        pwb.output('{} done found {}'.format(muni_name, len(church_dict)))
    return church_cats_per_muni


def loop_over_candidates(parent_cat, church_dict, parent_ending, depth=0):
    """Determine if a category is a candidate or if we should go deeper."""
    if depth > 3:
        # Don't go too deep but make sure it'snot completely discarded
        pwb.warning('Too deep: {}'.format(
            parent_cat.title(withNamespace=False)))
        add_if_likely_church(parent_cat, church_dict)
        return
    for church_cat in parent_cat.subcategories():
        name = church_cat.title(withNamespace=False)
        if (has_subcats(church_cat) and
                (name.endswith(parent_ending) or
                 name.startswith('Churches in '))):
            loop_over_candidates(
                church_cat, church_dict, parent_ending, depth=depth+1)
        else:
            add_if_likely_church(church_cat, church_dict)


def add_if_likely_church(church_cat, church_dict):
    """Determine if a category is that for a church, if so add to dict."""
    endings = (
        'kyrka', 'kyrkan',
        'kloster', 'klostret',
        'kapell', 'kapellet',
        'missionshus', 'missionshuset',
        'kyrkoruin', 'kyrkoruinen'
    )
    name = church_cat.title(withNamespace=False)
    name = name.partition(',')[0]
    if any(name.lower().endswith(end) for end in endings):
        church_dict[name] = church_cat.title()


def has_subcats(cat):
    """Whether a given Category contains any sub-categories."""
    return len(list(cat.subcategories())) > 0


if __name__ == '__main__':
    """Command-line entry point."""
    main()
