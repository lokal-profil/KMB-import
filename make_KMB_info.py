#!/usr/bin/python
# -*- coding: utf-8  -*-
"""
Construct Kulturmiljöbild-image templates and categories for KMB data.

Transforms the partially processed data from kmb_massload into a
BatchUploadTools compliant json file.
"""
from __future__ import unicode_literals
from collections import OrderedDict
import os.path
import requests

import pywikibot
from pywikibot.data import sparql

import batchupload.common as common
import batchupload.helpers as helpers
import batchupload.listscraper as listscraper
from batchupload.make_info import MakeBaseInfo


MAPPINGS_DIR = 'mappings'
BATCH_CAT = 'Media contributed by RAÄ'  # stem for maintenance categories
BATCH_DATE = '2017-05'  # branch for this particular batch upload
LOGFILE = 'kmb_processing.log'


class KMBInfo(MakeBaseInfo):
    """Construct file descriptions and filenames for the KMB batch upload."""

    def __init__(self, **options):
        """Initialise a make_info object."""
        super(KMBInfo, self).__init__(BATCH_CAT, BATCH_DATE, **options)
        self.commons = pywikibot.Site('commons', 'commons')
        self.wikidata = pywikibot.Site('wikidata', 'wikidata')
        self.category_cache = {}  # cache for category_exists()
        self.photographer_cache = {}
        self.log = common.LogFile('', LOGFILE)

    def load_data(self, in_file):
        """
        Load the provided data (output from kmb_massload).

        Return this as a dict with an entry per file which can be used for
        further processing.

        :param in_file: the path to the metadata file
        :return: dict
        """
        return common.open_and_read_file(in_file, as_json=True)

    # @todo: Not all problems should necessarily result in skipping the image
    #        completely. And some other issues possibly should - T164578
    def process_data(self, raw_data):
        """
        Take the loaded data and construct a KMBItem for each.

        Populates self.data

        :param raw_data: output from load_data()
        """
        d = {}
        for key, value in raw_data.iteritems():
            item = KMBItem(value, self)
            if item.problem:
                text = '{0} -- image was skipped because of: {1}'.format(
                    item.ID, '\n'.join(item.problem))
                pywikibot.output(text)
                self.log.write(text)
            else:
                d[key] = item

        self.data = d

    def load_mappings(self, update_mappings):
        """
        Update mapping files, load these and package appropriately.

        :param update_mappings: whether to first download the latest mappings
        """
        socken_file = os.path.join(MAPPINGS_DIR, 'socken.json')
        kommun_file = os.path.join(MAPPINGS_DIR, 'kommun.json')
        countries_file = os.path.join(MAPPINGS_DIR, 'countries_for_cats.json')
        tags_file = os.path.join(MAPPINGS_DIR, 'tags.json')
        primary_classes_file = os.path.join(
            MAPPINGS_DIR, 'primary_classes.json')
        photographer_file = os.path.join(MAPPINGS_DIR, 'photographers.json')
        kmb_files_file = os.path.join(MAPPINGS_DIR, 'kmb_files.json')
        commonscat_file = os.path.join(MAPPINGS_DIR, 'commonscat.json')
        photographer_page = 'Institution:Riksantikvarieämbetet/KMB/creators'

        if update_mappings:
            query_props = {'P373': 'commonscat'}
            self.mappings['socken'] = KMBInfo.query_to_lookup(
                KMBInfo.build_query('P777', optional_props=query_props.keys()),
                props=query_props)
            self.mappings['kommun'] = KMBInfo.query_to_lookup(
                KMBInfo.build_query('P525', optional_props=query_props.keys()),
                props=query_props)
            self.mappings['photographers'] = self.get_photographer_mapping(
                photographer_page)
            self.mappings['kmb_files'] = self.get_existing_kmb_files()
            self.mappings['commonscat'] = {'bbr': {}, 'fmis': {}}
            KMBInfo.get_commonscat_from_heritage(
                'se-bbr', limit=1000,
                data=self.mappings['commonscat']['bbr'])
            KMBInfo.get_commonscat_from_heritage(
                'se-fornmin', limit=1000,
                data=self.mappings['commonscat']['fmis'])

            # dump to mappings
            common.open_and_write_file(
                socken_file, self.mappings['socken'], as_json=True)
            common.open_and_write_file(
                kommun_file, self.mappings['kommun'], as_json=True)
            common.open_and_write_file(
                photographer_file, self.mappings['photographers'],
                as_json=True)
            common.open_and_write_file(
                kmb_files_file, self.mappings['kmb_files'], as_json=True)
            common.open_and_write_file(
                commonscat_file, self.mappings['commonscat'], as_json=True)
        else:
            self.mappings['socken'] = common.open_and_read_file(
                socken_file, as_json=True)
            self.mappings['kommun'] = common.open_and_read_file(
                kommun_file, as_json=True)
            self.mappings['photographers'] = common.open_and_read_file(
                photographer_file, as_json=True)
            self.mappings['kmb_files'] = common.open_and_read_file(
                kmb_files_file, as_json=True)
            self.mappings['commonscat'] = common.open_and_read_file(
                commonscat_file, as_json=True)

        self.mappings['countries'] = common.open_and_read_file(
            countries_file, as_json=True)
        self.mappings['tags'] = common.open_and_read_file(
            tags_file, as_json=True)
        self.mappings['primary_classes'] = common.open_and_read_file(
            primary_classes_file, as_json=True)

    def get_photographer_mapping(self, photographer_page):
        """
        Load needed values from Wikidata items for matched photographers.

        Loads commonscat (P373) and creator (P1472).

        :param photographer_page: page name on Wikimedia Commons containing
            photographer-wikidata mapping.
        """
        # scrape page
        page = pywikibot.Page(self.commons, photographer_page)
        data = listscraper.parseEntries(
            page.text,
            row_t='User:André Costa (WMSE)/mapping-row',
            default_params={'name': '', 'wikidata': '', 'frequency': ''})

        # load data on page
        photographer_ids = {}
        for entry in data:
            if entry['wikidata'] and entry['name']:
                wikidata = entry['wikidata'][0]
                name = entry['name'][0]
                if wikidata != '-':
                    photographer_ids[name] = wikidata

        # look up data on Wikidata
        photographer_props = {'P373': 'commonscat', 'P1472': 'creator'}
        photographers = {}
        for name, qid in photographer_ids.iteritems():
            photographers[name] = self.load_wd_value(
                qid, photographer_props, self.photographer_cache)
        return photographers

    # @todo:move to BatchUploadTools?
    @staticmethod
    def build_query(main_prop, optional_props=None):
        """
        Construct a sparql query returning items containing a given property.

        The main_prop is given the label 'value' whereas any optional_props
        use the property pid as the label.

        :param main_prop: property pid (with P-prefix) to require
        :param optional_props: list of other properties pids to include as
            optional
        """
        optional_props = optional_props or []
        query = 'SELECT ?item ?value '
        if optional_props:
            query += '?{0} '.format(' ?'.join(optional_props))
        query += 'WHERE { '
        query += '?item wdt:{0} ?value . '.format(main_prop)
        for prop in optional_props:
            query += 'OPTIONAL { ?item wdt:%s ?%s } ' % (prop, prop)
        query += '}'
        return query

    # @todo:move to BatchUploadTools?
    @staticmethod
    def query_to_lookup(query, item_label='item', value_label='value',
                        props=None):
        """
        Fetch sparql result and return it as a lookup table for wikidata id.

        If props are not provided the returned dict simply consists of
        value_label:item_label pairs. If props are provided the returned dict
        becomes value_label:{'wd':item_label, other props}

        :param item_label: the label of the selected wikidata id
        :param value_label: the label of the selected lookup key
        :param props: dict of other properties to save from the results using
            the format label_in_sparql:key_in_output.
        :return: dict
        """
        wdqs = sparql.SparqlQuery()
        result = wdqs.select(query, full_data=True)
        lookup = {}
        for entry in result:
            if entry[value_label] in lookup:
                raise pywikibot.Error('Non-unique value in lookup')
            key = str(entry[value_label])
            qid = entry[item_label].getID()
            if not props:
                lookup[key] = qid
            else:
                lookup[key] = {'wd': qid}
                for prop, label in props.iteritems():
                    if entry[prop] and not entry[prop].type:
                        # pywikibot sparql has issues with unicode
                        # this can be dumped when we switch to PY3
                        entry[prop] = repr(entry[prop]).decode('utf-8')
                    lookup[key][label] = entry[prop]
        return lookup

    # @todo:move to BatchUploadTools?
    def load_wd_value(self, qid, props, cache=None):
        """
        Load the required property values for a provided Wikidata item.

        If a property is not present in the item it is set to None. If the item
        has multiple values the first is selected.

        :param qid: The Qid of the Wikidata item
        :param props: A dict with Pid as key and where the value is used in
            the outputted dict.
        :param cache: The cache in which to store the values
        :return: dict
        """
        if cache and qid in cache:
            return cache[qid]

        data = {}
        wd_item = pywikibot.ItemPage(self.wikidata, qid)
        wd_item.exists()  # load data
        for pid, label in props.iteritems():
            value = None
            claims = wd_item.claims.get(pid)
            if claims:
                value = claims[0].getTarget()
            data[label] = value

        if cache:
            cache[qid] = data
        return data

    def get_existing_kmb_files(self):
        """
        Load all files on commons which contain recognisable external links to
        specific KMB images.

        Filenames include the 'File:' prefix.

        :return: dict with a KMB id as key and a set of matching images as the
            value.
        """
        kmb_files = {}
        self.find_files_from_pattern(
            'http://kmb.raa.se/cocoon/bild/show-image.html?id=', kmb_files)
        self.find_files_from_pattern(
            'http://kulturarvsdata.se/raa/kmb/', kmb_files)

        # convert sets to list (to allow for json storage)
        for k, v in kmb_files.iteritems():
            kmb_files[k] = list(v)

        return kmb_files

    def find_files_from_pattern(self, url_pattern, kmb_files):
        """
        Retrieve all files linking to a kmb file using the provided pattern.

        :param url_pattern: The url_pattern, including the protocol. Only
            url_patterns where the supplied string is directly followed by the
            numeric kmb_id are supported.
        :param kmb_files: the dict in which the found files are stored
            (using kmb_id as key)
        """
        gen = self.linksearch_generator(url_pattern, namespace=6)
        for entry in gen:
            kmb_id = entry['url'][len(url_pattern):]
            if not common.is_int(kmb_id):
                continue
            if kmb_id not in kmb_files:
                kmb_files[kmb_id] = set()
            kmb_files[kmb_id].add(entry['title'])

    # @todo: move this to common/helpers?
    def linksearch_generator(self, url, namespace=None):
        """
        Construct a generator for the list=exturlusage api call.

        A convenience function to wrap around list=exturlusage api call
        since pywikibot.Site.exturlusage only returns page objects
        (not the matched url value).

        :param url: the url to search for, with or without the protocol.
        :param namespace: namespaces (number) to filter by. Provided as either
            a list, a string or an integer.
        :return: generator
        """
        raw_url = url
        default_protocol = 'http'

        protocol, _, url = url.partition('://')
        if not url:
            url = raw_url
            protocol = default_protocol

        if isinstance(namespace, list):
            namespace = '|'.join(namespace)

        g = pywikibot.data.api.ListGenerator(
            "exturlusage", euquery=url, site=self.commons,
            eunamespace=namespace, euprotocol=protocol, euprop='title|url')
        return g

    @staticmethod
    def get_commonscat_from_heritage(dataset, data=None, props=None,
                                     limit=None, srcontinue=None):
        """
        Get all commonscat entries in a dataset from the heritage database.

        This is needed until all of the data exists on Wikidata.
        Also returns the wikidata id if any is known.

        :param dataset: string describing the dataset e.g. se-bbr
        :param data: dict to which information should be added
        :param props: properties to request. Defaults to 'id', 'commonscat' and
            'wd_item'.
        :param limit: the number of records to request at once
            (uses api default unless provided)
        :param srcontinue: continuation parameter to attach to the request
        :return: dict with found data
        """
        props = props or ('id', 'commonscat', 'wd_item')
        data = data or {}
        base_url = 'https://tools.wmflabs.org/heritage/api/api.php?action=search&format=json&srwithcommonscat=1'  # noqa E501
        url = '{0}&srcountry={1}&props={2}'.format(
            base_url, dataset, '|'.join(props))

        if limit:
            url += '&limit={0}'.format(limit)

        if srcontinue:
            url += '&srcontinue={0}'.format(srcontinue)

        # @todo add a try/except
        r = requests.get(url)
        req_data = r.json()

        for entry in req_data['monuments']:
            data[entry['id']] = {
                'wd': entry['wd_item'],
                'cat': entry['commonscat']}

        if req_data.get('continue'):
            KMBInfo.get_commonscat_from_heritage(
                dataset, data=data, props=props, limit=limit,
                srcontinue=req_data['continue']['srcontinue'])

        return data

    # @note: this differs from the one created in RAA-tools
    def generate_filename(self, item):
        """
        Given an item (dict) generate an appropriate filename.

        The filename has the shape: descr - Collection - id
        and does not include filetype

        :param item: the metadata for the media file in question
        :return: str
        """
        return helpers.format_filename(item.get_description(), 'KMB', item.ID)

    def make_info_template(self, item):
        """
        Create the description template for a single KMB entry.

        :param item: the metadata for the media file in question
        :return: str
        """
        template_name = 'Kulturmiljöbild-image'
        template_data = OrderedDict()
        template_data['short title'] = item.namn
        template_data['original description'] = item.beskrivning
        template_data['wiki description'] = item.get_wiki_description()
        template_data['photographer'] = item.get_photographer()
        template_data['depicted place'] = item.get_depicted_place()
        template_data['date'] = item.date
        template_data['permission'] = item.license_text
        template_data['ID'] = item.ID
        template_data['bildbeteckning'] = item.bildbeteckning
        template_data['source'] = item.get_source()
        template_data['notes'] = ''
        template_data['other versions'] = item.get_other_versions()
        txt = helpers.output_block_template(template_name, template_data, 0)

        # append object location if appropriate
        if item.latitude and item.longitude:
            txt += '\n{{Object location dec|%s|%s}}' % (
                item.latitude, item.longitude)

        return txt

    def get_original_filename(self, item):
        """
        Return the url from which the image is fetched.

        :param item: the KMBItem
        :return: str
        """
        return item.source

    def generate_content_cats(self, item):
        """
        Extract any mapped keyword categories or depicted categories.

        :param item: the KMBItem to analyse
        :return: list of categories (without "Category:" prefix)
        """
        # depicted
        found_commonscat = item.make_commonscat_categories(self.category_cache)

        # add tag categories unless a commonscat was found
        if not found_commonscat:
            item.make_item_class_categories(self.category_cache)
            item.make_item_keyword_categories(self.category_cache)

        # Add parish/municipality categorisation when needed
        if item.needs_place_cat:
            item.make_place_category()

        return list(item.content_cats)

    def generate_meta_cats(self, item, content_cats):
        """
        Produce maintenance categories related to a media file.

        :param item: the metadata for the media file in question
        :param content_cats: any content categories for the file
        :return: list of categories (without "Category:" prefix)
        """
        cats = item.meta_cats

        # base cats
        # "Images from the Swedish National Heritage Board" already added by
        # Kulturmiljöbild-image template
        cats.add(self.batch_cat)

        # problem cats
        if not content_cats:
            cats.add(self.make_maintenance_cat('needing categorisation'))
        # if not item.get_description():
        #     cats.append(self.make_maintenance_cat('add description'))

        # creator cats are classified as meta
        photographer_cat = item.get_photographer_cat()
        if photographer_cat:
            cats.add(photographer_cat)

        return list(cats)

    # @todo: move to BatchUploadTools?
    def category_exists(self, cat, cache=None):
        """
        Ensure a given category really exists on Commons.

        If a cache is provided the replies are cached to reduce the number of
        lookups.

        :param cat: category name (with or without "Category" prefix)
        :param cache: The cache in which to store the values
        :return: bool
        """
        if not cat.lower().startswith('category:'):
            cat = 'Category:{0}'.format(cat)

        if cache and cat in cache:
            return cache[cat]

        exists = pywikibot.Page(self.commons, cat).exists()

        if cache:
            cache[cat] = exists

        return exists

    @classmethod
    def main(cls, *args):
        """Command line entry-point."""
        usage = (
            'Usage:'
            '\tpython make_info.py -in_file:PATH -dir:PATH\n'
            '\t-in_file:PATH path to metadata file\n'
            '\t-dir:PATH specifies the path to the directory containing a '
            'user_config.py file (optional)\n'
            '\t-update_mappings:BOOL if mappings should first be updated '
            'against online sources (defaults to True)\n'
            '\tExample:\n'
            '\tpython make_KMB_info.py -in_file:kmb_data.json '
            '-base_name:kmb_output -update_mappings:True -dir:KMB\n'
        )
        info = super(KMBInfo, cls).main(usage=usage, *args)
        if info:
            pywikibot.output(info.log.close_and_confirm())


class KMBItem(object):
    """Store metadata and methods for a single media file."""

    def __init__(self, initial_data, kmb_info):
        """
        Create a KMBItem item from a dict where each key is an attribute.

        :param initial_data: dict of data to set up item with
        :param kmb_info: the KMBInfo instance
        """
        # ensure all required variables are present
        required_entries = ('latitude', 'longitude', 'avbildar')
        for entry in required_entries:
            if entry not in initial_data:
                initial_data[entry] = None

        for key, value in initial_data.iteritems():
            setattr(self, key, value)

        self.wd = {}  # store for relevant Wikidata identifiers
        self.content_cats = set()  # content relevant categories without prefix
        self.meta_cats = set()  # meta/maintenance proto categories
        self.kmb_info = kmb_info  # the KBMInfo instance creating this KMBItem
        self.needs_place_cat = True  # if item needs categorisation by place
        self.log = kmb_info.log

    def get_other_versions(self):
        """
        Build a gallery of all images already on Commons which depict
        (or link to) the same KMB image.

        :return: str
        """
        gallery = ''
        maybe_same = self.kmb_info.mappings['kmb_files'].get(self.ID)

        if maybe_same:
            gallery = '<gallery>\n{0}\n</gallery>'.format(
                '\n'.join(maybe_same))
            self.meta_cats.add('with potential duplicates')

        return gallery

    def get_wiki_description(self):
        """
        Generate the wikitext description.

        * self.motiv is either the same as the name or a free-text description
            of what the image depicts.
        * self.avbildar is a list of wikitext templates (bbr, fmis, shm) which
            all start with a linebreak.

        :return: str
        """
        wiki_description = ''
        if self.motiv != self.namn:
            wiki_description = self.motiv + ' '

        if self.avbildar:
            wiki_description += ' '.join(self.avbildar)

        return wiki_description.strip()

    # @todo: construct a fallback for descriptions,
    #        and ensure meta cats tie in to this
    def get_description(self):
        """Construct an appropriate description."""
        if self.namn:
            # handle problematic common colon in name
            return self.namn.replace('S:t', 'Sankt')
        else:
            raise NotImplementedError

    def make_commonscat_categories(self, cache):
        """
        Set categories based on depicted BBR/FMIS entries.

        Sets a commonscat if one is found else a fallback category for the
        object type.

        Populates self.content_cats and modifies self.needs_place_cat.

        :param cache: cache for category existence
        :return: bool whether a comonscat was found
        """
        commonscat_map = self.kmb_info.mappings['commonscat']

        found_commonscat = False
        for fmis_id in self.fmis:
            if fmis_id in commonscat_map['fmis']:
                found_commonscat = True
                self.content_cats.add(commonscat_map['fmis'][fmis_id]['cat'])
            else:
                self.make_default_fmis_category(cache)
        for bbr_id in self.bbr:
            if bbr_id in commonscat_map['bbr']:
                found_commonscat = True
                self.content_cats.add(commonscat_map['bbr'][bbr_id]['cat'])
            else:
                self.make_default_bbr_category(cache)

        if found_commonscat:
            self.needs_place_cat = False

        return found_commonscat

    def make_default_fmis_category(self, cache):
        """
        Set FMIS default categories based on municipality.

        Makes a guess for the category name. If not found it defaults to
        County (Län) + Province (Landskap) categories.

        Populates self.content_cats and modifies self.needs_place_cat.

        :param cache: cache for category existence
        """
        muni_cat = self.municipal_subcategory(
            'Archaeological monuments', cache)

        if muni_cat:
            self.needs_place_cat = False
            self.content_cats.add(muni_cat)
        else:
            self.content_cats.add(
                'Archaeological monuments in {}'.format(self.landskap))
            self.content_cats.add(
                'Archaeological monuments in {} County'.format(self.lan))

    def make_default_bbr_category(self, cache):
        """
        Set BBR default categories based on municipality.

        Makes a guess for the category name. If not found it defaults to
        County (Län) category.

        Populates self.content_cats and modifies self.needs_place_cat.

        :param cache: cache for category existence
        """
        muni_cat = self.municipal_subcategory('Listed buildings', cache)

        if muni_cat:
            self.needs_place_cat = False
            self.content_cats.add(muni_cat)
        else:
            test_cat = 'Listed buildings in {} County'.format(self.lan)
            if self.kmb_info.category_exists(test_cat, cache):
                self.content_cats.add(test_cat)
            else:
                self.log.write('{0} -- Had to fall back on "Listed buildings '
                               'in Sweden".'.format(self.ID))
                self.content_cats.add('Listed buildings in Sweden')

    def municipal_subcategory(self, cat_base, cache):
        """
        Find a suitable subcategory on municipality level for a category stem.

        :param cat_base: the base name/stem of the category
            e.g. "Listed buildings"
        :param cache: cache for category existence
        :return: a successful category match or None
        """
        test_cat = None
        if self.kommunName:
            test_cat = '{cat_base} in {muni} Municipality'.format(
                cat_base=cat_base, muni=self.kommunName)
            if not self.kmb_info.category_exists(test_cat, cache):
                test_cat = '{cat_base} in {}'.format(
                    cat_base=cat_base, muni=self.kommunName)
                if not self.kmb_info.category_exists(test_cat, cache):
                    test_cat = None
        return test_cat

    def make_item_class_categories(self, cache):
        """
        Construct categories from the item class values.

        :param cache: cache for category existence
        """
        primary_classes = self.kmb_info.mappings['primary_classes']

        # find the class/tag that is also in primary_classes
        primary_tag = None
        intersection = list(set(primary_classes) & set(self.item_classes))
        if len(intersection) == 1:
            primary_tag = intersection[0]
        elif len(intersection) > 1:
            pywikibot.warning(
                "Found two primary classes. Need to rethink the logic. "
                "{idno}: '{primary}'".format(
                    idno=self.ID, primary="', '".join(intersection)))

        if not primary_tag or not self.add_single_tag(primary_tag, cache):
            for tag in self.item_classes:
                if tag != primary_tag:
                    self.add_single_tag(tag, cache)

    def make_item_keyword_categories(self, cache):
        """
        Construct categories from the item keyword values.

        :param cache: cache for category existence
        """
        for tag in self.item_keywords:
            self.add_single_tag(tag, cache)

    def add_single_tag(self, tag, cache):
        """
        Construct a category from a single provided tag.

        The mapping follows four scenarios:
        * A guessed, and validated, category on municipal level in Sweden.
        * An exact category on national level for Sweden.
        * A guessed, and validated, category where 'Sweden' is replaced
          by the country name in the Sweden specific category.
        * A default category (either a "to be categorised by country" category
          or the subject category without any country information.

        Populates self.content_cats

        :param cache: cache for category existence
        :param tag: the tag (string) to be mapped to a category
        :return: bool whether a category was added
        """
        # avoid similar cat to fallback in make_commonscat_categories
        if (self.fmis and tag == 'Fornminnen') or \
                (self.bbr and tag.startswith('Byggnadsminnen')):
            return False

        tag_map = self.kmb_info.mappings['tags']
        country_map = self.kmb_info.mappings['countries']
        if tag in tag_map:
            cat = None
            if (not self.land or self.land == 'SE') and tag_map[tag].get('SE'):
                cat = tag_map[tag].get('SE')

                # attempt municipal categorisation
                if self.kommunName:
                    test_cat = tag_map[tag].get('SE').replace(
                        'Sweden', '{} Municipality'.format(self.kommunName))
                    if self.kmb_info.category_exists(test_cat, cache):
                        self.needs_place_cat = False
                        cat = test_cat
            elif self.land in country_map and tag_map[tag].get('base'):
                test_cat = tag_map[tag].get('base').format(
                    country_map(self.land))
                if self.kmb_info.category_exists(test_cat, cache):
                    self.needs_place_cat = False
                    cat = test_cat

            if not cat:
                # fallback independent of country
                cat = tag_map[tag].get('default')

            if cat:
                self.content_cats.add(cat)
                return True
        return False

    def get_photographer(self):
        """
        Return a correctly formated photographer value in wikitext.

        :return: str
        """
        photographer_map = self.kmb_info.mappings['photographers']
        photographer = None
        if (not self.byline.startswith('{{')) and \
                (self.byline in photographer_map):
            creator = photographer_map[self.byline].get('creator')
            if creator:
                photographer = 'Creator:{0}'.format(creator)

        return photographer or self.byline  # fallback on plain byline

    def get_photographer_cat(self):
        """
        Return the commonscat for the photographer.

        :return: str
        """
        photographer_map = self.kmb_info.mappings['photographers']
        if self.byline in photographer_map:
            return photographer_map[self.byline].get('commonscat')

    def get_source(self):
        """Produce a linked source statement."""
        template = '{{Riksantikvarieämbetet cooperation project|coh}}'
        txt = ''
        if self.byline:
            txt += '%s / ' % self.byline
        txt += 'Kulturmiljöbild, Riksantikvarieämbetet'
        return '[{url} {link_text}]\n{template}'.format(
            url=self.source, link_text=txt, template=template)

    def make_place_category(self):
        """Add category for parish or municipality."""
        kommun_map = self.kmb_info.mappings['kommun']
        socken_map = self.kmb_info.mappings['socken']
        cat = None

        if not self.land or self.land == 'SE':
            if self.socken:
                cat = socken_map[self.socken]['commonscat']

            if not cat and self.kommun:
                cat = kommun_map[self.kommun]['commonscat']

        if cat:
            self.content_cats.add(cat)
            return True
        else:
            self.meta_cats.add('needing categorisation (place)')
            return False

    def get_depicted_place(self):
        """
        Get a linked version of the depicted place.

        If no 'land' is given the image is assumed to depict Sweden.

        :return: depicted_place as wikitext
        """
        kommun_map = self.kmb_info.mappings['kommun']
        socken_map = self.kmb_info.mappings['socken']
        depicted_place = None
        if not self.land or self.land == 'SE':
            depicted_place = '{{Country|1=SE}}'

            if self.kommun:
                self.wd['kommun'] = kommun_map[self.kommun]['wd']
                depicted_place += ', {{city|%s}}' % self.wd['kommun']

                if self.socken:
                    self.wd['socken'] = socken_map[self.socken]['wd']
                    depicted_place += ', {{city|%s}}' % self.wd['socken']
            else:
                if self.lan:
                    depicted_place += ', %s' % self.lan
                elif self.landskap:
                    depicted_place += ', %s' % self.landskap
                else:
                    self.meta_cats.add(
                        'needing categorisation (no municipality)')
        else:
            depicted_place = '{{Country|1=%s}}' % self.land
            self.meta_cats.add('needing categorisation (not from Sweden)')

        return depicted_place


if __name__ == "__main__":
    KMBInfo.main()
