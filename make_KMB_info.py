#!/usr/bin/python
# -*- coding: utf-8  -*-
"""
Construct Kulturmiljöbild-image templates and categories for KMB data.

Transforms the partially processed data from kmb_massload into a
BatchUploadTools compliant json file.
"""
from __future__ import unicode_literals
from collections import OrderedDict

import pywikibot
import batchupload.helpers as helpers
import batchupload.common as common
from batchupload.make_info import MakeBaseInfo


BATCH_CAT = 'Media contributed by RAÄ'  # stem for maintenance categories
BATCH_DATE = '2017-05'  # branch for this particular batch upload


class KMBInfo(MakeBaseInfo):
    """Construct file descriptions and filenames for the KMB batch upload."""

    def __init__(self, **options):
        """Initialise a make_info object."""
        super(KMBInfo, self).__init__(BATCH_CAT, BATCH_DATE, **options)

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
            item = KMBItem(value)
            if item.problem:
                pywikibot.output(
                    'The {0} image was skipped because of: {1}'.format(
                        item.ID, item.problem))
            else:
                d[key] = item

        self.data = d

    # @todo: break out substed lists as mappings - T164567
    def load_mappings(self, update=True):
        """
        Update mapping files, load these and package appropriately.

        :param update: whether to first download the latest mappings
        """
        # Currently these are handled as substed templates.
        # redo as either:
        # * offline mappings: If so consider re-jiging make_info to not assume
        #   it is alway an online table.
        # * online tables: If so consider re-jiging make_info to not assume
        #   these need alway be dumped a offline files first.
        pass

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

    # @todo:
    # * Add a collaboration + COH template in the source field - T164569
    # * Add the source link to the source field - T164569
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
        template_data['photographer'] = self.get_photographer(item)
        template_data['depicted place'] = item.get_depicted_place()
        template_data['date'] = item.date
        template_data['permission'] = item.license
        template_data['ID'] = item.ID
        template_data['bildbeteckning'] = item.bildbeteckning
        template_data['source'] = item.get_source()
        template_data['notes'] = ''
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

    # @todo: load mapping list instead of substing template - T164567
    def get_photographer(self, item):
        """
        Return a correctly formated photographer value

        :param item: the KMBItem
        :return: str
        """
        template = 'User:Lokal_Profil/nycklar/creators'
        return '{{safesubst:%s|%s|t}}' % (template, item.byline)

    # @todo:
    # * use fmis + bbr for (cached) commonscat searches on wikidata. And if
    #   found then skip various other. - T164572
    # * move some of this to KMBItem and set cats at the same time as the
    #   text is processed.
    # * Implement taggs from mapping - T164567
    # * Add parish/municipality categorisation when needed - T164576
    def generate_content_cats(self, item):
        """
        Extract any mapped keyword categories or depicted categories.

        :param item: the KMBItem to analyse
        :return: list of categories (without "Category:" prefix)
        """
        cats = []

        # depicted
        if item.fmis:
            cats.append('Archaeological monuments in %s' % item.landskap)
            cats.append('Archaeological monuments in %s County' % item.lan)
        if item.bbr:
            cats.append('Listed buildings in Sweden')

        # must be better to do this via safesubst
        for tagg in item.tagg:
            # @todo: '{{safesubst:User:Lokal_Profil/nycklar/cats|%s|%s|%s}}' % (tagg, item.land.upper(), item.landskap)
            pass

        cats = list(set(cats))  # remove any duplicates
        return cats

    # @todo: Implement creator from mapping - T164567
    def generate_meta_cats(self, item, content_cats):
        """
        Produce maintenance categories related to a media file.

        :param item: the metadata for the media file in question
        :param content_cats: any content categories for the file
        :return: list of categories (without "Category:" prefix)
        """
        pass
        cats = []

        # base cats
        # "Images from the Swedish National Heritage Board" already added by
        # Kulturmiljöbild-image template
        cats.append(self.batch_cat)

        # problem cats
        if not content_cats:
            cats.append(self.make_maintenance_cat('improve categories'))
        # if not item.get_description():
        #     cats.append(self.make_maintenance_cat('add description'))

        # creator cats are classified as meta
        # @todo: '{{safesubst:User:Lokal_Profil/nycklar/creators|%s|c}} % item.byline

        cats = list(set(cats))  # remove any duplicates
        return cats

    @classmethod
    def main(cls, *args):
        """Command line entry-point."""
        usage = (
            'Usage:'
            '\tpython make_info.py -in_file:PATH -dir:PATH\n'
            '\t-in_file:PATH path to metadata file\n'
            '\t-dir:PATH specifies the path to the directory containing a '
            'user_config.py file (optional)\n'
            '\tExample:\n'
            '\tpython make_KMB_info.py -in_file:KMB/kmb_data.json '
            '-base_name:kmb_output -dir:KMB\n'
        )
        super(KMBInfo, cls).main(usage=usage, *args)


class KMBItem(object):
    """Store metadata and methods for a single media file."""

    def __init__(self, initial_data):
        """
        Create a KMBItem item from a dict where each key is an attribute.

        :param initial_data: dict of data to set up item with
        """
        # ensure all required varaibles are present
        required_entries = ('latitude', 'longitude', 'avbildar')
        for entry in required_entries:
            if entry not in initial_data:
                initial_data[entry] = None

        for key, value in initial_data.iteritems():
            setattr(self, key, value)

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

    def get_source(self):
        """Produce a linked source statement."""
        template = '{{Riksantikvarieämbetet cooperation project|coh}}'
        txt = ''
        if self.byline:
            txt += '%s / ' % self.byline
        txt += 'Kulturmiljöbild, Riksantikvarieämbetet'
        return '[{url} {link_text}]\n{template}'.format(
            url=self.source, link_text=txt, template=template)

    # @todo: use kommunkod/sockenkod to go via wikidata - T164576
    def get_depicted_place(self):
        """Get a linked version of the depicted place."""
        s_triggers = ('a', 'o', 'u', 'å', 'e', 'i', 'y', 'ä', 'ö', 's', 'x')
        depicted_place = None
        if not self.land or self.land == 'se':
            kommun_link = self.kommunName
            # add s if kommun does not end in s or a vowel
            if not any(self.kommunName.endswith(x) for x in s_triggers):
                kommun_link += 's'
            kommun_link += ' kommun'
            depicted_place = '{{Country|1=SE}}, %s, [[:sv:%s|%s]]' % (
                self.lan, kommun_link, self.kommunName)

            if self.socken:
                template = 'User:Lokal_Profil/nycklar/socknar'
                depicted_place += ', [[:sv:{{safesubst:%s|%s}}|%s (%s)]]' % (
                    template, self.socken, self.sockenName, self.landskap)
        else:
            depicted_place = '{{Country|1=%s}}' % self.land.upper()

        return depicted_place


if __name__ == "__main__":
    KMBInfo.main()
