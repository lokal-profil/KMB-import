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

import batchupload.helpers as helpers
import batchupload.common as common

SETTINGS = "settings.json"
THROTTLE = 0.5


def load_settings(filename=SETTINGS):
    """Load settings from file."""
    return common.open_and_read_file(filename, as_json=True)


def save_data(data, filename='kmb_data.json'):
    """Dump data as json blob."""
    common.open_and_write_file(filename, data, as_json=True)
    print("Saved file: {}.".format(filename))


class BbrTemplate(object):
    """Convenience class for BBR template formatting and logic."""

    def __init__(self, idno, bbr_type=None):
        """Initialise the template with an idno and optional type."""
        self.template_type = 'bbr'
        self.idno = idno
        self.bbr_type = bbr_type

    def output(self):
        """Output the template as wikitext."""
        if self.determine_type():
            return '{{BBR|%s|%s}}' % (self.idno, self.bbr_type)
        return '{{BBR|%s}}' % self.idno

    def determine_type(self):
        """Determine the bbr_type if not already known."""
        if not self.bbr_type:
            num = self.idno[:3]
            if num == '214':
                self.bbr_type = 'b'
            elif num == '213':
                self.bbr_type = 'a'
            elif num == '212':
                self.bbr_type = 'm'

        return (self.bbr_type is not None)


class FmisTemplate(object):
    """Convenience class for FMIS template formatting and logic."""

    def __init__(self, idno):
        """Initialise the template with an idno."""
        self.template_type = 'fmis'
        self.idno = idno

    def output(self):
        """Output the template as wikitext."""
        return '{{Fornminne|%s}}' % self.idno


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


def process_date(entry):
    """Create date field from date_to and date_from."""
    # (can one exist and the other not?)
    if entry['date_from'] == entry['date_to']:
        entry['date'] = entry['date_from']
    elif (entry['date_from'][:4] == entry['date_to'][:4]) and \
            (entry['date_from'][5:] == '01-01') and \
            (entry['date_to'][5:] == '12-31'):
        entry['date'] = entry['date_from'][:4]
    else:
        entry['date'] = '{{other date|between|%s|%s}}' % (
            entry['date_from'], entry['date_to'])


def process_byline(byline):
    """Handle unknown entries and rearrange names."""
    if byline:
        if 'okänd' in byline.lower():
            byline = '{{unknown}}'
        else:
            byline = helpers.flip_name(byline)
    else:
        byline = '{{not provided}}'
    return byline


def process_license(entry):
    """
    Identify the license, as wikitext, and store as new property.

    Must be called after process_byline().
    Possible licenses are listed in
    http://kulturarvsdata.se/resurser/license/license.owl
    Don't include name/byline if unknown.
    """
    entry['copyright'] = entry['copyright'].strip()
    template = None
    byline = None
    license_text = None

    if entry['license']:
        trim = 'http://kulturarvsdata.se/resurser/License#'
        entry['license'] = entry['license'].strip()[len(trim):]

    # determine template
    if (entry['license'] == 'pdmark') or \
            (entry['copyright'] == 'Utgången upphovsrätt'):
        template = 'PD-Sweden-photo'
    elif entry['license'] == 'by':
        template = 'CC-BY-2.5'
    elif entry['license'] == 'by-sa':
        template = 'CC-BY-SA-2.5'
    elif entry['license'] == 'cc0':
        template = 'CC0'

    # determine byline if possible
    if template in ('CC-BY-2.5', 'CC-BY-SA-2.5'):
        byline = []
        if entry['byline'] not in ('{{unknown}}', '{{not provided}}'):
            byline.append(entry['byline'])

        if entry['copyright'] == 'RAÄ':
            byline.append('Riksantikvarieämbetet')
        elif entry['copyright']:
            byline.append(entry['copyright'])

    if template:
        if byline:
            license_text = '{{%s|%s}}' % (template, ' / '.join(byline))
        else:
            license_text = '{{%s}}' % template
    else:
        entry['problem'].append(
            "It looks like the license isn't free. "
            'Copyright="{0}", License="{1}".'.format(
                entry['copyright'], entry['license']))
    entry['license_text'] = license_text


def normalize_ids(entry):
    """Normalize municipality, parish and country codes."""
    if entry['municipality']:
        entry['municipality'] = '{:04d}'.format(
            int(entry['municipality']))  # zero pad
    if entry['parish']:
        entry['parish'] = '{:04d}'.format(int(entry['parish']))  # zero pad
    if entry['country']:
        entry['country'] = entry['country'].upper()


def parse_record(dom):
    """Parse and process the xml metadata into a dict."""
    tagDict = {
        'byline': ('pres:byline', None),
        'copyright': ('pres:copyright', None),
        'country': ('ns5:country',
                    'rdf:resource',
                    'http://kulturarvsdata.se/resurser/aukt/geo/country#'),
        'county': ('ns5:countyName', None),
        'date_from': ('ns5:fromTime', None),
        'date_to': ('ns5:toTime', None),
        'description': ('pres:description', None),  # med ord
        'id_label': ('pres:idLabel', None),
        'id_no': ('pres:id', None),
        'license': ('ns5:mediaLicense', None),
        'motif': ('pres:motive', None),
        'municipality': ('ns6:municipality',
                         'rdf:resource',
                         ('http://kulturarvsdata.se/'
                          'resurser/aukt/geo/municipality#')),
        'municipality_name': ('ns5:municipalityName', None),
        'parish': ('ns6:parish',
                   'rdf:resource',
                   'http://kulturarvsdata.se/resurser/aukt/geo/parish#'),
        'parish_name': ('ns5:parishName', None),
        'province': ('ns5:provinceName', None),
        'source': ('ns5:lowresSource', None),
        'thumbnail': ('ns5:thumbnailSource', None),
        'uri': ('pres:entityUri', None),
        'label': ('ns5:itemLabel', None),
    }
    record_dict = {}
    record_dict["problem"] = []
    for tag in tagDict.keys():
        xmlTag = dom.getElementsByTagName(tagDict[tag][0])
        if tagDict[tag][1] is None:
            try:
                content = xmlTag[0].childNodes[0].data.strip('"')
            except IndexError:
                content = None
        else:
            try:
                content = xmlTag[0].attributes[
                    tagDict[tag][1]].value[len(tagDict[tag][2]):]
            except IndexError:
                content = None
        record_dict[tag] = content

    record_dict["byline"] = process_byline(record_dict["byline"])
    process_date(record_dict)
    process_coordinates(dom, record_dict)
    process_license(record_dict)

    xmlTag = dom.getElementsByTagName('ns5:visualizes')
    record_dict['bbr'] = []
    record_dict['fmis'] = []
    if not len(xmlTag) == 0:
        record_dict['depicts'] = []
        for x in xmlTag:
            url = x.attributes['rdf:resource'].value
            process_depicted(record_dict, url)

    process_tags(record_dict, dom, 'item_classes', 'ns5:itemClassName')
    process_tags(record_dict, dom, 'item_keywords', 'ns5:itemKeyWord')
    normalize_ids(record_dict)
    handle_gotland(record_dict)
    record_dict = OrderedDict(sorted(record_dict.items()))
    return record_dict


def handle_gotland(entry):
    """
    Ensure Gotland has municipality code and not just county/province.

    Relies on the fact that county/province and municipality are equivalent
    in this one case. Which is probably also why this particular municipality
    id is frequently left out.
    """
    if (not entry['municipality'] and
            'Gotland' in (entry['county'], entry['province'])):
        entry['municipality'] = '0980'  # Gotlands kommun
        entry['municipality_name'] = 'Gotland'


def process_tags(entry, dom, label, xml_tag):
    """
    Process tags of a given type.

    :param entry: the dict of parsed data for the image
    :param dom: the dom being analysed
    :param label: the label under which the processed tags should be stored
    :param xml_tag: the xml tag name to search for
    :param log: log to write to
    """
    entry[label] = []
    elements = dom.getElementsByTagName(xml_tag)
    for element in elements:
        try:
            entry[label].append(element.childNodes[0].data.strip())
        except IndexError:
            # Means data for this field was mising
            print("")


def process_depicted(entry, url):
    """
    Process any FMIS or BBR entries in depicted and store back in entry.

    Also store bbr, fmis ids that are encountered.
    Note that the url need not be for an fmi/bbr entry and there might
    be multiple entries of different or the same type.
    """
    idno = url.split('/')[-1]
    mapping = {
        'http://kulturarvsdata.se/raa/fmi/': FmisTemplate(idno),
        'http://kulturarvsdata.se/raa/bbra/': BbrTemplate(idno, 'a'),
        'http://kulturarvsdata.se/raa/bbrb/': BbrTemplate(idno, 'b'),
        'http://kulturarvsdata.se/raa/bbrm/': BbrTemplate(idno, 'm'),
        'http://kulturarvsdata.se/raa/bbr/': BbrTemplate(idno)
    }
    avbildar = None
    for pattern, template in mapping.items():
        if url.startswith(pattern):
            if idno != url[len(pattern):].strip():
                raise ValueError(
                    'Depicted started with "{0}" but idno has wrong '
                    'format: {1}'.format(pattern, url))
            entry[template.template_type].append(idno)
            avbildar = template.output()
            break

    if not avbildar:
        avbildar = url

    entry['depicts'].append(avbildar)


def process_coordinates(dom, record_dict):
    """Process coordinates from metadata."""
    xmlTag = dom.getElementsByTagName('georss:where')
    if not len(xmlTag) == 0:
        xmlTag = xmlTag[0].childNodes[0].childNodes[0]
        cs = xmlTag.attributes['cs'].value
        coords = xmlTag.childNodes[0].data.split(cs)
        if len(coords) == 2:
            record_dict['latitude'] = coords[1][:8]
            record_dict['longitude'] = coords[0][:8]
        else:
            record_dict['problem'].append(
                'Coord was not a point: "{0}"'.format(cs))


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


def get_data():
    """Get parsed data for given keywords and store as json files."""
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
                    processed_record = parse_record(record)
                    id_no = processed_record["id_no"]
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
