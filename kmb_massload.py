#!/usr/bin/python
# -*- coding: utf-8  -*-
"""Get parsed data for whole kmb hitlist and store as json."""
from __future__ import unicode_literals
import urllib2
import time
from xml.dom.minidom import parse
import pywikibot
import batchupload.helpers as helpers
import batchupload.common as common


THROTTLE = 0.5
LOGFILE = 'kmb_massloading.log'


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

    # @todo: consider using the kulturarvsdata tool to resolve bbr type
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


def parser(dom, A, log):
    """
    Parse and process the xml metadata into a dict.

    This is all legacy code from RAA-tools
    """
    # tags to get
    tagDict = {'namn': ('ns5:itemLabel', None),            # namn
               'beskrivning': ('pres:description', None),  # med ord
               'byline': ('pres:byline', None),            # Okänd, Okänd -> {{unknown}}. kasta om sa "efternamn, fornamn" -> "fornamn efternamn".
               'motiv': ('pres:motive', None),             # också namn? use only if different from itemLabel
               'copyright': ('pres:copyright', None),      # RAÄ or Utgången upphovsrätt note that ns5:copyright can be different
               'license': ('ns5:mediaLicense', None),      # good as comparison to the above
               'source': ('ns5:lowresSource', None),       # source for image (hook up to download) can I check for highres?
               'dateFrom': ('ns5:fromTime', None),
               'dateTo': ('ns5:toTime', None),             # datum kan saknas
               'bildbeteckning': ('pres:idLabel', None),   # bildbeteckning
               'landskap': ('ns5:provinceName', None),
               'lan': ('ns5:countyName', None),
               'land': ('ns5:country', 'rdf:resource', 'http://kulturarvsdata.se/resurser/aukt/geo/country#'),
               'kommun': ('ns6:municipality', 'rdf:resource', 'http://kulturarvsdata.se/resurser/aukt/geo/municipality#'),
               'kommunName': ('ns5:municipalityName', None),
               'socken': ('ns6:parish', 'rdf:resource', 'http://kulturarvsdata.se/resurser/aukt/geo/parish#'),
               'sockenName': ('ns5:parishName', None),
               'thumbnail': ('ns5:thumbnailSource', None)}
    # also has muni, kommun etc. combine some of these (linked to sv.wiki?) into "place"
    # if cc-by then include byline in copyright/license
    for tag in tagDict.keys():
        xmlTag = dom.getElementsByTagName(tagDict[tag][0])
        if not len(xmlTag) == 0:
            if tagDict[tag][1] is None:
                try:
                    A[tag] = xmlTag[0].childNodes[0].data.strip('"')
                except IndexError:
                    # Means data for this field was mising
                    A[tag] = None
            else:
                A[tag] = xmlTag[0].attributes[tagDict[tag][1]].value[len(tagDict[tag][2]):]
        else:
            A[tag] = ''

    # do coordinates separately
    xmlTag = dom.getElementsByTagName('georss:where')
    if not len(xmlTag) == 0:
        xmlTag = xmlTag[0].childNodes[0].childNodes[0]
        cs = xmlTag.attributes['cs'].value
        # dec = xmlTag.attributes['decimal'].value
        coords = xmlTag.childNodes[0].data.split(cs)
        if len(coords) == 2:
            A['latitude'] = coords[1][:8]
            A['longitude'] = coords[0][:8]
        else:
            A['problem'].append('Coord was not a point: "{0}"'.format(cs))

    # do ns5:visualizes separately
    A['bbr'] = set()
    A['fmis'] = set()
    xmlTag = dom.getElementsByTagName('ns5:visualizes')
    if not len(xmlTag) == 0:
        A['avbildar'] = []
        for x in xmlTag:
            url = x.attributes['rdf:resource'].value
            process_depicted(A, url)

    # attempt at determining tags (used for catgories)
    process_tags(A, dom, 'item_classes', 'ns5:itemClassName', log)
    process_tags(A, dom, 'item_keywords', 'ns5:itemKeyWord', log)

    # memory seems to be an issue so kill dom
    del dom, xmlTag

    process_date(A)
    process_byline(A)
    process_license(A)

    # convert sets to lists to allow for json storage)
    A['bbr'] = list(A['bbr'])
    A['fmis'] = list(A['fmis'])
    normalise_ids(A)
    handle_gotland(A)
    return A


def process_tags(entry, dom, label, xml_tag, log):
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
            log.write('{0} -- Empty "{1}"'.format(entry['ID'], xml_tag))


def normalise_ids(entry):
    """Normalise municipality, parish and country codes."""
    if entry['kommun']:
        entry['kommun'] = '{:04d}'.format(int(entry['kommun']))  # zero pad
    if entry['socken']:
        entry['socken'] = '{:04d}'.format(int(entry['socken']))  # zero pad
    if entry['land']:
        entry['land'] = entry['land'].upper()


def handle_gotland(entry):
    """
    Ensure Gotland has municipality code and not just county/province.

    Relies on the fact that county/province and municipality are equivalent
    in this one case. Which is probably also why this particular municipality
    id is frequently left out.
    """
    if not entry['kommun'] and 'Gotland' in (entry['lan'], entry['landskap']):
        entry['kommun'] = '0980'  # Gotlands kommun
        entry['kommunName'] = 'Gotland'


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
    for pattern, template in mapping.iteritems():
        if url.startswith(pattern):
            if idno != url[len(pattern):].strip():
                raise ValueError(
                    'Depicted started with "{0}" but idno has wrong '
                    'format: {1}'.format(pattern, url))
            entry[template.template_type].add(idno)
            avbildar = template.output()
            break

    if not avbildar:
        avbildar = url

    entry['avbildar'].append(avbildar)


def process_date(entry):
    """Create date field from dateTo and dateFrom."""
    # (can one exist and the other not?)
    if entry['dateFrom'] == entry['dateTo']:
        entry['date'] = entry['dateFrom']
    elif (entry['dateFrom'][:4] == entry['dateTo'][:4]) and \
            (entry['dateFrom'][5:] == '01-01') and \
            (entry['dateTo'][5:] == '12-31'):
        entry['date'] = entry['dateFrom'][:4]
    else:
        entry['date'] = '{{other date|between|%s|%s}}' % (
            entry['dateFrom'], entry['dateTo'])


def process_byline(entry):
    """Handle unknown entries and rearrange names."""
    if 'okänd' in entry['byline'].lower():
        entry['byline'] = '{{unknown}}'
    elif not entry['byline']:
        entry['byline'] = '{{not provided}}'
    else:
        entry['byline'] = helpers.flip_name(entry['byline'])


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


def kmb_wrapper(idno, log):
    """Get partially processed dataobject for a given kmb id."""
    A = {'ID': idno, 'problem': []}
    url = 'http://kulturarvsdata.se/raa/kmb/{0}'.format(idno)
    try:
        f = urllib2.urlopen(url)
    except urllib2.HTTPError as e:
        error_message = '{1}: {2}'.format(e, url)
        A['problem'].append(error_message)
        log.write('{0} -- {1}'.format(idno, error_message))
    else:
        dom = parse(f)
        A = parser(dom, A, log)
        f.close()
        del f

    return A


def load_list(filename='kmb_hitlist.json'):
    """Load json list."""
    return common.open_and_read_file(filename, as_json=True)


def output_blob(data, filename='kmb_data.json'):
    """Dump data as json blob."""
    common.open_and_write_file(filename, data, as_json=True)
    pywikibot.output('{0} created'.format(filename))


def run(start=None, end=None):
    """Get parsed data for whole kmb hitlist and store as json."""
    log = common.LogFile('', LOGFILE)
    hitlist = load_list()
    if start or end:
        hitlist = hitlist[start:end]
    data = {}
    total_count = len(hitlist)
    for count, kmb in enumerate(hitlist):
        data[kmb] = kmb_wrapper(kmb, log)
        time.sleep(THROTTLE)
        if count % 100 == 0:
            pywikibot.output(
                '{time:s} - {count:d} of {total:d} parsed'.format(
                    time=time.strftime('%H:%M:%S'), count=count,
                    total=total_count))
    output_blob(data)
    pywikibot.output(log.close_and_confirm())


if __name__ == '__main__':
    run()
