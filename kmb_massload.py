#!/usr/bin/python
# -*- coding: utf-8  -*-
"""Get parsed data for whole kmb hitlist and store as json."""
from __future__ import unicode_literals
import urllib2
import codecs
import time
import json
from xml.dom.minidom import parse
import batchupload.helpers as helpers
THROTTLE = 0.5


def parser(dom, A):
    """
    Parse and process the xml metadata into a dict.

    This is all legacy code from RAA-tools
    """
    A['problem'] = None
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
            A['problem'] = 'Complain to Lokal_Profil: coord was not a point : %s' % A['ID']
    # do visualizes separately need not be shm/fmi/bbr etc. can be multiple
    A['bbr'] = A['fmis'] = False
    xmlTag = dom.getElementsByTagName('ns5:visualizes')
    if not len(xmlTag) == 0:
        A['avbildar'] = []
        for x in xmlTag:
            url = x.attributes['rdf:resource'].value
            process_depicted(A, url)
    # and an attempt at determining categories
    xmlTag = dom.getElementsByTagName('ns5:itemClassName')
    if not len(xmlTag) == 0:
        A['tagg'] = []
        for x in xmlTag:
            try:
                A['tagg'].append(x.childNodes[0].data.strip())
            except IndexError:
                # Means data for this field was mising
                print "Empty 'ns5:itemClassName' in %s" % A['ID']
    else:
        A['tagg'] = []
    xmlTag = dom.getElementsByTagName('ns5:itemKeyWord')
    if not len(xmlTag) == 0:
        if len(A['tagg']) == 0:
            A['tagg'] = []
        for x in xmlTag:
            try:
                A['tagg'].append(x.childNodes[0].data.strip())
            except IndexError:
                # Means data for this field was mising
                print "Empty 'ns5:itemKeyWord' in %s" % A['ID']
    # memory seems to be an issue so kill dom
    del dom, xmlTag
    process_date(A)
    process_byline(A)
    # ##Creator
    process_license(A)
    return A


# @todo: consider using the kulturarvsdata tool to resolve bbr type
def process_depicted(A, url):
    """
    Process any FMIS or BBR entries in depicted and store back in entry.

    Also set bbr, fmis, shm if these are encountered.

    Note that the url need not be for a shm/fmi/bbr etc. entry and there might
    be multiple entries of different or same types.
    """
    if url.startswith('http://kulturarvsdata.se/raa/fmi/'):
        A['fmis'] = True
        crop = len('http://kulturarvsdata.se/raa/fmi/')
        A['avbildar'].append('{{Fornminne|%s}}' % url[crop:])
    elif url.startswith('http://kulturarvsdata.se/raa/bbr/'):
        A['bbr'] = True
        crop = len('http://kulturarvsdata.se/raa/bbr/')
        num = url[crop:crop+3]
        typ = ''
        if num == '214':
            typ = '|b'
        elif num == '213':
            typ = '|a'
        elif num == '212':
            typ = '|m'
        A['avbildar'].append('{{BBR|%s%s}}' % (url[crop:], typ))
    elif url.startswith('http://kulturarvsdata.se/raa/bbra/'):
        A['bbr'] = True
        crop = len('http://kulturarvsdata.se/raa/bbra/')
        A['avbildar'].append('{{BBR|%s|a}}' % url[crop:])
    elif url.startswith('http://kulturarvsdata.se/raa/bbrb/'):
        A['bbr'] = True
        crop = len('http://kulturarvsdata.se/raa/bbrb/')
        A['avbildar'].append('{{BBR|%s|b}}' % url[crop:])
    elif url.startswith('http://kulturarvsdata.se/raa/bbrm/'):
        A['bbr'] = True
        crop = len('http://kulturarvsdata.se/raa/bbrm/')
        A['avbildar'].append('{{BBR|%s|m}}' % url[crop:])
    else:
        A['avbildar'].append(url)


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
    if entry['byline'] in ('Okänd, Okänd', 'Okänd'):
        entry['byline'] = '{{unknown}}'
    elif not entry['byline']:
        entry['byline'] = '{{not provided}}'
    else:
        entry['byline'] = helpers.flip_name(entry['byline'])


# @todo: update this per new copyright rules - T164568
def process_license(entry):
    """
    Identify the license and store back in entry.

    Don't include name/byline if unknown.
    """
    if entry['license']:
        trim = 'http://kulturarvsdata.se/resurser/License#'
        entry['license'] = entry['license'].strip()[len(trim):]
    # if not copyright = RAÄ and if license='' then, this is probably unfree
    if (entry['license'] == 'pdmark') or \
            (entry['copyright'].strip() == 'Utgången upphovsrätt'):
        entry['license'] = '{{PD-Sweden-photo}}'
    elif (entry['license'] == 'by') or (entry['copyright'].strip() == 'RAÄ'):
        # consider changing this to AND since there might be a cc-by image which isn't from RAA.
        # Alternatively have another if inside which checks whethere copyright = RAA
        param = '}}'
        if (entry['byline'] == '{{unknown}}') or \
                (entry['byline'] == '{{not provided}}'):
            pass
        else:
            param = '|%s}}' % entry['byline']
        entry['license'] = '{{CC-BY-RAÄ%s' % param
    else:
        entry['problem'] = (
            'Det verkar tyvärr som om licensen inte är fri. Copyright="%s", License="%s".<br/>'
            '<small>Om informationen ovan är inkorrekt så informera gärna Lokal_Profil.</small>' % (entry['copyright'], entry['license']))


def kmb_wrapper(idno):
    """Get partially processed dataobject for a given kmb id."""
    A = {'ID': idno}
    fil = urllib2.urlopen('http://kulturarvsdata.se/raa/kmb/' + idno)
    dom = parse(fil)
    fil.close()
    del fil
    return parser(dom, A)


def load_list(filename='kmb_hitlist.json'):
    """Load json list."""
    f = codecs.open(filename, 'r', 'utf-8')
    data = json.load(f)
    f.close()
    return data


def output_blob(data, filename='kmb_data.json'):
    """Dump data as jon blob."""
    f = codecs.open(filename, 'w', 'utf-8')
    f.write(json.dumps(data, indent=2, ensure_ascii=False))
    print "%s created" % filename


def run(start=None, end=None):
    """Get parsed data for whole kmb hitlist and store as json."""
    hitlist = load_list()
    if start or end:
        hitlist = hitlist[start:end]
    data = {}
    total_count = len(hitlist)
    for count, kmb in enumerate(hitlist):
        data[kmb] = kmb_wrapper(kmb)
        time.sleep(THROTTLE)
        if count % 100 == 0:
            timestamp = time.strftime('%H:%M:%S')
            print '%s - %d of %d parsed' % (timestamp, count, total_count)
    output_blob(data)
