from datetime import datetime
import xml.etree.ElementTree as etree

class program:

    def __init__(self, title=None, start=None, stop=None, channel=None, sub_title=None, 
                 description=None, previously_shown=None, ratings=None, episode_num=None, 
                 categories=None, premiere=False, tz=None, icon=None, imdb_id=None):
        self.title = title
        self.start = start
        self.stop = stop
        self.channel = channel
        self.sub_title = sub_title
        self.description = description
        self.previously_shown = previously_shown
        self.ratings = ratings
        self.episode_num = episode_num
        self.categories = categories
        self.premiere = premiere
        self.tz = tz
        self.icon = icon
        self.imdb_id = imdb_id

    def parse_xml(self, program):
        # start & timezone
        start = program.attrib['start'].split()
        self.tz = start[1]
        self.start = datetime.strptime(start[0], "%Y%m%d%H%M%S") # Ignore the time zone callout

        # stop
        stop = program.attrib['stop'].split()
        self.stop = datetime.strptime(stop[0], "%Y%m%d%H%M%S") # Ignore the time zone callout

        # channel
        self.channel = program.attrib['channel']

        # title
        self.title = program.find('title').text

        # sub-title
        if program.find('sub-title') is not None:
            self.sub_title = program.find('sub-title').text

        # description
        if program.find('desc') is not None:
            self.description = program.find('desc').text

        # previously-shown
        if program.find('previously-shown') is not None:
            self.previously_shown = True
        else:
            self.previously_shown = False

        # icon
        if program.find('icon') is not None:
            self.icon = program.find('icon').attrib['src']

        # categories
        self.categories = None
        for cat in program.findall('category'):
            if self.categories == None:
                self.categories = [cat.text]
            else:
                self.categories.append(cat.text)

        # rating
        self.ratings = program.find('rating')
        if self.ratings is not None:
            rat = self.ratings
            self.ratings = None

            for val in rat.findall('value'):
                if self.ratings == None:
                    self.ratings = [val.text]
                else:
                    self.ratings.append(val.text)

        # episode-num - take the xmltv_ns thing and the imdb.com thing
        for epn in program.findall('episode-num'):
            if epn.attrib['system'] == 'xmltv_ns':
                self.episode_num = epn.text
            if epn.attrib['system'] == 'imdb.com':
                self.imdb_id = epn.text
                if self.imdb_id.startswith(r'title/'):
                    self.imdb_id = self.imdb_id[6:]

        # premiere
        if program.find('premiere') is not None:
            self.premiere = True

    def to_xml(self):
        start = '{} {}'.format(self.start.strftime("%Y%m%d%H%M%S"), self.tz)
        stop = '{} {}'.format(self.stop.strftime("%Y%m%d%H%M%S"), self.tz)
        program = etree.Element('programme', start=start, stop=stop, channel=self.channel)

        etree.SubElement(program, 'title').text = self.title
        if self.sub_title is not None:
            etree.SubElement(program, 'sub-title').text = self.sub_title
        if self.description is not None:
            etree.SubElement(program, 'desc').text = self.description
        if self.categories is not None:
            for cat in self.categories:
                etree.SubElement(program, 'category').text = cat
        if self.icon is not None:
            etree.SubElement(program, 'icon', src=self.icon)
        if self.episode_num is not None:
            etree.SubElement(program, 'episode-num', system='xmltv_ns').text = self.episode_num
        if self.previously_shown:
            etree.SubElement(program, 'previously-shown')
        if self.ratings is not None:
            rating = etree.SubElement(program, 'rating')
            for rat in self.ratings:
                etree.SubElement(rating, 'value').text = rat
        if self.premiere:
            etree.SubElement(program, 'premiere')

        return program