import xml.etree.ElementTree as etree

class channel:

    def __init__(self, id=None, display_name=None, lcn=None, icon=None):
        self.id = id
        self.display_name = display_name
        self.lcn = lcn
        self.icon = icon

    def parse_xml(self, channel):
        self.id = channel.attrib['id']
        
        # tvheadend outputs lcn as a display-name....
        names = channel.findall('display-name')
        if len(names) == 1:
            self.display_name = names[0].text
            self.lcn = channel.find('lcn').text
        elif len(names) == 2:
            self.display_name = names[0].text
            self.lcn = names[1].text

        if channel.find('icon') is not None:
            self.icon = channel.find('icon').attrib['src']

    def to_xml(self):
        channel = etree.Element('channel', id=self.id)

        if self.display_name is not None:
            etree.SubElement(channel, 'display-name').text = self.display_name
        if self.lcn is not None:
            etree.SubElement(channel, 'lcn').text = self.lcn
        if self.icon is not None:
            etree.SubElement(channel, 'icon', src=self.icon)
        
        return channel