import xml.etree.ElementTree as etree
from epg_tool.channel import channel
from epg_tool.program import program
import pandas as pd

def parse_xml(location):
    channels = {}
    programs = []

    tree = etree.parse(location)
    root = tree.getroot()

    # parse channels and add to dictionary
    for ch in root.findall('channel'):
        cur_channel = channel()
        cur_channel.parse_xml(ch)
        channels[cur_channel.id] = cur_channel

    # parse programs, add to list, and add to the lists which will make up the pandas array
    index = []
    pd_data = {'Start_Time':[], 'Stop_Time':[], 'Title':[], 'Subtitle':[], 'Channel':[], \
            'Description':[], 'Episode':[], 'Is_Movie':[], 'Array_Index':[]}
    for p in root.findall('programme'):
        cur_program = program(title=None, start=None, stop=None, channel=None, sub_title=None, \
                            description=None, previously_shown=None, ratings=None, episode_num=None, \
                            categories=None, premiere=False, tz=None, icon=None)
        cur_program.parse_xml(p)
        programs.append(cur_program)

        # Now create the pandas data
        index.append(cur_program.start)
        pd_data['Start_Time'].append(cur_program.start)
        pd_data['Stop_Time'].append(cur_program.stop)
        pd_data['Title'].append(cur_program.title)
        pd_data['Subtitle'].append(cur_program.sub_title)
        pd_data['Channel'].append(cur_program.channel)
        pd_data['Description'].append(cur_program.description)
        pd_data['Episode'].append(cur_program.episode_num)
        pd_data['Array_Index'].append(len(pd_data['Array_Index']))

        # There is no absolutely straightforward way of determining if something is a movie...
        if cur_program.title[:7].lower() == 'movie: ':
            pd_data['Is_Movie'].append(True)
        elif cur_program.categories is not None:
            is_a_movie = False
            for cat in cur_program.categories:
                if 'movie' in cat.lower():
                    is_a_movie = True
            
            pd_data['Is_Movie'].append(is_a_movie)
        else:
            pd_data['Is_Movie'].append(False)
    
    df = pd.DataFrame(pd_data, \
                                columns=['Start_Time', 'Stop_Time', 'Title', 'Subtitle', 'Channel', \
                                        'Description', 'Episode', 'Is_Movie', 'Array_Index'], \
                                index=index)

    return (programs, channels, df)
