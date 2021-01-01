from lxml import etree
import statistics
from datetime import timedelta
from epg_tool.channel import channel
from epg_tool.program import program
from fuzzywuzzy import process, fuzz
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
    
    df = pd.DataFrame(pd_data, \
                      columns=['Start_Time', 'Stop_Time', 'Title', 'Subtitle', 
                               'Channel', 'Description', 'Episode', 'Array_Index'], \
                      index=index)
    df.sort_index(inplace=True)

    return (programs, channels, df)

def write_xml(programs, channels, location):
    # Now that we have theoretically fixed all of the data we need to output it
    root = etree.Element('tv')
    root.attrib['source-info-name'] = 'http://xmltv.net' 
    root.attrib['generator-info-url'] = 'http://www.xmltv.org'
    for ch in channels.values():
        root.append(ch.to_xml())
    for p in programs:
        root.append(p.to_xml())

    with open(location, 'w') as xmltv_file:
        xmltv_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
        xmltv_file.write('<!DOCTYPE tv SYSTEM "xmltv.dtd">\n')

        tree = etree.ElementTree(root)
        xmltv_file.write(etree.tounicode(tree, pretty_print=True))

def transfer_channel_ids(to_channels, to_programs, from_channels):
    # We need to have both the channels and programs we are transfering information to.
    # That is because we need to adjust the programs to point to the right channels
    # after updating their id info.

    ch_mapping = {}
    return_channels = {}
    # Do this one channel at a time
    for to_ch in to_channels.values():
        # Search through the 'from_channels' until we find the right lcn
        found = False
        for from_ch in from_channels.values():
            if to_ch.lcn == from_ch.lcn:
                # Success!
                found = True
                ch_mapping[to_ch.id] = from_ch.id
                to_ch.id = from_ch.id
                return_channels[to_ch.id] = to_ch
        if not found:
            return_channels[to_ch.id] = to_ch

    # Now remap the programs to their possibly new channels
    for i in range(len(to_programs)):
        if to_programs[i].channel in ch_mapping:
            to_programs[i].channel = ch_mapping[to_programs[i].channel]

    return (return_channels, to_programs)

def __int_prog_to_eit(eit_prog, int_prog):
    eit_prog.title             = int_prog.title
    eit_prog.sub_title         = int_prog.sub_title
    eit_prog.description       = int_prog.description
    eit_prog.previously_shown  = int_prog.previously_shown
    eit_prog.ratings           = int_prog.ratings
    eit_prog.episode_num       = int_prog.episode_num
    eit_prog.imdb_id           = int_prog.imdb_id
    eit_prog.categories        = int_prog.categories
    eit_prog.premiere          = int_prog.premiere
    eit_prog.icon              = int_prog.icon

    return eit_prog

def __match_processor(program, search, df, internet_programs):
    if search == 'Title':
        if program.title is None:
            return None
        else:
            best_matches = process.extract(program.title, df['Title'], scorer=fuzz.ratio)
    elif search == 'Description':
        if program.description is not None:
            best_matches =  process.extract(program.description, df['Description'], scorer=fuzz.token_set_ratio)
        elif program.sub_title is not None:
            best_matches =  process.extract(program.sub_title, df['Description'], scorer=fuzz.token_set_ratio)
        else:
            return None
    else:
        # Unsupported mode
        return None
    
    # Did we find anything?
    if not best_matches:
        return None

    # Do we have duplicate titles in the window?
    if len(best_matches) > 1 and best_matches[0][0] == best_matches[1][0] \
                             and best_matches[0][1] > 85:
        # If we are in title search mode, check for full duplicates, then go to description search mode
        if search == 'Title':
            idx_1 = df.loc[best_matches[0][2]]['Array_Index']
            idx_2 = df.loc[best_matches[1][2]]['Array_Index']
            if isinstance(idx_1, pd.Series):
                idx_1 = idx_1.values[0]
            if isinstance(idx_2, pd.Series):
                idx_2 = idx_2.values[0]

            if __are_progs_same(internet_programs[idx_1], internet_programs[idx_2]):
                return idx_1
            else:
                df = df[df['Title'] == best_matches[0][0]]
                return __match_processor(program, 'Description', df, internet_programs)

        elif search == 'Description':
            # In this instance it appears that the episode is played more than once.
            # We can jus return it as is!
            idx = df.loc[best_matches[0][2]]['Array_Index']
            if isinstance(idx, pd.Series):
                idx = idx.values[0]

            return idx
    elif best_matches[0][1] > 85:
        idx = df.loc[best_matches[0][2]]['Array_Index']
        if isinstance(idx, pd.Series):
            idx = idx.values[0]

        return idx
    
    return None

def __are_progs_same(prog_1, prog_2):
    if prog_1.title == prog_2.title \
            and prog_1.sub_title == prog_2.sub_title \
            and prog_1.description == prog_2.description:
        return True
    else:
        return False

def match_headend_to_internet(tvhd_programs, internet_programs, internet_channels, internet_df):
    matches = []

    for i in range(len(tvhd_programs)):
        p = tvhd_programs[i]

        if p.channel not in internet_channels:
            # There is no channel to search on!
            continue

        # Create a range of +/- 8 hours in which to look for the specified program on the channel of interest
        # if it isn't in that time range we are just going to punt on it.
        td = timedelta(hours=8)
        df = internet_df.loc[(p.start-td).strftime("%Y-%m-%d %H:%M:%S"):(p.start+td).strftime("%Y-%m-%d %H:%M:%S")]
        df = df[df['Channel'] == p.channel]

        # First do a title search
        idx = __match_processor(p, 'Title', df, internet_programs)
        if idx:
            matches.append(i)
            tvhd_programs[i] = __int_prog_to_eit(p, internet_programs[idx])
            continue

        # Now try a description search
        idx = __match_processor(p, 'Description', df, internet_programs)
        if idx:
            matches.append(i)
            tvhd_programs[i] = __int_prog_to_eit(p, internet_programs[idx])
            continue

    return (tvhd_programs, len(matches))
