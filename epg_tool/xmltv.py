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
        for from_ch in from_channels.values():
            if to_ch.lcn == from_ch.lcn:
                # Success!
                ch_mapping[to_ch.id] = from_ch.id
                to_ch.id = from_ch.id
                return_channels[to_ch.id] = to_ch

    # Now remap the programs to their possibly new channels
    for i in range(len(to_programs)):
        if to_programs[i].channel in ch_mapping:
            to_programs[i].channel = ch_mapping[to_programs[i].channel]

    return (return_channels, to_programs)


def __array_has_data(array):
    output = False # Default to this
    for element in array:
        if element is not None:
            output = True

    return output

def __get_matching_index(program, df, search_type):
    if df is not None and len(df) != 0:
        search = None
        if search_type == 'Title':
            search = program.title
        elif search_type == 'Description':
            search = program.sub_title
        
        # An unsupported search was performed.
        if search is None:
            return None

        # Do everything with fuzzywuzzy to catch all the edge cases and such
        if __array_has_data(df[search_type]) and search is not None:
            # Find the best match in the data provided
            best_match = process.extractOne(search, df[search_type])

            # Ensure that it passes a minimum threshold
            if fuzz.token_set_ratio(best_match[0], search) > 85 or \
                search_type == 'Title' and fuzz.ratio(best_match[0], search) > 85:
                # Extract the episode in which we are interested
                episode = df[df[search_type] == best_match[0]]

                if episode is None or len(episode) == 0:
                    # Something bad happened somewhere...
                    return None
                if len(episode) == 1:
                    return episode.iloc[0].Array_Index
                elif len(episode) > 1:
                    # Do they have the same title, sub_title, and description? If so they are the same thing
                    if len(episode['Description'].unique()) == 1 and \
                            len(episode['Subtitle'].unique()) == 1 and \
                            len(episode['Title'].unique()):
                        return episode.iloc[0].Array_Index
                    elif search_type == 'Description' and len(episode['Title'].unique()) != 1:
                        # We found a decent description, but we don't have a good title. Just
                        # find the best title of the bunch and call it a day.
                        best_match = process.extractOne(program.title, episode['Title'])
                        episode = episode[episode['Title'] == best_match[0]]
                        return episode.iloc[0].Array_Index
                    elif search_type == 'Description':
                        # Just return the one that is closest in time
                        best_idx = None
                        best_time = None
                        for k in range(len(episode)):
                            if best_idx is None or program.start - episode.iloc[k].Start_Time < best_time:
                                best_idx = k
                                best_time = program.start - episode.iloc[k].Start_Time
                        return episode.iloc[best_idx].Array_Index
                    elif search_type == 'Title':
                        # If we arrived here we did a title search, and got more than one title. The
                        # only way we would be able to deconvolute this is by the description.
                        # Recursion is your friend here :)
                        desc_idx = __get_matching_index(program, episode, 'Description')
                        if desc_idx is None:
                            return desc_idx
                        else:
                            # That way we can determine whether or not it wasn't found or just whether 
                            # we found too many things
                            return -1
                    
    # We didn't find anything :(
    return None


def match_headend_to_internet(tvhd_programs, tvhd_channels, internet_programs, internet_channels, internet_df):
    # Make some things to be used later
    td = timedelta(hours=2)
    multiple_title_list = []
    channel_timediff = {}
    for ch in tvhd_channels:
        channel_timediff[ch] = []


    for i in range(len(tvhd_programs)):
        p = tvhd_programs[i]
        
        if p.channel not in internet_channels:
            # This channel isn't in the internet file
            continue

        # Now let's look in the range of 2 hrs before and 2 hrs after when this thing is supposed to 
        # be on and see if there is something in the internet file that could supplant it.
        df_window = internet_df.loc[(p.start-td).strftime("%Y-%m-%d %H:%M:%S"):(p.start+td).strftime("%Y-%m-%d %H:%M:%S")]

        # Now that we have things in the window, we really just want a specific channel
        if df_window is not None and len(df_window) != 0:
            df_window = df_window[df_window['Channel'] == p.channel]

        # See if we can find it.
        int_prog_idx = __get_matching_index(p, df_window, 'Title')

        if int_prog_idx is None:
            # No dice, try finding by description instead of title
            int_prog_idx = __get_matching_index(p, df_window, 'Description')

        if int_prog_idx is None:
            # This time just try everything in that channel by title
            df_channel = internet_df[internet_df['Channel'] == p.channel]
            int_prog_idx = __get_matching_index(p, df_channel, 'Title')

            if int_prog_idx is None:
                int_prog_idx = __get_matching_index(p, df_channel, 'Description')

        if int_prog_idx == -1:
            multiple_title_list.append(i)
        elif int_prog_idx is None:
            # Couldn't find anything
            pass
        else:
            # We found what we were looking for in these ones. Before we replace what we have
            # with this one, store the time differential so that we can do a better job with
            # the ones where we had multiple titles.
            time_diff = (p.start - internet_programs[int_prog_idx].start).total_seconds()
            channel_timediff[p.channel].append(time_diff)

            tvhd_programs[i].title = internet_programs[int_prog_idx].title
            tvhd_programs[i].sub_title = internet_programs[int_prog_idx].sub_title
            tvhd_programs[i].description = internet_programs[int_prog_idx].description
            tvhd_programs[i].previously_shown = internet_programs[int_prog_idx].previously_shown
            tvhd_programs[i].ratings = internet_programs[int_prog_idx].ratings
            tvhd_programs[i].episode_num = internet_programs[int_prog_idx].episode_num
            tvhd_programs[i].categories = internet_programs[int_prog_idx].categories
            tvhd_programs[i].premiere = internet_programs[int_prog_idx].premiere
            tvhd_programs[i].icon = internet_programs[int_prog_idx].icon

     # Calculate some median time differences
    for ch in tvhd_channels.keys():
        time_diff_list = channel_timediff[ch]
        if len(time_diff_list) > 0:
            channel_timediff[ch] = timedelta(seconds=statistics.median(time_diff_list))
        else:
            channel_timediff[ch] = timedelta(seconds=0)

    # We finished the first pass, now go through those with back-to-back airings
    for i in multiple_title_list:
        p = tvhd_programs[i]

        if p.channel not in internet_channels:
            # This channel isn't in the internet file
            continue

        new_start = p.start - channel_timediff[p.channel]
        td2 = timedelta(minutes=10)
        df_window = internet_df.loc[(new_start-td2).strftime("%Y-%m-%d %H:%M:%S"):(new_start+td2).strftime("%Y-%m-%d %H:%M:%S")]

        # Now that we have things in the window, we really just want a specific channel
        if df_window is not None and len(df_window) != 0:
            df_window = df_window[df_window['Channel'] == p.channel]

        # See if we can find it.
        int_prog_idx = __get_matching_index(p, df_window, 'Title')

        if int_prog_idx is None:
            # No dice, try finding by description instead of title
            int_prog_idx = __get_matching_index(p, df_window, 'Description')

        if int_prog_idx is None or int_prog_idx == -1:
            continue # can't do anything  here
        else:
            tvhd_programs[i].title = internet_programs[int_prog_idx].title
            tvhd_programs[i].sub_title = internet_programs[int_prog_idx].sub_title
            tvhd_programs[i].description = internet_programs[int_prog_idx].description
            tvhd_programs[i].previously_shown = internet_programs[int_prog_idx].previously_shown
            tvhd_programs[i].ratings = internet_programs[int_prog_idx].ratings
            tvhd_programs[i].episode_num = internet_programs[int_prog_idx].episode_num
            tvhd_programs[i].categories = internet_programs[int_prog_idx].categories
            tvhd_programs[i].premiere = internet_programs[int_prog_idx].premiere
            tvhd_programs[i].icon = internet_programs[int_prog_idx].icon

    return tvhd_programs
