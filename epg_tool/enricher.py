import os
import json
import time
import datetime
import pandas as pd
import tmdbsimple as tmdb

from fuzzywuzzy import process, fuzz

class enricher_tmdb:
    def __init__(self, cachedir):
        self.cachedir = cachedir
        self.pulled_series = []
        self.pulled_episodes = []
        self.update_written = False

        # Get the show_dataframe all ready to go
        if os.path.isfile(os.path.join(self.cachedir, 'show_dataframe.csv')):
            self.series_df = pd.read_csv(os.path.join(self.cachedir, 'show_dataframe.csv'))
        else:
            self.series_df = pd.DataFrame(columns=['series_name', 'channel_id', 'imdb_id', 'tmdb_id'])

        # Determine when the last update was
        if os.path.isfile(os.path.join(self.cachedir, 'last_update.txt')):
            with open(os.path.join(self.cachedir, 'last_update.txt')) as f:
                self.last_update = datetime.datetime.strptime(f.read(), '%Y-%m-%d %H:%M:%S')
        else:
            self.last_update = None

    def __write_update(self):
        self.update_written = True
        with open(os.path.join(self.cachedir, 'last_update.txt'), 'w') as f:
            cur_time = datetime.datetime.now()
            # We don't care about fractional seconds
            cur_time = cur_time - datetime.timedelta(microseconds=cur_time.microsecond)
            f.write(str(cur_time))

    def __get_series_info(self, tmdb_id, force_update=False):
        filepath = os.path.join(self.cachedir, '{}.json'.format(tmdb_id))

        # Even if we "forced_update" it will be ignored if we already updated this one this time around
        if os.path.isfile(filepath) and not force_update or \
                os.path.isfile(filepath) and tmdb_id in self.pulled_series:
            with open(filepath) as json_file:
                return json.load(json_file)
        else:
            try:
                result = tmdb.TV(tmdb_id).info()
            except:
                time.sleep(100)
                result = tmdb.TV(tmdb_id).info()

            # If we didn't find anything return nothing
            if not result:
                return None

            # Save that result to disk and then return the contents
            with open(filepath, 'w') as json_file:
                json.dump(result, json_file, indent=4)
            if not self.update_written:
                self.__write_update()
            self.pulled_series.append(tmdb_id)
            return result

    def __get_episode_info(self, tmdb_id, force_update=False):
        filepath = os.path.join(self.cachedir, '{}_episode_info.json'.format(tmdb_id))

        if (os.path.isfile(filepath) and not force_update) or \
                (os.path.isfile(filepath) and tmdb_id in self.pulled_episodes):
            with open(filepath) as json_file:
                return json.load(json_file)
        else:
            episodes = []
            series_info = self.__get_series_info(tmdb_id)

            # If we didn't find anything, return nothing
            if not series_info or not series_info['seasons']:
                return None

            for season in series_info['seasons']:
                try:
                    result = tmdb.TV_Seasons(tmdb_id, season['season_number']).info()
                except:
                    time.sleep(100)
                    result = tmdb.TV_Seasons(tmdb_id, season['season_number']).info()
                episodes += (result['episodes'])

            if not episodes:
                return None

            # Cache this baby to disk
            with open(filepath, 'w') as json_file:
                json.dump(episodes, json_file, indent=4)
            if not self.update_written:
                self.__write_update()
            self.pulled_episodes.append(tmdb_id)
            return episodes

    def __get_movie_info(self, tmdb_id):
        try:
            result = tmdb.Movies(tmdb_id).info()
        except:
            time.sleep(100)
            result = tmdb.Movies(tmdb_id).info()

        return result

    def __find_episode(self, program, episode_info):
        # Do we even have anything to do?
        if not episode_info:
            return (program, False)

        # First parse the episode_info to get the bits that we need
        sub_titles   = []
        descriptions = []
        for ep in episode_info:
            sub_titles.append(ep['name'])
            descriptions.append(ep['overview'])
        
        replace_subitle = True
        # Now do a search by the sub_title (episode name)
        if program.sub_title is not None:
            # Some episodes are really multiple episodes separated by a /
            if '/' in program.sub_title:
                sub_t = program.sub_title.split('/')[0]
                replace_subitle = False
            else:
                sub_t = program.sub_title
            
            best_match = process.extractOne(sub_t, sub_titles)
            # Check to make sure it is a good match
            if best_match[0] is not None and \
                    fuzz.token_sort_ratio(best_match[0].lower(), sub_t.lower()) > 85:
                # We have found our winner!
                idx = sub_titles.index(best_match[0])
                program = self.__embed_episode_info(program, episode_info[idx])
                if replace_subitle:
                    program.sub_title = episode_info[idx]['name']
                return (program, True)

            # Sometimes EIT data happens to use the "description" of the episode as the subtitle...
            best_match = process.extractOne(program.sub_title, descriptions)
            if best_match[0] is not None and \
                    fuzz.token_sort_ratio(best_match[0].lower(), program.sub_title.lower()) > 85:
                # We have found our winner!
                idx = descriptions.index(best_match[0])
                program = self.__embed_episode_info(program, episode_info[idx])
                if replace_subitle:
                    program.sub_title = episode_info[idx]['name']
                return (program, True)

        # If we made it to this point try looking by the description
        if program.description is not None:
            best_match = process.extractOne(program.description, descriptions)
            # Check to make sure the highest passes a decent threshold
            if best_match[0] is not None and fuzz.token_sort_ratio(best_match[0].lower(), program.description.lower()) > 85:
                # Send it back!
                idx = descriptions.index(best_match[0])
                program = self.__embed_episode_info(program, episode_info[idx])
                if replace_subitle:
                    program.sub_title = episode_info[idx]['name']
                return (program, True)

        return (program, False)

    def __embed_episode_info(self, program, ep_info):
        if ep_info['episode_number'] and ep_info['season_number']:
            program.episode_num = '{}.{}'.format(ep_info['season_number']-1, ep_info['episode_number']-1)
        return program

    def __embed_stubbed_episode_info(self, program):
        if program.episode_num is None:
            program.episode_num = '{}.{}{}{}'.format(program.start.year-1, program.start.month, program.start.day, program.start.minute-1)
        return program

    def get_series_id(self, program):
        # Prefer searching by the imdb_id - that will ultimately give the best results
        # this should be the only function that ever adds to the series_df
        if program.imdb_id:
            # Check first in pandas to see if we already queried that
            result = self.series_df[self.series_df['imdb_id'] == program.imdb_id]

            if len(result['tmdb_id']) > 0:
                return result['tmdb_id'].values[0]
            else:
                try:
                    result = tmdb.Find(program.imdb_id).info(external_source="imdb_id")
                except:
                    time.sleep(100)
                    result = tmdb.Find(program.imdb_id).info(external_source="imdb_id")
                # First look for the TV show
                if result['tv_results']:
                    return result['tv_results'][0]['id']
                # We are searching for a TV-Show and just found out it is a movie. Return None!
                elif result['movie_results']:                    
                    return None
        
        # Now search in the dataframe by the series_name and channel_id
        result = self.series_df[(self.series_df['series_name'] == program.title) & \
                                (self.series_df['channel_id'] == program.channel)]
        if len(result['tmdb_id']) > 0:
            return result['tmdb_id'].values[0]
        
        # Now just search by the series_name
        result = self.series_df[self.series_df['series_name'] == program.title]
        if len(result['tmdb_id']) > 0:
            # Make sure and put this series in there with the channel id
            new_row = dict(series_name=program.title, channel_id=program.channel, \
                           imdb_id=program.imdb_id, tmdb_id=result['tmdb_id'])
            to_app = pd.DataFrame([new_row], columns=['series_name', 'channel_id', 'imdb_id', 'tmdb_id'])
            self.series_df = self.series_df.append(to_app, ignore_index=True, sort=False)
            return result['tmdb_id'].values[0]

        # In this case we haven't seen it before, so let's search tmdb - doing a series search
        try:
            result = tmdb.Search().tv(query=program.title, include_adult=False)
        except:
            time.sleep(100)
            result = tmdb.Search().tv(query=program.title, include_adult=False)

        if result['results']:
            new_row = dict(series_name=program.title, channel_id=program.channel, \
                           imdb_id=program.imdb_id, tmdb_id=result['results'][0]['id'])
            to_app = pd.DataFrame([new_row], columns=['series_name', 'channel_id', 'imdb_id', 'tmdb_id'])
            self.series_df = self.series_df.append(to_app, ignore_index=True, sort=False)
            return result['results'][0]['id']

        # We didn't find a single thing! return None - this will have to be handled appropriately :)
        return None

    def get_movie_id(self, program):
        if program.imdb_id:
            try:
                result = tmdb.Find(program.imdb_id).info(external_source="imdb_id")
            except:
                time.sleep(100)
                result = tmdb.Find(program.imdb_id).info(external_source="imdb_id")

            if result['movie_results']:
                return result['movie_results'][0]['id']

        # We need to search for this one
        try:
            result = tmdb.Search().movie(query=program.title, include_adult=False)
        except:
            time.sleep(100)
            result = tmdb.Search().movie(query=program.title, include_adult=False)
        
        if result['results']:
            return result['results'][0]['id']

    def update_series_program(self, program, tmdb_id=None):
        if tmdb_id is None:
            # We need to find it ourselves
            tmdb_id = self.get_series_id(program)
        
        if tmdb_id is None:
            return (program, False)

        # First update the title and categories to make sure everything is clean
        series_info = self.__get_series_info(tmdb_id)
        if series_info['name']:
            program.title = series_info['name']
        if series_info['genres']:
            program.categories = []
            for cat in series_info['genres']:
                program.categories.append(cat['name'])

        # Then try and find the episode. First get the episode information.
        episode_info = self.__get_episode_info(tmdb_id)
        program, success = self.__find_episode(program, episode_info)

        # If we didn't have any success then maybe we should update things!
        print('success = {}'.format(success))
        print('tmdb_id = {}'.format(tmdb_id))
        print('pulled_episodes = {}'.format(self.pulled_episodes))
        if not success and tmdb_id not in self.pulled_episodes:
            episode_info = self.__get_episode_info(tmdb_id, force_update=True)
            program, success = self.__find_episode(program, episode_info)

        # Make sure there is some sort of something in there for the episode info
        program = self.__embed_stubbed_episode_info(program)

        # We tried our best now just return what we have :)
        return (program, success)

    def update_movie_program(self, program, tmdb_id=None):
        if tmdb_id is None:
            # We need to find it ourselves
            tmdb_id = self.get_movie_id(program)

        if tmdb_id is None:
            return (program, False)

        # What we really want to ensure is that there is no episode number in the program and ideally we would
        # want to add the year to the title in the format {title}_({year})
        movie_info = self.__get_movie_info(tmdb_id)

        if movie_info:
            if movie_info['overview']:
                program.description = movie_info['overview']
            if movie_info['title'] and movie_info['release_date']:
                program.title = '{}_({})'.format(movie_info['title'], movie_info['release_date'].split('-')[0])
            elif movie_info['title']:
                program.title = movie_info['title']
            if movie_info['genres']:
                program.categories = []
                for cat in movie_info['genres']:
                    program.categories.append(cat['name'])
        
        return program

    def write_series_csv(self):
        filepath = os.path.join(self.cachedir, 'show_dataframe.csv')
        self.series_df.to_csv(filepath, index=False)
