import os
import json
import datetime
import pandas as pd
import tmdbsimple as tmdb

from fuzzywuzzy import process, fuzz

class enricher_tmdb:
    def __init__(self, cachedir):
        super self.__init__()

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

        if os.path.isfile(filepath) and not force_update or \
                os.path.isfile(filepath) and tmdb_id in self.pulled_episodes:
            with open(filepath) as json_file:
                return json.load(json_file)
        else:
            episodes = []
            series_info = self.__get_series_info(tmdb_id)

            # If we didn't find anything, return nothing
            if not series_info or not series_info['seasons']
                return None

            for season in series_info['seasons']:
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
        
        # Now do a search by the sub_title (episode name)
        if program.sub_title is not None:
            # Some episodes are really multiple episodes separated by a /
            if '/' in program.sub_title:
                sub_t = program.sub_title.split('/')[0]
            else:
                sub_t = program.sub_title
            
            best_match = process.extractOne(sub_t, sub_titles)
            # Check to make sure it is a good match
            if best_match[0] is not None and \
                    fuzz.token_sort_ratio(best_match[0].lower(), sub_t.lower()) > 85:
                # We have found our winner!
                idx = sub_titles.index(best_match[0])
                program = self.__embed_episode_info(program, episode_info[idx])
                return (program, True)

            # Sometimes EIT data happens to use the "description" of the episode as the subtitle...
            best_match = process.extractOne(program.sub_title, descriptions)
            if best_match[0] is not None and \
                    fuzz.token_sort_ratio(best_match[0].lower(), program.sub_title.lower()) > 85:
                # We have found our winner!
                idx = descriptions.index(best_match[0])
                program = self.__embed_episode_info(program, episode_info[idx])
                return (program, True)

        # If we made it to this point try looking by the description
        if program.description is not None:
            best_match = process.extractOne(program.description, descriptions)
            # Check to make sure the highest passes a decent threshold
            if best_match[0] is not None and fuzz.token_sort_ratio(best_match[0].lower(), program.description.lower()) > 85:
                # Send it back!
                return self.__embed_episode_info(program, episode_info[idx])

        return (program, False)

    def __embed_episode_info(self, program, tmdb_episode):
        pass

    def get_series_id(self, program):
        # Prefer searching by the imdb_id - that will ultimately give the best results
        # this should be the only function that ever adds to the series_df
        if program.imdb_id:
            # Check first in pandas to see if we already queried that
            result = self.series_df[self.series_df['imdb_id'] == program.imdb_id]

            if result:
                return result['tmdb_id']
            else:
                result = tmdb.Find(program.imdb_id).info(external_source="imdb_id")
                # First look for the TV show
                if result['tv_results']:
                    return result['tv_results'][0]['id']
                # We are searching for a TV-Show and just found out it is a movie. Return None!
                elif result['movie_results']:                    
                    return None
        
        # Now search in the dataframe by the series_name and channel_id
        result = self.series_df[self.series_df['series_name'] == program.title and \
                                self.series_df['channel_id'] == program.channel]
        if result:
            return result['tmdb_id']
        
        # Now just search by the series_name
        result = self.series_df[self.series_df['series_name'] == program.title]
        if result:
            # Make sure and put this series in there with the channel id
            new_row = dict(series_name=program.title, channel_id=program.channel, \
                           imdb_id=program.imdb_id, tmdb_id=result['tmdb_id'])
            to_app = pd.DataFrame(new_row, columns=['series_name', 'channel_id', 'imdb_id', 'tmdb_id'])
            self.series_df = self.series_df.append(to_app, ignore_index=True, sort=False)
            return result['tmdb_id']

        # In this case we haven't seen it before, so let's search tmdb - doing a series search
        result = tmdb.Search().tv(query=program.title, include_adult=False)
        if result['results']:
            new_row = dict(series_name=program.title, channel_id=program.channel, \
                           imdb_id=program.imdb_id, tmdb_id=result['results'][0]['id'])
            to_app = pd.DataFrame(new_row, columns=['series_name', 'channel_id', 'imdb_id', 'tmdb_id'])
            self.series_df = self.series_df.append(to_app, ignore_index=True, sort=False)
            return result['results'][0]['id']

        # We didn't find a single thing! return None - this will have to be handled appropriately :)
        return None

    def get_movie_id(self, program):
        if program.imdb_id:
            result = tmdb.Find(program.imdb_id).info(external_source="imdb_id")
            if result['movie_results']:
                return result['movie_results'][0]['id']

        # We need to search for this one
        result = tmdb.Search().movie(query=program.title, include_adult=False)
        if result['results']:
            return result['results'][0]['id']

    def update_series_program(self, program, tmdb_id):
        # First update the title and categories to make sure everything is clean
        series_info = self.__get_series_info(tmdb_id)
        program.title = series_info['name']
        program.categories = []
        for cat in series_info['genres']:
            program.categories.append(cat['name'])

        # Then try and find the episode. First get the episode information.
        episode_info = self.__get_episode_info(tmdb_id)
        program, success = self.__find_episode(program, episode_info)

        # If we didn't have any success then maybe we should update things!
        if not success and tmdb_id not in self.pulled_episodes:
            episode_info = self.__get_episode_info(tmdb_id, force_update=True)
            program, success = self.__find_episode(program, episode_info)

        # We tried our best now just return what we have :)
        return (program, success)

    def update_movie_program(self, program, tmdb_id):
        return ''

