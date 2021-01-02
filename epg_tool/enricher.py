import os
import json
import time
import datetime
import pandas as pd
import tmdbsimple as tmdb
import epg_tool.tvmaze as tvm

from fuzzywuzzy import process, fuzz

class GenericEnricher:
    def __init__(self, cachedir):
        self.cachedir = cachedir
        self.pulled_series = []
        self.pulled_episodes = []
        self.update_written = False

        # Get the show_dataframe all ready to go
        if os.path.isfile(os.path.join(self.cachedir, 'show_dataframe.csv')):
            self.series_df = pd.read_csv(os.path.join(self.cachedir, 'show_dataframe.csv'))
        else:
            self.series_df = pd.DataFrame(columns=['series_name', 'channel_id', 'imdb_id', 'enricher_id'])
        
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

    def embed_stubbed_episode_info(self, program):
        if not program.episode_num:
            program.episode_num = '{}.{}{}{}'.format(program.start.year-1, program.start.month, program.start.day, program.start.minute-1)
        return program

    def get_info_generic(self, filepath):
        if os.path.isfile(filepath):
            with open(filepath) as json_file:
                return json.load(json_file)
        else:
            return None

    def save_info_generic(self, filepath, data):
        # Save it
        with open(filepath, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        
        # Point out we have done some updates
        if not self.update_written:
            self.__write_update()

    def get_series_info(self, id):
        # At this level we are just going to try and draw from cache :)
        filepath = os.path.join(self.cachedir, '{}.json'.format(id))
        return self.get_info_generic(filepath)

    def save_series_info(self, data, id):
        filepath = os.path.join(self.cachedir, '{}.json'.format(id))
        self.save_info_generic(filepath, data)

    def get_episode_info(self, id):
        # At this level we are just going to try and draw from cache :)
        filepath = os.path.join(self.cachedir, '{}_episode_info.json'.format(id))
        return self.get_info_generic(filepath)

    def save_episode_info(self, data, id):
        filepath = os.path.join(self.cachedir, '{}_episode_info.json'.format(id))
        self.save_info_generic(filepath, data)

    def get_series_id(self, program):
        # Prefer searching by the imdb_id - that will ultimately give the best results
        # this should be the only function that ever adds to the series_df
        if program.imdb_id:
            # Check first in pandas to see if we already queried that
            result = self.series_df[self.series_df['imdb_id'] == program.imdb_id]

            if len(result['enricher_id']) > 0:
                return result['enricher_id'].values[0]

        # Now search in the dataframe by the series_name and channel_id
        result = self.series_df[(self.series_df['series_name'] == program.title) & \
                                (self.series_df['channel_id'] == program.channel)]
        if len(result['enricher_id']) > 0:
            return result['enricher_id'].values[0]
        
        # Now just search by the series_name
        result = self.series_df[self.series_df['series_name'] == program.title]
        if len(result['enricher_id']) > 0:
            # Make sure and put this series in there with the channel id
            new_row = dict(series_name=program.title, channel_id=program.channel, \
                           imdb_id=program.imdb_id, enricher_id=result['enricher_id'].values[0])
            to_app = pd.DataFrame([new_row], columns=['series_name', 'channel_id', 'imdb_id', 'enricher_id'])
            self.series_df = self.series_df.append(to_app, ignore_index=True, sort=False)
            return result['enricher_id'].values[0]
        
        # We didn't find anything
        return None

    def find_episode(self, program, sub_titles, descriptions):
        # This function will just return the index!

        # Now do a search by the sub_title (episode name)
        if program.sub_title is not None:
            # Some episodes are really multiple episodes separated by a /
            if '/' in program.sub_title:
                sub_t = program.sub_title.split('/')[0]
            else:
                sub_t = program.sub_title
            
            best_match = process.extract(sub_t, sub_titles, scorer=fuzz.token_sort_ratio)
            # Check to make sure it is a good match
            if best_match[0][1] > 85:
                # We have found our winner!
                return sub_titles.index(best_match[0][0])

            # Sometimes EIT data happens to use the "description" of the episode as the subtitle...
            best_match = process.extract(program.sub_title, descriptions, scorer=fuzz.token_sort_ratio)
            if best_match[0][1] > 85:
                # We have found our winner!
                return descriptions.index(best_match[0][0])

        # If we made it to this point try looking by the description
        if program.description is not None:
            best_match = process.extract(program.description, descriptions, scorer=fuzz.token_sort_ratio)
            # Check to make sure the highest passes a decent threshold
            if best_match[0][1] > 85:
                # Send it back!
                return descriptions.index(best_match[0][0])

        return None

    def write_series_csv(self):
        filepath = os.path.join(self.cachedir, 'show_dataframe.csv')
        self.series_df.to_csv(filepath, index=False)

class TMDBEnricher(GenericEnricher):
    def __init__(self, cachedir):
        super().__init__(cachedir)

    def get_series_info(self, tmdb_id, force_update=False):
        # In this case we are just going to get new series info
        if force_update and tmdb_id not in self.pulled_series:
            result = tmdb.TV(tmdb_id).info()
            
            if not result:
                return None

            self.pulled_series.append(tmdb_id)
            self.save_series_info(result, tmdb_id)
            return result
        else:
            result = super().get_series_info(tmdb_id)

            if result is None:
                # it wasn't cached. Get it fresh!
                return self.get_series_info(tmdb_id, force_update=True)
            else:
                return result

    def get_episode_info(self, tmdb_id, force_update=False):
        # In this case we are just going to get new series info
        if force_update and tmdb_id not in self.pulled_episodes:
            # First we need the series info to figure out how many seasons...
            episodes = []
            series_info = self.get_series_info(tmdb_id, force_update=True)

            # If we didn't find anything, return nothing
            if not series_info or not series_info['seasons']:
                return None

            for season in series_info['seasons']:
                result = tmdb.TV_Seasons(tmdb_id, season['season_number']).info()
                episodes += (result['episodes'])

            if not episodes:
                return None
            
            self.pulled_episodes.append(tmdb_id)
            self.save_episode_info(episodes, tmdb_id)
            return episodes
        else:
            result = super().get_episode_info(tmdb_id)

            if result is None:
                # it wasn't cached. Get it fresh!
                return self.get_episode_info(tmdb_id, force_update=True)
            else:
                return result

    def __get_movie_info(self, tmdb_id):
        result = tmdb.Movies(tmdb_id).info()
        return result

    def get_series_id(self, program):
        # First see if it is in the dataframe
        result = super().get_series_id(program)

        if result:
            return result
        
        # Prefer searching by the imdb_id - that will ultimately give the best results
        if program.imdb_id:
            result = tmdb.Find(program.imdb_id).info(external_source="imdb_id")
            # First look for the TV show
            if result['tv_results']:
                return result['tv_results'][0]['id']
            # We are searching for a TV-Show and just found out it is a movie. Return None!
            elif result['movie_results']:                    
                return None

        # In this case we haven't seen it before, so let's search tmdb - doing a series search
        result = tmdb.Search().tv(query=program.title, include_adult=False)

        if result['results']:
            new_row = dict(series_name=program.title, channel_id=program.channel, \
                           imdb_id=program.imdb_id, enricher_id=result['results'][0]['id'])
            to_app = pd.DataFrame([new_row], columns=['series_name', 'channel_id', 'imdb_id', 'enricher_id'])
            self.series_df = self.series_df.append(to_app, ignore_index=True, sort=False)
            return result['results'][0]['id']

        # We didn't find a single thing! return None
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

        return None

    def __enrich_episode(self, program, ep_info):
        if program.sub_title and '/' in program.sub_title:
            replace_subtitle = False
        else:
            replace_subtitle = True

        if ep_info['episode_number'] and ep_info['season_number']:
            program.episode_num = '{}.{}'.format(ep_info['season_number']-1, ep_info['episode_number']-1)
        elif ep_info['season_number']:
            program.episode_num = '{}.'.format(ep_info['season_number'])
        if replace_subtitle:
            program.sub_title = ep_info['name']
        if ep_info['air_date']:
            program.airdate = datetime.datetime.strptime(ep_info['air_date'], '%Y-%m-%d')
        
        return program

    def update_series_program(self, program, tmdb_id=None):
        if tmdb_id is None:
            # We need to find it ourselves
            tmdb_id = self.get_series_id(program)
        
        if tmdb_id is None:
            return (program, False)

        # First update the title and categories to make sure everything is clean
        series_info = self.get_series_info(tmdb_id)
        if series_info['name']:
            program.title = series_info['name']
        if series_info['genres']:
            program.categories = []
            for cat in series_info['genres']:
                program.categories.append(cat['name'])

        # Then try and find the episode. First get the episode information.
        episode_info = self.get_episode_info(tmdb_id)

        # First parse the episode_info to get the bits that we need
        sub_titles   = []
        descriptions = []
        for ep in episode_info:
            sub_titles.append(ep['name'])
            descriptions.append(ep['overview'])

        # Then see if we can find a good index
        idx = self.find_episode(program, sub_titles, descriptions)

        success = False
        if idx:
            program = self.__enrich_episode(program, episode_info[idx])
            success = True

        # If we didn't have any success then maybe we should update things!
        if not success and tmdb_id not in self.pulled_episodes:
            episode_info = self.get_episode_info(tmdb_id, force_update=True)
            return self.update_series_program(program, tmdb_id=tmdb_id)

        # Make sure there is some sort of something in there for the episode info
        program = self.embed_stubbed_episode_info(program)

        # We tried our best now just return what we have :)
        return (program, success)

    def update_movie_program(self, program, tmdb_id=None):
        # Make sure there is no episode_num. that is how plex figures it all out
        program.episode_num = None

        if tmdb_id is None:
            # We need to find it ourselves
            tmdb_id = self.get_movie_id(program)

        if tmdb_id is None:
            return (program, False)

        # What we really want to ensure is that there is no episode number in the program and make sure we add a date
        movie_info = self.__get_movie_info(tmdb_id)

        if movie_info:
            if movie_info['overview']:
                program.description = movie_info['overview']
            if movie_info['release_date']:
                program.date = '{}'.format(movie_info['release_date'].split('-')[0])
            if movie_info['title']:
                program.title = movie_info['title']
            if movie_info['genres']:
                program.categories = []
                for cat in movie_info['genres']:
                    program.categories.append(cat['name'])
        
        return (program, True)

class TvMazeEnricher(GenericEnricher):
    def __init__(self, cachedir):
        super().__init__(cachedir)
    
    def get_series_info(self, tvmaze_id, force_update=False):
        # In this case we are just going to get new series info
        if force_update and tvmaze_id not in self.pulled_series:
            result = tvm.get_show_info(tvmaze_id)
            
            if result is None:
                return None

            self.pulled_series.append(tvmaze_id)
            self.save_series_info(result, tvmaze_id)
            return result
        else:
            result = super().get_series_info(tvmaze_id)

            if result is None:
                # it wasn't cached. Get it fresh!
                return self.get_series_info(tvmaze_id, force_update=True)
            else:
                return result

    def get_episode_info(self, tvmaze_id, force_update=False):
        # In this case we are just going to get new series info
        if force_update and tvmaze_id not in self.pulled_episodes:
            episodes = tvm.get_episode_info(tvmaze_id)

            if not episodes:
                return None
            
            self.pulled_episodes.append(tvmaze_id)
            self.save_episode_info(episodes, tvmaze_id)
            return episodes
        else:
            result = super().get_episode_info(tvmaze_id)

            if result is None:
                # it wasn't cached. Get it fresh!
                return self.get_episode_info(tvmaze_id, force_update=True)
            else:
                return result

    def get_series_id(self, program):
        # First see if it is in the dataframe
        result = super().get_series_id(program)

        if result:
            return result
        
        # Prefer searching by the imdb_id - that will ultimately give the best results
        if program.imdb_id:
            result = tvm.get_show_by_imdbid(program.imdb_id)
            if result:
                return result['id']

        # In this case we haven't seen it before, so let's search tmdb - doing a series search
        result = tvm.search_for_show(program.title)
        if result:
            # This actually returns exactly what get_series_info would return as well!
            self.pulled_series.append(result['id'])
            self.save_series_info(result, result['id'])

            new_row = dict(series_name=program.title, channel_id=program.channel, \
                           imdb_id=program.imdb_id, enricher_id=result['id'])
            to_app = pd.DataFrame([new_row], columns=['series_name', 'channel_id', 'imdb_id', 'enricher_id'])
            self.series_df = self.series_df.append(to_app, ignore_index=True, sort=False)
            return result['id']

        # We didn't find a single thing! return None
        return None

    def __enrich_episode(self, program, ep_info):
        if program.sub_title and '/' in program.sub_title:
            replace_subtitle = False
        else:
            replace_subtitle = True

        if ep_info['number'] and ep_info['season']:
            program.episode_num = '{}.{}'.format(ep_info['season']-1, ep_info['number']-1)
        elif ep_info['season']:
            program.episode_num = '{}.'.format(ep_info['season'])
        if replace_subtitle:
            program.sub_title = ep_info['name']
        if 'air_date' in ep_info and ep_info['air_date']:
            program.airdate = datetime.datetime.strptime(ep_info['air_date'], '%Y-%m-%d')
        
        return program

    def update_series_program(self, program, tvmaze_id=None):
        if tvmaze_id is None:
            # We need to find it ourselves
            tvmaze_id = self.get_series_id(program)
        
        if tvmaze_id is None:
            return (program, False)

        # First update the title and categories to make sure everything is clean
        series_info = self.get_series_info(tvmaze_id)
        if series_info['name']:
            program.title = series_info['name']
        if series_info['genres']:
            program.categories = series_info['genres']

        # Then try and find the episode. First get the episode information.
        episode_info = self.get_episode_info(tvmaze_id)

        # First parse the episode_info to get the bits that we need
        sub_titles   = []
        descriptions = []
        for ep in episode_info:
            sub_titles.append(ep['name'])
            desc = ep['summary']
            if desc:
                desc.replace('<p>', '').replace('</p>', '')
            descriptions.append(desc)

        # Then see if we can find a good index
        idx = self.find_episode(program, sub_titles, descriptions)

        success = False
        if idx:
            program = self.__enrich_episode(program, episode_info[idx])
            success = True

        # If we didn't have any success then maybe we should update things!
        if not success and tvmaze_id not in self.pulled_episodes:
            episode_info = self.get_episode_info(tvmaze_id, force_update=True)
            return self.update_series_program(program, tvmaze_id=tvmaze_id)

        # Make sure there is some sort of something in there for the episode info
        program = self.embed_stubbed_episode_info(program)

        # We tried our best now just return what we have :)
        return (program, success)
