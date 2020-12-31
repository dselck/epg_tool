import tmdbsimple as tmdb
import os
import epg_tool
from epg_tool.channel import channel
from epg_tool.program import program

class TestEnricher():
    def setup_class(self):
        tmdb.API_KEY = os.getenv('MOVIEDB_KEY')
        print('tmdb API KEY = {}'.format(tmdb.API_KEY))

        # Need to have some info in here :)
        assert(tmdb.API_KEY != '')

        self.cache = '/tmp/pyteststuff'
        self.enricher = epg_tool.enricher(self.cache)
        if not os.path.isdir(self.cache):
            os.mkdir(self.cache)

    def test_get_movie_id(self):
        tmdb.API_KEY = os.getenv('MOVIEDB_KEY')
        print('\nStarting test_get_movie_id\n')
        p0 = program(title='Better Off Dead')
        p1 = program(imdb_id='tt0088794')

        id0 = self.enricher.get_movie_id(p0)
        print('id0 = {}'.format(id0))

        id1 = self.enricher.get_movie_id(p1)
        print('id1 = {}'.format(id1))

        assert id0 == id1 == 13667

    def test_get_series_id(self):
        tmdb.API_KEY = os.getenv('MOVIEDB_KEY')
        print('\nStarting test_get_series_id\n')
        p0 = program(title='Ghosted')
        p1 = program(imdb_id='tt6053538')

        id1 = self.enricher.get_series_id(p0)
        print('id1 = {}'.format(id1))

        id0 = self.enricher.get_series_id(p1)
        print('id0 = {}'.format(id0))

        assert id0 == id1 == 71739

#    def test_get_series_info(self):
#        print('Starting test_get_series_info')
#        assert not os.path.isfile(self.cache + '/71739.json')
#        data = self.enricher.__get_series_info(71739)
#        assert os.path.isfile(self.cache + '/71739.json')

#        assert data['first_air_date'] == '2017-10-01'
#        assert data['name'] == 'Ghosted'

#    def test_get_episode_info(self):
#        print('Starting test_get_episode_info')
#        assert not os.path.isfile(self.cache + '/71739_episode_info.json')
#        data = self.enricher.__get_episode_info(71739)
#        assert os.path.isfile(self.cache + '/71739_episode_info.json')

#        assert data[-1]['id'] == 1516974
#        assert data[-1]['episode_number'] == 16

    def test_update_series_program_info(self):
        tmdb.API_KEY = os.getenv('MOVIEDB_KEY')
        print('Starting test_update_series_program_info')
        p0 = program(title='Ghosted', channel='fake', sub_title='Hello Boys')
        p1 = program(title='Ghosted', channel='fake2', sub_title='While trying to prove their worth to Captain LaFrey and the rest of The Bureau Underground, Leroy and Max finally have a lead on the whereabouts of Agent Checker. Leroy finds himself in the odd position of being the believer when Max loses faith that they have what it takes.')

        p0u, _ = self.enricher.update_series_program(p0)
        p0u_1, _ = self.enricher.update_series_program(p0, tmdb_id=71739)
        p1u, _ = self.enricher.update_series_program(p1)
        p1u_1, _ = self.enricher.update_series_program(p1, tmdb_id=71739)

#        assert len(self.enricher.series_df) == 2
        assert p0u.sub_title == p1u.sub_title == p0u_1.sub_title == p1u_1.sub_title

        assert os.path.isfile(self.cache + '/71739.json')
        assert os.path.isfile(self.cache + '/71739_episode_info.json')
        os.remove(self.cache + '/71739.json')
        os.remove(self.cache + '/71739_episode_info.json')

    def test_update_movie_program_info(self):
        tmdb.API_KEY = os.getenv('MOVIEDB_KEY')
        print('Starting test_update_movie_program_info')
        p = program(title='Better Off Dead')
        p_0 = self.enricher.update_movie_program(p)
        p_1 = self.enricher.update_movie_program(p, tmdb_id=13667)

        assert p_0.title == p_1.title == 'Better Off Dead..._(1985)'
        assert len(p_0.categories) > 0 and len(p_1.categories) > 0
        assert p_0.description is not None and p_1.description is not None
