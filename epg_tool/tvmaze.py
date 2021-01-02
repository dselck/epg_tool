import time
import requests

URL = 'http://api.tvmaze.com'

def __get(url, params):
    try:
        response = requests.get(url, params, timeout=10)
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
        print('Got hit with a retryable exception. Sleeping for 30 seconds and going at it again: {}'.format(e))
        time.sleep(30)
        return __get(url, params)

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 429:
        time.sleep(10)
        return __get(url, params)
    else:
        return None

def search_for_show(query):
    url = URL + '/singlesearch/shows'
    params = {'q':query}
    return __get(url, params)

def get_show_by_imdbid(query):
    url = URL + '/lookup/shows'
    params = {'imdb':query}
    return __get(url, params)

def get_show_info(tvmaze_id):
    url = URL + '/shows/{}'.format(tvmaze_id)
    return __get(url, None)

def get_episode_info(tvmaze_id):
    url = URL + '/shows/{}/episodes'.format(tvmaze_id)
    params = {'specials':1}
    return __get(url, params)