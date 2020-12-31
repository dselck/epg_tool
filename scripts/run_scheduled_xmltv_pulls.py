#!/usr/bin/env python
import os
import sys
import time
import schedule
import epg_tool
import requests
import tmdbsimple as tmdb

if __name__ == '__main__':
    # If the environment variables aren't set let's cancel
    if not os.getenv('DATA_VOLUME') or \
            not os.getenv('MOVIEDB_KEY') or \
            not os.getenv('XMLTV_URL') or \
            not os.getenv('TVHEADEND_URL'):
        print('Not all environment variables set properly')
        sys.exit(2)

    # Collect the Variables
    data_vol = os.getenv('DATA_VOLUME')
    apikey = os.getenv('MOVIEDB_KEY')
    cachedir = os.path.join(data_vol, 'tv_cache')
    internet_url = os.getenv('XMLTV_URL')
    xmltv_save = os.path.join(data_vol, 'xmltv.xml')
    tvheadend_url = os.getenv('TVHEADEND_URL')

    def job():
        class SessionTimeoutFix(requests.Session):
            def request(self, *args, **kwargs):
                _ = kwargs.pop('timeout')
                return super().request(*args, **kwargs, timeout=10)
        
        tmdb.API_KEY = apikey
        tmdb.REQUESTS_SESSION = SessionTimeoutFix
        enricher = epg_tool.enricher(cachedir)

        # Pull the files that we are going to need
        print('Pulling Files')
        internet_programs, internet_channels, internet_df = epg_tool.parse_xml(internet_url)
        tvhd_programs, tvhd_channels, tvhd_df = epg_tool.parse_xml(tvheadend_url)
        print('Finished Pulling Files')

        # Fix the channels for tvhd to match internet
        print('Fixing Channels')
        tvhd_channels, tvhd_programs = epg_tool.transfer_channel_ids(tvhd_channels, tvhd_programs, internet_channels)
        print('Finished Fixing Channels')

        # Pull the data from the internet programs (bad times) to the local times
        print('Adding internet xmltv data to EIT data')
        tvhd_programs = epg_tool.match_headend_to_internet(tvhd_programs,
                                                           tvhd_channels,
                                                           internet_programs,
                                                           internet_channels,
                                                           internet_df)
        print('Finished adding internet xmltv data to EIT data')

        # Now we can enrich all of the data!
        print('Enriching data')
        idx = 0
        successes = 0
        while idx < len(tvhd_programs):
            if idx % 100 == 0:
                print('Finished enriching {} of {} programs'.format(idx, len(tvhd_programs)))
            try:
                if tvhd_df.iloc[idx].Is_Movie:
                    tvhd_programs[idx], success = enricher.update_movie_program(tvhd_programs[idx])
                else:
                    tvhd_programs[idx], success = enricher.update_series_program(tvhd_programs[idx])
                if success:
                    successes += 1
                idx += 1
            except requests.exceptions.ConnectionError:
                # We ran into a timeout - something with the web not working currently...
                time.sleep(30)
        enricher.write_series_csv()
        print('Finished enriching data - had {} successes in {} items'.format(successes, len(tvhd_programs)))


        # We can now save all this to disk
        epg_tool.write_xml(tvhd_programs, tvhd_channels, xmltv_save)
        print('File saved to disk')

    schedule.every().day.at("08:00").do(job)
    schedule.every().day.at("20:00").do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)
