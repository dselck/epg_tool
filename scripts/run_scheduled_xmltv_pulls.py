import os
import schedule
import epg_tool
import tmdbsimple as tmdb

# Collect the Variables
apikey = os.getenv('MOVIEDB_KEY')
cachedir = os.path.join(os.getenv('DATA_VOLUME'), 'tv_cache')
internet_url = os.getenv('XMLTV_URL')
xmltv_save = os.path.join(os.getenv('DATA_VOLUME'), 'xmltv.xml')
tvheadend_url = os.getenv('TVHEADEND_URL')

def job():
    # Do some setup
    tmdb.API_KEY = apikey
    enricher = epg_tool.enricher(cachedir)

    # Pull the files that we are going to need
    internet_programs, internet_channels, internet_df = epg_tool.parse_xml(internet_url)
    tvhd_programs, tvhd_channels, tvhd_df = epg_tool.parse_xml(tvheadend_url)
    
    # Fix the channels for tvhd to match internet
    tvhd_channels = epg_tool.transfer_channel_ids(tvhd_channels, tvhd_programs, internet_channels)

    # Pull the data from the internet programs (bad times) to the local times
    tvhd_programs, tvhd_channels = epg_tool.match_headend_to_internet(tvhd_programs,
                                                                      tvhd_channels,
                                                                      internet_programs,
                                                                      internet_channels,
                                                                      internet_df)

    # Now we can enrich all of the data!
    for i in range(len(tvhd_programs)):
        if tvhd_df.iloc[i].Is_Movie:
            tvhd_programs[i] = enricher.update_movie_program(tvhd_programs[i])
        else:
            tvhd_programs[i] = enricher.update_series_program(tvhd_programs[i])

    # We can now save all this to disk
    epg_tool.write_xml(tvhd_programs, tvhd_channels, xmltv_save)

schedule.every().day.at("02:00").do(job)
schedule.every().dat.at("20:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
