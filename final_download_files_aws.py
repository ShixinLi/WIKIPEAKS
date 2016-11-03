import os
import boto
import urllib
import gzip
import shutil
from datetime import datetime, timedelta
from dateutil import tz

FILE_URL = 'https://dumps.wikimedia.org/other/pageviews'


def download_file():
    # Connects to S3
    s3 = boto.connect_s3('', '')

    # Gets the bucket
    bucket = s3.get_bucket('wikipedia-pageviews')

    # Should get the current day
    utc_date = datetime.now()

    # Setting the zones
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('America/New_York')

    utc_date = utc_date.replace(tzinfo=from_zone)
    local_date = utc_date.astimezone(to_zone)

    # Substracts one day
    yesterday = local_date - timedelta(days=1)

    yesterday_string = yesterday.strftime('%Y-%m-%d')

    year = yesterday.strftime('%Y')
    year_month = yesterday.strftime('%Y-%m')

    for hour_t in range(24):
        file_name = 'pageviews-%s-%02d0000.gz' % (yesterday.strftime('%Y%m%d'), hour_t)
        file_name_txt = 'pageviews-%s-%02d0000.txt' % (yesterday.strftime('%Y%m%d'), hour_t)

        complete_url = '%s/%s/%s/%s' % (FILE_URL, year, year_month, file_name)

        pageviews_file = urllib.URLopener()
        pageviews_file.retrieve(complete_url, file_name)

        # The File is Unzipped
        with gzip.open(file_name, 'rb') as f_in, open(file_name_txt, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

        print "Downloaded File:", file_name

        # The zip file is removed
        os.remove(file_name)
        print "File removed:", file_name
        # Loads the file to the S3
        file_key = '%s/%s' % (yesterday_string, file_name_txt)
        key = bucket.new_key(file_key)
        key.set_contents_from_filename(file_name_txt)
        print "File transfered to S3:", file_name_txt

        # Removes the txt file too
        os.remove(file_name_txt)
        print "Txt File removed:", file_name_txt

if __name__ == "__main__":
    download_file()

