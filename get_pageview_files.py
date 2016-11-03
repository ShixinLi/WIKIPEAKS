import os
import glob
import boto
import urllib
import gzip
import shutil
import logging
import pickle

FILE_URL = 'https://dumps.wikimedia.org/other/pageviews'

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# create a file handler
handler = logging.FileHandler('failed_downloads.log')
handler.setLevel(logging.ERROR)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)


def get_pageview_files():
    # Connects to S3
    s3 = boto.connect_s3('', '')

    # Gets the bucket
    bucket = s3.get_bucket('wikipedia-pageviews')

    # Read the pickle object with the dates saved
    download_dates = pickle.load(open("download_dates.p", "rb"))

    complete_url = ''
    current_date_string = ''

    while download_dates:
        try:
            current_date = download_dates.pop(0)

            current_date_string = current_date.strftime('%Y-%m-%d')
            year = current_date.strftime('%Y')
            year_month = current_date.strftime('%Y-%m')

            for hour_t in range(24):
                file_name = 'pageviews-%s-%02d0000.gz' % (current_date.strftime('%Y%m%d'), hour_t)
                file_name_txt = 'pageviews-%s-%02d0000.txt' % (current_date.strftime('%Y%m%d'), hour_t)

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
                file_key = '%s/%s' % (current_date_string, file_name_txt)
                key = bucket.new_key(file_key)
                key.set_contents_from_filename(file_name_txt)
                print "File transfered to S3:", file_name_txt

                # Removes the txt file too
                os.remove(file_name_txt)
                print "Txt File removed:", file_name_txt

            # Removes and saves again the pickle object
            os.remove('download_dates.p')
            pickle.dump(download_dates, open("download_dates.p", "wb"))

        except:
            # Removes all the files gz or txt
            txt_files = glob.glob("*.txt")
            for f in txt_files:
                os.remove(f)

            gz_files = glob.glob("*.gz")
            for g in gz_files:
                os.remove(g)
            logger.ERROR('Bucket Folder:%s - File Url:%s' % (current_date_string, complete_url))


if __name__ == "__main__":
    get_pageview_files()

