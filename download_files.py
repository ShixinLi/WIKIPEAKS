import os
import sys
import urllib
import gzip 
import shutil

FILE_URL = 'https://dumps.wikimedia.org/other/pageviews'
FILE_DOWNLOAD_PATH = 'data/'


def download_file(year, month, day):
    # TODO Checks for String date

    # Creates a directory for that date
    string_date = '%s-%s-%s' % (year, month, day)
    if not os.path.exists(FILE_DOWNLOAD_PATH + string_date):
        os.makedirs(FILE_DOWNLOAD_PATH + string_date)

    for hour in range(24):
        file_name = 'pageviews-%s%s%s-%02d0000.gz' % (year, month, day, hour)
        file_name_output = '%s%s/pageviews-%s%s%s-%02d0000.txt' % (FILE_DOWNLOAD_PATH,string_date,year, month, day, hour)
        print file_name
        complete_url = '%s/%s/%s-%s/%s' % (FILE_URL, year, year, month, file_name)
        print complete_url
        pageviews_file = urllib.URLopener()
        pageviews_file.retrieve(complete_url, file_name)

        with gzip.open(file_name,'rb') as f_in, open(file_name_output,'wb') as f_out:
            shutil.copyfileobj(f_in,f_out)

        print "Downloaded File:", file_name
        os.remove(file_name)
if __name__ == "__main__":
    download_file(sys.argv[1], sys.argv[2], sys.argv[3])
