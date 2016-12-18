from __future__ import print_function # Python 2/3 compatibility
import os
import boto
from datetime import datetime, timedelta as td

AWS_ACCESS_KEY = 'AKIAIQFMJ6TJ6NEKHIHQ'
AWS_SECRET_ACCESS = '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'

# Connects to S3
s3 = boto.connect_s3(AWS_ACCESS_KEY, AWS_SECRET_ACCESS)

# Gets the bucket
bucket = s3.get_bucket('wikipedia-dly-singlefiles')

# Creates the date range
d1 = datetime(2015, 12, 23)
d2 = datetime(2015, 12, 24)

delta = d2 - d1

for i in range(delta.days + 1):
    current_date = d1 + td(days=i)

    current_date_str = current_date.strftime('%Y-%m-%d')
    current_date_file = "%s.txt" % current_date_str
    # Copy the file from S3 to EC2
    s3_key = bucket.get_key(current_date_file)
    s3_key.get_contents_to_filename(current_date_file)

    with open(current_date_file) as daily_article_f:
        csv_outputfile = "%s.csv" % current_date_str
        with open(csv_outputfile, 'a') as output_csv:
            for line in daily_article_f:
                try:
                    values = line.strip().split('\t')

                    if len(values) > 1:
                        lang_article = values[0].split('::')
                        lang = lang_article[0].strip()
                        article = lang_article[1].strip()
                        views = values[1].strip()

                        if lang in ['zh', 'en', 'fr', 'es'] and isinstance(views, int):
                            line_csv = ',%s,%s,%s,%s\n' % (current_date_str, lang, article, views)
                            output_csv.write(line_csv)
                except IndexError:
                    pass

    # Copy the files to the new bucket
    #dest_bucket = s3.get_bucket('wikipedia-articles-json')
    #dest_key = dest_bucket.new_key(json_outputfile)
    #dest_key.set_contents_from_filename(json_outputfile)
    #print "File Transfered:", json_outputfile
    # Delete Files (txt and json)
    try:
        os.remove(current_date_file)
        #os.remove(json_outputfile)
    except OSError:
        pass


