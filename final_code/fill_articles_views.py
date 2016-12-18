from __future__ import print_function # Python 2/3 compatibility
import os
import boto
import boto3
import json
import decimal
from datetime import datetime, timedelta as td


AWS_ACCESS_KEY = 'AKIAIQFMJ6TJ6NEKHIHQ'
AWS_SECRET_ACCESS = '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'

# Connects to S3
s3 = boto.connect_s3(AWS_ACCESS_KEY, AWS_SECRET_ACCESS)

# Gets the bucket
bucket = s3.get_bucket('wikipedia-en-singlefiles')

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

article_views_table = dynamodb.Table('WK_Articles_Views')

# Get the dates
list_dates = []
d1 = datetime(2015, 12, 23)
d2 = datetime(2015, 12, 25)

delta = d2 - d1

for i in range(delta.days + 1):
    current_date = d1 + td(days=i)
    current_date_string = current_date.strftime('%Y-%m-%d')

    current_date_file = "%s.txt" % current_date_string
    # Copy the file from S3 to EC2
    s3_key = bucket.get_key(current_date_file)
    s3_key.get_contents_to_filename(current_date_file)

    with open(current_date_file) as daily_article_f:
        for line in daily_article_f:
            try:
                values = line.strip().split('::')

                if len(values) > 1:
                    lang_code = values[0].strip()
                    article_views = values[1].split('\t')
                    article = article_views[0].strip()
                    views = article_views[1].strip()

                    date_lang_article = "%s::%s::%s" % (current_date_string, lang_code, article)

                    article_views_table.put_item(
                        Item={
                            'Date_Lang_Article': date_lang_article,
                            'Views': views
                        }
                    )

                    #print("Article added. %s: %s - %s" % (current_date_string, lang_code, article))

            except IndexError:
                pass
        print("Finished:", current_date_file)

    #Deletes the file when its done
    os.remove(current_date_file)