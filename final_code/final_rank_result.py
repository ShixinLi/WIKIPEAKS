import boto3
import os
import numpy as np
import pandas as pd
import urllib
import re
import tinys3
s3 = boto3.resource("s3")
bucket = s3.Bucket('wikipedia-english-peak')

date_list=list()
times = pd.date_range('2016-06-11',periods=100)

for i in range(len(times)):
    date_list.append(str(times[i])[:10]+".txt")

for exact_date in range(len(date_list)):
    bucket1 = s3.Bucket('wikipedia-daily-peak')
    for obj in  bucket1.objects.filter(Prefix=date_list[exact_date]):
        reference_data =  obj.get()['Body'].read()
    reference_data_line_list = reference_data.split('\n')
    reference_dict = dict()
    for idx,line_list in enumerate(reference_data_line_list):
        if len(line_list.split('\t'))!=2:
            continue
        else:
            name,name_value = line_list.split('\t')
            if len(name_value.split(','))!=15:
                continue
            else:
                name_value_list = name_value[1:-1].split(',')
                try:
                    name_value_final = map(int,name_value_list)
                except ValueError:
                    continue

                reference_dict[name] = name_value_final


    for obj in  bucket.objects.filter(Prefix=date_list[exact_date]):
            current_date_data =  obj.get()['Body'].read()

    current_date_data_full_list = list()
    stat_list = list()
    for line_data in  current_date_data.split('\n'):
        if len(line_data.split('\t'))!=2:
            continue
        name,data = line_data.split('\t')
        if len(data.split(','))!=15:
            continue
        fifteen_days_data = data[1:-1].split(',')
        try:
            fifteen_days_data_list = map(int,fifteen_days_data)
        except ValueError:
            continue

        if fifteen_days_data_list[7]<1000:
            continue
        else:
            lucky_num = np.sum(fifteen_days_data_list[:7])+np.sum(fifteen_days_data_list[8:])
            if lucky_num>1000:
                stat_list.append(fifteen_days_data_list[7] )
                current_date_data_full_list.append([name,fifteen_days_data_list])

    stat_array = np.array(stat_list)
    average_page_views=np.mean(stat_array[stat_array>1000])

    # average page views
    for idx,data in enumerate(current_date_data_full_list):
        average =  (np.sum(current_date_data_full_list[idx][1])-current_date_data_full_list[idx][1][7])/float(14)
        current_date_data_full_list[idx].append(np.mean(average) )

    #calculate score
    lambda1 = 0.6
    lambda2 = 0.4
    for idx,data in enumerate(current_date_data_full_list):
        A1 = current_date_data_full_list[idx][2]/float(average_page_views)
        A2 =( current_date_data_full_list[idx][1][7]-current_date_data_full_list[idx][2])/float(current_date_data_full_list[idx][2] )
        score = lambda1 * A1 + lambda2 * A2
        current_date_data_full_list[idx].append(score)
    current_date_data_full_list.sort(key=lambda x: x[3],reverse=True)
    top_100_raw_data =  current_date_data_full_list[:100]
    top_100_final_data = list()
    for idx,data in enumerate( top_100_raw_data):
        article_name=data[0][4:]
        crawl_url = "https://en.wikipedia.org/wiki/{article_name}".format(article_name=article_name)
        handle = urllib.urlopen(crawl_url)
        html_gunk =  handle.read()
        france_name_groups = re.search("href=\"https://fr.wikipedia.org/wiki/(.*)\" title=\".*\" lang=\"fr\" hreflang=\"fr\"", html_gunk)
        if france_name_groups is None:
            france_name = None
        else:
            france_name = france_name_groups.groups()[0]

        china_name_groups = re.search("href=\"https://zh.wikipedia.org/wiki/(.*)\" title=\"(.*) \xe2.*\" lang=\"zh\" hreflang=\"zh\"", html_gunk)
        if china_name_groups is None:
            china_name = None
        else:
            china_name = china_name_groups.groups()[1]

        spanish_name_groups = re.search("href=\"https://es.wikipedia.org/wiki/(.*)\" title=\".*\" lang=\"es\" hreflang=\"es\"", html_gunk)
        if spanish_name_groups is None:
            spanish_name = None
        else:
            spanish_name = spanish_name_groups.groups()[0]

        print "france name is "+str(france_name)
        print "chinese name is "+str(china_name)
        print "spanish_name is "+str(spanish_name)
        print "===="
        france_list=list()
        china_list = list()
        spanish_list = list()
        if france_name is None:
            france_list=[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        else:
            france_real_name = "fr::"+france_name
            if france_real_name in reference_dict:
                france_list = reference_dict[france_real_name]
            else:
                france_list=[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        if china_name is None:
            china_list = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        else:
            china_real_name = "zh::"+china_name
            if china_real_name in reference_dict:
                china_list = reference_dict[china_real_name]
            else:
                china_list = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        if spanish_name is None:
            spanish_list = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        else:
            spanish_real_name = "es::"+spanish_name
            if spanish_real_name in reference_dict:
                spanish_list = reference_dict[spanish_real_name]
            else:
                spanish_list = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        top_100_final_data.append([data[0],data[1],france_list,china_list,spanish_list])


    writefile = open(date_list[exact_date],'w')
    for element in top_100_final_data:
        writefile.write("{name}\t{lst1}\t{lst2}\t{lst3}\t{lst4}\n".format(name=element[0],lst1=element[1],lst2=element[2],lst3=element[3],lst4=element[4] ) )
    writefile.close()
    conn = tinys3.Connection('AKIAJ75RITXR5M2XUFBQ','I8tTiWPo5xXBx1Ohf4oUBGAKSHRCgARVXk6g1k1Q',tls=True)
    f = open(date_list[exact_date],'rb')
    conn.upload(date_list[exact_date],f,'top-100-peaks')
    os.remove(date_list[exact_date])

