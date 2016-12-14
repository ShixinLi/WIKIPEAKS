import boto3
import pandas as pd
import os
import tinys3
s3 = boto3.resource("s3")
bucket = s3.Bucket('wikipedia-daily-pageviews')
date_list = list()

times = pd.date_range('2016-01-02',periods=305)

for i in range(len(times)):
    date_list.append(str(times[i])[:10]+".txt")

for exact_date in range(len(date_list)):

    date_before=list()
    for data in [7,6,5,4,3,2,1]:
        date_before.append(str( pd.to_datetime(date_list[exact_date][:10])-pd.DateOffset(days=data))[:10]+".txt" )

    date_after = list()
    for data in range(1,8):
        date_after.append(str( pd.to_datetime(date_list[exact_date][:10])+pd.DateOffset(days=data))[:10]+".txt" )

    for obj in  bucket.objects.filter(Prefix=date_list[exact_date]):
        current_date_data =  obj.get()['Body'].read()
    current_date_data_line_list = current_date_data.split('\n')
    final_list=[]
    for idx,line_data in enumerate(current_date_data_line_list):
        if idx==len(current_date_data_line_list)-1:
            break
        article,day_page_views = line_data.split('\t')
        if article[:2]=='en':
            continue
        else:
            final_list.append([article,int(day_page_views)])

    for obj in  bucket.objects.filter(Prefix=date_before[0]):
        before_date_data =  obj.get()['Body'].read()
    before_date_data_line_list = before_date_data.split('\n')
    before_dict = dict()
    for idx,line_data in enumerate(before_date_data_line_list):
        if idx==len(before_date_data_line_list)-1:
            break
        before_article,before_day_page_views = line_data.split('\t')
        if before_article[:2]=='en':
            continue
        else:
            before_dict[before_article]=int(before_day_page_views)
    peak_candidate_list=list()
    for idx,data in enumerate(final_list):
        if data[0] in before_dict:
            page_view_number = before_dict[data[0]]
        else:
            page_view_number = 0
        peak_candidate_list.append([data[0],[page_view_number]])


    for date in range(1,7):
        for obj in  bucket.objects.filter(Prefix=date_before[date]):
            before_date_data =  obj.get()['Body'].read()
        before_date_data_line_list = before_date_data.split('\n')
        before_dict = dict()
        for idx,line_data in enumerate(before_date_data_line_list):
            if idx==len(before_date_data_line_list)-1:
                break
            before_article,before_day_page_views = line_data.split('\t')
            if before_article[:2]=='en':
                continue
            else:
                before_dict[before_article]=int(before_day_page_views)
        for idx,data in enumerate(final_list):
            if data[0] in before_dict:
                page_view_number = before_dict[data[0]]
            else:
                page_view_number = 0
            peak_candidate_list[idx][1].append(page_view_number)

    for idx,data in enumerate(final_list):
        peak_candidate_list[idx][1].append(data[1])
    for date in range(7):
        for obj in  bucket.objects.filter(Prefix=date_after[date]):
            before_date_data =  obj.get()['Body'].read()
        before_date_data_line_list = before_date_data.split('\n')
        before_dict = dict()
        for idx,line_data in enumerate(before_date_data_line_list):
            if idx==len(before_date_data_line_list)-1:
                break
            before_article,before_day_page_views = line_data.split('\t')
            if before_article[:2]=='en':
                continue
            else:
                 before_dict[before_article]=int(before_day_page_views)


        for idx,data in enumerate(final_list):
            if data[0] in before_dict:
                page_view_number = before_dict[data[0]]
            else:
                page_view_number = 0
            peak_candidate_list[idx][1].append(page_view_number)

    writefile = open(date_list[exact_date][:-4]+"_all_language.txt",'w')

    for element in peak_candidate_list:
         writefile.write("{name}\t{lst}\n".format(name=element[0],lst=element[1]))

    conn = tinys3.Connection('AKIAJ75RITXR5M2XUFBQ','I8tTiWPo5xXBx1Ohf4oUBGAKSHRCgARVXk6g1k1Q',tls=True)
    f = open(date_list[exact_date][:-4]+"_all_language.txt",'rb')
    conn.upload(date_list[exact_date],f,'wikipedia-daily-peak')
    os.remove(date_list[exact_date][:-4]+"_all_language.txt")
