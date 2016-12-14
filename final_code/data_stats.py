import boto3
import os
import tinys3
import pandas as pd
s3= boto3.resource("s3")
bucket =  s3.Bucket('wikipedia-english-articles')
times = pd.date_range('2016-01-01',periods=306)
date_list=list()
for i in range(len(times)):
    date_list.append(str(times[i])[:10]+".txt")


date_before=list()
for data in [7,6,5,4,3,2,1]:
    date_before.append(str( pd.to_datetime(date_list[exact_date][:10])-pd.DateOffset(days=data) ))



for obj in  bucket.objects.filter(Prefix=date_list[0]):
    current_date_data =  obj.get()['Body'].read()
current_date_data_line_list = current_date_data.split('\n')
final_list=[]
for idx,line_data in enumerate(current_date_data_line_list):
    if idx==len(current_date_data_line_list)-1:
        break
    article,day_page_views = line_data.split('\t')
    final_list.append([article,int(day_page_views)])

date_before=['2015-12-25.txt','2015-12-26.txt','2015-12-27.txt','2015-12-28.txt','2015-12-29.txt','2015-12-30.txt','2015-12-31.txt']
date_after = ['2016-01-02.txt','2016-01-03.txt','2016-01-04.txt','2016-01-05.txt','2016-01-06.txt','2016-01-07.txt','2016-01-08.txt']

for obj in  bucket.objects.filter(Prefix=date_before[0]):
    before_date_data =  obj.get()['Body'].read()
before_date_data_line_list = before_date_data.split('\n')
before_dict = dict()
for idx,line_data in enumerate(before_date_data_line_list):
    if idx==len(before_date_data_line_list)-1:
        break
    before_article,before_day_page_views = line_data.split('\t')
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
        before_dict[before_article]=int(before_day_page_views)
    for idx,data in enumerate(final_list):
        if data[0] in before_dict:
            page_view_number = before_dict[data[0]]
        else:
            page_view_number = 0
        peak_candidate_list[idx][1].append(page_view_number)

peak_index=list()

for idx,data in enumerate(peak_candidate_list):
    if data[1][7]==max(data[1]):
        peak_index.append(idx)

peak_list = [peak_candidate_list[i] for i in peak_index]
testfile = open('test1.txt','w')

for element in peak_list:
    testfile.write("{name}\t{lst}\n".format(name=element[0],lst=element[1]))

conn = tinys3.Connection('AKIAJ75RITXR5M2XUFBQ','I8tTiWPo5xXBx1Ohf4oUBGAKSHRCgARVXk6g1k1Q',tls=True)
f = open('test1.txt','rb')
conn.upload('test1.txt',f,'wikipedia-english-peak')
os.remove('test.txt')
