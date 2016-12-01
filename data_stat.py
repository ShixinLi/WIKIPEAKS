import boto3
s3= boto3.resource("s3")
bucket =  s3.Bucket('wikipedia-english-articles')
for obj in  bucket.objects.filter(Prefix='2016-09-23.txt'):     
	print obj.get()['Body'].read() 