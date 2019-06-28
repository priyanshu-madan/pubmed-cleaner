import boto3
import pandas as pd
from io import BytesIO
import os
import csv
from io import StringIO
import s3fs
from config import S3_BUCKET, S3_KEY, S3_SECRET_ACCESS_KEY

s3 = boto3.resource('s3')
for bucket in s3.buckets.all():
    print(bucket.name)


client = boto3.client('s3') #low-level functional API

resource = boto3.resource('s3') #high-level object-oriented API
my_bucket = resource.Bucket('pubmed-db')


obj = client.get_object(Bucket='pubmed-db', Key='abstracts_tina.csv')
grid_sizes = pd.read_csv(obj['Body'])
print(grid_sizes)

abstract_no_db = grid_sizes.abstract_no
print(abstract_no_db)



bytes_to_write = abstract_no_db.to_csv(None).encode()
fs = s3fs.S3FileSystem(key=S3_KEY, secret=S3_SECRET_ACCESS_KEY)
with fs.open('s3://pubmed-db/file.csv', 'wb') as f:
    f.write(bytes_to_write)





# csv_buffer = StringIO()
# abstract_no_db.to_csv(csv_buffer)
# resource.object(my_bucket, 'df.csv').put(Body=csv_buffer.getvalue())


# abstract_no_db.to_csv("abs_test.csv",index=False)

# client.put_object(Body=more_binary_data, Bucket='my_bucket_name', Key='my/key/including/anotherfilename.txt')

# s3.Object('pubmed-db', 'abs_test.csv').put(Body=abstract_no_db.to_csv("abs_test.csv",index=False))

# data = open("abs_test.csv", 'rb')
# s3.Bucket('pubmed-db').put_object(Key='abs_test.csv', Body=data)

# print(grid_sizes)

# files = list(my_bucket.objects.filter())
# print(files)

# obj = files[0].get()
# print(obj)