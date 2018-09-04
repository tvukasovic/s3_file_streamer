import boto3
from os import remove
from os.path import getsize

s3 = boto3.client('s3')
s3_r = boto3.resource('s3')

req = s3.get_object(Bucket='#',
                    Key='#')
size = req['ContentLength']
reqs = 10
chunks = size // reqs
floor = 0
roof = chunks
while roof != size:
    if roof > size:
        roof = size
    req = s3.get_object(Bucket='#',
                    Key='#',
                    Range='bytes={}-{}'.format(floor, roof))
    with open('/tmp/processed.txt', 'wb') as f:
        f.write(req['Body'].read())
    with open('/tmp/processed.txt', 'rb') as f:
        content = f.read()
    last_index = content.rindex(b'\n')
    floor += last_index
    roof += last_index
    content = content[:content.rindex(b'\n')]
    remove('/tmp/processed.txt')
    with open('processed.txt', 'ab') as r:
        r.write(content)
print(size, getsize('processed.txt'))
