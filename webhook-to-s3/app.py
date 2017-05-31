from chalice import Chalice
from datetime import datetime
from urllib.parse import parse_qs
import json
import time
import boto3
from flatten_json import flatten
#from importlib import reload
#from hashlib import sha256

app = Chalice(app_name='webhook-to-s3')
app.debug = True

s3 = boto3.client('s3')
s3_bucket = 'librato-events-staging'

@app.route('/')
def index():
    return {'hello': 'world'}

@app.route('/chargify', methods=['POST'], content_types=['application/x-www-form-urlencoded'])
def chargify():
    #signature = request.headers.get('X-Chargify-Webhook-Signature-Hmac-Sha-256')
    #if not hmac.compare_digest(signature, hmac.new(CHARGIFY_SHARED_KEY, request.get_data(), sha256).hexdigest()): abort(404)

    request = app.current_request

    try:
        webhook_data = str(request.raw_body)
    except KeyError:
        raise BadRequestError()

    ## used for 'chalice local' testing
    #request = "insert dummy chargify payload"
    #data=parse_chargify_webhook(request)
    data=parse_chargify_webhook(webhook_data)

    json_str=json.dumps(data).replace('[', '').replace(']', '')
    json_dict=flatten(eval(json_str))
    json_str=str(json_dict)

    date = parse_time(str(datetime.now()))
    filename = data['id'][0] + '.csv'
    file_path = 'billing/year={year}/month={month}/date={day}/'.format(**date) + filename

    s3.put_object(
        Bucket=s3_bucket,
        Key=file_path,
        Body=json_str,
        ACL='private'
    )

    ## used for 'chalice local' testing
    '''
    file_path = 'data.json'

    with open(file_path, 'w+') as fp:
        json.dump(json_dict, fp)
    '''

    return {
        'url': 'https://s3.amazonaws.com/{}/{}'.format(s3_bucket, file_path)
    }

def parse_time(timestamp):
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S -%f"):
        try:
            date = time.strptime(timestamp, fmt)
            return {'year':date.tm_year, 'month':date.tm_mon, 'day':date.tm_mday, 'hour':date.tm_hour, 'minute':date.tm_min, 'second':date.tm_sec}
        except ValueError:
            pass
    raise ValueError('no valid date format found')

# Converts Chargify webhook parameters to a python dictionary of nested dictionaries
def parse_chargify_webhook(post_data):
    post_data = parse_qs(post_data)

    result = {}
    for k, v in iter(post_data.items()):
        keys = [x.strip(']') for x in k.split('[')]
        cur = result
        for key in keys[:-1]:
            cur = cur.setdefault(key, {})
        cur[keys[-1]] = v
    return result
