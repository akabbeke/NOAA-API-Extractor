import json


with open('config.json') as f:
    config = json.load(f)

auth_token = config['auth_token']
auth_email = config['auth_email']
base_url = config['base_url']
data_endpoint = config['data_endpoint']
request_limit = config['request_limit']
file_line_limit = config['file_line_limit']
output_dir = config['output_dir']
retry_limit = config['retry_limit']