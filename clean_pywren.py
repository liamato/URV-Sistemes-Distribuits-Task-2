#! /usr/bin/env python3

import cos

cos_config = {
    'endpoint': '',
    'secret_key': '',
    'access_key': '',

    'api_key': '',
    'private_endpoint': ''
}


back = cos.Backend(cos_config, 'test-buck-urv')
for obj in back.list_objects('pywren.jobs'):
    print("Removing:", obj['Key'])
    back.delete_object(obj['Key'])
