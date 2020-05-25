import ibm_boto3
import ibm_botocore

class Backend:
    def __init__ (self, cos_config, bucket_name):
            self.bucket_name = bucket_name

            if isinstance(cos_config, ibm_botocore.client.BaseClient):
                self.cos_client = cos_config
            else:
                service_endpoint = cos_config.get('endpoint').replace('http:', 'https:')
                secret_key = cos_config.get('secret_key')
                access_key = cos_config.get('access_key')
                client_config = ibm_botocore.client.Config(max_pool_connections=200, user_agent_extra='pywren-ibm-cloud')

                self.cos_client = ibm_boto3.client('s3',
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key,
                        config=client_config,
                        endpoint_url=service_endpoint)

    def put_object(self, key, data):

        try:
            res = self.cos_client.put_object(Bucket=self.bucket_name, Key=key, Body=data)
            status = 'OK' if res['ResponseMetadata']['HTTPStatusCode'] == 200 else 'Error'

            print('PUT Object {} - Size: {} - {}'.format(key, len(data), status))

        except ibm_botocore.exceptions.ClientError as e:
            raise e

    def get_object(self, key, stream=False, extra_get_args={}):

        r = self.cos_client.get_object(Bucket=self.bucket_name, Key=key, **extra_get_args)
        if stream:
            data = r['Body']
        else:
            data = r['Body'].read()

        return data

    def exist_object(self, key):
        try:
            self.head_object(key)
        except ibm_botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise e

        return True

    def get_modificationTime(self, key):
        return self.head_object(key)['last-modified']

    def get_etag(self, key):
        return self.head_object(key)['etag']

    def head_object(self, key):

        metadata = self.cos_client.head_object(Bucket=self.bucket_name, Key=key)
        return metadata['ResponseMetadata']['HTTPHeaders']

    def delete_object(self, key):
        return self.cos_client.delete_object(Bucket=self.bucket_name, Key=key)

    def list_objects(self, prefix=None):
        paginator = self.cos_client.get_paginator('list_objects_v2')

        if prefix is not None:
            page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
        else:
            page_iterator = paginator.paginate(Bucket=self.bucket_name)

        object_list = []
        for page in page_iterator:
            if 'Contents' in page:
                for item in page['Contents']:
                    object_list.append(item)
        return object_list
