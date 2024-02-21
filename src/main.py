import base64
import json
import os

import httpx

import functions_framework
from google.cloud import storage


class StorageNotification:
    def __init__(self, message: dict):
        # other properties are not used, therefore not parsed
        self.name = message['name']
        self.bucket = message['bucket']


class FileHandler:
    def __init__(self):
        # set http/https depending on env var. default: True
        self.url_scheme = f"http{'s' if bool(os.getenv('CRIBL_USESSL', 'True')) is True else ''}"

        # get parameters to send to Cribl
        self.hostname = os.getenv('CRIBL_HOST')
        self.port = int(os.getenv('CRIBL_PORT'))
        self.endpoint = os.getenv('CRIBL_ENDPOINT', '/')
        self.auth_token = os.getenv('CRIBL_AUTH_TOKEN')

        self.verify_ssl = bool(os.getenv('CRIBL_SSL_VERIFY', 'True'))

    def download_and_stream_file(self, notification):
        storage_client = storage.Client()
        bucket = storage_client.bucket(notification.bucket)
        blob = bucket.blob(notification.name)

        # utilizing a file like object to prevent having to read entire file into memory or write into disk
        file_content = blob.open(mode="rb")

        # send file to cribl stream
        r = httpx.post(f"{self.url_scheme}://{self.hostname}:{self.port}{self.endpoint}",
                       data=file_content, verify=self.verify_ssl, headers={"Authorization": self.auth_token,
                                                                           "Content-Encoding": "gzip",
                                                                           "Content-Type": "application/x-gzip"})

        if r.status_code != 200:
            # raise exception to allow google cloud functions to handle retry logic and backoff
            raise Exception(f'failed to upload file: bucket="{notification.bucket}" file="{notification.name}" '
                            f'reason={r.text}')


# register a CloudEvent function with the Functions Framework
@functions_framework.cloud_event
def storage_notification(cloud_event):
    file_handler = FileHandler()

    # get and decode base64 encoded message data
    notification = json.loads(base64.b64decode(cloud_event.data["message"]["data"]).decode())
    sn = StorageNotification(notification)

    # download and stream file to Cribl
    file_handler.download_and_stream_file(sn)
