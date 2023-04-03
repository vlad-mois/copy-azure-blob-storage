import logging
import os
import pickle
from collections import defaultdict
from dataclasses import dataclass
from humanize import naturalsize
from pathlib import Path
from typing import Dict, Optional, Union

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobProperties, BlobServiceClient, ContainerClient


def initialize_logging(
    logger: Optional[logging.Logger] = None, 
    log_format: Optional[str] = None, 
    logging_level: Union[str, int] = logging.INFO,
) -> logging.Logger:
    log_format = log_format or ' - '.join([
        '%(levelname)s',
        '%(asctime)s',
        'PID:%(process)d',
        '%(pathname)s',
        '%(funcName)s',
        '%(lineno)d',
        '%(message)s',
    ])

    log_formatter = logging.Formatter(log_format)
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(log_formatter)
    log_handler.setLevel(logging_level)

    logger = logger or logging.root
    logger.handlers = []
    logger.addHandler(log_handler)
    logger.setLevel(logging_level)

    return logger


ContainerName = str
BlobName = str

FILE_WITH_HIERARCHY_NAME = 'hierarchy.pickle'


@dataclass
class BlobInfo:
    properties: BlobProperties
    data_local_path: Optional[str] = None
    uploaded: bool = False

    @property
    def name(self) -> str:
        return self.properties.name

    @property
    def size(self) -> int:
        return self.properties.size

    def is_downloaded(self) -> bool:
        return bool(self.data_local_path)

    def is_uploaded(self) -> bool:
        return self.uploaded

    def download(self, container_client: ContainerClient, base_path: str) -> None:
        file_path = os.path.join(base_path, self.name)
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        stream = container_client.download_blob(self.name)
        with open(file_path, 'wb') as file:
            for chunk in stream.chunks():
                file.write(chunk)
        self.data_local_path = file_path

    def upload(self, container_client: ContainerClient, base_path: str) -> None:
        file_path = os.path.join(base_path, self.name)
        with open(file_path, 'rb') as file:
            container_client.upload_blob(
                name=self.name,
                data=file,
                blob_type=self.properties.blob_type,
                length=self.properties.size or None,
                metadata=self.properties.metadata,
                content_settings=self.properties.content_settings,
            )
        self.uploaded = True


def get_hierarchy(account_name: str, account_key: str) -> Dict[ContainerName, Dict[BlobName, BlobInfo]]:
    blob_service_client = BlobServiceClient.from_connection_string(
        f'AccountName={account_name};AccountKey={account_key};'
    )
    blobs_by_container = defaultdict(dict)
    total_size: int = 0

    for container in blob_service_client.list_containers():
        container_client = blob_service_client.get_container_client(container.name)
        container_size: int = 0

        for blob in container_client.list_blobs():
            if not blob.deleted and (blob.size or blob.content_settings.content_md5 is not None):
                blobs_by_container[container.name][blob.name] = BlobInfo(blob)
                container_size += blob.size

        total_size += container_size
        logging.info('Container "%s" blobs count: %d size: %s',
                     container.name, len(blobs_by_container[container.name]),
                     naturalsize(container_size, binary=True))

    logging.info('Total size: %s', naturalsize(total_size, binary=True))

    return blobs_by_container


def download_blobs(
    account_name: str, 
    account_key: str,
    blobs_by_container: Dict[ContainerName, Dict[BlobName, BlobInfo]], 
    local_storage_dir: str,
) -> None:
    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            f'AccountName={account_name};AccountKey={account_key};'
        )
        for container, blob_by_name in blobs_by_container.items():
            base_path = os.path.join(local_storage_dir, container)
            container_client = blob_service_client.get_container_client(container)
            for index, blob in enumerate(blob_by_name.values(), 1):
                if not blob.is_downloaded():
                    logging.info('Container "%s" downloading (%d / %d) "%s" of size %s ...',
                                 container, index, len(blob_by_name), blob.name,
                                 naturalsize(blob.size, binary=True))
                    blob.download(container_client, base_path)
    except (Exception, KeyboardInterrupt):
        raise
    finally:
        hierarchy_filename = os.path.join(local_storage_dir, FILE_WITH_HIERARCHY_NAME)
        logging.info('Saving hierarchy: %s', hierarchy_filename)
        with open(hierarchy_filename, 'wb') as hierarchy_file:
            pickle.dump(blobs_by_container, hierarchy_file)

    logging.info('Done.')


def open_hierarchy_local(local_storage_dir: str) -> Dict[ContainerName, Dict[BlobName, BlobInfo]]:
    hierarchy_filename = os.path.join(local_storage_dir, FILE_WITH_HIERARCHY_NAME)
    logging.info('Loading hierarchy: %s', hierarchy_filename)
    with open(hierarchy_filename, 'rb') as hierarchy_file:
        return pickle.load(hierarchy_file)


def upload_blobs(
    account_name: str,
    account_key: str,
    blobs_by_container: Dict[ContainerName, Dict[BlobName, BlobInfo]],
    local_storage_dir: str,
    create_containers: bool = False,
) -> None:
    blob_service_client = BlobServiceClient.from_connection_string(
        f'AccountName={account_name};AccountKey={account_key};'
    )
    for container, blob_by_name in blobs_by_container.items():
        base_path = os.path.join(local_storage_dir, container)
        container_client = blob_service_client.get_container_client(container)
        for index, blob in enumerate(blob_by_name.values(), 1):
            if blob.is_downloaded() and not blob.is_uploaded():
                logging.info('Container "%s" uploading (%d / %d) "%s" of size %s ...',
                             container, index, len(blob_by_name), blob.name,
                             naturalsize(blob.size, binary=True))
                try:
                    blob.upload(container_client, base_path)
                except ResourceNotFoundError as exc:
                    if exc.error_code == 'ContainerNotFound' and create_containers:
                        logging.warning('Create container: "%s" with default settings', container)
                        container_client.create_container()
                        blob.upload(container_client, base_path)
                    else:
                        raise


# ===== Usage =====
account_name = '...'
account_key = '...'
dst_account_name = '...'
dst_account_key = '...'

initialize_logging()
azure_logger = logging.getLogger('azure')
azure_logger.setLevel(logging.WARNING)

hierarchy = get_hierarchy(account_name, account_key)

local_storage = '/tmp/save-blobs-2'

download_blobs(
    account_name,
    account_key,
    hierarchy,
    local_storage,
)

upload_blobs(
    dst_account_name,
    dst_account_key,
    hierarchy2,
    local_storage,
)
