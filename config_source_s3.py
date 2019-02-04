# Copyright 2019 Luddite Labs Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
import os.path as op
import logging
import boto3
from io import BytesIO
from boto3.exceptions import Boto3Error
from config_source import config_source, load_to

__version__ = '0.0.3'

logger = logging.getLogger(__name__)


def get_bucket(bucket_name, profile=None, access_key=None, secret_key=None):
    """Get S3 bucket.

    Args:
        bucket_name:
        profile:
        access_key:
        secret_key:

    Returns:

    """
    logger.info('Connecting to S3...')
    session = boto3.Session(
        profile_name=profile,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)
    s3 = session.resource('s3')
    return s3.Bucket(bucket_name)


@config_source('s3')
def load_from_s3(config, bucket_name, filename, profile=None,
                 access_key=None, secret_key=None, cache_filename=None,
                 update_cache=False, silent=False):
    """Load configs from S3 bucket file.

    If ``cache_filename`` is not set then it downloads to memory,
    otherwise content is saved in ``cache_filename`` and reused on next calls
    unless ``update_cache`` is set.

    If ``update_cache`` is set then it downloads remote file and updates
    ``cache_filename``.

    Args:
        config: :class:`~configsource.Config` instance.
        bucket_name: S3 bucket name.
        filename: Path to file in the bucket.
        profile: AWS credentials profile.
        access_key: AWS access key id.
        secret_key: AWS secret access key.
        cache_filename: Filename to store downloaded content.
        update_cache: Use cached file or force download.
        silent: Don't raise an error on missing remote files or file loading
            errors.

    Returns:
        ``True`` if config is loaded and ``False`` otherwise.

    See Also:
        https://docs.aws.amazon.com/cli/latest/userguide/cli-multiple-profiles.html
    """
    # If cache file is not set or doesn't exist then force download.

    try:
        if cache_filename is None:
            src = BytesIO()
            bucket = get_bucket(bucket_name, profile, access_key, secret_key)
            bucket.download_fileobj(filename, src)
            src.flush()
            src.seek(0)
        else:
            src = cache_filename
            if not op.exists(cache_filename) or update_cache:
                bucket = get_bucket(bucket_name, profile, access_key, secret_key)
                bucket.download_file(filename, src)
    except Boto3Error:
        if silent:
            return False
        raise

    # If cache update is not required then just load configs from existing
    # cached file.
    return load_to(config, 'pyfile', 'dict', src, silent=silent)
