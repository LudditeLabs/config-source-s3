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

import pytest
try:
    from unittest.mock import patch, call, Mock
except ImportError:
    from mock import patch, call, Mock
from boto3.exceptions import Boto3Error
from config_source_s3 import load_from_s3, split_s3_path


# Test: split_s3_path().
@pytest.mark.parametrize('path,parts', [
    ('s3://my-bucket', ('my-bucket', '')),
    ('s3://my-bucket/filename', ('my-bucket', 'filename')),
    ('s3://my-bucket/dir/', ('my-bucket', 'dir')),
    ('s3://my-bucket/dir/filename', ('my-bucket', 'dir/filename')),
])
def test_split_s3_path(path, parts):
    assert split_s3_path(path) == parts


# Test: split_s3_path() with invalid paths.
@pytest.mark.parametrize('path', [
    's3:/my-bucket',
    's3:/my-bucket/filename',
    '/my-bucket/dir/',
    'my-bucket/dir/filename',
    's3://',
])
def test_split_s3_path_invalid(path):
    with pytest.raises(ValueError):
        split_s3_path(path)


# Test: pass invalid S3 filename.
@patch('config_source_s3.load_to')
@patch('config_source_s3.get_bucket')
def test_invalid_path(get_bucket, load_to):
    config = Mock()
    with pytest.raises(ValueError) as e:
        load_from_s3(config, '/somebucket/mycfg.py')
    assert str(e.value) == 'Invalid S3 path'


# Test: pass valid S3 path but without filename.
@patch('config_source_s3.load_to')
@patch('config_source_s3.get_bucket')
def test_empty_filename(get_bucket, load_to):
    config = Mock()
    with pytest.raises(ValueError) as e:
        load_from_s3(config, 's3://somebucket')
    assert str(e.value) == 'Empty filename'


# Test: download without caching, mock all calls.
@patch('config_source_s3.BytesIO')
@patch('config_source_s3.load_to')
@patch('config_source_s3.get_bucket')
def test_nocache_fake(get_bucket, load_to, BytesIO):
    sio = Mock()
    BytesIO.return_value = sio
    m = Mock()
    m.attach_mock(get_bucket, 'get_bucket')
    m.attach_mock(load_to, 'load_to')
    m.attach_mock(BytesIO, 'BytesIO')

    config = Mock()
    load_from_s3(config, 's3://somebucket/mycfg.py')

    assert m.mock_calls == [
        call.BytesIO(),
        call.get_bucket('somebucket', None, None, None),
        call.get_bucket().download_fileobj('mycfg.py', sio),
        call.BytesIO().flush(),
        call.BytesIO().seek(0),
        call.load_to(config, 'pyfile', 'dict', sio, silent=False)
    ]

# Test: download without caching.
@patch('config_source_s3.BytesIO')
@patch('config_source_s3.get_bucket')
def test_nocache(get_bucket, BytesIO):
    sio = Mock()
    sio.read.return_value = 'A=1'

    BytesIO.return_value = sio
    config = {}
    res = load_from_s3(config, filename='s3://somebucket/mycfg.py')

    assert res is True
    assert config == {'A': 1}


# Test: download config to cache file which doesn't exist yet.
@patch('config_source_s3.load_to')
@patch('config_source_s3.get_bucket')
def test_cache(get_bucket, load_to, tmpdir):
    config = Mock()

    out_filename = str(tmpdir.join('out.py'))

    load_from_s3(config, filename='s3://somebucket/mycfg.py',
                 cache_filename=out_filename)

    # It must download mycfg.py to cache file.
    assert get_bucket().method_calls == [
        call.download_file('mycfg.py', out_filename)
    ]

    # Then load config from the cached file.
    assert load_to.mock_calls == [call(config, 'pyfile', 'dict',
                                       out_filename, silent=False)]


# Test: download config to existing cache file.
@patch('config_source_s3.load_to')
@patch('config_source_s3.get_bucket')
def test_cache_exist(get_bucket, load_to, tmpdir):
    config = Mock()

    cache = tmpdir.join('out.py')
    cache.write('1')

    out_filename = str(cache)

    load_from_s3(config, filename='s3://somebucket/mycfg.py',
                 cache_filename=out_filename)

    # If cache file exists and 'update_cache' is not set then it won't
    # download remote file.
    get_bucket.assert_not_called()

    # Load config from the cached file must happen anyway.
    assert load_to.mock_calls == [call(config, 'pyfile', 'dict',
                                       out_filename, silent=False)]


# Test: download config to existing cache file and force update cache.
@patch('config_source_s3.load_to')
@patch('config_source_s3.get_bucket')
def test_cache_exist_force(get_bucket, load_to, tmpdir):
    config = Mock()

    cache = tmpdir.join('out.py')
    cache.write('1')

    out_filename = str(cache)

    load_from_s3(config, filename='s3://somebucket/mycfg.py',
                 cache_filename=out_filename, update_cache=True)

    # It must download mycfg.py and overwirte cache file
    # since update_cache=True.
    assert get_bucket().method_calls == [
        call.download_file('mycfg.py', out_filename)
    ]

    # Then load config from the cached file.
    assert load_to.mock_calls == [call(config, 'pyfile', 'dict',
                                       out_filename, silent=False)]


# Test: download non-existent remote config file.
# It must raise an error.
@patch('config_source_s3.load_to')
@patch('config_source_s3.get_bucket')
def test_remote_missing(get_bucket, load_to):
    config = Mock()

    get_bucket().download_fileobj.side_effect = Boto3Error('test')

    with pytest.raises(Boto3Error) as e:
        load_from_s3(config, filename='s3://somebucket/mycfg.py')

    assert str(e.value) == 'test'

    load_to.assert_not_called()


# Test: silently download non-existent remote config file.
# It must raise an error.
@patch('config_source_s3.load_to')
@patch('config_source_s3.get_bucket')
def test_remote_missing_silent(get_bucket, load_to):
    config = Mock()

    get_bucket().download_fileobj.side_effect = Boto3Error('test')

    res = load_from_s3(config, filename='s3://somebucket/mycfg.py', silent=True)

    assert res is False

    load_to.assert_not_called()
