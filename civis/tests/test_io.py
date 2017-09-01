from collections import OrderedDict
import json
import os
from six import StringIO, BytesIO
import tempfile
import zipfile

import pytest
import vcr

try:
    import pandas as pd
    has_pandas = True
except ImportError:
    has_pandas = False

import civis
from civis.compat import mock, FileNotFoundError
from civis.response import Response
from civis.base import CivisAPIError
from civis.resources._resources import get_api_spec, generate_classes
from civis.tests.testcase import (CivisVCRTestCase,
                                  cassette_dir,
                                  POLL_INTERVAL)
from civis.tests import mocks, TEST_SPEC

api_import_str = 'civis.resources._resources.get_api_spec'
with open(TEST_SPEC) as f:
    civis_api_spec = json.load(f, object_pairs_hook=OrderedDict)


class MockAPIError(CivisAPIError):
    """A fake API error with only a status code"""
    def __init__(self, sc):
        self.status_code = sc


@mock.patch(api_import_str, return_value=civis_api_spec)
class ImportTests(CivisVCRTestCase):
    # Note that all functions tested here should use a
    # `polling_interval=POLL_INTERVAL` input. This lets us use
    # sensible polling intervals when recording, but speed through
    # the calls in the VCR cassette when testing later.

    @classmethod
    def setUpClass(cls):
        get_api_spec.cache_clear()
        generate_classes.cache_clear()

    @classmethod
    def tearDownClass(cls):
        get_api_spec.cache_clear()
        generate_classes.cache_clear()

    @classmethod
    @mock.patch(api_import_str, return_value=civis_api_spec)
    def setup_class(cls, *mocks):
        setup_vcr = vcr.VCR(filter_headers=['Authorization'])
        setup_cassette = os.path.join(cassette_dir(), 'io_setup.yml')
        with setup_vcr.use_cassette(setup_cassette):
            # create a file
            buf = StringIO()
            buf.write('a,b,c\n1,2,3')
            buf.seek(0)
            file_id = civis.io.file_to_civis(buf, 'somename')
            cls.file_id = file_id

            # create the table. assumes this function works.
            sql = """
                DROP TABLE IF EXISTS scratch.api_client_test_fixture;

                CREATE TABLE scratch.api_client_test_fixture (
                    a int,
                    b int,
                    c int
                );

                INSERT INTO scratch.api_client_test_fixture
                VALUES (1,2,3);
            """
            res = civis.io.query_civis(sql, 'redshift-general',
                                       polling_interval=POLL_INTERVAL)
            res.result()  # block

            # create an export to check get_url. also tests export_csv
            with tempfile.NamedTemporaryFile() as tmp:
                sql = "SELECT * FROM scratch.api_client_test_fixture"
                database = 'redshift-general'
                result = civis.io.civis_to_csv(tmp.name, sql, database,
                                               polling_interval=POLL_INTERVAL)
                result = result.result()
                assert result.state == 'succeeded'

            cls.export_job_id = result.sql_id

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_get_url_from_file_id(self, *mocks):
        client = civis.APIClient()
        url = civis.io._files._get_url_from_file_id(self.file_id, client)
        assert url.startswith('https://civis-console.s3.amazonaws.com/files/')

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_zip_member_to_civis(self, *mocks):
        with tempfile.NamedTemporaryFile() as tmp:
            with zipfile.ZipFile(tmp, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.writestr(tmp.name, 'a,b,c\n1,2,3')
                zip_member = zip_file.namelist()[0]
                with zip_file.open(zip_member) as zip_member_buf:
                    result = civis.io.file_to_civis(zip_member_buf, zip_member)

        assert isinstance(result, int)

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_file_to_civis(self, *mocks):
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(b'a,b,c\n1,2,3')
            tmp.flush()
            tmp.seek(0)
            result = civis.io.file_to_civis(tmp, tmp.name)

        assert isinstance(result, int)

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_civis_to_file(self, *mocks):
        buf = BytesIO()
        civis.io.civis_to_file(self.file_id, buf)
        buf.seek(0)
        assert buf.read() == b'a,b,c\n1,2,3'

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_csv_to_civis(self, *mocks):
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(b'a,b,c\n1,2,3')
            tmp.flush()

            table = "scratch.api_client_test_fixture"
            database = 'redshift-general'
            result = civis.io.csv_to_civis(tmp.name, database, table,
                                           existing_table_rows='truncate',
                                           polling_interval=POLL_INTERVAL)
            result = result.result()  # block until done

        assert isinstance(result.id, int)
        assert result.state == 'succeeded'

    @pytest.mark.skipif(not has_pandas, reason="pandas not installed")
    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_read_civis_pandas(self, *mocks):
        expected = pd.DataFrame([[1, 2, 3]], columns=['a', 'b', 'c'])
        df = civis.io.read_civis('scratch.api_client_test_fixture',
                                 'redshift-general', use_pandas=True,
                                 polling_interval=POLL_INTERVAL)
        assert df.equals(expected)

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_read_civis_no_pandas(self, *mocks):
        expected = [['a', 'b', 'c'], ['1', '2', '3']]
        data = civis.io.read_civis('scratch.api_client_test_fixture',
                                   'redshift-general', use_pandas=False,
                                   polling_interval=POLL_INTERVAL)
        assert data == expected

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_read_civis_sql(self, *mocks):
        sql = "SELECT * FROM scratch.api_client_test_fixture"
        expected = [['a', 'b', 'c'], ['1', '2', '3']]
        data = civis.io.read_civis_sql(sql, 'redshift-general',
                                       use_pandas=False,
                                       polling_interval=POLL_INTERVAL)
        assert data == expected

    @pytest.mark.skipif(not has_pandas, reason="pandas not installed")
    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_dataframe_to_civis(self, *mocks):
        df = pd.DataFrame([['1', '2', '3']], columns=['a', 'b', 'c'])
        result = civis.io.dataframe_to_civis(df, 'redshift-general',
                                             'scratch.api_client_test_fixture',
                                             existing_table_rows='truncate',
                                             polling_interval=POLL_INTERVAL)
        result = result.result()
        assert result.state == 'succeeded'

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_civis_to_multifile_csv(self, *mocks):
        sql = "SELECT * FROM scratch.api_client_test_fixture"
        result = civis.io.civis_to_multifile_csv(
            sql, database='redshift-general', polling_interval=POLL_INTERVAL)
        assert set(result.keys()) == {'entries', 'query', 'header'}
        assert result['query'] == sql
        assert result['header'] == ['a', 'b', 'c']
        assert isinstance(result['entries'], list)
        assert set(result['entries'][0].keys()) == {'id', 'name', 'size',
                                                    'url', 'url_signed'}
        assert result['entries'][0]['url_signed'].startswith('https://civis-'
                                                             'console.s3.'
                                                             'amazonaws.com/')

    @mock.patch(api_import_str, return_value=civis_api_spec)
    def test_transfer_table(self, *mocks):
        result = civis.io.transfer_table('redshift-general', 'redshift-test',
                                         'scratch.api_client_test_fixture',
                                         'scratch.api_client_test_fixture',
                                         polling_interval=POLL_INTERVAL)
        result = result.result()
        assert result.state == 'succeeded'

        # check for side effect
        sql = 'select * from scratch.api_client_test_fixture'
        result = civis.io.query_civis(sql, 'redshift-test',
                                      polling_interval=POLL_INTERVAL).result()
        assert result.state == 'succeeded'

    def test_get_sql_select(self, *mocks):
        x = "select * from schema.table"
        y = "select a, b, c from schema.table"
        table = "schema.table"
        assert civis.io._tables._get_sql_select(table) == x
        assert civis.io._tables._get_sql_select(table, ['a', 'b', 'c']) == y
        with pytest.raises(TypeError):
            civis.io._tables._get_sql_select(table, "column_a")

    def test_download_file(self, *mocks):
        url = "https://httpbin.org/stream/3"
        x = '{"url": "https://httpbin.org/stream/3", "headers": {"Host": "httpbin.org", "Accept-Encoding": "gzip, deflate", "Accept": "*/*", "User-Agent": "python-requests/2.7.0 CPython/3.4.3 Linux/3.19.0-25-generic"}, "args": {}, "id": 0, "origin": "108.211.184.39"}\n'  # noqa: E501
        y = '{"url": "https://httpbin.org/stream/3", "headers": {"Host": "httpbin.org", "Accept-Encoding": "gzip, deflate", "Accept": "*/*", "User-Agent": "python-requests/2.7.0 CPython/3.4.3 Linux/3.19.0-25-generic"}, "args": {}, "id": 1, "origin": "108.211.184.39"}\n'  # noqa: E501
        z = '{"url": "https://httpbin.org/stream/3", "headers": {"Host": "httpbin.org", "Accept-Encoding": "gzip, deflate", "Accept": "*/*", "User-Agent": "python-requests/2.7.0 CPython/3.4.3 Linux/3.19.0-25-generic"}, "args": {}, "id": 2, "origin": "108.211.184.39"}\n'  # noqa: E501
        expected = x + y + z
        with tempfile.NamedTemporaryFile() as tmp:
            civis.io._tables._download_file(url, tmp.name)
            with open(tmp.name, "r") as f:
                data = f.read()
        assert data == expected


def test_file_id_from_run_output_exact():
    m_client = mocks.create_client_mock(reset=True)
    fid_post = m_client.files.post("spam").id
    m_client.scripts.post_containers_runs_outputs(300, 1000, "File", fid_post)

    fid = civis.io.file_id_from_run_output('spam', 300, 1000, client=m_client)
    assert fid == fid_post


def test_file_id_from_run_output_approximate():
    # Test fuzzy name matching
    m_client = mocks.create_client_mock(reset=True)
    fid_post = m_client.files.post(name="spam.csv.gz").id
    m_client.scripts.post_containers_runs_outputs(300, 1000, "File", fid_post)

    fid = civis.io.file_id_from_run_output('^spam', 300, 1000, regex=True,
                                           client=m_client)
    assert fid == fid_post


def test_file_id_from_run_output_approximate_multiple():
    # Fuzzy name matching with multiple matches should return the first
    m_client = mocks.create_client_mock(reset=True)
    fid1_post = m_client.files.post(name="spam.csv.gz").id
    fid2_post = m_client.files.post(name="eggs.csv.gz").id
    m_client.scripts.post_containers_runs_outputs(300, 1000, "File", fid1_post)
    m_client.scripts.post_containers_runs_outputs(300, 1000, "File", fid2_post)

    fid = civis.io.file_id_from_run_output('.csv', 300, 1000, regex=True,
                                           client=m_client)
    assert fid == fid1_post


def test_file_id_from_run_output_no_file():
    # Get an IOError if we request a file which doesn't exist
    m_client = mocks.create_client_mock(reset=True)

    with pytest.raises(FileNotFoundError) as err:
        civis.io.file_id_from_run_output('eggs', 300, 1000, client=m_client)
    assert 'not an output' in str(err.value)


def test_file_id_from_run_output_no_run():
    # Get an IOError if we request a file from a run which doesn't exist
    m_client = mocks.create_client_mock(reset=True)

    with pytest.raises(IOError) as err:
        civis.io.file_id_from_run_output('name', 300, 1001, client=m_client)
    assert 'could not find job/run id 300/1001' in str(err.value).lower()


def test_file_id_from_run_output_platform_error():
    # Make sure we don't swallow real Platform errors
    m_client = mocks.create_client_mock(reset=True)
    m_client.scripts.list_containers_runs_outputs.side_effect =\
        MockAPIError(500)  # Mock a platform error
    with pytest.raises(CivisAPIError):
        civis.io.file_id_from_run_output('name', 17, 13, client=m_client)


def _init_csv(dirname, fname, **kwargs):
    df = pd.DataFrame({'a': [0, 1, 2], 'b': ['one', 'two', 'one']})
    full_fname = os.path.join(dirname, fname)
    df.to_csv(full_fname, index=False, **kwargs)
    with open(full_fname, 'rb') as _fin:
        fid = mocks.mock_file_to_civis(_fin, fname, tempdir=dirname)
    return df, fid


@pytest.mark.skipif(not has_pandas, reason="pandas not installed")
def test_file_to_dataframe_infer():
    # Test that we can read a non-compressed CSV written to the files endpoint
    m_client = mocks.create_client_mock(reset=True)
    with tempfile.TemporaryDirectory() as tdir:
        df, fid = _init_csv(tdir, 'spam.csv')
        df_out = civis.io.file_to_dataframe(fid, client=m_client)
        assert df_out.equals(df)


@pytest.mark.skipif(not has_pandas, reason="pandas not installed")
def test_file_to_dataframe_infer_gzip():
    # Test we can properly infer the gzip compression of a file from its name
    m_client = mocks.create_client_mock(reset=True)
    with tempfile.TemporaryDirectory() as tdir:
        df, fid = _init_csv(tdir, 'spam.csv.gz', compression='gzip')
        df_out = civis.io.file_to_dataframe(fid, client=m_client)
        assert df_out.equals(df)


@pytest.mark.skipif(not has_pandas, reason="pandas not installed")
def test_file_to_dataframe_kwargs():
    # Test that we're correctly using keyword arguments when reading CSVs
    mcl = mocks.create_client_mock(reset=True)
    with tempfile.TemporaryDirectory() as tdir:
        df, fid = _init_csv(tdir, 'spam.csv', sep='|')
        df_out = civis.io.file_to_dataframe(fid, client=mcl, sep='|', nrows=1)
        assert df_out.equals(df.iloc[:1, :])


@mock.patch.object(civis.io._files, 'civis_to_file', mocks.mock_civis_to_file)
def test_load_json():
    mcl = mocks.create_client_mock(reset=True)
    obj = {'spam': 'eggs'}
    with tempfile.TemporaryDirectory() as tdir:
        buf = BytesIO(json.dumps(obj).encode())
        fid = mocks.mock_file_to_civis(buf, 'test', mcl, tempdir=tdir)
        out = civis.io.file_to_json(fid, client=mcl)
    assert out == obj
