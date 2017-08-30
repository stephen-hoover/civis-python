"""Mock client creation and tooling

Use the `create_client_mock` to get an autospecced APIClient
with a few of the more used / easier-to-mock endpoints mocked.
Modify the `side_effects` or `return_value` as needed for your specific test.

Keep "databases" of objects as module-level dictionaries.
Access them with side_effects rather than return_value so that they
can change during testing.
"""
import copy
from datetime import datetime, timedelta
from functools import partial
import os

import dateutil.parser

from civis import APIClient, find
from civis.base import CivisAPIError
from civis.compat import mock
from civis.resources import CACHED_SPEC_PATH
from civis.response import Response

# Note that the LIST and GET responses are slightly different for /users.
# For test mocking, it should be okay to use the same thing for each.
_users = {7: Response({'created_at': '2013-01-20T12:31:17.000Z',
                       'email': 'jsmith@mocking.org',
                       'feature_flags': {'old': True, 'new': False},
                       'id': 7,
                       'initials': 'JS',
                       'name': 'Jane Smith',
                       'organization_name': 'Mocking Co., Inc.',
                       'username': 'jsmith',
                       'groups': [],
                       }),
          }
users = copy.deepcopy(_users)

_credentials = {10: Response({'created_at': '2014-02-22T22:12:00.000Z',
                              'description': None,
                              'id': 10,
                              'name': 'jsmith',
                              'owner': 'jsmith',
                              'remote_host_id': None,
                              'remote_host_name': None,
                              'type': 'Database',
                              'updated_at': '2016-11-11T11:13:03.000Z',
                              'username': 'jsmith'}),
                }
credentials = copy.deepcopy(_credentials)

# Mock file object IDs start at 100
_files = {100: Response({'created_at': '2017-08-30T16:29:27.000Z',
                         'download_url': Response({'host': 'localhost',
                                                   'port': 65000,
                                                   'path': 'snakes.ipynb',
                                                   'query': ''}),
                         'expires_at': '2017-09-29T16:29:27.000Z',
                         'file_size': 1024,
                         'file_url': 'localhost:65000/snakes.ipynb',
                         'id': 100,
                         'name': 'snakes.ipynb'}),
          }
files = copy.deepcopy(_files)

# Mock table object IDs start at 200
_tables_l = {200: {'column_count': 6,
                   'database_id': 13,
                   'description': None,
                   'distkey': 'index',
                   'id': 200,
                   'is_view': False,
                   'last_refresh': '2017-08-09T21:02:40.000Z',
                   'last_run': Response(
                       {'createdAt': '2017-08-09T21:01:56.000Z',
                        'error': None,
                        'finishedAt': '2017-08-09T21:02:41.000Z',
                        'id': 1200,
                        'startedAt': '2017-08-09T21:01:56.000Z',
                        'state': 'succeeded'}),
                   'name': 'iris',
                   'owner': 'jsmith',
                   'refresh_id': 2200,
                   'refresh_status': 'current',
                   'row_count': 150,
                   'schema': 'datascience',
                   'size_mb': 558,
                   'sortkeys': 'index'}}
_tables_g = copy.deepcopy(_tables_l)  # GET responses have more info than LIST
_tables_g[200].update({'enhancements': [],
                       'joins': [],
                       'multipart_key': [],
                       'ontology_mapping': Response({}),
                       'outgoing_table_matches': [],
                       'table_def': 'CREATE TABLE "datascience"."iris"',
                       })
_tables_g[200]['columns'] = [
    Response({'name': 'sepal_length', 'sql_type': 'double precision'}),
    Response({'name': 'sepal_width', 'sql_type': 'double precision'}),
    Response({'name': 'petal_length', 'sql_type': 'double precision'}),
    Response({'name': 'petal_width', 'sql_type': 'double precision'}),
    Response({'name': 'type', 'sql_type': 'character varying(999)'}),
    Response({'name': 'index', 'sql_type': 'bigint'})
]
tables_g = copy.deepcopy(_tables_g)
tables_l = copy.deepcopy(_tables_l)


def _get_resource(res, _id):
    if _id in res:
        return res[_id]
    else:
        raise CivisAPIError(Response(
            {'status_code': 404,
             'reason': 'The requested resource could not be found.',
             'content': None}))


def _list_resource(res, *args, iterator=False, **kwargs):
    if iterator:
        return _list_iterator(res.values())
    else:
        return list(res.values())


def _list_iterator(res):
    for v in res:
        yield v


def insert_table(tb):
    """Set table metadata for the client mocks to find

    Parameters
    ----------
    tb : dictionary
    """
    tb = copy.deepcopy(tb)
    if 'id' not in tb:
        tb['id'] = max(tables_g) + 1
    tb.setdefault('schema', 'scratch')
    tb.setdefault('owner', 'jsmith')
    if 'columns' in tb:
        tb.setdefault('column_count', len(tb['columns']))
    tb.setdefault('row_count', 1001)
    tables_g[tb['id']] = Response(tb)

    # Remove keys not in the list view
    tb_l = copy.deepcopy(tb)
    for key in ['enhancements', 'joins', 'multipart_key', 'ontology_mapping',
                'outgoing_table_matches', 'table_def', 'columns']:
        tb_l.pop(key, None)
    tables_l[tb_l['id']] = Response(tb_l)


def create_client_mock(api_key=None, return_type='snake',
                       retry_total=6, api_version="1.0", resources="base",
                       local_api_spec=CACHED_SPEC_PATH,
                       reset=False, setup=True):
    """Create an APIClient mock from a cache of the API spec

    Parameters
    ----------
    api_key : str, optional
        IGNORED; provided to match signature of the APIClient
    return_type : str, optional
        IGNORED; provided to match signature of the APIClient
    retry_total : int, optional
        IGNORED; provided to match signature of the APIClient
    api_version : string, optional
        Mock this version of the API endpoints.
    resources : string, optional
        When set to "base", only the default endpoints will be exposed in the
        mock object. Set to "all" to include all endpoints available in
        your API cache, including those that may be in development and subject
        to breaking changes at a later date.
    local_api_spec : str, optional
        Location of the API spec on the local filesystem
    reset : bool, optional
        If False, the new mock will share underlying object dictionaries
        with all mocks created since the last reset. If True, the new
        mock will start with a fresh set of dictionaries.
    setup : bool, optional
        If True, the returned mock will provide sensible responses
        for many of the mocked endpoint calls.
    **kwargs:
        Absorbs additional keyword arguments which

    Returns
    -------
    mock.NonCallableMagicMock
        A `Mock` object which looks like an APIClient and which will
        error if any method calls have non-existent / misspelled parameters
    """
    if not local_api_spec:
        raise ValueError("Provide a cache of the API spec when creating "
                         "a mock for testing.")

    # Create a client from the cache. We'll use this for
    # auto-speccing. Prevent it from trying to talk to the real API.
    with mock.patch('requests.Session', mock.MagicMock):
        real_client = APIClient(local_api_spec=local_api_spec, api_key='none',
                                resources=resources, api_version=api_version,
                                return_type=return_type)
    real_client._feature_flags = {'noflag': None}
    if hasattr(real_client, 'channels'):
        # Deleting "channels" causes the client to fall back on
        # regular polling for completion, which greatly eases testing.
        delattr(real_client, 'channels')

    if reset:
        reset_mock_db()
    mock_client = mock.create_autospec(real_client, spec_set=True)

    if setup:
        _set_users(mock_client)
        _set_credentials(mock_client)
        _set_files(mock_client)
        _set_tables(mock_client)

    return mock_client


def mock_file_to_civis(buf, name, client=None, tempdir=None, **kwargs):
    """Mock version of civis.io.file_to_civis, suitable for testing

    If `tempdir` is provided, the buffer will be written to a file
    "[name]_[id]" in the temporary directory. Otherwise the buffer
    will be ignored. Either way, you get back the new file ID.
    """
    if client is None:
        client = create_client_mock()
    file_response = client.files.post(name, **kwargs)

    if tempdir:
        file_url = os.path.join(tempdir, "%s_%s" % (name, file_response.id))
        file_response['file_url'] = file_url
        with open(file_url, 'wb') as _fout:
            _fout.write(buf)

    return file_response.id


def mock_civis_to_file(file_id, buf, client=None):
    """Mock version of civis.io.civis_to_file, suitable for testing

    This function assumes it's reading files which were written by
    `mock_file_to_civis`. The file should be in binary format at
    the location given by the "file_url" in the GET /file/{id} Response.
    """
    if client is None:
        client = create_client_mock()
    file_response = client.files.get(file_id)
    with open(file_response.file_url, 'rb') as _fin:
        buf.write(_fin.read())


def reset_mock_db():
    """Reset the state of the module-level dictionaries

    This will not affect any existing client mocks, but it
    will allow new client mocks to start from a fresh state.
    """
    global users
    users = copy.deepcopy(_users)

    global credentials
    credentials = copy.deepcopy(_credentials)

    global files
    files = copy.deepcopy(_files)

    global tables_g
    global tables_l
    tables_g = copy.deepcopy(_tables_g)
    tables_l = copy.deepcopy(_tables_l)


def _set_users(mock_client):
    """Setup for the `users` endpoint (and associated properties)

    Mock returns for
        users.list
        users.list_me
        users.get
        client.username
        client.feature_flags
    """
    mock_client.users.list.side_effect = partial(_list_resource, users)
    mock_client.users.get.side_effect = partial(_get_resource, users)
    mock_client.users.list_me.side_effect = partial(_get_resource, users, 7)

    def get_uname():
        return mock_client.users.list_me()['username']
    type(mock_client).username = mock.PropertyMock(side_effect=get_uname)

    def get_ffs():
        return tuple(f for f, v in
                     mock_client.users.list_me()['feature_flags'].items() if v)
    type(mock_client).feature_flags = mock.PropertyMock(side_effect=get_ffs)


def _set_credentials(mock_client):
    """Setup for the `credentials` endpoint (and associated properties)

    Mock returns for
        credentials.list
        credentials.get
        client.default_credential
    """
    creds = credentials
    mock_client.credentials.get.side_effect = partial(_get_resource, creds)
    mock_client.credentials.list.side_effect = partial(_list_resource, creds)

    def get_d():
        return creds.get(1)
    type(mock_client).default_credential = mock.PropertyMock(side_effect=get_d)


def _set_files(mock_client):
    """Setup for the `files` endpoint

    Mock returns for
        files.get
        files.post
    """
    mock_client.files.get.side_effect = partial(_get_resource, files)

    def _post(name, *args, expires_at=-1, **kwargs):
        new_id = max(files) + 1
        now = datetime.utcnow()
        if expires_at == -1:
            expires = (now + timedelta(days=30)).isoformat() + 'Z'
        elif expires_at is None:
            expires = None
        else:
            expires = dateutil.parser.parse(expires_at).isoformat() + 'Z'
        reply = {'created_at': now.isoformat() + 'Z',
                 'expires_at': expires,
                 'file_size': 0,
                 'id': new_id,
                 'name': name,
                 'upload_fields': {
                     'key': name,
                     'policy': 'abc'},
                 'upload_url': 'localhost:65000/upload'}
        stored = reply.copy()
        stored.update({'download_url': _files[100]['download_url'].copy(),
                       'file_url': "%s_%s" % (name, new_id),
                       'file_size': 5011})
        stored['download_url']['path'] = stored['file_url']
        del stored['upload_fields']
        del stored['upload_url']
        files[new_id] = stored

        return Response(reply)
    mock_client.files.post.side_effect = _post


def _set_tables(mock_client):
    """Setup for the `tables` endpoint

    Mock returns for
        tables.get
        tables.list
        tables.list_columns
        tables.patch
    """
    mock_client.tables.get.side_effect = partial(_get_resource, tables_g)
    mock_client.tables.list.side_effect = partial(_list_resource, tables_l)

    def _list_columns(id, name=None, *args, iterator=False, **kwargs):
        cols = _get_resource(tables_g, id)['columns']
        if name:
            cols = find(cols, name=name)
        if iterator:
            return _list_iterator(cols)
        else:
            return cols
    mock_client.tables.list_columns.side_effect = _list_columns

    def _patch(id, ontology_mapping=-1, description=-1):
        tb = _get_resource(tables_g, id)
        if ontology_mapping != -1:
            tb['ontology_mapping'] = ontology_mapping
        if description != -1:
            tb['description'] = description
    mock_client.tables.patch.side_effect = _patch
