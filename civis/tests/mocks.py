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
import io
import json
import os

import dateutil.parser
from six import BytesIO

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

run_final_state = 'succeeded'  # Change runs to this status when they complete
_containers_runs = {1000: Response(
    {'container_id': 300,
     'error': None,
     'finished_at': '2017-08-15T20:39:55.000Z',
     'id': 1000,
     'is_cancel_requested': False,
     'started_at': '2017-08-15T20:38:49.000Z',
     'state': 'running'})
}
_containers_runs_state = {1000: {'final_state': run_final_state,
                                 'calls_remaining': 2}}
_containers_runs_outputs = {}
_containers = {300: Response(
    {'archived': False,
     'arguments': {'SPAM': 'eggs'},
     'cancel_timeout': 0,
     'category': 'script',
     'created_at': '2017-08-15T20:38:48.000Z',
     'docker_command': "python -c \"print(os.environ['SPAM'])\"",
     'docker_image_name': 'civisanalytics/datascience-python',
     'docker_image_tag': '3.1.0',
     'finished_at': '2017-08-15T20:39:55.000Z',
     'from_template_id': None,
     'git_credential_id': None,
     'hidden': False,
     'id': 300,
     'is_template': False,
     'last_run': _containers_runs[1000],
     'name': 'Spam mockery script',
     'params': [{'name': 'SPAM', 'required': True}],
     'projects': [],
     'published_as_template_id': None,
     'remote_host_credential_id': None,
     'repo_http_uri': None,
     'repo_ref': None,
     'required_resources': {'cpu': 256, 'diskSpace': 2.0, 'memory': 512},
     'state': 'succeeded',  # This may be inconsistent with the run.
     'target_project_id': None,
     'template_dependents_count': None,
     'template_script_name': None,
     'time_zone': 'America/Chicago',
     'type': 'Container',
     'updated_at': '2017-08-15T20:38:48.000Z',
     'user_context': 'runner'
     })}
containers = copy.deepcopy(_containers)
containers_runs = copy.deepcopy(_containers_runs)
containers_runs_outputs = copy.deepcopy(_containers_runs_outputs)
c_runs_state = copy.deepcopy(_containers_runs_state)

_workers_l = {}
_workers_g = {}
workers_l = copy.deepcopy(_workers_l)
workers_g = copy.deepcopy(_workers_g)


def _get_resource(res, id, **filter_args):
    if id in res and all(res[id][k] == v for k, v in filter_args.items()):
        return res[id]
    else:
        raise CivisAPIError(Response(
            {'status_code': 404,
             'reason': 'The requested resource could not be found.',
             'content': None}))


def _get_cont_run(id, run_id, type='Container'):
    run = _get_resource(containers_runs, run_id)
    id_name = '%s_id' % type.lower()
    cont = _get_resource(containers, run[id_name], type=type)
    if run[id_name] != id:
        raise CivisAPIError(Response(
            {'status_code': 404,
             'reason': 'The requested resource could not be found.',
             'content': None}))
    if c_runs_state[run_id]['calls_remaining'] > 0:
        c_runs_state[run_id]['calls_remaining'] -= 1
        if c_runs_state[run_id]['calls_remaining'] <= 0:
            run['state'] = c_runs_state[run_id]['final_state']
            run['finished_at'] = datetime.utcnow().isoformat() + "Z"
            cont['state'] = c_runs_state[run_id]['final_state']
            cont['finished_at'] = run['finished_at']
    return run


def _post_resource(res, sig, *args, _extra=None, **kwargs):
    new_id = max(res) + 1
    obj = sig.bind(*args, **kwargs).arguments
    obj.update(_extra or {})
    obj['id'] = new_id
    res[new_id] = Response(obj)
    return res[new_id]


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


def insert_clusters(**params):
    """Set cluster info for the mock clients to find

    Use a default cluster definition. Input keyword arguments
    override defaults.
    """
    # Initialize the list response with defaults
    _id = 1 if not workers_l else max(workers_l) + 1
    worker = Response({'active_jobs_count': 5,
                       'id': _id,
                       'instance_type': 'm4.2xlarge',
                       'max_instances': 100,
                       'min_instances': 1,
                       'queued_jobs_count': 0,
                       'region': 'us-east-1'})

    # Update the worker with the inputs, but only if they exist already.
    for k, v in params.items():
        if k in worker:
            worker[k] = v

    worker_g = copy.deepcopy(worker)
    worker_g.update({'instance_max_cpu': 1024,
                     'instance_max_disk_space': 10.0,
                     'instance_max_memory': 9000,
                     'instances': 7})
    for k, v in params.items():
        if k in worker_g:
            worker_g[k] = v

    workers_l[_id] = worker
    workers_g[_id] = worker_g


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
        _set_clusters(mock_client)
        _set_scripts(mock_client)
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

    If `tempdir` is not provided, it will default to the value in
    the environment variable "CIVIS_TEST_TMPDIR".
    """
    if client is None:
        client = create_client_mock()
    file_response = client.files.post(name, **kwargs)

    tempdir = tempdir or os.environ.get('CIVIS_TEST_TMPDIR', '')
    file_url = os.path.join(tempdir, "%s_%s" % (name, file_response.id))
    file_response['file_url'] = file_url
    file_response.update(
        {'file_url': file_url,
         'file_size': 5011,
         'download_url': _files[100]['download_url'].copy()})
    file_response = Response(file_response)  # Put new keys in the Response
    file_response['download_url']['path'] = file_response['file_url']
    files[file_response.id] = file_response  # Update DB with new response

    if tempdir:
        with open(file_url, 'wb') as _fout:
            _fout.write(buf.read())

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


def mock_file_to_json(file_id, client=None, **json_kwargs):
    """Mock version of civis.io.file_to_json"""
    buf = BytesIO()
    mock_civis_to_file(file_id, buf, client=client)
    txt = io.TextIOWrapper(buf, encoding='utf-8')
    txt.seek(0)
    return json.load(txt, **json_kwargs)


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

    global containers
    global containers_runs
    global containers_runs_outputs
    global c_runs_state
    containers = copy.deepcopy(_containers)
    containers_runs = copy.deepcopy(_containers_runs)
    containers_runs_outputs = copy.deepcopy(_containers_runs_outputs)
    c_runs_state = copy.deepcopy(_containers_runs_state)

    global workers_g
    global workers_l
    workers_g = copy.deepcopy(_workers_g)
    workers_l = copy.deepcopy(_workers_l)


def _set_clusters(mock_client):
    """Setup mocks for the `clusters` endpoint

    Mock returns for:
        clusters.get_workers
        clusters.list_workers
    """
    if not hasattr(mock_client, 'clusters'):
        return
    c = mock_client.clusters
    c.list_workers.side_effect = partial(_list_resource, workers_l)
    c.get_workers.side_effect = partial(_get_resource, workers_g)


def _set_scripts(mock_client):
    """Setup for the `scripts` endpoint

    Mock returns for
        scripts.get_containers
        scripts.post_containers
        scripts.get_containers_runs
        scripts.post_containers_runs
        scripts.list_containers_runs
        scripts.list_containers_runs_outputs
        scripts.post_containers_runs_outputs
        scripts.get_sql
        scripts.post_sql
        scripts.get_sql_runs
        scripts.post_sql_runs
        scripts.list_sql_runs
        scripts.post_cancel
    """
    s = mock_client.scripts
    s.get_containers.side_effect = partial(_get_resource, containers,
                                           type='Container')
    s.get_containers_runs.side_effect = partial(_get_cont_run,
                                                type='Container')
    s.post_containers.side_effect = partial(_post_resource, containers,
                                            s.post_containers._spec_signature,
                                            _extra={'type': 'Container'})

    def _post_run(id, type='Container'):
        cont = _get_resource(containers, id, type=type)
        run = Response(
            {'container_id': id,
             'error': None,
             'finished_at': None,
             'id': max(containers_runs) + 1,
             'is_cancel_requested': False,
             'started_at': datetime.utcnow().isoformat() + "Z",
             'state': 'running'
             })
        cont['state'] = 'running'
        cont['last_run'] = run
        c_runs_state[run.id] = {'final_state': run_final_state,
                                'calls_remaining': 2}
        containers_runs[run.id] = run
        return run
    s.post_containers_runs.side_effect = partial(_post_run, type='Container')

    def _list_runs(id, *args, iterator=False, type='Container', **kwargs):
        filter_args = {('%s_id' % type.lower()): id}
        runs = find(containers_runs.values(), **filter_args)
        if iterator:
            return _list_iterator(runs)
        else:
            return runs
    s.list_containers_runs.side_effect = partial(_list_runs, type='Container')

    def _post_output(id, run_id, object_type, object_id):
        _get_cont_run(id, run_id, type='Container')  # Trigger 404s
        endpoint = object_type.lower() + 's'
        name = getattr(mock_client, endpoint).get(object_id).name
        link = 'api.civisanalytics.com/%s/%s' % (endpoint, object_id)
        output = Response({'object_id': object_id, 'object_type': object_type,
                           'link': link, 'name': name})
        containers_runs_outputs.setdefault(run_id, []).append(output)
        return output
    s.post_containers_runs_outputs.side_effect = _post_output

    def _list_outputs(id, run_id, *args, iterator=False, **kwargs):
        mock_client.scripts.get_containers_runs(id, run_id)  # Triggers errors
        outputs = containers_runs_outputs.setdefault(run_id, [])
        if iterator:
            return _list_iterator(outputs)
        else:
            return outputs
    s.list_containers_runs_outputs.side_effect = _list_outputs

    # SQL scripts
    s.get_sql.side_effect = partial(_get_resource, containers, type='SQL')
    s.get_sql_runs.side_effect = partial(_get_cont_run, type='SQL')
    s.post_sql.side_effect = partial(_post_resource, containers,
                                     s.post_sql._spec_signature,
                                     _extra={'type': 'SQL'})
    s.post_sql_runs.side_effect = partial(_post_run, type='SQL')
    s.list_sql_runs.side_effect = partial(_list_runs, type='SQL')

    def _post_cancel(id):
        cont = _get_resource(containers, id)
        run = _get_cont_run(id, cont.get('last_run', {}).get('id'))
        c_runs_state[run.id]['calls_remaining'] = 0
        run['state'] = 'cancelled'
        run['finished_at'] = datetime.utcnow().isoformat() + "Z"
        run['is_cancel_requested'] = True
        cont['state'] = 'cancelled'
        cont['finished_at'] = run['finished_at']
        return Response({'id': run.id, 'is_cancel_requested': True,
                         'state': 'cancelled'})
    s.post_cancel.side_effect = _post_cancel


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
        stored.update({'download_url': None,
                       'file_url': None,
                       'file_size': 0})
        del stored['upload_fields']
        del stored['upload_url']
        files[new_id] = Response(stored)

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
