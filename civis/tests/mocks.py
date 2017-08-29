"""Mock client creation and tooling

Use the `create_client_mock` to get an autospecced APIClient
with a few of the more used / easier-to-mock endpoints mocked.
Modify the `side_effects` or `return_value` as needed for your specific test.

Keep "databases" of objects as module-level dictionaries.
Access them with side_effects rather than return_value so that they
can change during testing.
"""
import copy
from functools import partial

from civis import APIClient
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

_credentials = {1: Response({'created_at': '2014-02-22T22:12:00.000Z',
                             'description': None,
                             'id': 1,
                             'name': 'jsmith',
                             'owner': 'jsmith',
                             'remote_host_id': None,
                             'remote_host_name': None,
                             'type': 'Database',
                             'updated_at': '2016-11-11T11:13:03.000Z',
                             'username': 'jsmith'}),
                }
credentials = copy.deepcopy(_credentials)


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
        return _list_iterator(res)
    else:
        return list(res.values())


def _list_iterator(res):
    for v in res.values():
        yield v


def create_client_mock(cache=CACHED_SPEC_PATH, reset=False, setup=True):
    """Create an APIClient mock from a cache of the API spec

    Parameters
    ----------
    cache : str, optional
        Location of the API spec on the local filesystem
    reset : bool, optional
        If False, the new mock will share underlying object dictionaries
        with all mocks created since the last reset. If True, the new
        mock will start with a fresh set of dictionaries.
    setup : bool, optional
        If True, the returned mock will provide sensible responses
        for many of the mocked endpoint calls.

    Returns
    -------
    mock.NonCallableMagicMock
        A `Mock` object which looks like an APIClient and which will
        error if any method calls have non-existent / misspelled parameters
    """
    # Create a client from the cache. We'll use this for
    # auto-speccing. Prevent it from trying to talk to the real API.
    with mock.patch('requests.Session', mock.MagicMock):
        real_client = APIClient(local_api_spec=cache, api_key='none',
                                resources='all')
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

    return mock_client


def reset_mock_db():
    """Reset the state of the module-level dictionaries

    This will not affect any existing client mocks, but it
    will allow new client mocks to start from a fresh state.
    """
    global users
    users = copy.deepcopy(_users)

    global credentials
    credentials = copy.deepcopy(_credentials)


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
