from __future__ import absolute_import

from civis.io._databases import query_civis, transfer_table  # NOQA
from civis.io._files import *  # NOQA
from civis.io._tables import *  # NOQA

import os as _os
if _os.environ.get('CIVIS_API_KEY') == 'TEST':
    # If we're testing, replace all of the file interactions with their
    # mocked (local) versions.
    from civis.tests.mocks import mock_civis_to_file as civis_to_file  # NOQA
    from civis.tests.mocks import mock_file_to_civis as file_to_civis  # NOQA
    from civis.tests.mocks import mock_file_to_json as file_to_json  # NOQA
