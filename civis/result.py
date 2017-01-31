from civis import APIClient
from civis.pubnub import has_pubnub, SubscribableResult
from civis.polling import PollableResult


def PlatformFuture(poller, poller_args,
                   polling_interval=None,
                   api_key=None):
    client = APIClient(api_key=api_key, resources='all')
    if 'pubnub' in client.feature_flags and has_pubnub:
        klass = SubscribableResult
    else:
        klass = PollableResult
    return klass(poller=poller,
                 poller_args=poller_args,
                 polling_interval=polling_interval,
                 api_key=api_key)
