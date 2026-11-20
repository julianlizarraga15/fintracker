from collections import OrderedDict, deque
from threading import Lock
from time import monotonic

ATTEMPT_WINDOW_SECONDS = 60
MAX_LOGIN_ATTEMPTS = 10
MAX_TRACKED_CLIENTS = 4096

_attempts_by_client: OrderedDict[str, deque[float]] = OrderedDict()
_lock = Lock()


def allow_login_attempt(client_key: str) -> bool:
    with _lock:
        now = monotonic()
        attempts = _attempts_by_client.get(client_key)
        if attempts is None:
            attempts = deque()
            _attempts_by_client[client_key] = attempts
        else:
            _attempts_by_client.move_to_end(client_key)
        while attempts and now - attempts[0] >= ATTEMPT_WINDOW_SECONDS:
            attempts.popleft()
        if len(attempts) >= MAX_LOGIN_ATTEMPTS:
            return False
        attempts.append(now)
        while len(_attempts_by_client) > MAX_TRACKED_CLIENTS:
            _attempts_by_client.popitem(last=False)
        return True
