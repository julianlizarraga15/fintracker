from pathlib import Path

HEADER_LINES = (
    'add_header Strict-Transport-Security "max-age=15552000" always;',
    'add_header X-Content-Type-Options "nosniff" always;',
    'add_header X-Frame-Options "DENY" always;',
    'add_header Content-Security-Policy "frame-ancestors \'none\'" always;',
)
DEFAULT_SERVER_MARKER = "listen 443 ssl default_server;"


def _nginx_conf() -> Path:
    candidates = (
        Path("nginx.conf"),
        Path("/app/nginx.conf"),
        Path(__file__).resolve().parents[2] / "nginx.conf",
    )
    return next(path for path in candidates if path.is_file())


def test_fintracker_https_server_sets_browser_headers():
    text = _nginx_conf().read_text(encoding="utf-8")
    before, marker, after = text.partition(DEFAULT_SERVER_MARKER)
    assert marker
    for line in HEADER_LINES:
        assert line in after
        assert line not in before
