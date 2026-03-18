from urllib.parse import urlparse, urlunparse


def redact_url(url: str) -> str:
    """Return the URL with the password replaced by '***'."""
    parsed = urlparse(url)
    if parsed.password:
        netloc = f"{parsed.username or ''}:***@{parsed.hostname}"
        if parsed.port:
            netloc = f"{netloc}:{parsed.port}"
        return urlunparse(parsed._replace(netloc=netloc))
    return url
