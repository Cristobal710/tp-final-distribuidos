import json
import os
import time
import urllib.error
import urllib.request


original = urllib.request.urlopen
trace_path = os.environ.get('Q2_HTTP_TRACE')


def record(value):
    with open(trace_path, 'a', encoding='utf-8') as file:
        file.write(json.dumps(value, ensure_ascii=False) + '\n')


def traced_urlopen(request, *args, **kwargs):
    url = request.full_url if hasattr(request, 'full_url') else str(request)
    record({'event': 'start', 'url': url, 'monotonic': time.monotonic()})
    try:
        response = original(request, *args, **kwargs)
    except urllib.error.HTTPError as error:
        record({'event': 'error', 'url': url, 'status': error.code, 'reason': str(error.reason),
                'retry_after': error.headers.get('Retry-After'),
                'body': error.read(1024).decode('utf-8', errors='replace')})
        raise
    except Exception as error:
        record({'event': 'error', 'url': url, 'status': None, 'reason': str(error)})
        raise
    record({'event': 'response', 'url': url, 'status': response.status,
            'content_type': response.headers.get('Content-Type'),
            'content_length': response.headers.get('Content-Length')})
    return response


if trace_path:
    urllib.request.urlopen = traced_urlopen
