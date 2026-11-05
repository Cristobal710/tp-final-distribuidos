import csv
import hashlib
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import pika

from common.eof import UpstreamTracker
from common.protocol import internal
from common.protocol.articles import Article


phase = sys.argv[1]
output = Path('/test_artifacts') / phase
output.mkdir(exist_ok=True)
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=60))
channel = connection.channel()
channel.confirm_delivery()
for name in ('clasificacion', 'resultados.0'):
    channel.queue_declare(queue=name, durable=True)
clients = {}
started = time.monotonic()

for subset in ('subset_a', 'subset_b'):
    client = f'{phase}_{subset}'
    directory = Path('/repo/datasets') / subset
    rows = json.loads((directory / 'articles.json').read_text())
    articles = []
    for row in rows:
        for key in ('date_created', 'date_modified'):
            if row[key] is not None:
                row[key] = datetime.fromisoformat(row[key])
        if row['sections'] is not None:
            row['sections'] = json.dumps(row['sections'], ensure_ascii=False)
        articles.append(Article(*(row[key] for key in Article._fields)))
    with (directory / 'expected/q2.csv').open(newline='') as file:
        image_rows = list(csv.DictReader(file))
        expected = [(row['name'], row['url']) for row in image_rows]
    clients[client] = {'subset': subset, 'input_rows': len(articles), 'expected': expected, 'actual': [],
                       'image_urls': {row['url']: row['image_url'] for row in image_rows}}
    batches = [articles[index:index + 2] for index in range(0, len(articles), 2)]
    for batch in batches:
        channel.basic_publish('', 'clasificacion', internal.encode_articles(client, 0, batch),
                              pika.BasicProperties(delivery_mode=2), mandatory=True)
    channel.basic_publish('', 'clasificacion',
                          internal.encode_eof(client, 0, 'gateway_0', len(batches), len(batches), len(batches)),
                          pika.BasicProperties(delivery_mode=2), mandatory=True)
    print(f'{phase}: sent {len(articles)} reduced rows for {subset}; expected {len(expected)} Q2 images', flush=True)

tracker = UpstreamTracker(2, check_total=True, check_received=True)
deadline = started + int(os.environ.get('Q2_TEST_TIMEOUT_SECONDS', '900'))
while not all(tracker.is_complete(client) for client in clients) and time.monotonic() < deadline:
    method, properties, body = channel.basic_get('resultados.0', auto_ack=False)
    if method is None:
        connection.sleep(0.1)
        continue
    kind, value = internal.decode(body)
    client = value.get('client_id')
    if value.get('query') == 2 and client in clients:
        if kind == internal.MsgType.RESULTS:
            tracker.add_message(client)
            for name, url, values in value['rows']:
                data = bytes(values)
                if not data:
                    raise RuntimeError(f'Invalid image payload for {url}')
                digest = hashlib.sha256(data).hexdigest()
                (output / f'{digest}.bin').write_bytes(data)
                clients[client]['actual'].append([name, url, digest, len(data)])
                print(f'{phase}: {name}: {len(data)} original image bytes', flush=True)
        elif kind == internal.MsgType.EOF:
            tracker.add_eof(value)
        elif kind == internal.MsgType.LATE_EOF:
            tracker.add_late_eof(value)
    channel.basic_ack(method.delivery_tag)

passed = True
for client, state in clients.items():
    expected = Counter(tuple(row) for row in state['expected'])
    actual = Counter(tuple(row[:2]) for row in state['actual'])
    state['complete'] = tracker.is_complete(client)
    state['missing'] = list((expected - actual).elements())
    state['extra'] = list((actual - expected).elements())
    state['passed'] = state['complete'] and expected == actual
    passed = passed and state['passed']
    (output / f"{state['subset']}-q2.jsonl").write_text(''.join(json.dumps(row, ensure_ascii=False) + '\n'
                                                            for row in state['actual']))
    print(f"{phase}: {state['subset']}: {'PASS' if state['passed'] else 'FAIL'} "
          f"({len(state['actual'])}/{len(state['expected'])} images; complete={state['complete']})", flush=True)
report = {'phase': phase, 'passed': passed, 'elapsed_seconds': time.monotonic() - started, 'clients': clients}
(output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
connection.close()
sys.exit(0 if passed else 1)
