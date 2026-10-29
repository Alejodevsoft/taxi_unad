"""Conector y dependencias Maven fijados, descargados una vez durante build."""
import hashlib
import json
from pathlib import Path
from urllib.request import urlopen

COORDS = [
 ('org.apache.spark', 'spark-sql-kafka-0-10_2.12', '3.5.6'),
 ('org.apache.spark', 'spark-token-provider-kafka-0-10_2.12', '3.5.6'),
 ('org.apache.kafka', 'kafka-clients', '3.4.1'),
 ('org.apache.commons', 'commons-pool2', '2.11.1'),
 ('com.google.code.findbugs', 'jsr305', '3.0.0')]

if __name__ == '__main__':
    dest = Path(__file__).resolve().parents[1] / 'jars'
    dest.mkdir(exist_ok=True)
    manifest = []
    for group, artifact, version in COORDS:
        filename = f'{artifact}-{version}.jar'
        url = f'https://repo.maven.apache.org/maven2/{group.replace(".", "/")}/{artifact}/{version}/{filename}'
        path = dest / filename
        if not path.exists():
            with urlopen(url, timeout=120) as response:
                path.write_bytes(response.read())
        manifest.append(dict(url=url, sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    (dest/'manifest.json').write_text(json.dumps(manifest, indent=2))
