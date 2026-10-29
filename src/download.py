import hashlib
import json
from datetime import datetime, timezone
from urllib.request import urlopen
from common import DATA, OUT, save_json

SOURCES = {
 'yellow_tripdata_2025-01.parquet': 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet',
 'taxi_zone_lookup.csv': 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
}

def download():
    manifest_path = DATA/'raw'/'manifest.json'
    old = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    manifest = {}
    for name, url in SOURCES.items():
        path = DATA/'raw'/name
        reused = path.exists()
        headers = {}
        if not reused:
            temp = path.with_suffix(path.suffix+'.part')
            with urlopen(url, timeout=180) as response, temp.open('wb') as target:
                headers = {k:response.headers.get(k) for k in ['Last-Modified','ETag','Content-Length']}
                while chunk := response.read(1024*1024):
                    target.write(chunk)
            temp.replace(path)
        digest = hashlib.file_digest(path.open('rb'), 'sha256').hexdigest()
        if name in old and digest != old[name]['sha256']:
            raise ValueError(f'El original cambió: {name}')
        manifest[name] = dict(url=url, bytes=path.stat().st_size, sha256=digest,
            downloaded_at=old.get(name, {}).get('downloaded_at', datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()),
            reused=reused, headers=old.get(name, {}).get('headers',headers))
        print(name, manifest[name], flush=True)
        save_json(manifest_path,{**old,**manifest})
    save_json(manifest_path, manifest)
    save_json(OUT/'sources.json', manifest)
    return manifest

if __name__ == '__main__':
    download()
