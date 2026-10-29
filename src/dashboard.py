"""Panel local sin CDN. Sólo expone resultados agregados y conteos."""
import json
import csv
from http.server import ThreadingHTTPServer,SimpleHTTPRequestHandler
from urllib.parse import urlparse
from common import OUT,DATA
from store import snapshot

class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        route=urlparse(self.path).path
        if route=='/api':
            result=snapshot()
            for name in ['quality','trips_by_day','trips_by_hour','top_pickup_zones','duration_by_hour','statistics']:
                path=OUT/f'{name}.json'
                result[name]=json.loads(path.read_text()) if path.exists() else None
            zones=DATA/'raw'/'taxi_zone_lookup.csv'
            if zones.exists():
                with zones.open(encoding='utf-8-sig') as f:
                    result['zones']={r['LocationID']:r['Zone'] for r in csv.DictReader(f)}
            payload=json.dumps(result,ensure_ascii=False).encode()
            self.send_response(200);self.send_header('Content-Type','application/json; charset=utf-8')
            self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(payload)
        elif route=='/':
            payload=(__import__('pathlib').Path(__file__).parent/'dashboard.html').read_bytes()
            self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.end_headers();self.wfile.write(payload)
        else:
            self.send_error(404)

if __name__=='__main__':
    print('Panel local: http://localhost:8000',flush=True)
    ThreadingHTTPServer(('0.0.0.0',8000),Handler).serve_forever()
