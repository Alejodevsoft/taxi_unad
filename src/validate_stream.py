"""Prueba integral contra broker REAL. No borra topic, base ni checkpoints."""
import json
import subprocess
import sys
import time
from datetime import datetime,timezone
from urllib.request import urlopen
from common import EVIDENCE,ROOT,save_json
from producer import produce
from store import snapshot

def wait_for(predicate,process,timeout=180):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        if process.poll() is not None:
            raise RuntimeError('Spark terminó: consultar evidence/stream_test.log')
        s=snapshot()
        if predicate(s): return s
        time.sleep(2)
    raise TimeoutError('No se alcanzó el estado esperado del flujo')

def start(log):
    return subprocess.Popen([sys.executable,str(ROOT/'src'/'streaming.py')],stdout=log,stderr=subprocess.STDOUT)

def stop(p):
    if p.poll() is None:
        p.terminate()
        try: p.wait(timeout=60)
        except subprocess.TimeoutExpired:
            p.kill(); p.wait(); raise RuntimeError('Spark no detuvo sus consultas a tiempo')

def run():
    result={'started_at':datetime.now(timezone.utc).isoformat(),'status':'running'}
    base=snapshot(); base_count=base['counts']['messages']
    result['baseline']=base
    with (EVIDENCE/'stream_test.log').open('a') as log:
        p=start(log)
        try:
            wait_for(lambda s:len(s['batches'])==2,p)
            a=produce(count=150,rate=2,invalid=True)
            s=wait_for(lambda s:s['counts']['messages']>=base_count+151,p)
            # Esperar que la consulta de ventanas alcance todos los eventos válidos.
            expected=base['aggregate_trips']+150
            s=wait_for(lambda s:s['aggregate_trips']>=expected,p)
            save_json(EVIDENCE/'stream_active_snapshot.json',s)
            assert s['counts']['valid']==base['counts']['valid']+150,s
            assert s['counts']['invalid']==base['counts']['invalid']+1,s
            assert any(a['run_id'] in (r['event_id'] or '') for r in s['invalid'])
            time.sleep(10)
            paused=snapshot()
            assert paused['counts']==s['counts']
            assert paused['aggregate_trips']==s['aggregate_trips']
            result['pause_stable']=True
            stop(p)
            save_json(EVIDENCE/'stream_before_restart.json',paused)
            p=start(log)
            # Cambia runId, se conserva query id y batch ids avanzan.
            old_batch={r['query_name']:r['batch_id'] for r in paused['batches']}
            time.sleep(15)
            assert snapshot()['counts']==paused['counts']
            b=produce(count=40,rate=4)
            final=wait_for(lambda s:s['counts']['messages']==base_count+191,p)
            final=wait_for(lambda s:s['aggregate_trips']==expected+40,p)
            assert final['counts']['valid']==base['counts']['valid']+190
            assert all(r['batch_id']>old_batch[r['query_name']] for r in final['batches'])
            save_json(EVIDENCE/'stream_after_restart.json',final)
            # Verifica lo que consume el panel, servido en el contenedor app.
            with urlopen('http://app:8000/api',timeout=10) as response:
                panel=json.load(response)
            assert panel['counts']==final['counts']
            assert panel['aggregate_trips']==expected+40
            result.update(status='passed',sent_valid=190,sent_invalid=1,
                restart_preserved_counts=True,panel_api_verified=True,final_counts=final['counts'],
                windows_count=len(final['windows']),producer_runs=[a['run_id'],b['run_id']])
        except Exception as exc:
            result.update(status='failed',error=repr(exc))
            raise
        finally:
            stop(p)
            result['finished_at']=datetime.now(timezone.utc).isoformat()
            save_json(EVIDENCE/'stream_validation.json',result)
    print(json.dumps(result,indent=2))

if __name__=='__main__': run()
