"""SIMULACIÓN: viajes históricos reales emitidos con reloj actual UTC."""
import argparse
import json
import time
import uuid
from datetime import datetime,timezone
from kafka import KafkaProducer
from common import DATA,EVIDENCE,save_json
from topic import TOPIC,BOOTSTRAP,ensure_topic

def produce(count=300,rate=10,invalid=False,late=False):
    if count<1 or rate<=0: raise ValueError('count y rate deben ser positivos')
    ensure_topic()
    p=KafkaProducer(bootstrap_servers=BOOTSTRAP,acks='all',retries=5,enable_idempotence=True,
        key_serializer=lambda key:str(key).encode(), value_serializer=lambda v:json.dumps(v,allow_nan=False).encode())
    rows=[json.loads(line) for line in (DATA/'replay.jsonl').read_text().splitlines()]
    run_id=str(uuid.uuid4()); emitted=0; invalid_sent=0; receipts=[]
    started=time.monotonic()
    try:
        for i in range(count):
            r=rows[i%len(rows)]
            event=dict(event_id=f'{run_id}:{i}',simulation=True,
              historical_pickup=r['tpep_pickup_datetime'],historical_dropoff=r['tpep_dropoff_datetime'],
              emitted_at=datetime.now(timezone.utc).isoformat(),pickup_zone=r['PULocationID'],dropoff_zone=r['DOLocationID'],
              distance_miles=r['trip_distance'],duration_minutes=r['duration_minutes'],total_amount_usd=r['total_amount'])
            if late and i==count-1:
                from datetime import timedelta
                event['emitted_at']=(datetime.now(timezone.utc)-timedelta(minutes=5)).isoformat()
            receipt=p.send(TOPIC,key=event['pickup_zone'],value=event).get(timeout=30)
            receipts.append(dict(partition=receipt.partition,offset=receipt.offset))
            emitted+=1
            time.sleep(max(0,(i+1)/rate-(time.monotonic()-started)))
        if invalid:
            receipt=p.send(TOPIC,key=0,value={'event_id':run_id+':invalid','duration_minutes':-1}).get(timeout=30)
            receipts.append(dict(partition=receipt.partition,offset=receipt.offset))
            invalid_sent=1
    finally:
        p.flush(); p.close()
        result=dict(run_id=run_id,simulation=True,sent_valid=emitted,sent_invalid=invalid_sent,rate=rate,receipts=receipts)
        save_json(EVIDENCE/f'producer_{run_id}.json',result)
    print({k:v for k,v in result.items() if k!='receipts'},flush=True)
    return result

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--count',type=int,default=300)
    parser.add_argument('--rate',type=float,default=10)
    parser.add_argument('--invalid',action='store_true')
    parser.add_argument('--late',action='store_true')
    args=parser.parse_args(); produce(**vars(args))
