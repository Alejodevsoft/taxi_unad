import json
import signal
import time
from datetime import datetime,timezone
from pyspark.sql import functions as F, types as T
from pyspark.sql.streaming import StreamingQueryListener
from common import DATA, EVIDENCE, spark_session, save_json
from topic import TOPIC,BOOTSTRAP,ensure_topic
from store import connect

SCHEMA=T.StructType([
    T.StructField('event_id',T.StringType()),T.StructField('simulation',T.BooleanType()),
    T.StructField('historical_pickup',T.StringType()),T.StructField('historical_dropoff',T.StringType()),
    T.StructField('emitted_at',T.StringType()),T.StructField('pickup_zone',T.IntegerType()),
    T.StructField('dropoff_zone',T.IntegerType()),T.StructField('distance_miles',T.DoubleType()),
    T.StructField('duration_minutes',T.DoubleType()),T.StructField('total_amount_usd',T.DoubleType()),
    T.StructField('_corrupt_record',T.StringType())])

def parse_messages(raw):
    from common import DATA
    import csv
    with (DATA/'raw'/'taxi_zone_lookup.csv').open(encoding='utf-8-sig') as f:
        ids=[int(r['LocationID']) for r in csv.DictReader(f)]
    parsed=raw.select('topic','partition','offset',F.col('key').cast('string').alias('message_key'),
        F.col('value').cast('string').alias('payload')).withColumn('event',F.from_json('payload',SCHEMA))
    p=parsed.select('*','event.*').drop('event').withColumn('event_time',F.to_timestamp_ltz('emitted_at'))
    pickup=F.to_timestamp_ntz('historical_pickup'); dropoff=F.to_timestamp_ntz('historical_dropoff')
    rules={
      'json_or_schema_invalid': F.col('_corrupt_record').isNotNull(),
      'event_id_missing': F.col('event_id').isNull() | (F.length('event_id')==0),
      'simulation_required': ~F.coalesce(F.col('simulation'),F.lit(False)),
      'emission_invalid': F.col('event_time').isNull() | ~F.col('emitted_at').rlike(r'(Z|\+00:00)$') |
          (F.col('event_time')>F.current_timestamp()+F.expr('INTERVAL 5 SECONDS')),
      'historical_dates_invalid': pickup.isNull() | dropoff.isNull() | (dropoff<=pickup) |
          (pickup<F.lit('2025-01-01')) | (pickup>=F.lit('2025-02-01')) |
          (dropoff<F.lit('2025-01-01')) | (dropoff>=F.lit('2025-02-01')),
      'zone_invalid': F.col('pickup_zone').isNull() | F.col('dropoff_zone').isNull() |
          ~F.col('pickup_zone').isin(ids) | ~F.col('dropoff_zone').isin(ids),
      'key_mismatch': F.col('message_key').isNull() | (F.col('message_key')!=F.col('pickup_zone').cast('string')),
      'distance_invalid': F.col('distance_miles').isNull() | ~F.col('distance_miles').between(0,200) | F.isnan('distance_miles'),
      'duration_invalid': F.col('duration_minutes').isNull() | ~F.col('duration_minutes').between(.000001,1440) |
          F.isnan('duration_minutes'),
      'duration_mismatch': F.abs(F.col('duration_minutes')-
          (dropoff.cast('timestamp_ltz').cast('double')-pickup.cast('timestamp_ltz').cast('double'))/60)>.01,
      'amount_invalid': F.col('total_amount_usd').isNull() | ~F.col('total_amount_usd').between(0,2000) | F.isnan('total_amount_usd')}
    return p.withColumn('reasons',F.filter(F.array(*[
        F.when(F.coalesce(condition,F.lit(False)),F.lit(name)) for name,condition in rules.items()
    ]),lambda x:x.isNotNull()))

def sink(name):
    def write(batch,batch_id):
        db=connect()
        try:
            # Una transacción engloba datos y ledger: no hay commit parcial.
            with db:
                if db.execute('SELECT 1 FROM batches WHERE query_name=? AND batch_id=?',(name,batch_id)).fetchone():
                    return
                now=datetime.now(timezone.utc).isoformat()
                for r in batch.toLocalIterator():
                    if name=='audit':
                        db.execute('INSERT OR IGNORE INTO messages VALUES (?,?,?,?,?,?,?)',
                          (r.topic,r.partition,r.offset,r.event_id,int(len(r.reasons)==0),json.dumps(r.reasons),r.payload))
                    else:
                        db.execute('INSERT INTO windows VALUES (?,?,?,?,?,?,?) ON CONFLICT(start,pickup_zone) DO UPDATE SET '
                          'trips=excluded.trips,avg_duration_minutes=excluded.avg_duration_minutes,'
                          'avg_amount_usd=excluded.avg_amount_usd,updated_at=excluded.updated_at',
                          (str(r.window.start),str(r.window.end),r.pickup_zone,r.trips,r.avg_duration_minutes,r.avg_amount_usd,now))
                db.execute('INSERT INTO batches VALUES (?,?,?)',(name,batch_id,now))
        finally:
            db.close()
    return write

class Progress(StreamingQueryListener):
    def onQueryStarted(self,event):
        with (EVIDENCE/'query_lifecycle.jsonl').open('a') as f:
            f.write(json.dumps(dict(id=str(event.id),run_id=str(event.runId),name=event.name))+'\n')
    def onQueryProgress(self,event):
        with (EVIDENCE/'stream_progress.jsonl').open('a') as f:
            f.write(event.progress.json+'\n')
    def onQueryTerminated(self,event): pass
    def onQueryIdle(self,event): pass

def run():
    ensure_topic(); spark=spark_session('UNAD Taxi Streaming',kafka=True)
    spark.streams.addListener(Progress())
    raw=(spark.readStream.format('kafka').option('kafka.bootstrap.servers',BOOTSTRAP)
        .option('subscribe',TOPIC).option('startingOffsets','earliest')
        .option('failOnDataLoss','true').option('maxOffsetsPerTrigger',10000).load())
    parsed=parse_messages(raw)
    windows=(parsed.filter(F.size('reasons')==0).withWatermark('event_time','30 seconds')
        .groupBy(F.window('event_time','1 minute'),'pickup_zone')
        .agg(F.count('*').alias('trips'),F.avg('duration_minutes').alias('avg_duration_minutes'),
            F.avg('total_amount_usd').alias('avg_amount_usd')))
    queries=[]
    for name,frame,mode in [('audit',parsed,'append'),('windows',windows,'update')]:
        queries.append(frame.writeStream.queryName(name).outputMode(mode).foreachBatch(sink(name))
          .option('checkpointLocation',str(DATA/'checkpoints'/name)).trigger(processingTime='5 seconds').start())
    stop=False
    def finish(*args):
        nonlocal stop
        stop=True
    signal.signal(signal.SIGTERM,finish); signal.signal(signal.SIGINT,finish)
    try:
        while not stop:
            for q in queries:
                if q.exception(): raise q.exception()
                if not q.isActive: raise RuntimeError('Consulta detenida inesperadamente')
            time.sleep(1)
    finally:
        for q in queries:
            if q.isActive: q.stop()
        spark.stop()

if __name__=='__main__': run()
