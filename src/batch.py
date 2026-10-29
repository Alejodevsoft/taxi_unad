import csv
import time
from pyspark.sql import functions as F
from pyspark import StorageLevel
from common import DATA, OUT, EVIDENCE, save_json, spark_session
from download import download

START, END = '2025-01-01','2025-02-01'

def classify(df, zone_ids):
    df = (df.withColumn('duration_minutes',
          (F.col('tpep_dropoff_datetime').cast('timestamp_ltz').cast('double')-
           F.col('tpep_pickup_datetime').cast('timestamp_ltz').cast('double'))/60)
        .withColumn('trip_date', F.to_date('tpep_pickup_datetime'))
        .withColumn('hour', F.hour('tpep_pickup_datetime'))
        .withColumn('weekday_iso', F.pmod(F.dayofweek('tpep_pickup_datetime')+5,F.lit(7))+1))
    rules = {
      'missing_required': F.col('tpep_pickup_datetime').isNull() | F.col('tpep_dropoff_datetime').isNull() |
          F.col('trip_distance').isNull() | F.col('total_amount').isNull(),
      'pickup_outside_period': ~F.col('tpep_pickup_datetime').between(START, '2025-01-31 23:59:59.999999'),
      'dropoff_outside_period': (F.col('tpep_dropoff_datetime')<F.lit(START)) | (F.col('tpep_dropoff_datetime')>=F.lit(END)),
      'duration_nonpositive': F.col('duration_minutes')<=0,
      'duration_over_24h': F.col('duration_minutes')>1440,
      'distance_invalid': (F.col('trip_distance')<0) | (F.col('trip_distance')>200) | F.isnan('trip_distance'),
      'amount_invalid': (F.col('total_amount')<0) | (F.col('total_amount')>2000) | F.isnan('total_amount'),
      'unknown_pickup_zone': F.col('PULocationID').isNull() | ~F.col('PULocationID').isin(zone_ids),
      'unknown_dropoff_zone': F.col('DOLocationID').isNull() | ~F.col('DOLocationID').isin(zone_ids),
    }
    return df.withColumn('reasons', F.filter(F.array(*[
        F.when(F.coalesce(condition,F.lit(False)),F.lit(name)) for name,condition in rules.items()
    ]), lambda x:x.isNotNull()))

def export(df, name):
    rows = [r.asDict(recursive=True) for r in df.collect()]
    save_json(OUT/f'{name}.json', rows)
    with (OUT/f'{name}.csv').open('w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=df.columns)
        writer.writeheader(); writer.writerows(rows)
    return rows

def charts():
    import pandas as pd
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt.rcParams.update({'axes.spines.top':False,'axes.spines.right':False,'font.size':10})
    specs = [('trips_by_day','trip_date','trips','Viajes por día','Fecha local de Nueva York','Viajes'),
      ('trips_by_hour','hour','trips','Viajes por hora','Hora local de Nueva York','Viajes'),
      ('top_pickup_zones','Zone','trips','Diez zonas de origen con más viajes','Zona de origen','Viajes'),
      ('duration_by_hour','hour','avg_duration_minutes','Duración promedio por hora','Hora local de Nueva York','Minutos')]
    for name,x,y,title,xlabel,ylabel in specs:
        p = pd.read_csv(OUT/f'{name}.csv')
        fig, ax = plt.subplots(figsize=(11,5.5))
        ax.bar(p[x].astype(str),p[y],color='#147d92')
        ax.set(title=title+' · enero de 2025',xlabel=xlabel,ylabel=ylabel)
        ax.tick_params(axis='x',rotation=60 if x!='hour' else 0)
        ax.grid(axis='y',alpha=.2)
        fig.tight_layout(); fig.savefig(OUT/f'{name}.png',dpi=150); plt.close(fig)

def run():
    started=time.time(); download()
    spark=spark_session('UNAD Taxi Batch')
    raw=spark.read.parquet(str(DATA/'raw'/'yellow_tripdata_2025-01.parquet')).persist(StorageLevel.MEMORY_AND_DISK)
    (OUT/'schema.txt').write_text(raw._jdf.schema().treeString(),encoding='utf-8')
    save_json(OUT/'schema.json',raw.schema.jsonValue())
    n=raw.count()
    nulls=raw.select(*[F.sum(F.col(c).isNull().cast('long')).alias(c) for c in raw.columns]).first().asDict()
    save_json(OUT/'nulls.json',nulls)
    range_rules={
        'negative_distance':F.col('trip_distance')<0,'distance_over_200_miles':F.col('trip_distance')>200,
        'zero_distance':F.col('trip_distance')==0,'negative_total':F.col('total_amount')<0,
        'total_over_2000_usd':F.col('total_amount')>2000,'zero_total':F.col('total_amount')==0,
        'negative_fare':F.col('fare_amount')<0,'negative_tip':F.col('tip_amount')<0,
        'passengers_outside_0_8':~F.col('passenger_count').between(0,8),
        'unknown_payment_code':~F.col('payment_type').isin([0,1,2,3,4,5,6]),
        'nan_distance':F.isnan('trip_distance'),'nan_total':F.isnan('total_amount')}
    save_json(OUT/'range_diagnostics.json',raw.select(*[
        F.sum(F.coalesce(condition,F.lit(False)).cast('long')).alias(name)
        for name,condition in range_rules.items()]).first().asDict())
    export(raw.agg(F.min('tpep_pickup_datetime').alias('min_pickup'),F.max('tpep_pickup_datetime').alias('max_pickup'),
        F.min('tpep_dropoff_datetime').alias('min_dropoff'),F.max('tpep_dropoff_datetime').alias('max_dropoff'),
        F.min('trip_distance').alias('min_distance'),F.max('trip_distance').alias('max_distance'),
        F.min('total_amount').alias('min_amount'),F.max('total_amount').alias('max_amount')),'raw_ranges')
    zones=spark.read.option('header',True).option('inferSchema',True).csv(str(DATA/'raw'/'taxi_zone_lookup.csv'))
    ids=[r.LocationID for r in zones.select('LocationID').collect()]

    grouped=raw.groupBy(*raw.columns).agg(F.count('*').alias('copies'))
    classified=classify(grouped,ids).persist(StorageLevel.MEMORY_AND_DISK)
    stats=classified.agg(F.count('*').alias('unique_rows'),F.sum(F.col('copies')-1).alias('duplicate_excess'),
        F.sum((F.size('reasons')>0).cast('long')).alias('rejected_unique'),
        F.sum(F.when(F.size('reasons')>0,F.col('copies')).otherwise(0)).alias('invalid_original_rows')).first().asDict()
    rejected=classified.filter((F.size('reasons')>0)|(F.col('copies')>1)).withColumn('duplicate_excess',F.col('copies')-1)
    rejected.write.mode('overwrite').parquet(str(DATA/'rejected'))
    clean=classified.filter(F.size('reasons')==0).drop('copies','reasons').persist(StorageLevel.MEMORY_AND_DISK)
    accepted=clean.count()
    assert n==accepted+stats['rejected_unique']+stats['duplicate_excess']
    assert n==stats['unique_rows']+stats['duplicate_excess']
    clean.write.mode('overwrite').parquet(str(DATA/'clean'))
    quality=dict(read_rows=n,accepted_rows=accepted,**stats,rejected_original_rows=n-accepted,
        equation='read_rows = accepted_rows + rejected_unique + duplicate_excess',
        nulls=nulls, elapsed_seconds=round(time.time()-started,2))
    save_json(OUT/'quality.json',quality)
    export(classified.select(F.explode('reasons').alias('reason'),'copies').groupBy('reason').agg(
        F.count('*').alias('unique_rows'), F.sum('copies').alias('original_rows')).orderBy('reason'),'rejection_reasons')
    export(raw.filter((F.col('total_amount')<0)|(F.col('fare_amount')<0)).groupBy('payment_type').agg(
        F.count('*').alias('rows'),F.min('total_amount').alias('min_total'),F.max('total_amount').alias('max_total')),
        'negative_amounts_by_payment')
    export(clean.groupBy('trip_date').agg(F.count('*').alias('trips')).orderBy('trip_date'),'trips_by_day')
    export(clean.groupBy('hour').agg(F.count('*').alias('trips')).orderBy('hour'),'trips_by_hour')
    export(clean.groupBy('trip_date','hour').count().orderBy('trip_date','hour'),'trips_by_day_hour')
    export(clean.groupBy('weekday_iso').count().orderBy('weekday_iso'),'trips_by_weekday')
    export(clean.groupBy('PULocationID').count().join(zones,F.col('PULocationID')==F.col('LocationID'))
        .select('PULocationID','Borough','Zone',F.col('count').alias('trips'))
        .orderBy(F.desc('trips'),'PULocationID').limit(10),'top_pickup_zones')
    export(clean.groupBy('hour').agg(F.avg('duration_minutes').alias('avg_duration_minutes')).orderBy('hour'),'duration_by_hour')
    metrics=[]
    for col in ['trip_distance','duration_minutes','total_amount']:
        metric=clean.agg(F.count(col).alias('n'),F.min(col).alias('min'),F.max(col).alias('max'),
          F.avg(col).alias('mean'),F.stddev(col).alias('stddev'),
          F.percentile_approx(col,[.25,.5,.75,.95],10000).alias('p25_p50_p75_p95')).first().asDict()
        metrics.append(dict(variable=col,**metric))
    save_json(OUT/'statistics.json',metrics)

    sample=clean.select('tpep_pickup_datetime','tpep_dropoff_datetime','PULocationID','DOLocationID',
        'trip_distance','duration_minutes','total_amount').limit(10000)
    with (DATA/'replay.jsonl').open('w',encoding='utf-8') as f:
        for row in sample.toLocalIterator():
            import json
            f.write(json.dumps(row.asDict(),default=str)+'\n')
    charts()
    save_json(EVIDENCE/'batch_validation.json',dict(status='passed',**quality,
        spark_version=spark.version,charts=4,full_file=True))
    print(quality,flush=True)
    spark.stop()

if __name__=='__main__': run()
