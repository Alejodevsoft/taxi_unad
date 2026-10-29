import json
from pathlib import Path
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data'
OUT = ROOT / 'outputs'
EVIDENCE = ROOT / 'evidence'
for p in [DATA / 'raw', OUT, EVIDENCE]:
    p.mkdir(parents=True, exist_ok=True)

def save_json(path, obj):
    path = Path(path)
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=str), encoding='utf-8')
    temp.replace(path)

def spark_session(name, kafka=False):
    builder = (SparkSession.builder.master('local[2]').appName(name)
        .config('spark.driver.memory', '2g')
        .config('spark.sql.shuffle.partitions', '4')
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.sql.timestampType', 'TIMESTAMP_NTZ'))
    if kafka:
        builder = builder.config('spark.jars', ','.join(str(p) for p in sorted((ROOT/'jars').glob('*.jar'))))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    return spark
