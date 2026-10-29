"""Destino local transaccional. Reintentos técnicos usan offsets y batch_id."""
import sqlite3
from common import DATA

DB=DATA/'stream.sqlite'

def connect():
    db=sqlite3.connect(DB,timeout=60)
    db.execute('PRAGMA journal_mode=WAL')
    db.executescript('''
      CREATE TABLE IF NOT EXISTS messages (
        topic TEXT, partition_id INTEGER, offset_id INTEGER, event_id TEXT,
        valid INTEGER, reasons TEXT, payload TEXT,
        PRIMARY KEY(topic,partition_id,offset_id));
      CREATE TABLE IF NOT EXISTS windows (
        start TEXT, end TEXT, pickup_zone INTEGER, trips INTEGER,
        avg_duration_minutes REAL, avg_amount_usd REAL, updated_at TEXT,
        PRIMARY KEY(start,pickup_zone));
      CREATE TABLE IF NOT EXISTS batches (
        query_name TEXT, batch_id INTEGER, committed_at TEXT,
        PRIMARY KEY(query_name,batch_id));
    ''')
    return db

def snapshot():
    db=connect(); db.row_factory=sqlite3.Row
    with db:
        counts=dict(db.execute('SELECT count(*) AS messages, coalesce(sum(valid),0) AS valid, '
            'coalesce(sum(1-valid),0) AS invalid FROM messages').fetchone())
        aggregate_trips=db.execute('SELECT coalesce(sum(trips),0) FROM windows').fetchone()[0]
        rows=[dict(r) for r in db.execute('SELECT * FROM windows ORDER BY start DESC,trips DESC LIMIT 200')]
        batches=[dict(r) for r in db.execute('SELECT query_name,max(batch_id) AS batch_id,max(committed_at) AS committed_at FROM batches GROUP BY query_name')]
        invalid=[dict(r) for r in db.execute('SELECT event_id,reasons,payload FROM messages WHERE valid=0 LIMIT 20')]
    db.close()
    return dict(counts=counts,aggregate_trips=aggregate_trips,windows=rows,batches=batches,invalid=invalid)
