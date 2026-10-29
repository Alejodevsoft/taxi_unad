"""Casos de borde que protegen el cómputo de calidad y la interpretación temporal."""
import sys
import unittest
from datetime import datetime
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from common import spark_session
from batch import classify
from pyspark.sql import functions as F

class Rules(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.spark=spark_session('Reglas de calidad')
    @classmethod
    def tearDownClass(cls): cls.spark.stop()

    def test_overlap_and_duplicates(self):
        row=(datetime(2025,1,1,23,55),datetime(2025,1,2,0,5),1.,10.,1,2)
        bad=(datetime(2025,1,2),datetime(2025,1,1),-1.,-20.,999,2)
        raw=self.spark.createDataFrame([row,row,bad],
            'tpep_pickup_datetime timestamp_ntz,tpep_dropoff_datetime timestamp_ntz,trip_distance double,total_amount double,PULocationID int,DOLocationID int')
        grouped=raw.groupBy(*raw.columns).count().withColumnRenamed('count','copies')
        out=classify(grouped,[1,2]).collect()
        valid=[r for r in out if not r.reasons]; invalid=[r for r in out if r.reasons]
        self.assertEqual(len(valid),1); self.assertEqual(valid[0].duration_minutes,10)
        self.assertEqual(valid[0].hour,23); self.assertEqual(valid[0].weekday_iso,3)
        self.assertGreater(len(invalid[0].reasons),1)
        duplicates=sum(r.copies-1 for r in out)
        self.assertEqual(raw.count(),len(valid)+len(invalid)+duplicates)

    def test_month_boundary_and_zero(self):
        rows=[(datetime(2025,1,31,23,59),datetime(2025,2,1,0,1),0.,0.,1,2),
              (datetime(2025,1,1),datetime(2025,1,1),0.,0.,1,2)]
        df=self.spark.createDataFrame(rows,'tpep_pickup_datetime timestamp_ntz,tpep_dropoff_datetime timestamp_ntz,trip_distance double,total_amount double,PULocationID int,DOLocationID int')
        result=classify(df,[1,2]).collect()
        self.assertIn('dropoff_outside_period',result[0].reasons)
        self.assertIn('duration_nonpositive',result[1].reasons)
        self.assertNotIn('amount_invalid',result[1].reasons)

if __name__=='__main__': unittest.main()
