"""Offline checks of the Spark driver's Kafka planning (spark_ingest.kafka_plan); the end-to-end
behaviour is covered live by src/test/connect-spark-s4.e2e.test.ts."""
import sys, unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent))
from spark_ingest import kafka_plan
def offsets(plan):
    out={}
    for p,a,b in plan:
        self_check=out.setdefault(p,[])
        self_check.extend(range(a,b))
    return out
class KafkaPlan(unittest.TestCase):
    def test_chunks_cover_each_window_exactly_once(self):
        starts={0:5,1:0,2:7}; highs={0:1005,1:0,2:8}
        plan=kafka_plan(starts,highs,300,None)
        got=offsets(plan)
        self.assertEqual(got[0],list(range(5,1005))); self.assertNotIn(1,got); self.assertEqual(got[2],[7])
        self.assertTrue(all(b-a<=300 for _,a,b in plan))
    def test_partitions_are_interleaved(self):
        plan=kafka_plan({0:0,1:0},{0:900,1:900},300,None)
        self.assertEqual([p for p,_,_ in plan],[0,1,0,1,0,1])
    def test_max_run_records_cuts_the_window_fairly(self):
        plan=kafka_plan({0:0,1:0,2:0},{0:1000,1:1000,2:1000},100,450)
        self.assertEqual(sum(b-a for _,a,b in plan),450)
        per={p:sum(b-a for q,a,b in plan if q==p) for p in range(3)}
        self.assertEqual(per,{0:200,1:150,2:100})
        # Each partition's chunks stay contiguous from its start.
        for p,offs in offsets(plan).items(): self.assertEqual(offs,list(range(len(offs))))
    def test_never_more_than_4096_chunks(self):
        plan=kafka_plan({p:0 for p in range(8)},{p:10**7 for p in range(8)},10,None)
        self.assertLessEqual(len(plan),4096)
        self.assertEqual(sum(b-a for _,a,b in plan),8*10**7)
if __name__=='__main__': unittest.main()
