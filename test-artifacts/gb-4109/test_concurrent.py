"""真实线程竞争：两个执行者并发，接管后旧执行者迟到写入被击剑拒绝，新执行者完成。"""
import os,sys,tempfile,pathlib,logging,threading
logging.disable(logging.CRITICAL)
ROOT=pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'backend'))
os.environ.setdefault('JWT_SECRET','x')
os.environ['DATABASE_URL']='sqlite:///'+os.path.join(tempfile.mkdtemp(prefix='conc-'),'c.db')
from src.config.database import Base,engine,Session
from src.models.batch import Batch
from src.models.mapping import Mapping
from src.models.run import Run,TargetRow
from src.services import runs as rs
import src.worker as w
from src.services.migration import migrate
class Q:
 def rpush(self,k,v):return 1
 def blpop(self,k,timeout=0):return None
rs.queue=Q();w.q=rs.queue
Base.metadata.create_all(engine)
RULES=[{'source':a,'target':b,'type':c,'required':True} for a,b,c in [
 ('委托号','sample_code','text'),('检测项','test_name','text'),('结果','result_value','decimal'),
 ('单位','unit','text'),('时间','measured_at','datetime')]]
rows=[{'委托号':f'JS-{i:03d}','检测项':'T','结果':'420.1','单位':'MPa','时间':'2026-11-17T09:00:00+08:00'} for i in range(12)]
with Session() as s:
 s.add(Batch(name='并发批',source_file='f',rows=rows));s.flush()
 s.add(Mapping(name='并发映射',batch_id=s.query(Batch).one().id,rules=RULES,chunk_size=2))
 s.commit();mid=s.query(Mapping).one().id
class D:pass
d=D();d.name='并发运行';d.mapping_id=mid
rid=rs.create(d)['id']
expected=migrate(rows,RULES,2)
entered=threading.Event();proceed=threading.Event();outcome={}
orig_pc=w.process_chunk
def gated_pc(chunk,start,rules,seen):
 entered.set()
 proceed.wait(5)  # 等待接管发生后再继续，模拟旧执行者正在处理块
 return orig_pc(chunk,start,rules,seen)
# A 领取
with Session() as s:_,tokA=w.claim_run(s,rid)
def worker_a():
 w.process_chunk=gated_pc
 try:w.execute_run(rid,tokA);outcome['A']='ran-unfenced(!!)'
 except w.FencedOut:outcome['A']='fenced'
 except Exception as e:outcome['A']=type(e).__name__
 finally:w.process_chunk=orig_pc
ta=threading.Thread(target=worker_a);ta.start()
assert entered.wait(5),'A 未进入块处理'
# B 在 A 处理期间接管（行锁领取，轮换令牌）
with Session() as s:_,tokB=w.claim_run(s,rid)
proceed.set()
ta.join(5)
def worker_b():
 try:w.execute_run(rid,tokB);outcome['B']='completed'
 except Exception as e:outcome['B']=type(e).__name__+':'+str(e)[:60]
tb=threading.Thread(target=worker_b);tb.start();tb.join(10)
with Session() as s:
 r=s.get(Run,rid);cnt=s.query(TargetRow).filter_by(run_id=rid).count()
 ords=[x.ordinal for x in s.query(TargetRow).filter_by(run_id=rid).order_by(TargetRow.ordinal)]
checks=[
 ('旧执行者并发中被击剑',outcome.get('A')=='fenced',str(outcome)),
 ('新执行者完成',outcome.get('B')=='completed',str(outcome)),
 ('最终状态 completed',r.status=='completed',r.status),
 ('目标行数正确无重复',cnt==expected['accepted_count']==12,str(cnt)),
 ('ordinal 连续',ords==list(range(12)),str(ords)),
 ('摘要与不停机一致',r.result['checksum']==expected['checksum'],r.result['checksum'] if r.result else 'no result'),
]
ok=all(c[1] for c in checks)
for n,cond,detail in checks:print(('  ✓ ' if cond else '  ✗ ')+n+('' if cond else ' '+detail))
print('\n并发接管测试：'+('全部通过' if ok else '存在失败'))
sys.exit(0 if ok else 1)
