"""属性测试：任意块大小、任意中断点，续迁结果与不停机迁移逐字段一致。"""
import os,sys,tempfile,pathlib,logging,random
logging.disable(logging.CRITICAL)
ROOT=pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'backend'))
TMP=tempfile.mkdtemp(prefix='prop-')
os.environ.setdefault('JWT_SECRET','x')
os.environ['DATABASE_URL']='sqlite:///'+os.path.join(TMP,'p.db')
from src.config.database import Base,engine,Session
from src.models.batch import Batch
from src.models.mapping import Mapping
from src.models.run import Run,TargetRow,RejectedRow
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
def row(code,val='420.1',unit='MPa',t='2026-11-17T09:00:00+08:00'):
 return {'委托号':code,'检测项':'T','结果':val,'单位':unit,'时间':t}
def dataset(seed):
 rnd=random.Random(seed);n=rnd.randint(0,40);rows=[]
 for i in range(n):
  v=rnd.choice(['420.1','0','-9.99','1000000000','四百二十','inf','',None,'12.5'])
  u=rnd.choice(['MPa','',None])
  tm=rnd.choice(['2026-11-17T09:00:00+08:00','bad-time','2026-11-17 09:00:00'])
  r=row(f'JS-{seed}-{i:03d}',v,u,tm)
  if rnd.random()<0.15:del r['单位']
  rows.append(r)
 if rows and rnd.random()<0.5:rows.append(dict(rows[rnd.randrange(len(rows))]))  # 重复
 return rows
class D:pass
total=0
for seed in range(24):
 rows=dataset(seed)
 for chunk in (1,2,3,7,50):
  if chunk and len(rows)>2000:continue
  with Session() as s:
   b=Batch(name=f'b{seed}-{chunk}',source_file='f',rows=rows);s.add(b);s.flush()
   m=Mapping(name=f'm{seed}-{chunk}',batch_id=b.id,rules=RULES,chunk_size=chunk);s.add(m);s.commit()
   mid=m.id
  d=D();d.name=f'r{seed}-{chunk}';d.mapping_id=mid
  rid=rs.create(d)['id']
  expected=migrate(rows,RULES,chunk)
  if not expected['chunks']:  # 空输入一次完成，没有中断机会
   w.handle(rid)
  else:
   # 随机中断点：在第 k 个块处理后失败（k=1 表示首块后失败）
   k=random.Random(seed*100+chunk).randint(1,len(expected['chunks']))
   orig=w.process_chunk;cnt={'n':0}
   def stopper(chunk_,start,rules,seen,c=cnt,k=k):
    c['n']+=1;out=orig(chunk_,start,rules,seen)
    if c['n']==k:raise RuntimeError('crash')
    return out
   w.process_chunk=stopper;w.handle(rid);w.process_chunk=orig
  with Session() as s:
   st=s.get(Run,rid)
   if not expected['chunks']:
    assert st.status=='completed'
   else:assert st.status=='failed',(seed,chunk,st.status)
  if expected['chunks']:
   rs.resume(rid);w.handle(rid)
  with Session() as s:
   r=s.get(Run,rid)
   assert r.status=='completed',(seed,chunk,r.error)
   got=r.result
   assert got['accepted_rows']==expected['accepted_rows'],(seed,chunk,'accepted')
   assert got['rejected_rows']==expected['rejected_rows'],(seed,chunk,'rejected')
   assert got['checksum']==expected['checksum'] and got['row_hashes']==expected['row_hashes'],(seed,chunk,'hash')
   assert got['chunks']==expected['chunks'],(seed,chunk,'chunks')
   tn=s.query(TargetRow).filter_by(run_id=rid).count()
   rn=s.query(RejectedRow).filter_by(run_id=rid).count()
   assert tn==expected['accepted_count'] and rn==expected['rejected_count'],(seed,chunk,'counts')
   ords=[x.ordinal for x in s.query(TargetRow).filter_by(run_id=rid)]
   assert ords==list(range(tn)),(seed,chunk,'ordinal')
  total+=1
print(f'属性测试通过：{total} 组（种子×块大小×随机中断点）全部与不停机迁移逐字段一致')
