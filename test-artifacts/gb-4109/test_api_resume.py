"""HTTP 层集成：登录、创建/恢复/取消接口、权限、拒绝区与目标行读取。

使用 FastAPI TestClient + SQLite + 内存队列替身，worker.handle 手动驱动。
运行：python3 test-artifacts/gb-4109/test_api_resume.py
"""
import os,sys,tempfile,pathlib,logging
logging.disable(logging.CRITICAL)
ROOT=pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'backend'))
TMP=tempfile.mkdtemp(prefix='api-test-')
os.environ.setdefault('JWT_SECRET','test-secret')
os.environ['DATABASE_URL']='sqlite:///'+os.path.join(TMP,'api.db')

from fastapi.testclient import TestClient
from src.main import app
from src.config.database import Base,engine,Session
from src.models.batch import Batch
from src.models.mapping import Mapping
from src.models.run import Run,TargetRow,RejectedRow
from src.services import runs as runs_svc
import src.worker as worker_mod
from src.utils.security import hash_password
from src.models.identity import User

class FakeRedis:
 def __init__(self):self.items=[]
 def rpush(self,k,v):self.items.append(int(v));return len(self.items)
 def blpop(self,k,timeout=0):return (k,str(self.items.pop(0)).encode()) if self.items else None
 def ping(self):return True
fake_q=FakeRedis()
runs_svc.queue=fake_q;worker_mod.q=fake_q

Base.metadata.create_all(engine)
RULES=[{'source':a,'target':b,'type':c,'required':True} for a,b,c in [
 ('委托号','sample_code','text'),('检测项','test_name','text'),('结果','result_value','decimal'),
 ('单位','unit','text'),('时间','measured_at','datetime')]]
def row(code,val='420.1'):
 return {'委托号':code,'检测项':'拉伸强度','结果':val,'单位':'MPa','时间':'2026-11-17T09:00:00+08:00'}
with Session() as s:
 s.add(User(email='operator@migration.local',role='operator',password=hash_password('x')))
 s.add(User(email='viewer@migration.local',role='viewer',password=hash_password('x')))
 rows=[row(f'JS-{i:03d}') for i in range(1,9)]
 rows.append(row('JS-001'))  # 跨块重复自然键
 rows.append(row('JS-BAD','四百二十'))  # 非法数字
 s.add(Batch(name='API批次',source_file='a.json',description='t',rows=rows));s.flush()
 s.add(Mapping(name='API映射',batch_id=s.query(Batch).one().id,rules=RULES,chunk_size=3));s.commit()
 mid=s.query(Mapping).one().id

c=TestClient(app)
def login(role):
 r=c.post('/auth/login',json={'email':f'{role}@migration.local','password':'x'});assert r.status_code==200,r.text
 return {'Authorization':'Bearer '+r.json()['token']}
op=login('operator');vw=login('viewer')
PASS=0;FAIL=0
def check(name,cond,detail=''):
 global PASS,FAIL
 if cond:PASS+=1;print(f'  ✓ {name}')
 else:FAIL+=1;print(f'  ✗ {name} {detail}')

print('=== A. 权限与接口边界 ===')
r=c.post('/runs',json={'name':'API运行','mapping_id':mid})  # 未登录
check('未登录401',r.status_code==401)
r=c.post('/runs',headers=vw,json={'name':'API运行-viewer','mapping_id':mid})
check('观察员不能创建运行403',r.status_code==403)
r=c.post('/runs',headers=op,json={'name':'API运行','mapping_id':mid});check('操作员创建200',r.status_code==200,r.text)
rid=r.json()['id'];check('初始 queued',r.json()['status']=='queued')
r=c.post(f'/runs/{rid}/resume',headers=vw)
check('观察员不能恢复403',r.status_code==403)
r=c.post(f'/runs/{99999}/resume',headers=op)
check('恢复不存在运行404',r.status_code==404)

print('=== B. 中断 -> 恢复续迁全链路（含拒绝区/目标行/进度可接回） ===')
# 第一阶段：让 worker 处理两块后失败
orig=worker_mod.process_chunk;state={'n':0}
def flaky(chunk,start,rules,seen):
 state['n']+=1;out=orig(chunk,start,rules,seen)
 if state['n']==2:raise RuntimeError('模拟停机')
 return out
worker_mod.process_chunk=flaky
worker_mod.handle(rid)
worker_mod.process_chunk=orig
r=c.get(f'/runs/{rid}',headers=op).json()
check('中断后 failed',r['status']=='failed')
check('页面可接回持久断点',r['progress'] and r['progress']['chunk']==1 and r['progress']['accepted']==3,str(r['progress']))
r=c.get(f'/runs/{rid}/targets',headers=vw)
check('目标行接口可读已持久化行',r.status_code==200 and r.json()['count']==3)
r=c.get(f'/runs/{rid}/rejected',headers=vw)
check('拒绝原行接口可读',r.status_code==200 and all(set(x)=={'row_number','reason','source'} for x in r.json()['rows']))
# 恢复原运行
r=c.post(f'/runs/{rid}/resume',headers=op)
check('恢复接口200且仍为原运行',r.status_code==200 and r.json()['id']==rid and r.json()['status'] in ('running','failed'))
worker_mod.handle(rid)
r=c.get(f'/runs/{rid}',headers=op).json()
check('恢复后 completed',r['status']=='completed',r.get('error'))
check('最终拒绝含重复与非法数字',{x['reason'].split(':')[0] if ':' in x['reason'] else x['reason'] for x in r['result']['rejected_rows']}>= {'duplicate_natural_key','result_value'})
r=c.get(f'/runs/{rid}/targets',headers=op).json()
final_detail=c.get(f'/runs/{rid}',headers=op).json()
check('目标表checksum与结果一致',r['checksum']==final_detail['result']['checksum'])
r=c.get(f'/runs/{rid}/rejected',headers=op).json()
check('拒绝区落库数与结果一致',r['count']==final_detail['result']['rejected_count'])
# 对账凭据与备份
r=c.get(f'/runs/{rid}/export',headers=vw)
check('对账凭据可下载',r.status_code==200 and 'reconciliation' in r.headers['content-disposition'])
r=c.get(f'/runs/{rid}/backup',headers=vw)
check('备份可下载且结构完整',r.status_code==200 and r.json()['version']==1 and r.json()['run_id']==rid)
backup=r.json()
r=c.post(f'/runs/{rid}/restore',headers=op,json={'backup':backup})
check('操作员不能恢复403（仅管理员）',r.status_code==403)

print('=== C. 完成前取消：接口取消后无目标行残留 ===')
r=c.post('/runs',headers=op,json={'name':'API取消运行','mapping_id':mid});rid2=r.json()['id']
with Session() as s:
 rr=s.get(Run,rid2);rr.status='running';rr.executor_token=worker_mod.new_token();s.commit()
import src.worker as W
opc=W.process_chunk;st={'n':0}
def cancel_mid(chunk,start,rules,seen):
 st['n']+=1;out=opc(chunk,start,rules,seen)
 if st['n']==2:
  rr=c.post(f'/runs/{rid2}/cancel',headers=op);assert rr.status_code==200 and rr.json()['cancel_requested']
 return out
W.process_chunk=cancel_mid;W.handle(rid2);W.process_chunk=opc
r=c.get(f'/runs/{rid2}',headers=op).json()
check('取消生效状态 cancelled',r['status']=='cancelled' and r['progress'] is None)
check('取消后目标行为空',c.get(f'/runs/{rid2}/targets',headers=op).json()['count']==0)
check('取消后拒绝行为空',c.get(f'/runs/{rid2}/rejected',headers=op).json()['count']==0)
check('备份接口拒绝未完成运行',c.get(f'/runs/{rid2}/backup',headers=op).status_code==409)
check('已取消不能再恢复',c.post(f'/runs/{rid2}/resume',headers=op).status_code==409)
r=c.post(f'/runs/{rid2}/cancel',headers=op)
check('终态运行重复取消409',r.status_code==409)

print('=== D. 排队取消即时生效，worker 领取空转 ===')
r=c.post('/runs',headers=op,json={'name':'API排队取消','mapping_id':mid});rid3=r.json()['id']
check('排队取消200',c.post(f'/runs/{rid3}/cancel',headers=op).status_code==200)
worker_mod.handle(rid3)
check('worker 不复活已取消运行',c.get(f'/runs/{rid3}',headers=op).json()['status']=='cancelled')

print('=== E. 重复恢复入队幂等 ===')
r=c.post('/runs',headers=op,json={'name':'API幂等运行','mapping_id':mid});rid4=r.json()['id']
state2={'n':0}
def fail3(chunk,start,rules,seen):
 state2['n']+=1;out=orig(chunk,start,rules,seen)
 if state2['n']==2:raise RuntimeError('停')
 return out
worker_mod.process_chunk=fail3;worker_mod.handle(rid4);worker_mod.process_chunk=orig
for _ in range(3):check('重复恢复请求均200',c.post(f'/runs/{rid4}/resume',headers=op).status_code==200)
worker_mod.handle(rid4);worker_mod.handle(rid4);worker_mod.handle(rid4)
final=c.get(f'/runs/{rid4}',headers=op).json()
check('幂等续迁只完成一次',final['status']=='completed')
with Session() as s:
 check('目标行无重复',s.query(TargetRow).filter_by(run_id=rid4).count()==final['result']['accepted_count'])

print(f'\n结果：{PASS} 通过 / {FAIL} 失败')
sys.exit(1 if FAIL else 0)
