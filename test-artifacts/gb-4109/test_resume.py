"""断点续迁验证：中断恢复、击剑接管、完成前取消、不重复入库、快照隔离、备份恢复。

本环境无 Docker/PostgreSQL/Redis，使用 SQLite 文件库 + 内存队列替身运行同一套
src 代码（with_for_update 在 SQLite 下为接受的空操作；并发安全由击剑令牌与
唯一约束保证，PostgreSQL 下行锁额外串行化领取/块提交）。
运行：python3 test-artifacts/gb-4109/test_resume.py
"""
import os,sys,json,tempfile,pathlib,threading,time,logging
logging.disable(logging.CRITICAL)  # 模拟中断的预期异常不打堆栈
ROOT=pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'backend'))
TMP=tempfile.mkdtemp(prefix='resume-test-')
os.environ.setdefault('JWT_SECRET','test-secret')
os.environ['DATABASE_URL']='sqlite:///'+os.path.join(TMP,'test.db')

from src.config.database import Base,engine,Session
from src.models.batch import Batch
from src.models.mapping import Mapping
from src.models.run import Run,TargetRow,RejectedRow
from src.services import runs as runs_svc
import src.worker as worker_mod
from src.services.migration import migrate,digest,key_digest,validate_backup
from src.utils.security import hash_password

# ---------- 内存 Redis 替身（线程安全 list 队列） ----------
class FakeRedis:
 def __init__(self):self.items=[];self.lock=threading.Lock()
 def rpush(self,k,v):
  with self.lock:self.items.append(int(v));return len(self.items)
 def blpop(self,k,timeout=0):
  deadline=time.time()+timeout
  while time.time()<deadline:
   with self.lock:
    if self.items:return k,str(self.items.pop(0)).encode()
   time.sleep(0.01)
  return None
fake_q=FakeRedis()
runs_svc.queue=fake_q
worker_mod.q=fake_q

Base.metadata.create_all(engine)
RULES=[{'source':a,'target':b,'type':c,'required':True} for a,b,c in [
 ('委托号','sample_code','text'),('检测项','test_name','text'),('结果','result_value','decimal'),
 ('单位','unit','text'),('时间','measured_at','datetime')]]
def row(code,val='420.1',dup_idx=None):
 return {'委托号':code,'检测项':'拉伸强度','结果':val,'单位':'MPa','时间':'2026-11-17T09:00:00+08:00'}
def make_rows(n):return [row(f'JS-{i:04d}') for i in range(1,n+1)]
def trow(code):return {'sample_code':code,'test_name':'拉伸强度','result_value':'420.100000','unit':'MPa','measured_at':'2026-11-17T01:00:00+00:00'}
PASS=0;FAIL=0
def check(name,cond,detail=''):
 global PASS,FAIL
 if cond:PASS+=1;print(f'  ✓ {name}')
 else:FAIL+=1;print(f'  ✗ {name} {detail}')
def _try_resume(run_id):
 from fastapi import HTTPException
 try:runs_svc.resume(run_id);return 'ok'
 except HTTPException as e:return str(e.status_code)
def setup_batch(name,rows,chunk_size):
 with Session() as s:
  b=Batch(name=name,source_file=name+'.json',description='t',rows=rows);s.add(b);s.flush()
  m=Mapping(name=name+'-m',batch_id=b.id,rules=RULES,chunk_size=chunk_size);s.add(m);s.commit()
  return b.id,m.id
def enqueue_run(name,bid,mid):
 class D:pass
 d=D();d.name=name;d.mapping_id=mid
 return runs_svc.create(d)['id']
def expected_result(rows,chunk_size):
 return migrate(rows,RULES,chunk_size)

print('=== 1. 正常迁移与续迁结果完全等价（拒绝/重复/摘要一致） ===')
# 含：重复自然键（跨块）、非法数字、空值、缺列
good=make_rows(10)
bad=good[:2]+[
 good[0],                                            # 跨块重复（chunk_size=4，位于第3块），最早合法行保留
 {'委托号':'JS-BAD1','检测项':'拉伸强度','结果':'四百二十','单位':'MPa','时间':'2026-11-17T09:00:00+08:00'},
 {'委托号':'JS-BAD2','检测项':'拉伸强度','结果':None,'单位':'MPa','时间':'2026-11-17T09:00:00+08:00'},
 {'委托号':'JS-BAD3','检测项':'拉伸强度','结果':'430','时间':'2026-11-17T09:00:00+08:00'},
]
bid,mid=setup_batch('异常批次',bad,4)
rid=enqueue_run('运行-等价',bid,mid)
worker_mod.handle(rid)
with Session() as s:
 r=s.get(Run,rid);res=r.result
 exp=expected_result(bad,4)
check('完成状态',r.status=='completed',r.status)
check('接受行一致',res['accepted_rows']==exp['accepted_rows'])
check('拒绝原行/原因一致',res['rejected_rows']==exp['rejected_rows'])
check('checksum/row_hashes 一致',res['checksum']==exp['checksum'] and res['row_hashes']==exp['row_hashes'])
check('分块证据一致',res['chunks']==exp['chunks'])
check('拒绝区已持久化',s.query(RejectedRow).filter_by(run_id=rid).count()==exp['rejected_count'])
check('目标表已持久化',s.query(TargetRow).filter_by(run_id=rid).count()==exp['accepted_count'])
# 备份可恢复
proof=runs_svc.restore(rid,type('D',(),{'backup':res['backup']})())
check('备份恢复校验通过',proof['verified'] and proof['restored_count']==exp['accepted_count'])

print('=== 2. 验证块落库后未更新进度即停机：恢复按断点修剪并续迁 ===')
bid2,mid2=setup_batch('停机批次',make_rows(10),2)  # 5 块
rid2=enqueue_run('运行-停机',bid2,mid2)
# 手工模拟 worker：只处理第 1 块并提交，随后制造“行已落库、进度未更新”的残留
with Session() as s:
 r=s.get(Run,rid2)
 r.status='running';r.executor_token=worker_mod.new_token();tok=r.executor_token
 s.commit()
with Session() as s:
 r=s.get(Run,rid2);snap=r.snapshot
 a,re_=worker_mod.process_chunk(snap['batch']['rows'][0:2],0,RULES,set())
 for i,x in enumerate(a):s.add(TargetRow(run_id=rid2,ordinal=i,key_digest=key_digest(x),payload=x))
 # 故意写一个“超前”目标行（模拟第2块行落库但进度未更新即崩溃）
 s.add(TargetRow(run_id=rid2,ordinal=99,key_digest=key_digest(trow('JS-STRAY')),payload=trow('JS-STRAY')))
 s.commit()  # 进度保持 None
with Session() as s:check('停机时刻无进度、有残留行',s.get(Run,rid2).progress is None and s.query(TargetRow).filter_by(run_id=rid2).count()==3)
# 恢复：claim 应修剪超断点残留（断点为空 -> 全部修剪），随后完整续迁
runs_svc.resume(rid2)
worker_mod.handle(rid2)
with Session() as s:
 r=s.get(Run,rid2)
 exp2=expected_result(make_rows(10),2)
 check('续迁后完成',r.status=='completed',r.status+' '+(r.error or ''))
 check('残留行被修剪、无重复入库',s.query(TargetRow).filter_by(run_id=rid2).count()==exp2['accepted_count'])
 ords=[t.ordinal for t in s.query(TargetRow).filter_by(run_id=rid2)]
 check('ordinal 连续无空洞',ords==list(range(exp2['accepted_count'])),str(ords))
 check('结果与不停机一致',r.result['checksum']==exp2['checksum'] and r.result['rejected_rows']==exp2['rejected_rows'])

print('=== 3. 断点续迁：中途失败，已确认块不再转换，重试不重复入库 ===')
bid3,mid3=setup_batch('续迁批次',make_rows(10),2)
rid3=enqueue_run('运行-续迁',bid3,mid3)
calls={'n':0}
orig=worker_mod.process_chunk
def flaky(chunk,start,rules,seen):
 calls['n']+=1
 out=orig(chunk,start,rules,seen)
 if calls['n']==3:raise RuntimeError('模拟块处理中断')
 return out
worker_mod.process_chunk=flaky
worker_mod.handle(rid3)
worker_mod.process_chunk=orig
with Session() as s:
 r=s.get(Run,rid3)
 check('中断后 failed 且保留断点',r.status=='failed' and r.progress and r.progress['chunk']==2,str(r.progress))
 persisted=s.query(TargetRow).filter_by(run_id=rid3).count()
 check('已确认块目标行保留',persisted==r.progress['accepted']==4,str(persisted))
# 重复调用 resume 两次（消息重复/并发接管），只应产生一个最终结果
runs_svc.resume(rid3);runs_svc.resume(rid3)
worker_mod.handle(rid3)          # 第一条消息接管续迁
worker_mod.handle(rid3)          # 重复消息：已完成，领取空转
with Session() as s:
 r=s.get(Run,rid3);exp3=expected_result(make_rows(10),2)
 check('续迁完成',r.status=='completed',r.error or '')
 check('重试不重复入库',s.query(TargetRow).filter_by(run_id=rid3).count()==exp3['accepted_count'])
 check('续迁摘要与不停机一致',r.result['checksum']==exp3['checksum'])
 check('分块证据为完整5块',len(r.result['chunks'])==5)

print('=== 4. 接管竞争：新令牌生效，旧执行者迟到写入被拒 ===')
bid4,mid4=setup_batch('接管批次',make_rows(6),2)
rid4=enqueue_run('运行-接管',bid4,mid4)
with Session() as s:
 r=s.get(Run,rid4);r.status='running';old_tok=worker_mod.new_token();r.executor_token=old_tok
 r.progress={'chunk':1,'processed':2,'accepted':2,'rejected':0,'chunks':[{'chunk':1,'processed':2,'accepted':2,'rejected':0}]}
 snap=r.snapshot;s.commit()
# 旧执行者已确认的首块目标行（与断点一致）
with Session() as s:
 snap=s.get(Run,rid4).snapshot
 a,_=worker_mod.process_chunk(snap['batch']['rows'][0:2],0,RULES,set())
 for i,x in enumerate(a):s.add(TargetRow(run_id=rid4,ordinal=i,key_digest=key_digest(x),payload=x))
 s.commit()
# 新执行者接管：resume 入队，worker 行锁领取时轮换击剑令牌
runs_svc.resume(rid4)
with Session() as s:
 _,claimed_tok=worker_mod.claim_run(s,rid4)
with Session() as s:
 new_tok=s.get(Run,rid4).executor_token
 check('接管轮换令牌',new_tok!=old_tok and new_tok==claimed_tok)
# 旧执行者尝试用过期令牌提交一个新块 -> 必须被拒
with Session() as s:
 r=s.query(Run).filter_by(id=rid4).with_for_update().first()
 stale_row=trow('JS-STALE')
 try:
  # 复刻 worker 块提交的击剑检查
  if r.executor_token!=old_tok:raise worker_mod.FencedOut('执行权已被接管，拒绝写入')
  s.add(TargetRow(run_id=rid4,ordinal=50,key_digest=key_digest(stale_row),payload=stale_row));s.commit()
  stale_written=True
 except worker_mod.Cancelled:stale_written=False
 check('旧执行者迟到写入被拒',stale_written is False)
with Session() as s:check('无脏目标行',s.query(TargetRow).filter_by(run_id=rid4).count()==2)
# 新执行者完成（再次领取会再轮换令牌，旧令牌依旧失效）
worker_mod.handle(rid4)
with Session() as s:
 r=s.get(Run,rid4);exp4=expected_result(make_rows(6),2)
 check('接管后正常完成',r.status=='completed' and r.result['checksum']==exp4['checksum'],r.error or '')
 check('无重复/陈旧行',s.query(TargetRow).filter_by(run_id=rid4).count()==exp4['accepted_count'])

print('=== 4b. FencedOut 不误报取消；取消悬空时恢复由新执行者落定取消 ===')
# 构造一个被接管的运行
bid4b,mid4b=setup_batch('击剑批次',make_rows(6),2)
rid4b=enqueue_run('运行-击剑',bid4b,mid4b)
with Session() as s:
 r=s.get(Run,rid4b);r.status='running';old=worker_mod.new_token();r.executor_token=old
 r.progress={'chunk':1,'processed':2,'accepted':2,'rejected':0,'chunks':[{'chunk':1,'processed':2,'accepted':2,'rejected':0}]}
 snap=r.snapshot;s.commit()
# 首块已确认行（与断点一致，供续迁重建 seen）
with Session() as s:
 snap=s.get(Run,rid4b).snapshot
 a,_=worker_mod.process_chunk(snap['batch']['rows'][0:2],0,RULES,set())
 for i,x in enumerate(a):s.add(TargetRow(run_id=rid4b,ordinal=i,key_digest=key_digest(x),payload=x))
 s.commit()
new=worker_mod.new_token()
with Session() as s:
 r=s.get(Run,rid4b);r.executor_token=new;s.commit()
fenced=False
try:worker_mod.execute_run(rid4b,old)
except worker_mod.FencedOut:fenced=True
check('过期执行者抛出 FencedOut',fenced)
with Session() as s:check('被击剑后状态未被改写',s.get(Run,rid4b).executor_token==new and s.get(Run,rid4b).status=='running')
# 悬空取消：worker 已死、cancel_requested=True，resume 入队后新执行者落定取消且清空
with Session() as s:
 r=s.get(Run,rid4b);r.cancel_requested=True;s.commit()
 # 预置额外已确认块行，取消必须全部清除
 s.add(TargetRow(run_id=rid4b,ordinal=77,key_digest=key_digest(trow('JS-X')),payload=trow('JS-X')))
 s.add(RejectedRow(run_id=rid4b,row_number=99,reason='x',payload='{}'));s.commit()
runs_svc.resume(rid4b)
worker_mod.handle(rid4b)
with Session() as s:
 r=s.get(Run,rid4b)
 check('悬空取消被落定为 cancelled',r.status=='cancelled' and r.progress is None)
 check('取消清空目标行与拒绝行',s.query(TargetRow).filter_by(run_id=rid4b).count()==0 and s.query(RejectedRow).filter_by(run_id=rid4b).count()==0)

print('=== 4c. 跨块重复委托：最早合法行保留，后续重复进入拒绝区 ===')
dup_rows=make_rows(6)+[make_rows(6)[1]]  # 第7行与第2行同自然键，跨块（chunk_size=3）
bid4c,mid4c=setup_batch('跨块重复批',dup_rows,3)
rid4c=enqueue_run('运行-跨块重复',bid4c,mid4c)
worker_mod.handle(rid4c)
with Session() as s:
 r=s.get(Run,rid4c);exp=expected_result(dup_rows,3)
 check('接受数等于去重后合法行',r.result['accepted_count']==exp['accepted_count']==6)
 check('重复行被拒绝且原因正确',any(x['row_number']==7 and x['reason']=='duplicate_natural_key:sample_code+test_name' for x in r.result['rejected_rows']))
 check('目标表无重复自然键',len({t.key_digest for t in s.query(TargetRow).filter_by(run_id=rid4c)})==6)
# 中断后续迁结论一致：第一块后停机再恢复
bid4d,mid4d=setup_batch('跨块重复续迁批',dup_rows,3)
rid4d=enqueue_run('运行-跨块重复续迁',bid4d,mid4d)
calls4={'n':0};orig_pc2=worker_mod.process_chunk
def stop_after_1(chunk,start,rules,seen):
 calls4['n']+=1;out=orig_pc2(chunk,start,rules,seen)
 if calls4['n']==2:raise RuntimeError('停机')
 return out
worker_mod.process_chunk=stop_after_1;worker_mod.handle(rid4d);worker_mod.process_chunk=orig_pc2
runs_svc.resume(rid4d);worker_mod.handle(rid4d)
with Session() as s:
 r=s.get(Run,rid4d)
 check('续迁后重复判定与不停机一致',r.status=='completed' and r.result['checksum']==exp['checksum'] and r.result['rejected_rows']==exp['rejected_rows'])

print('=== 5. 完成前取消：途中取消不留下目标行；排队取消立即生效 ===')
bid5,mid5=setup_batch('取消批次',make_rows(10),2)
rid5=enqueue_run('运行-取消',bid5,mid5)
# 先跑两块后由用户取消（cancel_requested 在块间被发现）
with Session() as s:
 r=s.get(Run,rid5);r.status='running';tok=worker_mod.new_token();r.executor_token=tok;s.commit()
import src.worker as W
orig_pc=W.process_chunk
state={'k':0}
def cancel_after_two(chunk,start,rules,seen):
 state['k']+=1
 out=orig_pc(chunk,start,rules,seen)
 if state['k']==2:runs_svc.cancel(rid5)
 return out
W.process_chunk=cancel_after_two
W.handle(rid5)
W.process_chunk=orig_pc
with Session() as s:
 r=s.get(Run,rid5)
 check('运行已取消',r.status=='cancelled')
 check('取消不留下目标行',s.query(TargetRow).filter_by(run_id=rid5).count()==0)
 check('取消不留下拒绝行/断点',s.query(RejectedRow).filter_by(run_id=rid5).count()==0 and r.progress is None)
 check('已取消恢复被409拒绝',_try_resume(rid5)=='409')
# 排队阶段取消
rid5b=enqueue_run('运行-排队取消',*setup_batch('排队取消批',make_rows(4),2))
runs_svc.cancel(rid5b)
worker_mod.handle(rid5b)  # worker 收到消息但状态已 cancelled，领取空转
with Session() as s:
 r=s.get(Run,rid5b)
 check('排队取消保持 cancelled',r.status=='cancelled' and s.query(TargetRow).filter_by(run_id=rid5b).count()==0)

print('=== 6. 快照隔离：源批次/映射后续修改不影响续迁与最终结果 ===')
rows6=make_rows(8)
bid6,mid6=setup_batch('快照批次',rows6,2)
rid6=enqueue_run('运行-快照',bid6,mid6)
# 跑两块后中断
with Session() as s:
 r=s.get(Run,rid6);r.status='running';r.executor_token=worker_mod.new_token();tok=r.executor_token;s.commit()
def fail_once_factory():
 k={'n':0}
 def f(chunk,start,rules,seen):
  k['n']+=1;out=orig_pc(chunk,start,rules,seen)
  if k['n']==3:raise RuntimeError('中断')
  return out
 return f
W.process_chunk=fail_once_factory()
W.handle(rid6)
W.process_chunk=orig_pc
with Session() as s:check('中断保留两块断点',s.get(Run,rid6).status=='failed' and s.get(Run,rid6).progress['chunk']==2)
# 篡改源批次与映射（改 chunk_size、改行、改规则源字段）
with Session() as s:
 b=s.get(Batch,bid6);b.rows=[row('JS-HACKED')]*20
 m=s.get(Mapping,mid6);m.chunk_size=7;m.rules=[dict(x,source='被篡改字段') for x in RULES];s.commit()
runs_svc.resume(rid6);W.handle(rid6)
with Session() as s:
 r=s.get(Run,rid6);exp6=expected_result(rows6,2)
 check('续迁使用冻结快照',r.status=='completed' and r.result['checksum']==exp6['checksum'],r.error or '')
 check('块大小仍为快照值2',all(c['processed'] in (2,4,6,8) for c in r.result['chunks']))

print('=== 7. 空批次：可完成、备份/恢复可用，恢复接口边界 ===')
bid7,mid7=setup_batch('空批次',[],5)
rid7=enqueue_run('运行-空',bid7,mid7)
worker_mod.handle(rid7)
with Session() as s:
 r=s.get(Run,rid7)
 check('空运行完成',r.status=='completed' and r.result['accepted_count']==0 and r.result['checksum']==digest([]))
proof=runs_svc.restore(rid7,type('D',(),{'backup':r.result['backup']})())
check('空备份恢复通过',proof['verified'] and proof['restored_count']==0)
# 完成的运行不能 resume
check('完成运行恢复被409拒绝',_try_resume(rid7)=='409')

print('=== 8. 唯一约束直接保证重试不重复入库 ===')
with Session() as s:
 existing=s.query(TargetRow).filter_by(run_id=rid).order_by(TargetRow.ordinal).first()
 # 同 ordinal 重复
 s.add(TargetRow(run_id=rid,ordinal=existing.ordinal,key_digest=key_digest(trow('JS-DUP1')),payload=trow('JS-DUP1')))
 dup_ok=False
 try:s.commit()
 except Exception:dup_ok=True;s.rollback()
 check('(run_id,ordinal) 唯一约束拦截重复',dup_ok)
 # 同自然键 key_digest 重复
 s.add(TargetRow(run_id=rid,ordinal=9999,key_digest=existing.key_digest,payload=existing.payload))
 dup_ok2=False
 try:s.commit()
 except Exception:dup_ok2=True;s.rollback()
 check('(run_id,key_digest) 唯一约束拦截重复',dup_ok2)

print(f'\n结果：{PASS} 通过 / {FAIL} 失败')
sys.exit(1 if FAIL else 0)
