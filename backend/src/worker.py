import json,time,logging,uuid
from pathlib import Path
from src.config.settings import REDIS_URL
from src.config.database import Session
from src.models.run import Run,TargetRow,RejectedRow
from src.services.migration import (make_backup,key_digest,validate_rules,check_quota,
 process_chunk,assemble_result,Cancelled,FencedOut)
from src.services.audit import log
try:
 import redis
 q=redis.Redis.from_url(REDIS_URL)
except Exception: # 本地无 Redis 时由测试注入替身
 q=None
QUEUE_KEY='migration-runs'
def heartbeat():Path('/tmp/worker-heartbeat').touch()
def new_token():return uuid.uuid4().hex

def trim_to_checkpoint(s,run_id,cp):
 """删除超过持久断点的任何残留行（块落库后、进度未及更新即崩溃的防御性收敛）。"""
 s.query(TargetRow).filter(TargetRow.run_id==run_id,TargetRow.ordinal>=cp['accepted']).delete(synchronize_session=False)
 s.query(RejectedRow).filter(RejectedRow.run_id==run_id,RejectedRow.row_number>cp['processed']).delete(synchronize_session=False)

def claim_run(s,run_id):
 """行锁领取/接管运行。返回 (run, token)；该运行当前不应被本进程处理则返回 (None, None)。"""
 r=s.query(Run).filter_by(id=run_id).with_for_update().populate_existing().first()
 if not r:return None,None
 if r.status not in ('queued','running','failed'):return None,None
 if r.status=='queued':
  r.status='running'
 # 原执行者已死、取消请求悬空：由新执行者代为落定取消，且不留下任何目标行
 if r.cancel_requested or r.status=='cancelled':
  s.query(TargetRow).filter_by(run_id=run_id).delete(synchronize_session=False)
  s.query(RejectedRow).filter_by(run_id=run_id).delete(synchronize_session=False)
  r.status='cancelled';r.error='用户取消，未提交目标行';r.progress=None;s.commit()
  return None,None
 token=new_token();r.executor_token=token
 cp=r.progress or {'chunk':0,'processed':0,'accepted':0,'rejected':0,'chunks':[]}
 trim_to_checkpoint(s,run_id,cp)
 s.commit()
 return r,token

def load_state(s,run_id):
 """从持久层重建续迁状态：已接受行、seen 集合、断点、拒绝行。"""
 targets=s.query(TargetRow).filter_by(run_id=run_id).order_by(TargetRow.ordinal).all()
 accepted_all=[t.payload for t in targets]
 seen={key_digest(x) for x in accepted_all}
 rejects=s.query(RejectedRow).filter_by(run_id=run_id).order_by(RejectedRow.row_number).all()
 rejected_all=[{'row_number':x.row_number,'reason':x.reason,'source':json.loads(x.payload)} for x in rejects]
 run=s.get(Run,run_id)
 cp=run.progress if run.progress else {'chunk':0,'processed':0,'accepted':0,'rejected':0,'chunks':[]}
 return accepted_all,seen,rejected_all,cp

def finalize_cancel(s,run_id,token):
 r=s.query(Run).filter_by(id=run_id).with_for_update().populate_existing().first()
 if not r or r.executor_token!=token:return
 # 取消：不得留下任何目标行；拒绝原行与断点一并清除，运行回到干净的已取消状态
 s.query(TargetRow).filter_by(run_id=run_id).delete(synchronize_session=False)
 s.query(RejectedRow).filter_by(run_id=run_id).delete(synchronize_session=False)
 r.status='cancelled';r.error='用户取消，未提交目标行';r.progress=None;s.commit()

def finalize_failure(s,run_id,token,exc):
 r=s.query(Run).filter_by(id=run_id).with_for_update().populate_existing().first()
 if not r or r.executor_token!=token:return
 # 已确认的块保留落库与断点，标记 failed 等待恢复原运行续迁
 r.status='failed';r.error=str(exc)[:1000];s.commit()

def execute_run(run_id,token):
 with Session() as s:
  r=s.get(Run,run_id)
  if not r:return
  if r.executor_token!=token:raise FencedOut('执行权已被接管，拒绝写入')
  snap=r.snapshot
 rows=snap['batch']['rows'];rules=snap['mapping']['rules'];chunk_size=snap['mapping']['chunk_size']
 validate_rules(rules);check_quota(rows,chunk_size)
 with Session() as s:
  r=s.query(Run).filter_by(id=run_id).first()
  if not r:return
  if r.executor_token!=token:raise FencedOut('执行权已被接管，拒绝写入')
  if r.cancel_requested or r.status=='cancelled':raise Cancelled()
  accepted_all,seen,rejected_all,cp=load_state(s,run_id)
  chunks=list(cp.get('chunks',[]))
 boundaries=list(range(0,len(rows),chunk_size))
 for start in boundaries[cp['chunk']:]:
  chunk=rows[start:start+chunk_size]
  a,re_=process_chunk(chunk,start,rules,seen)
  item={'chunk':len(chunks)+1,'processed':start+len(chunk),'accepted':len(accepted_all)+len(a),'rejected':len(rejected_all)+len(re_)}
  # 同事务：目标行、拒绝原行、断点进度一次落库
  rr=s.query(Run).filter_by(id=run_id).with_for_update().populate_existing().first()
  if not rr or rr.executor_token!=token:raise FencedOut('执行权已被接管，拒绝写入')
  if rr.cancel_requested or rr.status=='cancelled':raise Cancelled()
  base_ord=len(accepted_all)
  for i,row in enumerate(a):s.add(TargetRow(run_id=run_id,ordinal=base_ord+i,key_digest=key_digest(row),payload=row))
  for x in re_:s.add(RejectedRow(run_id=run_id,row_number=x['row_number'],reason=x['reason'],payload=json.dumps(x['source'],ensure_ascii=False)))
  accepted_all.extend(a);rejected_all.extend(re_);chunks.append(item)
  rr.progress={'chunk':item['chunk'],'processed':item['processed'],'accepted':item['accepted'],'rejected':item['rejected'],'chunks':chunks}
  s.commit()
  heartbeat()
 # 最终提交前锁定 run 再次检查取消与执行权
 with Session() as s:
  r=s.query(Run).filter_by(id=run_id).with_for_update().populate_existing().first()
  if not r:return
  if r.executor_token!=token:raise FencedOut('执行权已被接管，拒绝写入')
  if r.cancel_requested or r.status=='cancelled':raise Cancelled()
  actual=[t.payload for t in s.query(TargetRow).filter_by(run_id=run_id).order_by(TargetRow.ordinal)]
  if actual!=accepted_all:raise RuntimeError('持久目标行与迁移结果不一致')
  result=assemble_result(accepted_all,rejected_all,chunks,len(rows))
  result['backup']=make_backup(result,run_id,snap['batch']['name'])
  r.result=result;r.status='completed';r.error=None;s.commit()
 log('worker','run.completed',str(run_id))

def handle(run_id):
 heartbeat()
 with Session() as s:
  r,token=claim_run(s,run_id)
 if not r:return
 try:
  execute_run(run_id,token)
 except FencedOut:
  # 运行已被其他执行者接管：本执行者静默退出，不得改写新进度或误报取消
  logging.info('run %s fenced out by a newer executor',run_id)
 except Cancelled:
  with Session() as s:finalize_cancel(s,run_id,token)
  log('worker','run.cancelled',str(run_id))
 except Exception as exc:
  logging.exception('migration interrupted')
  with Session() as s:finalize_failure(s,run_id,token,exc)

def loop():
 while True:
  heartbeat()
  try:
   item=q.blpop(QUEUE_KEY,timeout=2)
   if not item:continue
   handle(int(item[1]))
  except Exception:logging.exception('worker loop');time.sleep(2)
if __name__=='__main__':loop()
