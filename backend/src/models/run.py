from sqlalchemy import Column,Integer,String,JSON,DateTime,func,ForeignKey,Boolean,UniqueConstraint,Text
from src.config.database import Base
class Run(Base):
 __tablename__='runs'
 id=Column(Integer,primary_key=True)
 name=Column(String(120),unique=True,nullable=False)
 batch_id=Column(Integer,ForeignKey('batches.id'),nullable=False)
 mapping_id=Column(Integer,ForeignKey('mappings.id'),nullable=False)
 status=Column(String(20),nullable=False,default='queued')
 cancel_requested=Column(Boolean,nullable=False,default=False)
 # 击剑令牌：每次领取/接管生成新值；旧执行者迟到的写入必须被拒绝
 executor_token=Column(String(64),nullable=True)
 snapshot=Column(JSON,nullable=False)
 # 断点进度：{'chunk','processed','accepted','rejected','chunks':[...]}，与已落库块同事务提交
 progress=Column(JSON,nullable=True)
 result=Column(JSON,nullable=True)
 recovery=Column(JSON,nullable=True)
 error=Column(String(1000),nullable=True)
 created_at=Column(DateTime,server_default=func.now())
class TargetRow(Base):
 __tablename__='target_rows'
 __table_args__=(UniqueConstraint('run_id','key_digest'),UniqueConstraint('run_id','ordinal'))
 id=Column(Integer,primary_key=True)
 run_id=Column(Integer,ForeignKey('runs.id'),nullable=False)
 ordinal=Column(Integer,nullable=False)
 key_digest=Column(String(64),nullable=False)
 payload=Column(JSON,nullable=False)
class RejectedRow(Base):
 """逐块持久化的拒绝原行，保证中断续迁后拒绝区与摘要和不停机完全一致。"""
 __tablename__='rejected_rows'
 __table_args__=(UniqueConstraint('run_id','row_number'),)
 id=Column(Integer,primary_key=True)
 run_id=Column(Integer,ForeignKey('runs.id'),nullable=False)
 row_number=Column(Integer,nullable=False)
 reason=Column(Text,nullable=False)
 payload=Column(Text,nullable=False)
