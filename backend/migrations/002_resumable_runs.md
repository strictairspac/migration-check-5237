# Schema v2

在 v1（users/audits/batches/mappings/runs/target_rows）基础上支持中断续迁：

- `runs` 增加 `executor_token`（击剑令牌，每次领取/接管轮换；旧执行者迟到写入被拒）。
- 新增 `rejected_rows`：逐块持久化拒绝原行（run_id, row_number 唯一），续迁后拒绝区与摘要与不停机一致。
- `target_rows` 增加 `(run_id, ordinal)` 唯一约束，配合 `(run_id, key_digest)` 保证重试不重复入库。
- `runs.progress` 成为断点：每块的目标行、拒绝行与进度在同一事务提交；恢复时按 `progress.chunk` 跳过已确认块，并防御性删除超过断点的残留行。
- 取消清空该运行全部 target_rows/rejected_rows 与断点，不留下任何目标行；失败保留已确认块与断点，可对原运行发起 `/runs/{id}/resume` 续迁。

SQLAlchemy `Base.metadata.create_all` 会在新库创建完整 v2 结构；v1 旧库需补列/补表（本演练环境以重建卷方式升级）。
