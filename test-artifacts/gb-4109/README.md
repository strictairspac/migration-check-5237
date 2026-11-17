# gb-4109 断点续迁验证

本目录为「中断后恢复原运行、断点续迁」需求的验证产物。本机无 Docker/PostgreSQL/Redis，
测试使用 SQLite 文件库与内存队列替身运行同一套 `src` 代码；`with_for_update` 在 SQLite
下为空操作，并发安全由 `runs.executor_token` 击剑令牌与唯一约束保证（PostgreSQL 下另有行锁串行化）。

## 运行

```sh
pip install SQLAlchemy==2.0.36 redis==5.2.1 PyJWT==2.10.1 fastapi==0.115.6 httpx
python3 test-artifacts/gb-4109/test_resume.py        # 45 项：断点/修剪/续迁/击剑/取消/快照/约束
python3 test-artifacts/gb-4109/test_api_resume.py    # 31 项：HTTP 接口、角色权限、全链路
python3 test-artifacts/gb-4109/test_equivalence.py   # 120 组：随机数据×块大小×随机中断点等价性
python3 test-artifacts/gb-4109/test_concurrent.py    # 真实双线程接管竞争（重复执行不抖动）
```

## 覆盖的验收点

- 块落库后未更新进度即停机：领取时按断点修剪超界残留，续迁结果与不停机一致。
- 已确认块不再转换；`(run_id,key_digest)`、`(run_id,ordinal)`、`(run_id,row_number)` 唯一约束保证重试不重复入库。
- 跨块重复委托保留最早合法行，后续重复进入拒绝区；拒绝原行、row_hashes、checksum、分块证据与一次性迁移逐字段一致。
- 恢复期间可取消、可并发接管：每次领取轮换 `executor_token`，旧执行者迟到写入抛 `FencedOut` 被拒，且不误报取消。
- 完成前取消不留下任何目标行/拒绝行/断点；排队取消即时生效；原执行者已死时的悬空取消由新执行者落定。
- 源批次与映射快照在创建运行时冻结，后续篡改不影响本次运行；原备份恢复链路保持可用。
- 页面可接回持久进度（`GET /runs/{id}` 的 `progress`）、已持久化目标行与拒绝原行（新增 `/runs/{id}/rejected` 与 `/runs/{id}/resume`）。
