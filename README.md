# Paired-end FASTQ Receiver

单进程接收 API，位于测序仪导出与比对流水线之间。按样本建批，R1/R2 各以
**请求体流式上传一次**（单成员 gzip 包裹的四行制 FASTQ），两端全部校验通过
并保存后才允许下载展开的 FASTQ。坏压缩包、错配、超限整批拒收；取消与重启
都会回收空间。

## 快速开始

```bash
docker compose up --build          # 服务监听 http://127.0.0.1:8080
# Swagger UI: http://127.0.0.1:8080/docs   (OpenAPI: /openapi.json)
```

不用 Docker 本地跑：

```bash
pip install -r requirements.txt
DATA_DIR=./data MAX_FILE_BYTES=67108864 \
  uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
```

演示（正常 / 错配 / 坏包 / 多成员 / 超限）：

```bash
./scripts/demo.sh  http://127.0.0.1:8080     # 五个场景一键跑完
./scripts/demo_cancel.sh http://127.0.0.1:8080  # 慢上传中途取消
```

## 工作流程

```
POST /api/batches            建批（sample_name）→ batch_id，状态 RECEIVING
PUT  /api/batches/{id}/r1    请求体流式上传 R1 的 .fq.gz（边收边写边计配额）
PUT  /api/batches/{id}/r2    同上，可与 r1 并行；多批也并行，慢上传不挡别人
                             两端到齐 → VALIDATING，后台流式展开+校验
GET  /api/batches/{id}       轮询状态：PASSED 后可下载
GET  /api/batches/{id}/r1.fastq   下载展开的 R1（仅 PASSED）
GET  /api/batches/{id}/r2.fastq   下载展开的 R2（仅 PASSED）
```

校验规则（任一失败 → 整批 FAILED 并清除本批全部文件）：

- 必须是**单成员 gzip**（拼接成员、尾随垃圾、截断的流都拒收）；
- 严格**四行制 FASTQ**：`@name` / seq / `+` / qual，行数是 4 的倍数，
  每条 **序列与质量串等长**，至少一条记录；
- 两端**条数相同**，且逐对比较读段名——取 `@` 后第一个空白前的 token，
  去掉末尾 `/1`、`/2` 后必须一致。

## 状态机

```
RECEIVING → VALIDATING → PASSED      可下载展开文件
    │           │
    ▼           ▼
  FAILED（校验/超限/重启中断，文件已回收）   CANCELLED（取消，文件已回收）
```

- `POST /api/batches/{id}/cancel` —— 取消。确认返回时本批**写入已停止、
  文件已删除、配额已释放**，此后任何对该批的写入都返回 409，不会再落盘。
- `DELETE /api/batches/{id}` —— 删除终态批；PASSED 批删除后释放其占用的配额。
- `GET /api/quota` —— 查看共享配额用量。

## 文件字节上限（配额）

- 环境变量 `MAX_FILE_BYTES`，默认 **64 MiB**（`67108864`）。
- 上传临时 `.fq.gz` 与展开的 `.fastq` **共用**这一个预算；**通过批的文件
  也计入**，直到批被删除。
- 每次写盘**之前**先申请配额——超限在接收/展开过程中即失败，不会等整包
  落盘再检查；超限批被清除，其他批不受影响。

## 数据卷与重启恢复

`DATA_DIR`（容器内 `/data`，compose 命名卷 `fastq_data`）保存 SQLite
状态库与全部批次文件。服务启动时、接受请求**之前**完成恢复：

- **PASSED 批保留**（文件齐全才保留，仍计入配额）；
- 未完成的批（RECEIVING/VALIDATING）**标记 FAILED 并回收文件**；
- 孤儿目录一并清理，配额按磁盘实际占用重建。

## 样例数据（samples/）

已生成可直接用；`python3 samples/generate_samples.py` 可重新生成。

| 文件 | 说明 | 预期 |
|---|---|---|
| `normal_r1/r2.fq.gz` | 3 对 150 bp，名字带 `/1`、`/2` | PASSED |
| `mismatch_r1/r2.fq.gz` | 第 2 对读段名不一致 | FAILED（name mismatch） |
| `corrupt_r1.fq.gz` | 截断的 gzip | FAILED（truncated gzip） |
| `multimember_r1.fq.gz` | 两个拼接的 gzip 成员 | FAILED（not single-member） |
| `oversize_r1/r2.fq.gz` | 展开约 75 MiB（gz 本身很小） | FAILED（quota exceeded） |

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/batches` | 建批，body `{"sample_name": "..."}` |
| GET | `/api/batches` | 列出所有批 |
| GET | `/api/batches/{id}` | 批状态/进度/错误 |
| PUT | `/api/batches/{id}/r1`、`/r2` | 流式上传该端 `.fq.gz`（请求体） |
| POST | `/api/batches/{id}/cancel` | 取消（确认即已停写、删文件、放配额） |
| DELETE | `/api/batches/{id}` | 删除终态批，释放配额 |
| GET | `/api/batches/{id}/r1.fastq`、`/r2.fastq` | 下载展开 FASTQ（仅 PASSED） |
| GET | `/api/quota` | 配额用量 |
| GET | `/api/health` | 存活探针 |

## 设计要点

- **单进程**：uvicorn 单 worker；SQLite（WAL）存状态，内存态（取消事件、
  写入计数）只在单进程内有效，故不可多副本水平扩展。
- **流式**：上传按块读请求体、按块申请配额、按块落盘；解压用
  `zlib.decompressobj` 边解压边校验边写展开文件，全程不需要整包入内存
  或先落盘再检查。
- **取消确认语义**：`_terminate` 先置取消标志，等待本批活跃写者归零
  （`asyncio.Condition`），再删目录、释放配额、落终态——确认返回后
  该批不会再有任何落盘。
- **并发**：每批独立的运行时对象；上传/后台校验注册为写者，退出时注销，
  终止流程与写者通过条件变量协调，避免"边删边写"。
