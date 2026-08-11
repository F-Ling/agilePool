# go-agile-pool 深度解析

## 一、项目概览

**go-agile-pool** 是一个高性能的自适应 goroutine 池（Worker Pool）。它的核心思路与传统固定大小的 goroutine 池不同：**它不是让你预先指定池子的大小，而是根据任务提交速率和消耗速率，自动、动态地伸缩 worker 数量**。

类比理解：这就像一个智能的工厂调度系统——订单少时少开工位，订单暴增时自动加人，订单消化完后多出来的人下班待命。

---

## 二、核心架构总览

整个池子由以下几个关键角色协作运行：

```
┌──────────────────────────────────────┐
│                Pool                   │
│  ┌──────────┐   ┌──────────────────┐ │
│  │  Scaler   │   │  Task Queue      │ │
│  │ (调度器)  │   │ ┌──────┬──────┐  │ │
│  │           │   │ │chan  │chunk │  │ │
│  └──────────┘   │ │10000 │linked│  │ │
│                  │ │      │list  │  │ │
│  ┌──────────┐   │ └──────┴──────┘  │ │
│  │Expired   │   └──────────────────┘ │
│  │Cleaner   │                        │
│  │(回收器)  │   ┌──────────────────┐ │
│  └──────────┘   │ Idle Workers     │ │
│                  │ (LinkedList等)   │ │
│  ┌──────────┐   └──────────────────┘ │
│  │Stats     │                        │
│  │Sampler   │   ┌──────────────────┐ │
│  │(采样器)  │   │ sync.Pool        │ │
│  └──────────┘   │ (worker对象池)   │ │
│                  └──────────────────┘ │
└──────────────────────────────────────┘
```

三个后台 goroutine 持续运行：

- **`scaler()`**：每隔 10ms 判断是否需要扩缩容（核心调度逻辑）
- **`expiredWorkerCleaner()`**：每隔 500ms 回收空闲超时的 worker
- **`statsSampler()`**：每隔 100ms 采样提交/消费/退出速率

---

## 三、数据结构详解

### 3.1 两级任务队列

这是池子最精妙的设计之一。传统做法是直接用一个 channel 做任务队列，但 channel 的容量必须在创建时固定，容量设小了会阻塞提交者，设大了会浪费内存。

**go-agile-pool 采用"小channel + 分块链表缓冲"的两级架构**：

```
提交任务 Task
     │
     ▼
┌─────────────┐  满了?  ┌─────────────────────────────────┐
│ taskQueue   │ ──────► │  Chunked Linked-List Buffer     │
│ (chan,cap=  │         │                                 │
│  10000)     │         │ headChunk ──► chunk ──► tailChunk│
└─────────────┘         │ [4096]task  [4096]task ...      │
     │                  │                                 │
     ▼                  └────────────┬────────────────────┘
  Worker直接取           Worker批量取(batchSize=8)
```

#### 第一级：handoff channel (`taskQueue`)

- 固定容量 10,000 的 channel
- 正常负载下，提交者直接将任务放入 channel，worker 从 channel 取
- 这是最快的路径（lock-free 的 channel 操作）

#### 第二级：分块链表缓冲 (Chunked Linked-List)

- 当 channel 满时，任务被溢出到此缓冲
- 每个 chunk 是一个 **固定大小 4096** 的任务数组
- chunk 之间用单向链表连接

**为什么用分块（chunk）而不是一个大的动态 slice？**

| 方案 | 问题 |
|------|------|
| 单个大slice | 扩容时 `append` 会触发 `2×` 内存分配，瞬间内存翻倍 |
| 分块链表 | 每次只新增一个 4096 大小的 chunk，内存增长平滑可控 |

**关键常量 `maxChunkLen = 100,000`**：当整个链表缓冲的任务数达到 10 万时，提交者不再向链表追加，而是**阻塞在 channel 的 send 上**，给 worker 争取时间消化积压。这叫"背压（Backpressure）"机制，防止无限制内存增长。

#### pushTail 与 popHead 操作

- **`pushTail(task)`**：在 tailChunk 当前位置写入任务，`tailIdx++`。若当前 chunk 满了（4096 个），则从 `sync.Pool` 获取新 chunk 挂到链表末尾。
- **`popHead()`**：从 headChunk 当前位置读取任务，`headIdx++`。若当前 chunk 读完，将其回收进 `sync.Pool`，指针移到下一个 chunk。
- 两个操作都需要持有 `taskMu` 锁，但 O(1) 完成。

#### Worker 侧批量取任务

Worker 在 channel 空时，不是一次只取一个任务，而是一次拿 **最多 8 个**（`batchSize = 8`），代码在 `worker.go`：

```go
// 一次加锁批量取最多 8 个任务，摊薄锁开销
const batchSize = 8
for n < batchSize {
    t, ok := w.pool.popHead()
    if !ok { break }
    batch[n] = t
    n++
}
```

这样一次锁操作可以服务多个任务，大幅减少锁竞争。

---

### 3.2 空闲 Worker 容器（4 种实现）

当 worker 暂时没有任务可做时，它不会直接退出，而是进入"空闲池"等待复用。空闲池有 4 种数据结构可选：

#### ① LinkedList（默认，双向链表）

```
[head] ⇄ [node] ⇄ [node] ⇄ [tail]
```

- **Add**: O(1) — 插到尾部
- **Pop**: O(1) — 从头部取出
- **RemoveExpired**: O(n) — 必须遍历全部，因为 `lastActiveAt` 与插入顺序无关
- **最适合**：通用场景，FIFO 语义

#### ② MinHeap（最小堆）

```
         [最旧的worker]
        /              \
   [较旧]              [较新]
   /    \              /    \
 ...    ...          ...    ...
```

- 按 `lastActiveAt`（最后活跃时间）排序，根节点总是最久未活跃的 worker
- **Add**: O(log n)
- **Pop**: O(log n)
- **RemoveExpired**: O(k log n) — 只需检查根节点，如果根节点未过期，其他一定也没过期。这是**最大亮点**，过期清理效率远高于其他结构
- **最适合**：空闲 worker 很多且需要高效回收的场景

#### ③ Slice（动态数组）

```
[0] [1] [2] [3] ...
 ↑               ↑
head            tail
```

- **Add**: O(1) 均摊 — `append`
- **Pop**: O(n) — 从头部移除后需要移动所有元素
- **RemoveExpired**: O(n) — 全扫描
- **最适合**：空闲 worker 极少的小规模场景（否则 Pop 太慢）

#### ④ RingQueue（环形队列）

```
  head        tail
   ↓           ↓
[ w3 ][ w4 ][ ___ ][ ___ ][ w1 ][ w2 ]
        ←── 循环缓冲 ←──
```

- **Add**: O(1) — 写到 tail，tail 前进
- **Pop**: O(1) — 从 head 取出，head 前进
- **RemoveExpired**: O(n) — 全扫描后重新紧凑排列
- 容量不足时自动翻倍扩容（类似 slice）
- **最适合**：需要 Pop O(1) 性能，且空闲 worker 数量适中的场景

**选择建议**：大多数场景用默认的 LinkedList 即可。如果有大量空闲 worker 且关心回收效率，选择 MinHeap。如果 Pop 非常频繁且空闲数稳定，可选 RingQueue。

---

### 3.3 Histogram（滑动窗口直方图）

这是 scaler 自适应伸缩的"数据大脑"。

```go
type histogram struct {
    buckets []bucketDef   // 预定义的桶边界，如 [0,0], [1,5], [6,20]...
    counts  []int64       // 每个桶当前的计数
    total   int64         // 总样本数
    samples []int         // 环形缓冲区，记录每个样本落在哪个桶
    pos     int           // 环形缓冲区当前写入位置
    filled  bool          // 环形缓冲区是否已满（开始覆盖旧数据）
}
```

**工作方式**：

1. 每 100ms 采样一次 submit/consume/exit 速率
2. 样本写入环形缓冲区（容量 = `statsWindowSize = 10`）
3. 超过 10 个窗口后，新样本覆盖最旧的样本（`filled = true` 时先减去旧样本的计数）
4. 中位数计算：遍历所有桶，累计计数直到超过 `total/2`，返回该桶的中点值

**为什么用中位数而不是平均值？**

平均值容易被极端值（突发流量尖峰）拉偏，中位数更能反映"典型负载"，使伸缩决策更稳定。

预定义的桶边界（每 100ms 窗口）：

```
提交/消费桶:  [0] [1-5] [6-20] [21-100] [101-500] [501-2000] [2001-10000] [10001+]
退出桶:      [0] [1-2] [3-5] [6-10] [11-20] [21-50] [51-100] [101+]
```

---

### 3.4 sync.Pool — Worker 对象复用

```go
p.workerPool.New = func() interface{} {
    atomic.AddInt64(&p.workerCreateCount, 1)
    w := &worker{pool: p}
    return w
}
```

- worker 通过 `sync.Pool` 复用，减少 GC 压力
- 每个 worker 只是一个轻量级 struct（包含 `pool` 指针和 `lastActiveAt` 时间戳）
- 真正运行的是 goroutine（`go w.run(nil)`），与 worker struct 分离

---

## 四、调度算法深度解析

这是整个项目最核心、最精巧的部分。

### 4.1 整体流程

```
Scaler 每 10ms 触发一次
        │
        ▼
获取中位数速率: submitMed, consumeMed, exitMed
        │
        ▼
计算 target（目标 worker 数）
        │
        ▼
target > running? ──No──► 不做操作
        │Yes
        ▼
计算需要新启动的 worker 数: toSpawn
        ▼
补偿即将退出的 worker: toSpawn += exitPerTick
        ▼
尝试从空闲池 Pop（复用）→ 不够则从 sync.Pool 新建
        ▼
go w.run(nil)  启动新 worker
```

### 4.2 `scaleIfNeeded()` 核心算法

算法入口在 `pool.go` 的 `scaleIfNeeded()` 方法，核心有三步：

#### 第一步：基于速率的计算

```go
if running > 0 && consumeMed > 0 && submitMed > 0 {
    target = int64(submitMed * float64(running) / consumeMed)
}
```

这是一个**比例估算模型**：

\[
\text{target} = \frac{\text{submitMed} \times \text{running}}{\text{consumeMed}}
\]

**直观理解**：

- 如果提交速率 > 消费速率（积压），target > running → 扩容
- 如果提交速率 < 消费速率（空闲），target < running → 不操作（靠自然退出缩容）
- 如果两者持平，target ≈ running → 稳态

**例**：当前有 10 个 worker 在跑，每 100ms 提交 50 个任务，消费 40 个任务，则：
\[
target = \frac{50 \times 10}{40} = 12.5 \rightarrow 12
\]
需要 12 个 worker 才能消化当前负载，当前 10 个不够，扩容 2 个。

#### 第二步：积压加权补偿

```go
if totalBacklog > 0 {
    bufPressure := min(1.0, float64(bufDepth)/consumeMed * 0.15)
    dynamicDecay := config.backlogDecayFactor + 
                    (1 - config.backlogDecayFactor) * bufPressure
    effectiveSubmit := submitMed + float64(totalBacklog) * dynamicDecay
    qTarget := int64(effectiveSubmit * float64(running) / consumeMed)
    if qTarget > target { target = qTarget }
}
```

这里引入了一个精妙的**动态衰减因子**机制：

- **`totalBacklog`**（`pendingTasks`）：包含所有已提交但未开始处理的任务（channel 里的 + chunk 缓冲里的 + 正阻塞在 channel send 上的提交者）
- **`bufPressure`**（缓冲压力）：取值 `min(1.0, bufCycles * 0.15)`，表示当前 chunk 缓冲深度相对于消费速率需要多少周期排空
  - 缓冲越深，`bufPressure` 越大
  - 缓冲很浅时，`bufPressure` 接近 0
- **`dynamicDecay`**：在 `[decayFactor, 1.0]` 之间动态调整
  - 默认 `decayFactor = 0.3`
  - 缓冲压力大时 → `dynamicDecay` 趋近 1.0 → 积压权重最大化 → **激进扩容**
  - 缓冲压力小时 → `dynamicDecay` 趋近 0.3 → 积压权重适度 → **温和扩容**

**直观理解**：这就像开车——前方堵车（大积压）时猛踩油门（快速扩容），路面畅通时轻踩油门（温和调整）。

#### 第三步：退出补偿

```go
exitPerTick := int64(exitMed * float64(config.scalerPeriod) / float64(config.statsSamplePeriod))
if exitPerTick > 0 {
    toSpawn += exitPerTick
}
```

scaling period (10ms) 内会有 worker 因空闲自然退出，这部分需要提前补偿，否则会出现"扩了又退，退了又扩"的振荡。

**例**：每 100ms 有 3 个 worker 退出，则 per 10ms 约退出 0.3 个 → `exitPerTick ≈ 0`（向下取整时不补）。如果退出速率很高，比如每 100ms 有 20 个退出，则 per 10ms 为 2 个 → 扩容时额外多扩 2 个作为预付。

---

### 4.3 Worker 生命周期

```
         sync.Pool.Get() 或 idleWorks.Pop()
                     │
                     ▼
             go w.run(task)
                     │
         ┌───────────┴───────────┐
         │     Worker 主循环      │
         │                       │
         │  select {             │
         │  case <-taskQueue:    │──► 拿到任务 → runTask() → 继续循环
         │  default:             │
         │    从 chunk 缓冲取    │──► 批量取最多8个
         │    (batchSize=8)      │
         │  }                    │
         │                       │
         │  二次select再检查      │──► 拿到任务 → runTask() → 继续循环
         │  default:             │
         │    进入空闲态          │
         │    addToIdle(w)       │
         │    break loop         │
         └───────────────────────┘
                     │
                     ▼
            worker 留在 idleWorks 中
            (runningWorkersNum 已减1)
                     │
          ┌──────────┴──────────┐
          │                     │
    scaler 扩容时           cleaner 回收时
    Pop() 复用             RemoveExpired()
          │                     │
          ▼                     ▼
    go w.run(nil)          worker 被丢弃
                           (由 GC 回收)
```

**关键设计点**：

1. **先检查 channel，再查 chunk 缓冲，再做二次检查** — 三级轮询确保不遗漏任务
2. **二次检查（second check）** — 在 chunk 缓冲空之后，再非阻塞地检查一次 channel，捕获在这微小时间窗口内到达的任务，避免虚假空闲
3. **空闲时 `runningWorkersNum--` 但不销毁 worker** — worker struct 留在 idleWorks 中被复用，下次 scaler 扩容时优先 `Pop()` 而不是新建

### 4.4 冷启动

```go
// 如果当前没有任何 worker 在运行，立即启动一个
if atomic.LoadInt64(&p.runningWorkersNum) == 0 {
    if atomic.CompareAndSwapInt64(&p.runningWorkersNum, 0, 1) {
        w := p.workerPool.Get().(*worker)
        go w.run(nil)
    }
}
```

第一个任务到达时，不会等 scaler 的 10ms tick，而是立即 CAS 启动一个 worker。后续任务如果不够，10ms 内 scaler 会补上。

### 4.5 阻塞模式 vs 非阻塞模式

| 模式 | 行为 |
|------|------|
| **BLOCK（默认）** | channel 满时写入 chunk 缓冲；缓冲满（10万任务）时阻塞在 channel send 上，等待 worker 消化 |
| **NONBLOCK** | channel 满时直接丢弃任务（`select default`），调用 `done()` 归还 WaitGroup |

---

## 五、Task 类型系统

```go
type Task interface {
    Process()
}
```

最简单的使用：`TaskFunc(func() error { ... })`

内置两种高级 Task：

### 5.1 TaskWithRetry（重试任务）

- 指定重试次数 `RetryNum`
- 默认退避策略：指数退避 `MinBackOff × 2^retryNum`，不超过 `MaxBackOff`
- 支持自定义退避策略函数：`BackOffStrategy func(min, max time.Duration, retryNum uint) time.Duration`

### 5.2 contextTask（带 context 的任务）

- 通过 `SubmitCtx(ctx, task)` 提交
- context 取消后在 `Process()` 中提前返回，不再执行实际任务

---

## 六、关键配置参数速查

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `cleanPeriod` | 500ms | 空闲 worker 回收检查间隔 |
| `workerNumCapacity` | `math.MaxInt64` | worker 数量上限（**建议设置合理值！**） |
| `statsSamplePeriod` | 100ms | 速率采样间隔 |
| `statsWindowSize` | 10 | 滑动窗口数量（10×100ms=1s 窗口） |
| `scalerPeriod` | 10ms | scaler 检查间隔 |
| `backlogDecayFactor` | 0.3 | 积压权重因子（0=忽略积压，1=积压最大权重） |

---

## 七、总结：为什么这个 pool 设计出色？

1. **自适应伸缩**：不需要预先估计合适的 worker 数量，系统自动根据负载调整
2. **两级任务队列**：兼顾了 channel 的高效（无锁快速路径）和分块链表的弹性（内存平滑增长）
3. **基于中位数的平滑决策**：用滑动窗口直方图 + 中位数代替瞬时值，避免毛刺导致的错误伸缩
4. **动态衰减因子**：在积压严重时激进扩容，积压轻微时温和调整，避免过度反应
5. **worker 复用**：空闲 worker 不销毁而是暂存，扩容时优先复用，减少创建开销
6. **批量取任务**：worker 一次取最多 8 个，摊薄锁竞争
7. **退出补偿**：扩容时预补偿即将退出的 worker，避免振荡

所有这些机制协同工作，使得 go-agile-pool 在低负载时几乎不消耗资源，在高负载时快速响应，在负载波动时保持稳定。
