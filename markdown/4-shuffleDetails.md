# Spark Shuffle 过程详解（基于 Spark 2.2.1）

本文档基于 Spark 2.2.1 源码分析 Shuffle 过程的核心机制。与早期版本的 Hash-based Shuffle 不同，Spark 2.x 默认采用 Sort-based Shuffle，在内存管理、文件组织等方面都有显著改进。

## 📚  目录

### 基础章节
- [关于本文档中的图片](#关于本文档中的图片)
- [一、Shuffle 概述](#一shuffle-概述)
  - [1.1 什么是 Shuffle](#11-什么是-shuffle)
  - [1.2 Shuffle 的触发条件](#12-shuffle-的触发条件)
  - [1.3 ShuffleDependency 与 ShuffledRDD](#13-shuffledependency-与-shuffledrdd)

### 架构设计
- [二、Sort-based Shuffle 架构](#二sort-based-shuffle-架构)
  - [2.1 ShuffleManager 是什么？](#21-shufflemanager-是什么)
  - [2.2 ShuffleManager 演进与当前版本](#22-shufflemanager-演进与当前版本)
  - [2.3 SortShuffleManager 核心接口](#23-sortshuffle-manager-核心接口)
  - [2.4 三种 Shuffle 写方案概览](#24-三种-shuffle-写方案概览)

### Shuffle Write（Map 端）
- [三、Shuffle Write（Map 端）](#三shuffle-writemap-端)
  - [3.1 整体架构：从 Hash Shuffle 到 Sort Shuffle](#31-整体架构从-hash-shuffle-到-sort-shuffle)
    - [3.1.1 Hash Shuffle 的演进（了解历史）](#311-hash-shuffle-的演进了解历史)
    - [3.1.2 Sort Shuffle 的彻底优化](#312-sort-shuffle-的彻底优化)
    - [3.1.3 从宏观设计到具体实现](#313-从宏观设计到具体实现)
  - [3.2 Sort Shuffle 文件组织：IndexShuffleBlockResolver](#32-sort-shuffle-文件组织indexshuffleblockresolver)
    - [3.2.1 ShuffleWriter 体系：从决策到执行](#321-shufflewriter-体系从决策到执行)
    - [3.2.2 三种 Shuffle 写方案的对比与选择](#322-三种-shuffle-写方案的对比与选择)
  - [3.3 SortShuffleWriter（BaseShuffleHandle - 最通用）](#33-sortsufflewriterbaseshufflehandle---最通用)
  - [3.4 BypassMergeSortShuffleWriter（优化：Partition ≤ 200，无聚合）](#34-bypassmergesortsufflewriter优化partition--200无聚合)
  - [3.5 UnsafeShuffleWriter（Tungsten 优化：Partition > 200，无聚合）](#35-unsafeshufflewriter-tungsten-优化partition--200无聚合)
    - [3.5.1 为什么不序列化就不能用指针排序？](#351-为什么不序列化就不能用指针排序)
    - [3.5.2 Unsafe 和 Base 的本质关系](#352-unsafe-和-base-的本质关系)
  - [3.6 ExternalSorter 深入详解（Base Shuffle 的核心）](#36-externalsorter-深入详解base-shuffle-的核心)
    - [3.6.1 Buffer 管理：两种内存结构](#361-buffer-管理两种内存结构)
    - [3.6.2 Spill 机制：内存压力下的溅出](#362-spill-机制内存压力下的溅出)
    - [3.6.3 Sort 机制：内存排序 + 多路归并](#363-sort-机制内存排序--多路归并)
    - [3.6.4 ExternalSorter 完整流程演示](#364-externalsorter-完整流程演示)
    - [3.6.5 性能对比：何时选择 ExternalSorter](#365-性能对比何时选择-externalsorter)
  - [3.7 ShuffleExternalSorter 详解（Unsafe 的优化引擎）](#37-shuffleexternalsorter-详解unsafe-的优化引擎)
    - [3.7.1 核心职责](#371-核心职责)
    - [3.7.2 内存结构对比](#372-内存结构对比)
    - [3.7.3 核心流程](#373-核心流程)
    - [3.7.4 关键设计决策](#374-关键设计决策)
    - [3.7.5 与 ExternalSorter 的本质区别](#375-与-externalsorter-的本质区别)
    - [3.7.6 Unsafe 优化的性能优势](#376-unsafe-优化的性能优势)
    - [3.7.7 对比：ExternalSorter vs ShuffleExternalSorter](#377-对比externalsorter-vs-shuffleexternalsorter)

### Shuffle Read（Reduce 端）
- [四、Shuffle Read（Reduce 端）](#四shuffle-readreduce-端)
  - [4.1 概述：Shuffle Read 的三个阶段](#41-概述shuffle-read-的三个阶段)
  - [4.2 BlockStoreShuffleReader：Shuffle Read 的协调者](#42-blockstoreshuffle-readershuffle-read-的协调者)
  - [4.3 ShuffleBlockFetcherIterator：数据拉取的背压控制](#43-shuffleblockfetcheriterator数据拉取的背压控制)
  - [4.4 数据聚合：AppendOnlyMap 和 ExternalAppendOnlyMap](#44-数据聚合appendonlymap-和-externalappendonlymap)

### 典型操作
- [五、典型 Transformation 的 Shuffle 行为](#五典型-transformation-的-shuffle-行为)
  - [5.1 reduceByKey](#51-reducebykey)
  - [5.2 groupByKey](#52-groupbykey)
  - [5.3 sortByKey](#53-sortbykey)
  - [5.4 join](#54-join)

### 附录
- [附录 A：FileConsolidation 并发设计详解](#附录-afileconsolidation-并发设计详解)
- [附录 B：Shuffle 写方案选择决策详解](#附录-bshuffle-写方案选择决策详解)
- [附录 C：AppendOnlyMap 深度理解](#附录-cappendonlymap-深度理解)
- [附录 D：关键问题答疑总结](#附录-d关键问题答疑总结)

---

## 关于本文档中的图片

本文档引入的图片来自开源项目 [SparkInternals](https://github.com/JerryLead/SparkInternals)，这些图片对于理解 Spark Shuffle 机制的演进和当前实现都具有指导价值：

| 图片 | 准确性 | 说明 |
|-----|-------|------|
| shuffle-write-no-consolidation.png | ✅ 准确 | 展示 Hash Shuffle 无优化版本，帮助理解 Spark 2.x Sort Shuffle 的改进 |
| shuffle-write-consolidation.png | ✅ 准确 | 展示 FileConsolidation 优化，虽已被 Sort Shuffle 替代，但说明演进过程 |
| reduceByKeyStage.png | ✅ 准确 | 展示 reduceByKey 的逻辑和物理执行图，对所有 Spark 版本都适用 |
| reduceByKeyRecord.png | ✅ 准确 | 展示 HashMap 增量式 aggregate 逻辑，在 Spark 2.x 中仍然适用 |
| appendonlymap.png | ✅ 准确 | 展示 AppendOnlyMap 的开放寻址和二次探测原理，数据结构未变 |
| ExternalAppendOnlyMap.png | ✅ 准确 | 展示 spill 和 merge-sort-combine 流程，在 Spark 2.x 中通过 ExternalSorter 实现 |

## 一、Shuffle 概述

### 1.1 什么是 Shuffle
我自己的理解：在 Spark 中，我们所处理的数据是分布式的，也就是说数据分别存储在不同的物理节点上。但在很多计算场景下，我们需要将这些原本分散的数据，按照某一个特定的维度集中到一起统一处理。

举例来说，如果我们想把具有相同 Key 的数据聚合在一起计算总和，那么这些相同 Key 的数据就必须先汇集到同一个节点。

为了实现这种汇集，我们就必须让分散在各地的数据，按照某种既定的规则进行一次重新分区和重新派发，以确保特征相同的数据最终流向同一个数据处理节点。

而这一整套 “按照规则重新调整数据分布” 的过程，就是 Shuffle。

### 1.2 Shuffle 的触发条件

以下 transformation 操作会触发 Shuffle：

| 操作 | 说明 |
|-----|------|
| `reduceByKey` | 按 key 聚合，需要 Shuffle |
| `groupByKey` | 按 key 分组，需要 Shuffle |
| `sortByKey` | 按 key 排序，需要 Shuffle |
| `join` | 两个 RDD 按 key 连接，需要 Shuffle |
| `cogroup` | 多个 RDD 按 key 分组，需要 Shuffle |
| `repartition` | 重新分区，需要 Shuffle |
| `distinct` | 去重，需要 Shuffle |

### 1.3 ShuffleDependency 与 ShuffledRDD

从源码角度理解 Shuffle 的触发：

```scala
// Dependency.scala
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],
    val partitioner: Partitioner,
    val serializer: Serializer = SparkEnv.get.serializer,
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false)
  extends Dependency[Product2[K, V]] {

  val shuffleId: Int = _rdd.context.newShuffleId()

  // 注册 shuffle 时，ShuffleManager 会根据条件选择最优的 ShuffleHandle
  val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    shuffleId, _rdd.partitions.length, this)
}
```

`ShuffledRDD` 是 Shuffle 的入口点：

```scala
// ShuffledRDD.scala
class ShuffledRDD[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient var prev: RDD[_ <: Product2[K, V]],
    part: Partitioner)
  extends RDD[(K, C)](prev.context, Nil) {

  override def getDependencies: Seq[Dependency[_]] = {
    List(new ShuffleDependency(prev, part, serializer, keyOrdering, aggregator, mapSideCombine))
  }

  // compute 方法从 ShuffleManager 获取 Reader 读取 shuffle 数据
  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }
}
```

---

## 二、Sort-based Shuffle 架构

### 2.1 ShuffleManager 是什么？

**通俗解释**：

Shuffle 是 Spark 中重新分发数据的过程。`ShuffleManager` 就是**管理这个过程的"指挥官"**：

```
数据流：
MapTask 输出数据
       ↓
   ShuffleManager（决策者）
       ├─ 注册 Shuffle → 决定用什么方式写
       ├─ 获取 Writer → 负责如何快速写入
       └─ 获取 Reader → 负责如何快速读取
       ↓
ReduceTask 读取数据
```

**关键澄清：箭头方向表示"服务流向"，不是"数据流向"**

上图中的箭头需要仔细理解：

- **MapTask → ShuffleManager**：MapTask **请求** ShuffleManager 获取 Writer 对象
- **ShuffleManager → ReduceTask**：ReduceTask **请求** ShuffleManager 获取 Reader 对象

**实际的数据流向**是通过磁盘文件进行的：

```
真实的数据流向（物理层）：

MapTask（Producer）
   ↓（通过 ShuffleWriter 写入）
 ┌─────────────────────────────┐
 │  磁盘 Shuffle 文件系统      │
 │  shuffle_0_0.data           │  ← ShuffleManager 协调存储位置
 │  shuffle_0_0.index          │     但数据不经过 Manager 对象本身
 └─────────────────────────────┘
   ↑（通过 ShuffleReader 读取）
ReduceTask（Consumer）

关键点：
 - ShuffleManager 本身不存储数据，不是数据的"中转站"
 - ShuffleManager 只负责"决策"和"协调"，提供合适的 Reader/Writer
 - 数据直接在磁盘文件中交换，通过索引和位移定位数据位置
```

**三个核心职责**：

1. **`registerShuffle`（注册）**：当 Shuffle 发起时，决定使用哪种优化方式
   - 分析数据特征（Partition 数、是否需要聚合等）
   - 选择最适合的 ShuffleHandle（是 Bypass、Serialized、还是 Base）

2. **`getWriter`（写）**：返回合适的 Writer 对象给 MapTask
   - 不同 Handle 对应不同的 Writer
   - Writer 负责如何快速地把数据写到磁盘

3. **`getReader`（读）**：返回合适的 Reader 对象给 ReduceTask
   - 根据 Handle 类型，选择如何读取 shuffle 数据
   - 处理数据反序列化、聚合等后续处理

### 2.2 ShuffleManager 演进与当前版本

| Spark 版本 | 默认 ShuffleManager | 说明 |
|-----------|---------------------|------|
| < 1.1 | HashShuffleManager | 每个 task 产生 R 个文件 |
| 1.1 - 1.1.x | HashShuffleManager | 引入 `spark.shuffle.manager` 可配置 |
| 1.2+ | **SortShuffleManager** | sort 成为默认 |
| 1.6+ | SortShuffleManager | 移除 `spark.shuffle.spill=false` 支持 |
| 2.x | SortShuffleManager | 引入 Tungsten 优化 |

从源码可以看到，Spark 2.2.1 只保留了 SortShuffleManager：

```scala
// SparkEnv.scala
val shortShuffleMgrNames = Map(
  "sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
  "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName)
val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")
```

### 2.3 SortShuffleManager 核心接口

```scala
// ShuffleManager.scala
private[spark] trait ShuffleManager {
  // 注册 shuffle，返回 ShuffleHandle
  def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  // 获取 Writer（map 端调用）
  def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V]

  // 获取 Reader（reduce 端调用）
  def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C]

  // 解析 shuffle block 数据
  def shuffleBlockResolver: ShuffleBlockResolver
}
```

### 2.4 三种 Shuffle 写方案概览

SortShuffleManager 根据条件选择不同的写方案（对应不同的优化策略）：

```scala
// SortShuffleManager.scala
override def registerShuffle[K, V, C](
    shuffleId: Int,
    numMaps: Int,
    dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {

  if (SortShuffleWriter.shouldBypassMergeSort(conf, dependency)) {
    // 方案 1: BypassMergeSort - partition 数量少且无 map-side aggregation
    new BypassMergeSortShuffleHandle[K, V](...)
  } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
    // 方案 2: Serialized Shuffle - Tungsten 优化方案
    new SerializedShuffleHandle[K, V](...)
  } else {
    // 方案 3: Base Shuffle - 反序列化对象排序
    new BaseShuffleHandle(shuffleId, numMaps, dependency)
  }
}
```

**简单对比**：

| 方案 | 何时使用 | 特点 |
|-----|--------|------|
| **BaseShuffleHandle** | 最常见，支持所有功能 | 反序列化对象，支持聚合和排序 |
| **BypassMergeSort** | Partition ≤ 200 且无聚合 | 最快，无排序和 merge |
| **SerializedShuffleHandle** | 大 Partition 且无聚合 | 次快，二进制排序优化 |

在接下来的详细章节中，我们先从最常见、最通用的 BaseShuffleHandle 开始讲解，然后介绍两种优化方案。

---

## 三、Shuffle Write（Map 端）

### 3.1 整体架构：从 Hash Shuffle 到 Sort Shuffle

#### 3.1.1 Hash Shuffle 的演进（了解历史）

下图展示了 Hash Shuffle 的两个版本，帮助理解文件数量的优化过程：

**版本 1：Hash Shuffle 无优化**

![shuffle-write-no-consolidation](figures/shuffle-write-no-consolidation.png)

**说明**：
- 4 个 ShuffleMapTask 在 2 个 core 上运行
- 无优化情况下：每个 task 产生 R（3 个 reducer）个文件
- 总文件数：M × R = 4 × 3 = 12 个文件
- 每个 task 有 R 个 bucket 缓冲区

**问题**：
- 文件数过多导致磁盘 I/O 压力大
- 文件描述符占用多

**版本 2：Hash Shuffle + FileConsolidation 优化**

![shuffle-write-consolidation](figures/shuffle-write-consolidation.png)

**改进思路**：
- 同一 core 连续执行的 task 可以**共用同一个输出文件**
- 不同 task 的数据追加到同一文件中形成 FileSegment
- 文件数从 M × R 降至 **cores × R**

**具体例子**：
- Core 1 上 Task 0 和 Task 1 共用一个文件，形成 2 个 FileSegment
- Core 2 上 Task 2 和 Task 3 共用一个文件，形成 2 个 FileSegment
- 总文件数：2 (cores) × 3 (R) = 6 个文件（相比 12 个减少了 50%）

**为什么是同一 core 而不是所有 cores 共用一个文件**？

核心原因是**并发安全和性能**：同一 core 上任务是顺序执行的，避免了多线程并发写入同一文件时的锁竞争和性能开销。详见附录 A：FileConsolidation 并发设计。

#### 3.1.2 Sort Shuffle 的彻底优化

**版本 3：Sort Shuffle（Spark 1.2+ 默认）**

Sort Shuffle **彻底改变了设计思路**，与 core 数无关：

```
关键改进：
✓ 每个 MapTask 产生 1 个合并的数据文件 + 1 个索引文件
✓ 数据文件内按 partition 组织所有数据
✓ 索引文件存储每个 partition 的起始和结束偏移量

文件数：2 × M（只与 MapTask 数量相关）

例如：
  4 个 MapTask → 8 个文件（4 个 data + 4 个 index）
  100 个 MapTask → 200 个文件（100 个 data + 100 个 index）
  1000 个 MapTask → 2000 个文件（1000 个 data + 1000 个 index）
```

**为什么 Sort Shuffle 更优**：
1. **文件数固定**：2 × M，与 core 数、partition 数都无关
2. **可扩展性好**：大规模 partition（R 很大）时优势明显
3. **数据定位精准**：通过索引快速定位任意 partition 的数据范围，不需要逐个查找小文件

#### 3.1.3 从宏观设计到具体实现

上面介绍了 Sort Shuffle 的整体设计思想，现在需要进一步理解：**这两个文件（data + index）具体是怎么在代码中组织和管理的**？

在 Spark 中，`IndexShuffleBlockResolver` 就是负责这个工作的核心组件。它的职责是：

- **管理文件映射**：维护 Shuffle 数据文件和索引文件的位置信息
- **提供数据定位**：当 Reduce Task 需要读取数据时，通过索引文件快速定位数据在磁盘上的位置
- **处理文件 I/O**：负责索引文件的读写操作

接下来的小节将深入探讨 `IndexShuffleBlockResolver` 的设计和实现。

### 3.2 Sort Shuffle 文件组织：IndexShuffleBlockResolver

#### 什么是 IndexShuffleBlockResolver？

`IndexShuffleBlockResolver` 是 Spark 中**管理 Sort Shuffle 文件组织的核心组件**。它的名字含义是：

- **Index**：管理索引文件
- **Shuffle**：与 Shuffle 过程相关
- **BlockResolver**：解析 Block（数据块）的位置

它的核心职责是：
1. **管理两个关键文件**：数据文件（`.data`）和索引文件（`.index`）
2. **维护文件位置信息**：记录这些文件在磁盘上的存储位置
3. **支持高效数据定位**：让 Reduce Task 能通过索引快速找到自己需要的数据

#### IndexShuffleBlockResolver 的设计

与旧版 Hash Shuffle 不同，Sort Shuffle 的核心改进是**每个 ShuffleMapTask 只产生两个文件**，由 `IndexShuffleBlockResolver` 统一管理：

```scala
// IndexShuffleBlockResolver.scala
private[spark] class IndexShuffleBlockResolver(
    conf: SparkConf,
    _blockManager: BlockManager = null)
  extends ShuffleBlockResolver {

  // 数据文件：shuffle_{shuffleId}_{mapId}_0.data
  // 包含所有 partition 的数据，按 partition 顺序排列
  def getDataFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  // 索引文件：shuffle_{shuffleId}_{mapId}_0.index
  // 包含每个 partition 数据的起始和结束位置（偏移量）
  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }
}
```

**关键设计点**：
- **统一的数据文件**：所有分区的数据都在一个 `.data` 文件中，按 partition 顺序排列
- **轻量的索引文件**：`.index` 文件只存储偏移量数组，非常小（通常只有几 KB）
- **快速定位**：Reduce Task 只需读索引文件，就能知道自己的数据在哪个位置范围

**索引文件格式**：

```
[offset_0][offset_1][offset_2]...[offset_R]
# 每个 offset 占 8 字节
# partition i 的数据范围：[offset_i, offset_{i+1})
```

**文件数量对比**：

| Shuffle 类型 | 每个 Task 产生文件数 | M 个 Task 总文件数 |
|-------------|---------------------|------------------|
| Hash Shuffle (无优化) | R | M × R |
| Hash Shuffle (Consolidate) | cores × R | cores × R |
| **Sort Shuffle** | 2 (1 data + 1 index) | 2 × M |

**关键理解**：

#### 为什么 Sort Shuffle 每个 Task 只产生 2 个文件？

Sort Shuffle 的核心思想是**一个 Task 产生一个合并的数据文件 + 一个索引文件**，而不是为每个 reducer 产生一个单独的文件。

```
Hash Shuffle（无优化）：每个 MapTask 产生 R 个文件
┌─────────────────────────────────────────┐
│  ShuffleMapTask 1                       │
│  ├─ bucket 0 → file 0 (for Reducer 0)  │
│  ├─ bucket 1 → file 1 (for Reducer 1)  │
│  └─ bucket 2 → file 2 (for Reducer 2)  │
└─────────────────────────────────────────┘
总文件数：M × R = 4 × 3 = 12 个文件


Sort Shuffle（2 个文件）：
┌───────────────────────────────────────────────────────┐
│  ShuffleMapTask 1                                     │
│                                                       │
│  数据（一个文件内按 partition 组织）：               │
│  ┌──────────────────────────────────┐                │
│  │ Partition 0 的数据                │ offset_0      │
│  ├──────────────────────────────────┤               │
│  │ Partition 1 的数据                │ offset_1      │
│  ├──────────────────────────────────┤               │
│  │ Partition 2 的数据                │ offset_2      │
│  └──────────────────────────────────┘               │
│                                                       │
│  索引（offset 数组）：                               │
│  [offset_0, offset_1, offset_2, offset_3]            │
│                                                       │
│  产生：                                              │
│  • data file: shuffle_1_0_0.data (5 MB)             │
│  • index file: shuffle_1_0_0.index (24 bytes)       │
└───────────────────────────────────────────────────────┘
总文件数：2 × M = 2 × 4 = 8 个文件
```

#### Reduce Task 如何从中读取自己的数据？

关键在于**索引文件**！每个 Reduce Task 不是读取一个完整的数据文件，而是根据索引文件定位自己在数据文件中的位置范围。

```
Reduce Task 1 读取 Partition 1 的数据：

Step 1: 查询索引文件
┌─────────────────────────────────────┐
│ index file: shuffle_1_0_0.index     │
│                                     │
│ [offset_0=0]                        │ ← Partition 0 起始位置
│ [offset_1=1500]    ← Partition 1 起始位置（Reduce Task 1 需要）
│ [offset_2=3200]    ← Partition 1 结束位置
│ [offset_3=5000]                     │
└─────────────────────────────────────┘

Step 2: 计算数据范围
Reduce Task 1 只需读取：
  [offset_1, offset_2) = [1500, 3200) 的 1700 字节数据

Step 3: 从 data file 中读取指定范围
┌──────────────────────────────────────────┐
│ data file: shuffle_1_0_0.data (5000 字节)│
│                                          │
│ [0 -------- 1500) Partition 0 的数据     │
│ [1500 ---- 3200) ◄─ Partition 1 的数据   │ 只读这部分！
│ [3200 ---- 5000) Partition 2 的数据      │
└──────────────────────────────────────────┘
```

#### 数据流图解

```
=== Map 端（Shuffle Write） ===

4 个 ShuffleMapTask，每个产生 2 个文件：

  Task 0 → shuffle_0_0.data + shuffle_0_0.index
  Task 1 → shuffle_0_1.data + shuffle_0_1.index
  Task 2 → shuffle_0_2.data + shuffle_0_2.index
  Task 3 → shuffle_0_3.data + shuffle_0_3.index

  总共 8 个文件存储在磁盘上


=== Reduce 端（Shuffle Read） ===

3 个 ReduceTask，每个从 4 个 data file 中读取对应 partition 的数据：

  ReduceTask 0 (读 Partition 0):
    ├─ 从 shuffle_0_0.data 读取 [offset_0, offset_1)
    ├─ 从 shuffle_0_1.data 读取 [offset_0, offset_1)
    ├─ 从 shuffle_0_2.data 读取 [offset_0, offset_1)
    └─ 从 shuffle_0_3.data 读取 [offset_0, offset_1)

  ReduceTask 1 (读 Partition 1):
    ├─ 从 shuffle_0_0.data 读取 [offset_1, offset_2)  ← 不同范围！
    ├─ 从 shuffle_0_1.data 读取 [offset_1, offset_2)
    ├─ 从 shuffle_0_2.data 读取 [offset_1, offset_2)
    └─ 从 shuffle_0_3.data 读取 [offset_1, offset_2)

  ReduceTask 2 (读 Partition 2):
    ├─ 从 shuffle_0_0.data 读取 [offset_2, offset_3)  ← 又是不同范围！
    ├─ 从 shuffle_0_1.data 读取 [offset_2, offset_3)
    ├─ 从 shuffle_0_2.data 读取 [offset_2, offset_3)
    └─ 从 shuffle_0_3.data 读取 [offset_2, offset_3)
```

#### 与 Hash Shuffle 的对比

```
Hash Shuffle 方式（读取完整独立的小文件）：
  ReduceTask 0: 直接读取 4 个文件
    └─ file_0_0, file_1_0, file_2_0, file_3_0 （4 个完整的小文件）

  ReduceTask 1: 直接读取 4 个文件
    └─ file_0_1, file_1_1, file_2_1, file_3_1 （4 个完整的小文件）

  ReduceTask 2: 直接读取 4 个文件
    └─ file_0_2, file_1_2, file_2_2, file_3_2 （4 个完整的小文件）

  总共 12 个小文件


Sort Shuffle 方式（从大文件中读取指定范围）：
  每个 ReduceTask 都读取相同的 4 个大文件，但范围不同

  总共 8 个文件（每个 MapTask 2 个）
```

#### 从文件组织到写入实现

上面我们深入理解了 Sort Shuffle 如何通过 **IndexShuffleBlockResolver** 管理数据文件和索引文件，实现了文件数量的大幅优化。这是 Shuffle 设计的**文件层**。

但是，**真正的数据是谁写入这些文件的呢？** 这就涉及到了 Shuffle Write 的**执行层**：

```
架构层：
  ShuffleManager (决策者)
    └─ 决定用什么方案

文件层：
  IndexShuffleBlockResolver (管理者)
    └─ 组织 data 文件和 index 文件

执行层：（接下来讨论的内容）
  ShuffleWriter (执行者)
    └─ 按照 ShuffleManager 的决策，实际执行数据写入
```

在实际实现中，Spark 为了适应**不同的场景和优化需求**，设计了**多种 ShuffleWriter**：

- **BypassMergeSortShuffleWriter**：分区少、无聚合时的极速版本
- **UnsafeShuffleWriter**：大数据量、无聚合时的高效版本
- **SortShuffleWriter**：通用版本，支持所有功能

那么问题来了：**ShuffleManager 如何选择用哪个 Writer？** 答案就在下一节的 **ShuffleHandle 体系** 中。

---

### 3.2.1 ShuffleWriter 体系：从决策到执行

到目前为止，我们了解了 Shuffle 的整体架构和文件组织方式。但还有一个重要的环节：**ShuffleManager 如何根据数据特征做出决策，以及如何执行这个决策**。

这就涉及到两个关键的角色：

**1. ShuffleHandle：决策者**
- 由 `ShuffleManager.registerShuffle()` 返回
- 作为一个"策略标记"，告诉 MapTask "应该用什么方式写"
- 类比：餐厅菜单，标记了"今天推荐的烹饪方式"

**2. ShuffleWriter：执行者**
- 由 `ShuffleManager.getWriter()` 返回
- 根据 ShuffleHandle 的指示，按特定方式执行写入逻辑
- 类比：厨师，根据菜单标记来烹饪

#### 三种 ShuffleHandle 与 ShuffleWriter 的对应关系

```
ShuffleManager.registerShuffle()
       ↓
    决策：根据数据特征分析

       ├─ 分区少 & 无聚合 → BypassMergeSortShuffleHandle
       │   └─ 创建 → BypassMergeSortShuffleWriter
       │      特点：极速路径，无排序，直接分区写入
       │
       ├─ 分区多 & 无聚合 & relocation serializer → SerializedShuffleHandle
       │   └─ 创建 → UnsafeShuffleWriter
       │      特点：高效路径，二进制排序，Tungsten 优化
       │
       └─ 其他情况（有聚合 / 分区超大 / 无 relocation serializer）→ BaseShuffleHandle
           └─ 创建 → SortShuffleWriter
              特点：通用路径，支持所有功能，完整排序
```

#### MapTask 执行流程

```
MapTask 获得数据记录
       ↓
ShuffleManager.getWriter(handle, ...)
       ↓
根据 handle 类型返回对应的 Writer
       ├─ if handle is BypassMergeSortShuffleHandle → BypassMergeSortShuffleWriter
       ├─ if handle is SerializedShuffleHandle → UnsafeShuffleWriter
       └─ if handle is BaseShuffleHandle → SortShuffleWriter
       ↓
Writer.write(records)
       ↓
数据写入磁盘（data file + index file）
```

#### 为什么需要这种分层设计？

```
统一接口，多种实现：
├─ 简化 MapTask 的代码：只需调用统一的 Writer 接口
├─ 灵活的优化空间：不同场景用不同的 Writer 实现
└─ 扩展性好：添加新的 Writer 不影响现有逻辑
```

### 3.2.2 三种 Shuffle 写方案的对比与选择

现在我们已经知道了 ShuffleHandle 和 ShuffleWriter 的关系。接下来需要理解：**为什么会有三种方案，它们分别解决什么问题**。

#### 三种方案的核心差异

| 方案 | 对应 Handle | 对应 Writer | 核心特点 | 适用场景 |
|------|-----------|-----------|--------|--------|
| **Bypass** | BypassMergeSortShuffleHandle | BypassMergeSortShuffleWriter | 无排序，直接分散写 | 分区 ≤200，无聚合 |
| **Unsafe** | SerializedShuffleHandle | UnsafeShuffleWriter | 二进制排序，内存优化 | 分区>200，无聚合，大数据量 |
| **Base** | BaseShuffleHandle | SortShuffleWriter | 完整功能，对象排序 | 有聚合 / 排序，或分区超大 |

#### 选择策略的决策树

```
进入 SortShuffleManager.registerShuffle()
       ↓
检查条件 1：是否有 map-side combine？
       ├─ YES → 必须用 Base（BaseShuffleHandle）
       │   原因：需要 HashMap 进行聚合
       │
       └─ NO → 继续检查条件 2
              ↓
              检查条件 2：Partition 数量是否 ≤ 200？
              ├─ YES → 检查条件 3
              │   ↓
              │   检查条件 3：Serializer 是否支持 relocation？
              │   ├─ YES → 可用 Bypass（推荐）
              │   │   原因：分区少，Bypass 最快
              │   │
              │   └─ NO → 用 Base（BaseShuffleHandle）
              │       原因：无法用 Serialized
              │
              └─ NO（>200）→ 检查条件 4
                     ↓
                     检查条件 4：Serializer 是否支持 relocation？
                     ├─ YES → 用 Unsafe（SerializedShuffleHandle）
                     │   原因：分区多，Unsafe 内存效率高
                     │
                     └─ NO → 用 Base（BaseShuffleHandle）
                         原因：无法用 Serialized
```

#### 为什么这样选择？

**Bypass 的优势与限制**：

```
优势：
- 实现简单：直接分区写，无需排序
- 速度快：I/O 和 CPU 都快
- 无排序开销：跳过了排序这个昂贵操作

限制：
- 分区多时内存压力大（同时打开 N 个文件缓冲）
- 磁盘 I/O 随机分散（不能聚合）
- 无法支持聚合（需要 HashMap）
- 限制在 200 个分区以内
```
**Unsafe 的优势与限制**：

```
优势：
- 内存效率高：8 字节指针 vs 120+ 字节对象
- GC 压力低：堆外内存，不受 GC 影响
- 排序高效：只移动指针，不移动对象
- 支持 16M 分区：完全覆盖大规模场景

限制：
- 无法支持聚合（数据已序列化）
- Serializer 需要支持 relocation 属性
- 实现复杂：涉及 Tungsten、PackedRecordPointer 等
```

**Base 的优势与适用**：

```
优势：
- 功能完整：支持聚合、排序等所有功能
- 场景覆盖：处理所有无法用 Bypass/Unsafe 的情况
- 兼容性好：不依赖 Serializer 特性

适用：
- 有 map-side combine（reduceByKey、combineByKey）
- 无法用 Serialized 的场景（Serializer 不支持 relocation）
- 分区超大且有聚合需求（>16M partition）
```

---

### 3.3 SortShuffleWriter（BaseShuffleHandle - 最通用）

**何时使用**：

- **有 map-side combine**（reduceByKey、combineByKey 等）
- **或者 Partition 数量很大**（>16M，Serialized 不支持）
- **或者需要排序**（sortByKey）

这是最通用和完整的 Shuffle 写方案。

#### 通俗解释：BaseShuffleHandle 为什么支持聚合？

**核心需求**：

reduceByKey 场景下，相同 key 的值需要被聚合。这需要一个 HashMap 来存储中间结果：

```
输入：(k1, v1), (k1, v2), (k1, v3)
处理过程：
  ├─ 遇到 (k1, v1) → HashMap[k1] = v1
  ├─ 遇到 (k1, v2) → HashMap[k1] = reduce(v1, v2)  （需要取出 v1！）
  └─ 遇到 (k1, v3) → HashMap[k1] = reduce(v1+v2, v3)

为什么 Bypass 和 Serialized 不行：
  ├─ Bypass：只能直接写分区文件，没有 HashMap 结构，无法聚合
  └─ Serialized：数据已序列化成字节流，无法反序列化后进行聚合
```

**BaseShuffleHandle 的解决方案**：

使用 `PartitionedAppendOnlyMap`（基于 HashMap）来维护聚合状态：

```scala
// ExternalSorter.scala - BaseShuffleHandle 的核心
if (aggregator.isDefined) {
  // 使用 HashMap 结构进行 in-memory combine
  while (records.hasNext) {
    val kv = records.next()
    map.changeValue((getPartition(kv._1), kv._1), update)
    maybeSpillCollection(usingMap = true)
  }
}

// 关键代码：changeValue
map.changeValue((partitionId, key), updateValue)
// 等价于：
// if (map.contains((partitionId, key))) {
//     map.update((partitionId, key), func(old_value, new_value))
// } else {
//     map.insert((partitionId, key), new_value)
// }
```

#### SortShuffleWriter 完整流程

```scala
// SortShuffleWriter.scala
private[spark] class SortShuffleWriter[K, V, C](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, C],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] {

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // 根据是否需要 map-side combine 创建不同的 ExternalSorter
    sorter = if (dep.mapSideCombine) {
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }

    // 插入所有 records
    sorter.insertAll(records)

    // 写入分区文件
    val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
    shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
  }
}
```

**执行流程（包括 combine）**：

1. **初始化阶段**：
   - 如果 `mapSideCombine = true`（如 reduceByKey）：创建 **PartitionedAppendOnlyMap**（HashMap 结构）
   - 如果 `mapSideCombine = false`（如 groupByKey）：创建 **PartitionedPairBuffer**（只是缓冲）

2. **插入 + Combine 阶段**（`insertAll()`）：
   - **逐条读取** record (k, v)
   - **计算 partition ID**：partId = partitioner.getPartition(k)
   - **如果启用 combine**：
     - 检查 (partId, k) 是否已在 HashMap 中
     - **若存在**：调用 `combineValue(oldVal, newVal)` 进行**聚合**
     - **若不存在**：直接插入 (partId, k) → v
   - **内存管理**：若内存满，触发 **spill 到磁盘**（保留 spill 文件列表）

3. **排序 + Merge 阶段**（`writePartitionedFile()`）：
   - 内存中的数据按 **partition ID 主序、key 次序排序**
   - 多路 merge 磁盘的 spill 文件（保持 partition 有序）
   - **注意**：这里的排序**只保证 partition 内有序**，不是全局 key 有序

4. **输出阶段**：
   - 写入 `.data` 文件（所有数据顺序拼接）
   - 写入 `.index` 文件（每个 partition 的偏移量）
   - 原子提交（重命名）

**关键理解**：

- **Combine 发生在插入阶段**，不是独立步骤
- Combine 的目的是**减少内存占用**和**减少网络传输数据量**
- Combine 后，相同 key 的多个值已合并成一个
- 排序**只是按 partition 分组有序**，不改变 partition 内的整体顺序


### 3.4 BypassMergeSortShuffleWriter（优化：Partition ≤ 200，无聚合）

**何时使用**：

- **Partition 数量 ≤ 200**
- **无 map-side combine**（mapSideCombine = false）

这是优化方案，设计用于小规模 partition 场景，追求最快速度。

**核心特点**：

1. **无排序**：数据按 partition 号直接写入，无需排序
2. **无 spill**：小 partition 数量时，内存缓冲足以容纳，无需溅出磁盘
3. **无 merge**：完成后直接逐个读取和拼接分区文件
4. **最少 I/O**：只需"写分区文件 + 拼接"两次 I/O

**为什么限制在 200？**

这是内存缓冲和文件系统性能的平衡点：

```
内存缓冲限制：
├─ spark.shuffle.file.buffer = 32KB （默认）
├─ 同时打开文件数 = Partition 数量
└─ 总缓冲内存 = Partition × 32KB
   ├─ 200 个 partition：6.4MB ✓ 可接受
   └─ 2000 个 partition：64MB ❌ 浪费内存

文件系统性能：
├─ 200 个文件并发写：还可以接受
└─ 2000 个文件并发写：文件系统压力过大
   ├─ 文件描述符数量限制
   ├─ 内核跟踪成本高
   ├─ 磁盘寻址频繁切换
   └─ 内存页缓存污染
```

**性能对比**：

当 Partition > 200 时，为什么 Bypass 反而慢？

```
Bypass 模式（Partition = 1000，❌ 性能差）：
├─ 同时打开 1000 个文件缓冲
├─ 对每条 record 进行 partition 计算并写入
└─ 完成后读取 1000 个文件进行拼接
   └─ 磁盘频繁寻址，随机 I/O 性能差

Sort 模式（✓ 性能好）：
├─ 所有 records 进入内存排序结构
├─ 按 Partition ID 有序排列
├─ 写入时顺序操作 1 个 data file
└─ 完成后只需拼接 1 个 data file
   └─ 顺序 I/O 性能最高
```

### 3.5 UnsafeShuffleWriter（Tungsten 优化：Partition > 200，无聚合）

**何时使用**：

- **Partition 数量 201 ~ 16,777,215（2^24-1）**
- **无 map-side combine**（mapSideCombine = false）
- **Serializer 支持 relocation 属性**

这是 Spark 2.x 的核心优化，使用**序列化二进制数据**直接操作，避免 Java 对象开销。

**核心优化点**：

1. **序列化排序**：直接对序列化二进制数据排序，无需反序列化
2. **压缩指针**：每条记录仅 8 字节存储（partition ID 24 bit + page number 13 bit + offset 27 bit）
3. **缓存友好**：排序数组紧凑，适合 CPU cache
4. **零拷贝合并**：spill 文件合并时使用 NIO transferTo

**内存效率对比**：

| 操作 | BaseShuffleHandle | SerializedShuffleHandle |
|-----|-----------------|------------------------|
| 每条记录占用空间 | 120+ 字节（对象） | 8 字节（压缩指针） |
| 排序对象 | Java 对象 | 二进制指针 |
| GC 压力 | 高（反序列化对象多） | 低（直接操作字节） |
| 适用场景 | 小数据量 | 大数据量 |

**16M Partition 限制的原因**：

```
PackedRecordPointer 结构：[24 bit partition ID][13 bit page number][27 bit offset]
                          └─ 最多支持 2^24 - 1 = 16,777,215 个 partition

如果 partition ID > 16M → 溢出，导致数据损坏
```

#### 3.5.1 为什么不序列化就不能用指针排序？

这是理解 Unsafe 优化的关键问题。简短答案：**堆地址随机、无法编码排序键、GC 会移动**。

**三个层面的原因**：

1. **堆地址没有语义意义**
   - JVM 堆地址完全随机分配
   - 比较地址大小 ≠ 比较数据大小
   - 指针值在两次运行中完全不同

2. **无法编码分区信息**
   - 不序列化：排序键必须从对象中读取
   - 每次排序比较都要解引用 → 频繁内存访问
   - Unsafe 方式：分区 ID 已编码在指针的最高 24 位 → 直接比较

3. **GC 会破坏指针有效性**
   - 堆对象被 GC 移动后，指针全部失效
   - 需要重新更新所有指针引用
   - 序列化后的数据在堆外 Tungsten 内存 → 不受 GC 影响

**结论**：序列化是为了把数据放到**"有规律、可编码、不会移动"的内存空间**，这样指针才能有意义。

#### 3.5.2 Unsafe 和 Base 的本质关系

这两个方式看起来差异大，但**本质流程完全相同**：

```
都经历：内存积累 → 按需 Spill → 多路归并 → 单文件输出

Base (ExternalSorter)：
  ├─ insertAll(records) → 内存中积累 Java 对象
  ├─ maybeSpill() → 当内存满时，排序后溅出到磁盘文件
  ├─ spill 文件已排序（按 partitionId + key）
  └─ 最后多路归并所有 spill 文件 → output.data

Unsafe (UnsafeShuffleWriter)：
  ├─ insertRecord() → 内存中积累序列化数据 + 排序指针
  ├─ spill() → 当指针数组满时，排序指针后溅出到磁盘文件
  ├─ spill 文件已排序（按 partitionId）
  └─ 最后多路归并所有 spill 文件 → output.data
```

**关键差异只有两点**：

| 方面 | Base | Unsafe |
|------|------|--------|
| **排序对象** | Java 对象（120+ 字节） | 8 字节压缩指针 |
| **内存管理** | JVM 堆 + 对象内存 | Tungsten 堆外内存 |
| **GC 压力** | 高（对象多） | 低（只有指针） |
| **适用** | 小数据量或需要聚合 | 大数据量、无聚合 |

**为什么说 Bypass 是特例，Unsafe/Base 是主流**：

```
Bypass：
  分散写 → partition_0.tmp + partition_1.tmp + ...
       ↓ 最后简单拼接
    output.data

Unsafe/Base：
  集中内存 → 多个 spill 文件（已排序）
       ↓ 智能多路归并
    output.data

Bypass 的优势只在"特定场景"（小分区、无聚合）发挥
Unsafe/Base 则是"通用场景"的标准方案
```

### 3.6 ExternalSorter 深入详解（Base Shuffle 的核心）

`ExternalSorter` 是 BaseShuffleHandle 的核心组件，它完成了数据的 **in-memory 聚合 → spill 到磁盘 → 多路归并排序** 的完整流程。这一节深入讲解 buffer、spill、sort 三个关键机制。

#### 3.6.1 Buffer 管理：两种内存结构

ExternalSorter 使用两种不同的内存结构，根据是否需要聚合选择：

**情景 1：有聚合需求（map-side combine）**

```scala
// ExternalSorter.scala
@volatile private var map = new PartitionedAppendOnlyMap[K, C]

// PartitionedAppendOnlyMap 的结构：
// Key: (partitionId, keyValue)
// Value: aggregatedValue
// 内部实现：HashMap，使用开放寻址法 + 二次探测
```

**工作原理**：

```
输入 records: (k1, v1), (k1, v2), (k2, v3), ...

Step 1: 插入 (k1, v1)
  map.changeValue((partitionId, k1), v1)
  → HashMap 中新增条目：(partitionId, k1) → v1

Step 2: 插入 (k1, v2)
  map.changeValue((partitionId, k1), func(v1, v2))
  → 如果 key 存在，执行 update 操作
  → HashMap 中更新：(partitionId, k1) → func(v1, v2)
  → 比如 reduceByKey 中，func 是 + 操作

Step 3: 持续插入更多 records
  → 每次插入前检查 key 是否存在
  → 存在则聚合，不存在则插入新条目
```

**PartitionedAppendOnlyMap 的内存布局**：

```java
// PartitionedAppendOnlyMap 内部的 keys 和 values 数组
class PartitionedAppendOnlyMap[K, V] {
  private var keys: Array[AnyRef] = ...     // 存放 (partitionId, key) 元组
  private var values: Array[AnyRef] = ...   // 存放对应的 aggregated value

  // 每个数组条目占用的空间（最坏情况）
  // keys[i]：对象引用（8 字节）+ (partitionId, key) 对象（56+ 字节）
  // values[i]：对象引用（8 字节）+ value 对象（56+ 字节）
  // 总计：单条记录约 120+ 字节
}
```

**情景 2：无聚合需求**

```scala
// 只需要分区，不需要聚合
@volatile private var buffer = new PartitionedPairBuffer[K, C]

// PartitionedPairBuffer 的结构：
// 简单的数组缓冲，只存储 (partitionId, key, value) 三元组
// 内存占用更少：单条记录约 32-48 字节（对比 Map 的 120+ 字节）
```

**对比**：

| 指标 | PartitionedAppendOnlyMap | PartitionedPairBuffer |
|-----|------------------------|---------------------|
| **数据结构** | HashMap（开放寻址） | Array 线性缓冲 |
| **聚合能力** | ✓ 支持 changeValue 增量聚合 | ❌ 无聚合机制 |
| **内存占用（单条）** | ~120+ 字节 | ~32-48 字节 |
| **查找效率** | O(1) 平均 | O(N) 线性扫描 |
| **适用场景** | reduceByKey、combineByKey | groupByKey、排序前的缓冲 |

#### 3.6.2 Spill 机制：内存压力下的溅出

**何时触发 Spill？**

```scala
// Spillable.scala
protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
  var shouldSpill = false

  // 条件 1：每插入 32 条记录检查一次（减少检查开销）
  if (elementsRead % 32 == 0) {
    // 条件 2：当前内存使用量超过阈值
    if (currentMemory >= myMemoryThreshold) {
      // 条件 3：申请扩展内存失败
      val granted = acquireMemory(currentMemory * 2 - myMemoryThreshold)
      if (granted < currentMemory * 2 - myMemoryThreshold) {
        shouldSpill = true
      }
    }
  }
  shouldSpill
}
```

**具体例子**：

```
场景：Executor 总内存 = 2GB，Shuffle 内存占比 20%
      实际分配给 Shuffle = 2GB × 20% × 80% = 320MB（安全系数 80%）

MapTask 1 启动：
  可用内存 = 320MB

插入 records 到 ExternalSorter：
  ├─ 插入 1000 条 → 约 120MB 内存
  ├─ 继续插入到 5000 条 → 约 600MB（超过 320MB，触发 Spill 检查）
  ├─ maybeSpill 检查失败（无法再申请内存）
  └─ SPILL 触发！

Spill 过程：
  ├─ 对内存中的 Map 按 (partitionId, key) 排序
  ├─ 将排序后的数据写入磁盘文件（shuffle_0_0.spill）
  ├─ 清空内存中的 Map，释放 600MB
  └─ 继续插入剩余的 records 到新的 Map

最终状态：
  ├─ 内存中：新的 Map（剩余 records）
  ├─ 磁盘上：多个 spill 文件（可能有 5-10 个）
  └─ 需要 merge-sort-combine
```

**Spill 文件的内容**：

```
shuffle_0_0.spill 文件（已排序）：
┌──────────────────────────────────────┐
│ Partition 0 的数据（已按 key 排序）    │
│ (0, key_a) → value_1                 │
│ (0, key_b) → value_2                 │
│ (0, key_z) → value_3                 │
├──────────────────────────────────────┤
│ Partition 1 的数据（已按 key 排序）    │
│ (1, key_p) → value_4                 │
│ (1, key_q) → value_5                 │
└──────────────────────────────────────┘

shuffle_0_1.spill 文件（第二次 spill）：
┌──────────────────────────────────────┐
│ 同样的结构，但数据来自新一轮 records   │
└──────────────────────────────────────┘
```

#### 3.6.3 Sort 机制：内存排序 + 多路归并

**Phase 1：内存中的排序**

```scala
// ExternalSorter 中的排序方式
private def mergeSortedIterators(): Iterator[(K, C)] = {
  // 1. 对内存中的 Map 排序
  val sorted = if (aggregator.isDefined) {
    // 有聚合时：map 本身是 PartitionedAppendOnlyMap
    // 需要按 (partitionId, key) 排序
    map.iterator.map { case ((partId, key), value) =>
      ((partId, key), value)
    }.toSeq.sortBy(_._1._1).iterator  // 按 partitionId 排序
  } else {
    // 无聚合时：buffer 是简单数组，直接排序
    buffer.iterator.toSeq.sortBy(_._1).iterator
  }
  sorted
}
```

**具体排序流程**：

```
PartitionedAppendOnlyMap 的排序：

内存中的数据（无序）：
  (partitionId=2, key_x) → v1
  (partitionId=0, key_a) → v2
  (partitionId=1, key_c) → v3
  (partitionId=0, key_b) → v4
  ...

排序后（按 partitionId 优先，同一 partition 内按 key 排序）：
  (partitionId=0, key_a) → v2
  (partitionId=0, key_b) → v4
  (partitionId=1, key_c) → v3
  (partitionId=2, key_x) → v1
  ...
```

**Phase 2：多路归并排序（如果有多个 spill 文件）**

当有多个 spill 文件时，需要进行 **k-way merge**：

```scala
// ExternalSorter 中的 merge 流程
private def mergeSpilledFiles(): Iterator[(K, C)] = {
  // 1. 打开所有 spill 文件的迭代器
  val spillIterators = spilledMaps.map { file =>
    readSpilledFile(file)
  }

  // 2. 加上内存中的数据迭代器
  val allIterators = spillIterators :+ inMemoryIterator

  // 3. 使用优先队列（堆）进行 k-way merge
  val heap = new scala.collection.mutable.PriorityQueue[Iterator[(K, C)]]()(
    Ordering.by[Iterator[(K, C)], (K, C)](_.next()).reverse
  )
  allIterators.foreach(heap.enqueue(_))

  // 4. 逐个从堆中弹出最小的 (partition, key)
  // 5. 如果是相同 key，进行 combine
  while (heap.nonEmpty) {
    val iter = heap.dequeue()
    val (key, value) = iter.next()

    // 聚合逻辑
    if (aggregator.isDefined && lastKey == key) {
      accumulatedValue = aggregator.get.mergeCombiners(accumulatedValue, value)
    } else {
      // 输出上一个 key 的结果
      yield (lastKey, accumulatedValue)
      lastKey = key
      accumulatedValue = value
    }
  }
}
```

**多路归并的图解**：

```
3 个 spill 文件 + 1 个内存缓冲 = 4 路数据流：

Spill 1:   (0, k_a)→1, (0, k_b)→2, (1, k_c)→3, (2, k_x)→4
Spill 2:   (0, k_a)→5, (0, k_d)→6, (1, k_p)→7
Spill 3:   (0, k_b)→8, (1, k_c)→9
Memory:    (0, k_e)→10, (1, k_q)→11, (2, k_z)→12

优先队列选择最小的（按 partition, key 排序）：
1. (0, k_a) from Spill 1 → output: (0, k_a) → 1
2. (0, k_a) from Spill 2 → COMBINE: 1 + 5 = 6 → output: (0, k_a) → 6
3. (0, k_b) from Spill 1 → output: (0, k_b) → 2
4. (0, k_b) from Spill 3 → COMBINE: 2 + 8 = 10 → output: (0, k_b) → 10
5. (0, k_d) from Spill 2 → output: (0, k_d) → 6
6. (0, k_e) from Memory → output: (0, k_e) → 10
...

最终输出（已聚合且按 partition 有序）：
(0, k_a) → 6
(0, k_b) → 10
(0, k_d) → 6
(0, k_e) → 10
(1, k_c) → 3 + 9 = 12
(1, k_p) → 7
(1, k_q) → 11
...
```

#### 3.6.4 ExternalSorter 完整流程演示

**场景**：reduceByKey，2000 条 records，内存阈值 100MB

```
Step 1: 初始化
  sorter = new ExternalSorter[K, V, C](
    context,
    aggregator = Some(new Aggregator(...)),  // reduceByKey 的聚合器
    partitioner = Some(hashPartitioner),
    ...
  )

Step 2: 插入 records（第 1 轮）
  ├─ 插入 records [1-250]（约 30MB）
  │  └─ 存储在 PartitionedAppendOnlyMap 中
  │
  ├─ 插入 records [251-500]（总 60MB）
  │  └─ 继续在 Map 中聚合
  │
  ├─ 插入 records [501-700]（总 85MB）
  │  └─ 接近阈值
  │
  └─ 插入 records [701-767]（总 98MB）
     └─ 快要触发检查了

Step 3: 第一次 Spill 触发
  ├─ 插入 record 768 → 检查 768 % 32 == 0 ✓ → 触发检查
  │  └─ 当前内存 102MB > 阈值 100MB → 满足条件！
  │
  ├─ 申请内存扩展失败 → **第 1 次 SPILL！**
  │
  └─ Spill 过程：
     ├─ 对 Map 中的 768 条 records 按 (partitionId, key) 排序
     ├─ 写入 shuffle_0_0.spill（约 95MB）
     ├─ 清空 Map，释放内存
     └─ 状态：内存清空，继续接收新 records

Step 4: 继续插入（第 2 轮）
  ├─ 插入 records [769-1024]（256 条，约 32MB）
  │  └─ 新 Map 中继续累积
  │
  ├─ 插入 records [1025-1280]（总 512 条，约 64MB）
  │  └─ 继续插入
  │
  ├─ 插入 records [1281-1536]（总 768 条，约 96MB）
  │  └─ 再次接近阈值
  │
  └─ 检查 1536 % 32 == 0 ✓ → 触发检查
     └─ 当前内存 98MB > 阈值？→ 还差一点点，继续...

Step 5: 第二次 Spill 触发
  ├─ 插入 records [1537-1599]（总 832 条，约 104MB）
  │  └─ 超过阈值了！
  │
  ├─ 检查 1600 % 32 == 0 ✓ → 触发检查
  │  └─ 当前内存 104MB > 阈值 100MB → 满足条件！
  │
  ├─ 申请内存扩展失败 → **第 2 次 SPILL！**
  │
  └─ Spill 过程：
     ├─ 对 Map 中的 832 条 records 按 (partitionId, key) 排序
     ├─ 写入 shuffle_0_1.spill（约 98MB）
     ├─ 清空 Map，释放内存
     └─ 状态：内存清空，剩余 400 条待处理

Step 6: 最后插入（第 3 轮）
  ├─ 插入 records [1600-2000]（401 条，约 50MB）
  │  └─ 剩余所有 records 插入完毕
  │
  └─ **第 3 轮未触发 Spill**（50MB < 100MB 阈值）
     └─ 数据停留在内存中

Step 7: Merge 和输出
  ├─ 所有 2000 条 records 已处理完毕
  │  ├─ 磁盘 spill 文件 1：768 条（shuffle_0_0.spill）
  │  ├─ 磁盘 spill 文件 2：832 条（shuffle_0_1.spill）
  │  └─ 内存中：400 条（未 spill）
  │
  └─ 开始 merge-sort-combine：
     ├─ 打开 spill 文件 shuffle_0_0.spill 的迭代器
     ├─ 打开 spill 文件 shuffle_0_1.spill 的迭代器
     ├─ 加上内存中 400 条的迭代器
     ├─ 使用 **3-way merge**（k=3）逐个比较
     ├─ 相同 key 则使用 reduceByKey 的 func 进行聚合
     └─ 最终输出已聚合且已排序的数据

Step 8: 写入最终文件
  ├─ 将 merge 后的数据按 partition 边界分割
  ├─ 写入 shuffle_0_0.data（约 180MB，包含所有 2000 条聚合后的数据）
  ├─ 记录每个 partition 的 offset 到 index 文件
  └─ 原子提交：重命名临时文件为正式文件
```

**总结**：

- **总 records**：2000 条
- **Spill 次数**：2 次（生成 2 个 spill 文件）
- **Merge 方式**：3-way merge（2 个 spill 文件 + 1 份内存数据）
- **触发条件**：每次检查点（32 的倍数）且内存 > 100MB 阈值


**对比理解**：

| 场景 | Records | Spill 次数 | Merge 方式 | 适用情况 |
|------|---------|-----------|-----------|---------|
| 小数据 | 1000 条 | 1 次 | 2-way merge | 内存适中，大部分数据可缓存 |
| 本示例 | 2000 条 | 2 次 | 3-way merge | 数据量大，需要多次 spill |
| 大数据 | 10000+ 条 | 10+ 次 | k-way merge | 超大数据集，频繁 spill |

**关键观察**：

1. **Spill 不是每次都触发**：只有检查点（32 的倍数）且内存超阈值才会触发
2. **最后一次通常不 spill**：剩余数据往往小于阈值，直接留在内存
3. **Merge 时统一聚合**：即使分多次 spill，最终在 merge 时统一 combine，结果正确性不受影响

---

#### 3.6.5 性能对比：何时选择 ExternalSorter

```
场景 1：数据量小（≤ 50MB）
  ├─ 全部在内存中，无需 Spill
  ├─ 只进行一次排序
  ├─ ExternalSorter 性能 = Map 排序 ✓
  └─ 开销：最小

场景 2：数据量中等（50-500MB）
  ├─ 可能需要 1-3 次 Spill
  ├─ 进行少量的 merge-sort
  ├─ ExternalSorter 性能 = 多次排序 + 少量 merge
  └─ 开销：中等

场景 3：数据量大（> 500MB）
  ├─ 需要 5+ 次 Spill
  ├─ 进行复杂的 k-way merge
  ├─ merge-sort-combine 成为主要开销
  └─ 开销：显著（可能比内存排序慢 2-3 倍）

性能对比（100GB reduceByKey）：
─────────────────────────────────────
场景          内存      Spill 次数   耗时(相对)
─────────────────────────────────────
内存充足      1GB       0 次        1.0x
正常场景      512MB     15 次       2.5x
内存紧张      256MB     40 次       5.0x
─────────────────────────────────────
```

---

## 3.7 ShuffleExternalSorter 详解（Unsafe 的优化引擎）

**ShuffleExternalSorter** 是 UnsafeShuffleWriter 的核心组件，类似于 Base 模式中的 ExternalSorter，但专为**序列化二进制数据**设计。它在 Base 模式的基础上进行了深度优化，使用 Tungsten 内存页和压缩指针，从而大幅降低内存开销和提升排序性能。

#### 3.7.1 核心职责

| 功能 | 说明 |
|------|------|
| **内存页管理** | 使用 Tungsten 堆外内存页存储序列化数据 |
| **指针数组维护** | 维护 PackedRecordPointer 数组（每个 record 8 字节） |
| **排序** | 对指针数组排序（按 partition ID），不移动实际数据 |
| **Spill 触发** | 内存满时将排序后的数据写入磁盘 spill 文件 |
| **Merge** | 多路归并多个 spill 文件和内存数据 |

#### 3.7.2 内存结构对比

**Base 模式 vs Unsafe 模式**：

```
ExternalSorter（Base）                    ShuffleExternalSorter（Unsafe）
┌─────────────────────────────┐          ┌─────────────────────────────┐
│  PartitionedAppendOnlyMap   │          │  Memory Pool (Tungsten)     │
│  ├─ keys[]: Java objects    │          │  ├─ Page 0: 128MB           │
│  ├─ values[]: Java objects  │          │  ├─ Page 1: 128MB           │
│  └─ 开放寻址 HashMap        │          │  └─ ...                     │
│                             │          │                             │
│  每条 record: 120+ 字节     │          │  序列化数据存储在页中        │
│  GC 压力大                  │          │  不受 GC 影响               │
└─────────────────────────────┘          └─────────────────────────────┘
           │                                        │
           ▼                                        ▼
┌─────────────────────────────┐          ┌─────────────────────────────┐
│  排序：交换 Java 对象引用     │          │  PackedRecordPointer[]      │
│  比较：对象解引用 + key 比较  │          │  ├─ 8 字节/record           │
│                             │          │  ├─ [24bit partId]          │
│  对象在堆上随机分布           │          │  ├─ [13bit pageNum]         │
│  CPU cache 不友好            │          │  └─ [27bit offset]          │
│                             │          │                             │
│  排序过程大量 cache miss     │          │  排序：只交换 8 字节指针     │
│                             │          │  比较：直接比较压缩指针高 24位│
│                             │          │                             │
│                             │          │  指针数组紧凑，cache 友好    │
└─────────────────────────────┘          └─────────────────────────────┘
```

#### 3.7.3 核心流程

```scala
// ShuffleExternalSorter.java 简化版
class ShuffleExternalSorter {

  // 1. 插入 record
  def insertRecord(recordBytes: Array[Byte], partitionId: Int): Unit = {
    // 申请 Tungsten 内存页空间
    val (pageNum, offset) = allocateSpace(recordBytes.length)

    // 写入序列化数据到堆外内存
    Platform.copyMemory(recordBytes, baseOffset,
                        memoryPages(pageNum), offset,
                        recordBytes.length)

    // 创建压缩指针并加入数组
    val pointer = PackedRecordPointer.packPointer(pageNum, offset, partitionId)
    pointerArray.add(pointer)

    // 检查是否需要 spill
    if (shouldSpill()) {
      spill()
    }
  }

  // 2. Spill 到磁盘（类似 Base 模式的 spill 机制）
  def spill(): Unit = {
    // 对指针数组排序（按 partitionId）
    pointerArray.sortBy(p => PackedRecordPointer.getPartitionId(p))

    // 按排序顺序读取数据并写入 spill 文件
    val spillFile = createSpillFile()
    for (pointer <- pointerArray) {
      val (pageNum, offset, length) = unpackPointer(pointer)
      val data = readFromPage(pageNum, offset, length)
      spillFile.write(data)
    }

    // 清空指针数组，释放内存页
    pointerArray.clear()
    memoryPages.release()
  }

  // 3. 最后合并所有 spill 文件（k-way merge）
  def mergeAndWrite(outputFile: File): Unit = {
    // 创建所有 spill 文件的迭代器
    val spillIterators = spillFiles.map(f => new SpillFileIterator(f))

    // 加上内存中的数据迭代器
    val memoryIterator = new PointerArrayIterator(pointerArray)

    // k-way merge（按 partitionId 合并，不进行 combine）
    val merger = new UnsafeShuffleMergeIterator(
      spillIterators ++ Seq(memoryIterator)
    )

    // 写入最终输出文件
    for (record <- merger) {
      outputFile.write(record)
    }
  }
}
```

#### 3.7.4 关键设计决策

| 设计 | 原因 | 收益 |
|------|------|------|
| **序列化数据存堆外** | 避免 GC 移动对象 | 指针稳定、低 GC 压力 |
| **8 字节压缩指针** | 64 位长整型足够编码位置 | 排序快、cache 友好 |
| **先排序指针，再读数据** | 避免频繁移动大块序列化数据 | I/O 局部性好 |
| **16M partition 限制** | 24 bit 最多表示 2^24-1 | 覆盖绝大多数场景 |

#### 3.7.5 与 ExternalSorter 的本质区别

| 维度 | ExternalSorter（Base） | ShuffleExternalSorter（Unsafe） |
|------|------------------------|--------------------------------|
| **数据形态** | 反序列化后的 Java K-V 对象 | 序列化后的二进制字节数组 |
| **内存位置** | JVM 堆内 | Tungsten 堆外内存页 |
| **排序对象** | Java 对象引用（交换对象引用） | 8 字节压缩指针（交换长整型） |
| **比较方式** | 对象解引用 + key 比较 | 直接比较指针高 24 位（partitionId）|
| **聚合能力** | ✓ 支持（changeValue） | ❌ 不支持（数据已序列化） |
| **Spill 机制** | 排序 Map 后写入文件 | 排序指针数组后按顺序读写 |
| **Merge 方式** | k-way merge with combine | k-way merge without combine |
| **适用场景** | reduceByKey、combineByKey、sortByKey | groupByKey、repartition（无聚合） |

#### 3.7.6 Unsafe 优化的性能优势

**为什么 Unsafe 更快**：

1. **排序对象更小**：8 字节指针 vs 120+ 字节对象
   - 更少的内存占用 → 更好的 CPU cache 命中率
   - 每次交换只移动 8 字节 → 排序速度提升 10 倍

2. **无 GC 压力**
   - Base：频繁创建 Java 对象 → 触发 GC → 暂停应用
   - Unsafe：序列化数据在堆外 → GC 无法触及

3. **内存利用率更高**
   - 相同堆内存下，Unsafe 能处理更多数据
   - Base 可能需要扩大堆内存才能达到相同吞吐

**性能对比**：

```
场景：处理 100GB 数据，reduceByKey

Base 模式（ExternalSorter）：
  ├─ 内存占用：每条记录 120 字节
  ├─ 堆内存需求：> 10GB（防止频繁 spill）
  ├─ GC 暂停：100+ ms，可能达到 1s+
  └─ 吞吐：~50MB/s

Unsafe 模式（ShuffleExternalSorter）：
  ├─ 内存占用：每条记录 8 字节指针
  ├─ 堆内存需求：可在 2GB 内完成
  ├─ GC 暂停：< 10ms（堆外数据不触发 GC）
  └─ 吞吐：~500MB/s（10 倍性能提升）
```

#### 3.7.7 对比：ExternalSorter vs ShuffleExternalSorter

现在我们已经详细了解了两个引擎的工作原理，让我们用一个综合表格来对标它们的关键差异：

| 维度 | ExternalSorter（Base） | ShuffleExternalSorter（Unsafe） |
|------|------------------------|--------------------------------|
| **操作对象** | 反序列化后的 Java K-V 对象 | 序列化二进制数据 |
| **内存位置** | JVM 堆内 | Tungsten 堆外内存页 |
| **Buffer 类型** | PartitionedAppendOnlyMap / PartitionedPairBuffer | 指针数组 + Tungsten 内存页 |
| **排序对象** | Java 对象引用 | 8 字节压缩指针 |
| **排序效率** | 对象比较，可能涉及 GC | 二进制比较，无 GC |
| **内存占用（单条）** | 120+ 字节（对象开销） | 8 字节（压缩指针） |
| **聚合能力** | ✓ 支持 changeValue（map-side combine） | ❌ 不支持（数据已序列化） |
| **Spill 机制** | 排序 Map 后写入文件 | 排序指针数组后按顺序读写 |
| **Merge 方式** | k-way merge with combine | k-way merge without combine |
| **性能特点** | 对象多 → GC 压力大、排序慢 | 指针小 → GC 压力低、排序快 |
| **适用场景** | reduceByKey、combineByKey、sortByKey | groupByKey、repartition（无聚合） |
| **何时选择** | 数据量小、需要聚合 | 数据量大、无聚合需求 |

---

## 四、Shuffle Read（Reduce 端）

### 4.1 reduceByKey 的 Shuffle 流程

首先看一下 reduceByKey 的逻辑执行图：

Shuffle Read 是指 Reduce Task 从多个 Map Task 的输出中读取属于自己的数据的过程。这是 Shuffle 阶段的第二部分，通常包含三个关键步骤：

1. **数据拉取（Fetch）**：从远程节点拉取 Shuffle 数据块
2. **数据反序列化**：将字节流转换回 Key-Value 对
3. **数据聚合/排序**：根据操作需求进行聚合或排序

**为什么需要这样设计？**

- **分阶段处理**：避免一次性将所有数据加载到内存，造成 OOM
- **流式处理**：通过迭代器模式，边拉取边处理数据
- **灵活的操作**：不同的 Transformation 可以选择性地进行聚合或排序

**整体流程图**：

![reduceByKeyStage](figures/reduceByKeyStage.png)

**说明**：
- Reduce Task 通过 `ShuffleReader` 接口统一数据读取
- 具体实现：`BlockStoreShuffleReader`（从本地或远程 Block Store 读取）
- 内部使用 `ShuffleBlockFetcherIterator` 拉取数据
- 最后根据需求进行聚合或排序

**完整示例：reduceByKey 的 Shuffle Read 过程**

以 `rdd.reduceByKey(_ + _)` 为例，从 Stage 级别和 Record 级别看整个过程：

**Stage 级别流程**：

![reduceByKeyStage](figures/reduceByKeyStage.png)

```
第一个 Stage（Map 端）：
  data → ParallelCollectionRDD → MapPartitionsRDD
           （map-side combine：每个 partition 独立进行部分聚合）

通过 ShuffleDependency 传输：
  数据按 key 被分发到不同的 Reduce Task

第二个 Stage（Reduce 端）：
  ShuffledRDD → MapPartitionsRDD
  （reduce-side aggregate：合并多个 Map 输出的聚合结果）
```

**Record 粒度流程**：

![reduceByKeyRecord](figures/reduceByKeyRecord.png)

```
Reduce 端收到数据后的处理：
  1. ShuffleBlockFetcherIterator 流式拉取来自多个 Map Task 的数据块
  2. 每个 block 反序列化为 (Key, Value) 对
  3. AppendOnlyMap 进行增量式聚合：
     - (K1, 10) 进入 → map[K1] = 10
     - (K1, 5)  进入 → map[K1] = 10 + 5 = 15
     - (K2, 20) 进入 → map[K2] = 20
     - (K1, 3)  进入 → map[K1] = 15 + 3 = 18
  4. 最终输出已聚合的结果
```

这个设计的优势：
- **流式处理**：不需要一次性将所有数据加载到内存
- **增量聚合**：相同 Key 的数据一到达就立即合并，减少内存占用
- **适应大数据**：通过 AppendOnlyMap 的溅出机制，支持任意大的数据集

---

### 4.2 BlockStoreShuffleReader：Shuffle Read 的协调者

**什么是 BlockStoreShuffleReader？**

`BlockStoreShuffleReader` 是 `ShuffleReader` 接口的具体实现，负责整个 Shuffle Read 过程的协调。它是 Reduce Task 获取 Shuffle 数据的入口点。

**为什么需要它？**

- **统一接口**：提供统一的数据读取接口，隐藏底层复杂性
- **流程协调**：组织四个关键步骤（拉取→反序列化→聚合→排序）
- **配置灵活**：支持多种数据格式、聚合器、排序器

**核心设计：四步流程**

```scala
// BlockStoreShuffleReader.scala
private[spark] class BlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext)
  extends ShuffleReader[K, C] {

  override def read(): Iterator[Product2[K, C]] = {
    // 第 1 步：创建 ShuffleBlockFetcherIterator，负责远程数据拉取
    // 这是一个流式迭代器，支持背压控制，不会一次性加载所有数据
    val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
      serializerManager.wrapStream,
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
      ...
    )

    // 第 2 步：反序列化流中的数据，将字节流转换为 Key-Value 对
    val recordIter = wrappedStreams.flatMap { case (blockId, wrappedStream) =>
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // 第 3 步：如果需要聚合，在 Reduce 端进行聚合
    // 这里支持两种聚合模式：
    //   - 如果 Map 端已经做过 combine（mapSideCombine=true），合并多个 Combiner
    //   - 如果 Map 端未做 combine，直接聚合所有 values
    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // 第 4 步：如果需要排序（如 sortByKey），使用 ExternalSorter
    // 这里的 ExternalSorter 支持内存溢写到磁盘，处理超大数据集
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        val sorter = new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAll(aggregatedIter)
        sorter.iterator
      case None =>
        aggregatedIter
    }
  }
}
```

**关键点**：
- 四步操作形成一个流水线，支持流式处理（惰性求值）
- 每一步都是可选的，根据实际需求启用或跳过
- 充分利用了迭代器模式，内存占用可控

---

### 4.3 ShuffleBlockFetcherIterator：数据拉取的背压控制

**什么是 ShuffleBlockFetcherIterator？**

`ShuffleBlockFetcherIterator` 是一个流式迭代器，负责从远程节点拉取 Shuffle 数据块。它的核心设计是**背压控制**（backpressure control），即控制同时在途的数据量，避免 Reduce Task 内存溢出。

**为什么需要背压控制？**

假设一个 Reduce Task 需要从 1000 个 Map Task 拉取数据：
- ❌ 错误做法：同时发起 1000 个请求，可能导致网络拥塞和内存爆炸
- ✅ 正确做法：限制同时在途的数据量（如 48MB），当有数据完成消费后，再拉取新数据

**核心设计：`maxBytesInFlight` 限制**

```scala
// ShuffleBlockFetcherIterator.scala
private[spark] final class ShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: ShuffleClient,
    blockManager: BlockManager,
    blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,      // 默认 48MB，控制背压
    maxReqsInFlight: Int,        // 最多同时发起的请求数
    maxBlocksInFlightPerAddress: Int,  // 每个地址最多的待处理 block 数
    maxReqSizeShuffleToMem: Long,
    detectCorrupt: Boolean)
  extends Iterator[(BlockId, InputStream)] {

  // 核心算法：背压控制
  // 维护一个 fetchRequests 队列，存放待拉取的数据块
  // bytesInFlight 记录当前在途数据的大小
  private def fetchUpToMaxBytes(): Unit = {
    // 只有在以下条件都满足时才发起新请求：
    // 1. 还有待拉取的数据块
    // 2. 当前在途数据量 < maxBytesInFlight
    // 3. 当前请求数 < maxReqsInFlight
    while (fetchRequests.nonEmpty && bytesInFlight < maxBytesInFlight) {
      val request = fetchRequests.dequeue()
      shuffleClient.fetchBlocks(...)
      bytesInFlight += request.size  // 记录新增的在途数据量
    }
  }

  // 当消费完一个 block 的数据后，更新在途数据量
  // 这会触发新的 fetchUpToMaxBytes() 调用
  override def next(): (BlockId, InputStream) = {
    ...
    bytesInFlight -= completedBlock.size
    fetchUpToMaxBytes()  // 尝试拉取更多数据
    ...
  }
}
```

**背压控制的效果**：

```
时间轴：
  T1: 拉取 0-48MB 的数据（发起多个请求）→ bytesInFlight = 48MB
  T2: 消费完 10MB → bytesInFlight = 38MB → 拉取新数据补充到 48MB
  T3: 消费完 15MB → bytesInFlight = 33MB → 拉取新数据补充到 48MB
  ...
  Tn: 所有数据消费完毕

效果：内存占用始终在 48MB 左右，不会突增
```

**相关配置**：
- `spark.reducer.maxSizeInFlight`：在途数据的最大字节数（默认 48MB）
- `spark.reducer.maxReqsInFlight`：最多同时请求数（默认10）
- `spark.shuffle.maxBlocksInFlightPerAddress`：每个地址的最多待处理块数（默认10）

---

### 4.4 数据聚合：AppendOnlyMap 和 ExternalAppendOnlyMap

**什么是数据聚合？**

Reduce 端数据聚合是指将来自多个 Map Task 的相同 Key 的 Value 合并到一起。例如 `reduceByKey(+)` 需要对相同 Key 的所有数值求和。

**为什么需要特殊的数据结构？**

- ❌ 使用普通 HashMap：浪费空间（指针开销）、排序困难、GC 压力大
- ✅ 使用 AppendOnlyMap：紧凑存储、支持原地排序、内存效率高

**两层设计：AppendOnlyMap + ExternalAppendOnlyMap**

**第一层：AppendOnlyMap（内存聚合）**

Spark 使用 `AppendOnlyMap` 进行内存中的高效聚合。它采用**紧凑数组 + 开放寻址法 + 二次探测**的设计：
- 一块连续内存存储 (Key, Value) 对
- 利用率达到 70% 时扩容 2 倍
- 支持原地排序（不需要额外内存）

这个数据结构相比普通 HashMap 有显著的性能优势（内存效率、Cache 局部性、GC 压力），详见附录 D。

**第二层：ExternalAppendOnlyMap（溢写聚合）**

当内存不足时，AppendOnlyMap 会 spill 到磁盘，使用 merge-sort-combine 处理：

![ExternalAppendOnlyMap](figures/ExternalAppendOnlyMap.png)

**说明**：
- **内存部分**：当前的 AppendOnlyMap
- **磁盘部分**：多个已排序的 spilledMap 文件（每个文件内按 Key 排序）
- **合并算法**：使用优先队列（mergeHeap）按 Key 顺序读取所有数据块
- **StreamBuffer**：缓存同一 Key 的相邻 records，支持增量式 combine

**工作流程**：

```
场景：Reduce Task 需要聚合 1 亿条记录，内存只有 100MB

初始状态：
  ┌─ AppendOnlyMap（内存，50MB）
  ├─ spilledMap_0（磁盘，已排序，K1-K3）
  ├─ spilledMap_1（磁盘，已排序，K2-K4）
  └─ spilledMap_2（磁盘，已排序，K3-K5）

聚合过程：
  mergeHeap 获取所有来源中最小的 Key，如 K1
  ↓
  从所有数据源（内存 + 磁盘）读取 K1 的所有 values
  ↓
  使用 combine 函数聚合这些 values
  ↓
  输出 (K1, aggregated_value)
  ↓
  重复上述过程直到所有数据处理完毕

优点：
  - 内存占用恒定（只需要存放当前处理 Key 的 values）
  - 磁盘 I/O 有序且可预测
  - 支持无限大的数据集
```

**相关配置**：
- `spark.shuffle.spill`：是否启用溅出（默认 true）
- `spark.shuffle.memoryFraction`：Shuffle 使用的堆内存比例（默认 0.2）

---

## 五、典型 Transformation 的 Shuffle 行为

### 5.1 reduceByKey

```scala
// 流程：Map 端 combine + Shuffle + Reduce 端 combine
rdd.reduceByKey(_ + _)

// map 端：使用 ExternalSorter + PartitionedAppendOnlyMap 进行预聚合
// shuffle：按 key 分区
// reduce 端：合并所有 map 输出后再次聚合
```

**Record 粒度的处理流程**：

```
Map 端：
  record → HashMap.aggregate(func) → spill to disk → merge-sort

Shuffle：
  partition → fetch blocks → deserialize

Reduce 端：
  record → HashMap.aggregate(func) → final result
```

### 5.2 groupByKey

```scala
// 无 map 端 combine，直接 shuffle
rdd.groupByKey()

// map 端：不进行 combine，直接写入 buffer
// shuffle：按 key 分区
// reduce 端：将所有 values 收集到 ArrayBuffer
```

**注意**：`groupByKey` 没有 map 端 combine，可能导致大量数据传输。

### 5.3 sortByKey

```scala
// 需要 shuffle + 排序
rdd.sortByKey()

// map 端：不 combine
// shuffle：按 RangePartitioner 分区
// reduce 端：使用 ExternalSorter 按 key 排序
```

### 5.4 cogroup

```scala
// 多个 RDD 的 cogroup
rdd1.cogroup(rdd2)

// 使用 CoGroupedRDD，所有 ShuffleDependency 共用一个 HashMap
// HashMap value 为 Array[ArrayBuffer]，每个 ArrayBuffer 对应一个 RDD 的 values
```

---

## 六、内存管理

### 6.1 内存分配策略

Spark 使用 `MemoryManager` 管理 shuffle 内存：

```scala
// StaticMemoryManager.scala
private def getMaxExecutionMemory(conf: SparkConf): Long = {
  val systemMaxMemory = Runtime.getRuntime.maxMemory
  val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
  val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)
  (systemMaxMemory * memoryFraction * safetyFraction).toLong
}
```

**默认配置**：
- `spark.shuffle.memoryFraction = 0.2`（20% 的堆内存）
- `spark.shuffle.safetyFraction = 0.8`（实际使用 16% 的堆内存）

### 6.2 内存共享机制

Executor 上**所有并发运行的 task 共享** shuffle 内存：

```scala
// ShuffleMemoryManager (已废弃，逻辑整合到 MemoryManager)
// 每个 task 通过 TaskMemoryManager 申请内存
// 当总使用量超过限制时，触发 spill
```

### 6.3 Spill 触发条件

```scala
// Spillable.scala
protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
  var shouldSpill = false
  if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
    val granted = acquireMemory(currentMemory * 2 - myMemoryThreshold)
    shouldSpill = granted < currentMemory * 2 - myMemoryThreshold
  }
  shouldSpill
}
```

**触发条件**：
1. 每插入 32 条记录检查一次
2. 当前内存使用量超过阈值
3. 无法申请到足够的扩展内存

---

## 七、关键配置参数

### 7.1 Shuffle Write 配置

| 配置 | 默认值 | 说明 |
|-----|-------|------|
| `spark.shuffle.manager` | `sort` | ShuffleManager 实现，2.x 只有 SortShuffleManager |
| `spark.shuffle.sort.bypassMergeThreshold` | 200 | 使用 BypassMergeSort 的 partition 阈值 |
| `spark.shuffle.file.buffer` | 32k | 写磁盘的缓冲区大小 |
| `spark.shuffle.compress` | true | 是否压缩 shuffle 输出 |
| `spark.shuffle.spill.compress` | true | 是否压缩 spill 文件 |

### 7.2 Shuffle Read 配置

| 配置 | 默认值 | 说明 |
|-----|-------|------|
| `spark.reducer.maxSizeInFlight` | 48m | 同时 fetch 的最大数据量 |
| `spark.reducer.maxReqsInFlight` | Int.MaxValue | 同时进行的 fetch 请求数 |
| `spark.reducer.maxBlocksInFlightPerAddress` | Int.MaxValue | 每个地址的最大并发 block 数 |
| `spark.shuffle.detectCorrupt` | true | 是否检测损坏的 shuffle block |

### 7.3 内存配置

| 配置 | 默认值 | 说明 |
|-----|-------|------|
| `spark.shuffle.memoryFraction` | 0.2 | Shuffle 内存占比（StaticMemoryManager） |
| `spark.shuffle.safetyFraction` | 0.8 | 安全系数 |
| `spark.shuffle.spill.numElementsForceSpillThreshold` | 1G | 强制 spill 的元素数阈值 |
| `spark.memory.fraction` | 0.6 | UnifiedMemoryManager 总内存占比 |
| `spark.memory.storageFraction` | 0.5 | Storage 内存占比 |

### 7.4 外部 Shuffle Service

| 配置 | 默认值 | 说明 |
|-----|-------|------|
| `spark.shuffle.service.enabled` | false | 是否启用外部 Shuffle Service |
| `spark.shuffle.service.port` | 7337 | 外部 Shuffle Service 端口 |

---

## 八、与 Hadoop MapReduce Shuffle 的对比

### 8.1 架构对比

| 维度 | Hadoop MapReduce | Spark Sort Shuffle |
|-----|-----------------|-------------------|
| **排序策略** | 强制排序 | 可选排序（sortByKey 除外） |
| **combine 时机** | map 端 + reduce 端 | map 端 + reduce 端 |
| **数据组织** | 环形缓冲区 → sort → spill → merge | 内存结构 → sort → spill → merge |
| **文件数量** | map 输出：M × R 个文件（合并后减少） | map 输出：2 × M 个文件 |

### 8.2 Reduce 函数差异

**MapReduce**：
```java
// 一批 values 一次性处理
void reduce(K key, Iterable<V> values, Context context) {
    result = process(key, values);  // 可以自定义任意逻辑
    context.write(key, result);
}
```

**Spark**：
```scala
// 增量式处理，必须是 commulative 的
def reduce(key: K, values: Iterable[V]): C = {
    var result: C = null
    for (value <- values) {
        result = func(result, value)  // 如：result = result + value
    }
    result
}
```

### 8.3 内存使用对比

| 组件 | MapReduce | Spark |
|-----|-----------|-------|
| Map 输出缓冲 | 环形缓冲区（默认 100MB） | PartitionedAppendOnlyMap / 内存页 |
| Spill | 排序后 spill | 排序后 spill |
| Reduce 缓冲 | 内存 + 磁盘 merge | softBuffer + ExternalAppendOnlyMap |
| Combine | 可以复用排序结果 | HashMap 直接 combine |

---

## 九、最佳实践

### 9.1 优化 Shuffle 性能

1. **减少 Shuffle 数据量**
   - 使用 `reduceByKey` 而非 `groupByKey`
   - 在 shuffle 前过滤不必要的数据

2. **调整内存配置**
   - 增加 `spark.shuffle.memoryFraction`（使用 StaticMemoryManager 时）
   - 或使用 UnifiedMemoryManager（Spark 1.6+ 默认）

3. **调整并行度**
   - 合理设置 partition 数量
   - 避免小文件过多

4. **使用外部 Shuffle Service**
   - 启用 `spark.shuffle.service.enabled`
   - 减少 executor 压力，支持动态资源分配

### 9.2 常见问题排查

1. **OOM in Shuffle**
   - 检查 `spark.shuffle.memoryFraction`
   - 减小 `spark.reducer.maxSizeInFlight`
   - 检查是否有数据倾斜

2. **Shuffle 慢**
   - 检查网络带宽
   - 调整 `spark.shuffle.file.buffer`
   - 启用压缩

3. **磁盘空间不足**
   - 检查 spill 文件清理
   - 增加磁盘空间或使用 SSD

---

## 十、总结

Spark 2.x 的 Sort-based Shuffle 相比早期版本的 Hash-based Shuffle 有以下优势：

1. **文件数量大幅减少**：从 M×R 降至 2×M
2. **内存管理更精细**：统一的 MemoryManager 管理
3. **Tungsten 优化**：UnsafeShuffleWriter 直接操作序列化二进制数据
4. **灵活性**：三种写方案适配不同场景

理解 Shuffle 机制对于 Spark 性能调优至关重要，建议结合具体业务场景选择合适的配置和优化策略。

---

## 附录 A：FileConsolidation 并发设计详解

### 问题背景

在 Hash Shuffle FileConsolidation 中，为什么选择**"同一 core 连续执行的 task 共用文件"**而不是**"所有 cores 共用一个文件"**？这个决策涉及并发控制和 I/O 性能。

### 方案对比

#### 方案 1：所有 cores 共用一个输出文件（❌ 不可行）

```
Executor （假设 4 个 cores）
│
├─ Core 1: Task 0 写入 Partition 0、1、2 的数据
├─ Core 2: Task 1 并发写入 Partition 0、1、2 的数据
├─ Core 3: Task 2 并发写入 Partition 0、1、2 的数据
└─ Core 4: Task 3 并发写入 Partition 0、1、2 的数据
           ↓
        共用 data_file.txt

问题：
✗ 4 个线程并发写入同一文件
✗ 文件指针位置冲突（Race Condition）
✗ Task 0 和 Task 1 的数据可能交错混乱
✗ 无法准确定位每个 FileSegment 的边界
✗ 需要频繁加锁，引入严重的同步开销
```

**并发写入的具体困境**：

| 场景 | 问题描述 |
|-----|--------|
| **文件指针冲突** | Core 1 刚获得文件位置是 offset=1000，准备写入 500 字节。但在写入前，Core 2 已经把 800 字节数据写入到 offset=1000，导致 Core 1 的数据覆盖或交错。|
| **FileSegment 边界混乱** | FileSegment 需要精确记录起始和结束位置（例如 Partition 0：offset 100-300，Partition 1：offset 300-600）。但多线程并发追加时，边界无法确定。|
| **加锁的性能开销** | 每次写入都需要加锁确保原子性。假设每个 Task 追加 1000 次小数据块，那么需要 1000 × 4 = 4000 次加锁/解锁，这在高并发下极其昂贵。|
| **数据一致性** | 如果 Task 0 正在写入 Partition 0 的数据，同时 Task 1 也在写入 Partition 0 的数据，Reduce Task 读取时无法区分哪些字节属于哪个 MapTask。|

#### 方案 2：同一 core 顺序执行，共用文件（✅ Spark 的选择）

```
Executor 的任务调度器（TaskScheduler）
│
├─ Core 1: 顺序执行
│  ├─ Task 0 (串行) → 打开 core1_data_file.txt
│  │  ├─ 写入 Partition 0 数据：offset 0 - 100     ← FileSegment 0
│  │  ├─ 写入 Partition 1 数据：offset 100 - 200
│  │  └─ 写入 Partition 2 数据：offset 200 - 300
│  │
│  ├─ Task 4 (串行) → 继续使用 core1_data_file.txt
│  │  ├─ 写入 Partition 0 数据：offset 300 - 400   ← FileSegment 1（自动追加）
│  │  ├─ 写入 Partition 1 数据：offset 400 - 500
│  │  └─ 写入 Partition 2 数据：offset 500 - 600
│
├─ Core 2: 顺序执行
│  ├─ Task 1 (串行) → 打开 core2_data_file.txt
│  │  └─ 写入 Partition 0、1、2 的数据：offset 0 - 300
│  │
│  └─ Task 5 (串行) → 继续使用 core2_data_file.txt
│     └─ 写入 Partition 0、1、2 的数据：offset 300 - 600
│
├─ Core 3: ...
└─ Core 4: ...


优点：
✓ 同一时刻只有一个线程操作该文件
✓ 无需加锁，无线程竞争
✓ 文件指针自然增长，位置清晰
✓ FileSegment 边界精确可控
✓ 零同步开销，最高性能
```

### 文件写入的时间轴

**方案 1：所有 cores 共用一个文件（需要频繁加锁）**

```
T0: 初始状态，文件长度 = 0

T1: Core 1 获得文件锁，写入 1000 字节
    ├─ 锁定文件
    ├─ 写入 [0, 1000)
    ├─ 释放锁
    └─ 文件长度: 0 → 1000

T2: Core 2 等待锁（锁竞争）...获得文件锁，写入 1500 字节
    ├─ 锁定文件（等了多少时间？）
    ├─ 写入 [1000, 2500)
    ├─ 释放锁
    └─ 文件长度: 1000 → 2500

T3: Core 3 等待锁...获得文件锁，写入 800 字节
    ├─ 锁定文件（又等了）
    ├─ 写入 [2500, 3300)
    ├─ 释放锁
    └─ 文件长度: 2500 → 3300

T4: Core 4 等待锁...
    ├─ 问题：Core 1、2、3 都在竞争这个锁，效率低下
    ├─ 上下文切换频繁，缓存失效
    └─ 性能严重下降
```

**方案 2：同一 core 顺序执行（无需加锁）**

```
T0: 初始状态

T1: Core 1 执行 Task 0，写入 1000 字节
    ├─ 无需等待（没有竞争）
    ├─ 直接写入 [0, 1000)
    └─ 文件长度: 0 → 1000

T2: Core 1 继续执行 Task 4（前面 Task 0 已完成），写入 1500 字节
    ├─ 无需等待（同一线程顺序操作）
    ├─ 直接写入 [1000, 2500)
    └─ 文件长度: 1000 → 2500

T3: Core 1 完成，文件写入完毕
    ├─ 总耗时 = T2 - T0
    ├─ 零锁竞争
    └─ 最高性能


（与此同时）

T1: Core 2 执行 Task 1，写入 1000 字节
    ├─ 独立的文件，无竞争
    ├─ 直接写入 [0, 1000)
    └─ 文件长度: 0 → 1000

T2: Core 2 继续执行 Task 5，写入 1500 字节
    ├─ 继续使用同一个文件
    ├─ 直接写入 [1000, 2500)
    └─ 文件长度: 1000 → 2500
```

### FileSegment 位置定位

**方案 1 的困境**：

```
假设需要记录 FileSegment 的位置信息：

Partition 0:
  FileSegment from Task 0: [0, 100)  ← Task 0 的 Partition 0
  FileSegment from Task 1: [100, 200) ← Task 1 的 Partition 0  // 但哪些字节真的是 Task 1 的？

问题：多线程交错写入，无法确定 Task 1 的数据从哪里开始、到哪里结束
```

**方案 2 的优势**：

```
同一 core 顺序执行，清晰的边界：

Core 1 的文件：
  Task 0 的 Partition 0: [0, 100)        ← 精确
  Task 0 的 Partition 1: [100, 200)
  Task 0 的 Partition 2: [200, 300)

  Task 4 的 Partition 0: [300, 400)      ← 精确（紧接着 Task 0 之后）
  Task 4 的 Partition 1: [400, 500)
  Task 4 的 Partition 2: [500, 600)

FileSegment 信息非常清晰，无需加锁记录
```

### 性能对比

在 MapReduce 和 Spark 的实际测试中，单线程顺序 I/O 往往比多线程竞争 I/O 快 **5 - 10 倍**：

| 指标 | 方案 1（共用文件+加锁） | 方案 2（core 级别的文件） |
|-----|-----------------|-------------------|
| **锁竞争** | 严重（每次 I/O 都竞争） | 无（顺序执行） |
| **线程切换** | 频繁 | 无 |
| **缓存失效** | 多线程导致缓存抖动 | 单线程，缓存热度高 |
| **文件指针跟踪** | 需要原子操作确保位置正确 | 简单的指针增长 |
| **吞吐量** | 较低（锁竞争开销） | 更高（无竞争） |
| **实现复杂度** | 高（需要精细的同步控制） | 低（顺序即可） |

### 为什么 Sort Shuffle 完全避免了这个问题

```
Sort Shuffle：每个 Task 产生独立的 data + index 文件

Task 0 → shuffle_0_0.data + shuffle_0_0.index
Task 1 → shuffle_0_1.data + shuffle_0_1.index
Task 2 → shuffle_0_2.data + shuffle_0_2.index
Task 3 → shuffle_0_3.data + shuffle_0_3.index

优势：
✓ 完全避免竞争（不同 task 的文件完全独立）
✓ 无需任何加锁机制
✓ 与 core 数无关（每个 task 有自己的文件）
✓ 代码实现最简洁
```

### 总结

FileConsolidation 限制在**同一 core 的原因**：

1. **避免并发冲突**：同一时刻只有一个线程操作该文件
2. **性能最优**：无锁竞争、无上下文切换、无缓存抖动
3. **实现简洁**：顺序执行天然保证一致性，无需复杂的同步机制
4. **可靠性高**：FileSegment 位置精确可控

相比之下，Sort Shuffle 进一步优化，让**每个 task 有自己的文件**，彻底避免竞争问题，这也是为什么 Sort Shuffle 成为 Spark 的默认 shuffle 方式。

---

## 附录 B：Shuffle 写方案选择决策详解

### 问题：为什么有三种不同的 Shuffle 写方案？

不同场景对 Shuffle 性能的需求不同，Spark 设计了三种优化方案来应对：

1. **BypassMergeSort**：追求极致速度（避免排序和合并）
2. **SerializedShuffleHandle**：追求二进制优化（减少内存和 GC）
3. **BaseShuffleHandle**：追求通用性（支持聚合、排序等所有功能）

### 关键概念澄清

#### 维度 1：Partition 数量（Count）

这是指 Shuffle 输出的分区数量，即 Reduce Task 的数量。

- **≤ 200**：小规模（同时打开 200 个文件的缓冲可以接受）
- **201 ~ 16,777,215**：中大规模（2^24 - 1 是 Serialized 模式的技术限制）
  - 为什么是 2^24？因为 `PackedRecordPointer` 用 24 bit 存储 partition ID
  - 结构：[24 bit partition ID][13 bit page number][27 bit offset in page]
- **> 16,777,215**：超大规模（技术上不支持 Serialized 模式）

#### 维度 2：聚合需求（Aggregation）

这是指是否需要进行 map-side combine（如 reduceByKey、combineByKey）。

- **无聚合**：只做分区，不合并相同 key（如 groupByKey、repartition）
- **有聚合**：需要合并相同 key 的值（如 reduceByKey、combineByKey）

### 决策流程详解

```
检查顺序：聚合 → Partition 数量 → Serializer 支持

Step 1: 是否需要聚合？
  ├─ YES（mapSideCombine = true）
  │  └─ 直接选 BaseShuffleHandle ✓
  │     理由：只有 BaseShuffleHandle 的 ExternalSorter 支持聚合
  │           Bypass 无聚合机制，Serialized 无法对二进制数据聚合
  │           (不再检查其他条件)
  │
  └─ NO（mapSideCombine = false）→ 继续 Step 2

Step 2: Partition 数量范围？
  ├─ ≤ 200
  │  └─ 选 BypassMergeSort ✓ 最快
  │     理由：无需 spill、无需 merge、无需排序
  │           只需顺序写然后拼接文件
  │           (不再检查 Serializer)
  │
  └─ > 200 → 继续 Step 3

Step 3: Partition ≤ 16M 且 Serializer 支持 relocation？
  ├─ YES
  │  └─ 选 SerializedShuffleHandle ✓ 次快
  │     理由：二进制排序避免反序列化开销
  │           压缩指针（8 字节）更省内存和 cache
  │
  ├─ NO（partition > 16M 或 serializer 不支持）
  │  └─ 选 BaseShuffleHandle ✓ 通用
  │     理由：超出 Serialized 的技术限制
  │           或 Serializer 不支持 relocation 属性
```

### 为什么每个条件下选这个而不选其他？

#### BypassMergeSort（partition ≤ 200，无聚合）

**为什么选 Bypass？**
- ✅ **无 Spill**：如果内存足够，所有数据直接写分区文件，无需溅出到磁盘
- ✅ **无 Merge**：完成后直接拼接 200 个文件，无需重新读取和合并
- ✅ **无排序**：数据直接按 partition 号写入，无需排序开销
- ✅ **最少 I/O**：只需"写分区文件 + 拼接"，共 2 次 I/O

**为什么不选 Serialized？**
- ❌ 对小 partition 无优势（二进制优化在大 partition 时才明显）
- ❌ 多了序列化、排序等额外开销，反而更慢

**为什么不选 Base？**
- ❌ 需要 spill、merge、排序，性能不如 Bypass

#### SerializedShuffleHandle（201 ~ 16M，无聚合，serializer 支持 relocation）

**为什么选 Serialized？**
- ✅ **二进制排序**：直接操作序列化数据，避免反序列化对象开销
- ✅ **压缩指针**：每条记录只占 8 字节（partition ID + 指针），cache 友好
- ✅ **高效合并**：spill 文件合并时直接拼接序列化数据，无需反序列化
- ✅ **大 partition 优势**：partition 越大优势越明显

**为什么不选 Bypass？**
- ❌ partition > 200 时同时打开 200+ 个文件，缓冲内存压力大
- ❌ 文件系统 I/O 性能下降（太多小文件）
- ❌ 磁盘寻址开销增加

**为什么不选 Base？**
- ❌ 需要反序列化到对象，比二进制排序慢
- ❌ GC 压力大（反序列化对象占内存）

#### BaseShuffleHandle（其他所有情况）

**为什么选 Base？**
- ✅ **支持聚合**：内置 ExternalSorter 支持 reduceByKey、combineByKey 等聚合
- ✅ **支持排序**：支持二次排序（sortByKey）
- ✅ **任意 serializer**：不依赖 serializer 的 relocation 属性
- ✅ **任意 partition 数**：不受 16M 限制
- ✅ **最通用**：功能最完整，适应所有场景

**为什么其他方式不适用？**
- ❌ 有聚合时 Bypass 无机制、Serialized 无法对二进制聚合
- ❌ partition > 16M 时 Serialized 技术限制（24 bit 限制）
- ❌ serializer 不支持 relocation 时 Serialized 无法使用

### 常见场景速查表

| 业务场景 | Partition | 聚合? | Serializer | 选择 | 原因 |
|--------|---------|------|-----------|------|------|
| `rdd.groupByKey()` | 默认 200 | ✗ | 任意 | Bypass | P ≤ 200 无聚合，最快 |
| `rdd.reduceByKey(f, 100)` | 100 | ✓ | 任意 | Base | 有聚合优先级最高 |
| `rdd.groupByKey(new HashPartitioner(1000))` | 1000 | ✗ | Kryo | Serialized | 1000 ≤ 16M 无聚合，二进制优化 |
| `rdd.reduceByKey(f, 1000)` | 1000 | ✓ | 任意 | Base | 有聚合，partition 数无关 |
| `df.repartition(50000)` 纯分区 | 50000 | ✗ | 任意 | Base | 50k > 16M，超出 Serialized 限制 |
| `df1.join(df2, new HashPartitioner(10000))` | 10000 | ✗ | Java (不支持) | Base | Serializer 不支持 relocation |

### 性能对比（1GB 数据）

```
场景：partition=100，无聚合，内存 500MB（需要 spill）

BypassMergeSort：
  写 100 个分区文件 + 拼接 → 耗时：最短
  I/O 次数：2 次（写 + 拼接）
  CPU：最少（无排序）
  内存：100 个缓冲，共 6.4MB

SerializedShuffleHandle：
  写 → 排序 → spill → 合并 → 写最终文件 → 耗时：中等
  I/O 次数：5 次（写 + spill 读写 + merge + 最终）
  CPU：中等（二进制排序）
  内存：精简（8 字节指针）

BaseShuffleHandle：
  写 → 排序 → spill → 合并（可能多次）→ 写最终文件 → 耗时：最长
  I/O 次数：5+ 次（取决于 spill 次数）
  CPU：最高（对象反序列化、比较）
  内存：对象在内存（占用多）
```

### 总结

| 原则 | 说明 |
|-----|------|
| **聚合为王** | 有聚合需求时，聚合的实现方式（需要反序列化对象）决定了必须用 Base |
| **Partition 递进式限制** | 200 和 16M 不是武断的，而是不同设计权衡的结果 |
| **Serializer 是可选优化** | 只在满足其他条件时才去检查 serializer 的能力 |
| **从快到通用** | Bypass（最快） → Serialized（次快） → Base（最通用） |

---


## 附录 C：AppendOnlyMap 深度理解

本附录详细解析 `ExternalSorter` 内部使用的 `AppendOnlyMap` 数据结构，揭示其设计如何支持高效的内存聚合、溅出和原地排序。

### C.1 设计目标与核心问题

**场景**：Spark Shuffle 的 Map Task 需要在内存中对大量 Key-Value 对进行聚合，最终输出为排序的键值流。

**约束**：
```
1. 内存有限，可能需要溅出到磁盘
2. 最终必须输出为已排序的迭代器（按 key 排序）
3. 要求高吞吐量，排序过程不能占用额外内存
4. 需要支持增量聚合（combine 函数）
```

**AppendOnlyMap 的设计选择**：
- ❌ 不使用 HashMap：内存碎片大，指针多，排序低效
- ❌ 不使用 TreeMap：插入 O(log n)，排序是隐含的但删除操作复杂
- ✅ 使用紧凑数组 + 开放寻址：一块连续内存，支持原地排序

### C.2 数据结构：紧凑数组 + 开放寻址 + 二次探测

#### 基本存储设计

```scala
// AppendOnlyMap 的核心存储
private var data = new Array[AnyRef](2 * capacity)
//                                    ↑ 为什么是 2 倍？
//                                    因为存储 (key, value) 对
//                                    索引 i 存 key，索引 i+1 存 value

// 示例：capacity = 4，最多存 4 对 KV
// data 数组长度 = 8
//
// 初始状态：
// [null, null, null, null, null, null, null, null]
//  0     1     2     3     4     5     6     7
//  ↑key  ↑val  ↑key  ↑val  ↑key  ↑val  ↑key  ↑val
//  pair0      pair1      pair2      pair3
```

#### 插入与查询流程（开放寻址 + 二次探测）

```scala
// ========== 插入流程 ==========
def update(key: K, value: V): Unit = {
  // 1. 计算初始哈希位置
  var pos = hashcode(key) & (capacity - 1)  // pos 在 [0, capacity)
  var index = pos << 1                       // 转换为数组索引（key 位置）

  // 2. 开放寻址 + 二次探测找空位或 key 匹配位置
  var i = 0
  while (i < capacity) {
    val stored_key = data(index)

    if (stored_key == null) {
      // ✓ 找到空位，插入新 KV 对
      data(index) = key
      data(index + 1) = value
      elementCount += 1
      return

    } else if (stored_key == key) {
      // ✓ 找到相同 key，更新 value（聚合）
      data(index + 1) = mergeValue(data(index + 1), value)
      return
    }

    // ✗ 位置被占用且 key 不同，继续探测
    i += 1
    pos = (pos + i * i) & (capacity - 1)  // 二次探测：+1, +4, +9, ...
    index = pos << 1
  }
}

// ========== 查询流程 ==========
def apply(key: K): V = {
  // 逻辑完全相同，只是查找而不插入
  // 最坏情况：表满，需要遍历 capacity 个位置
}
```

**二次探测的优势**：
```
问题：为什么不是线性探测（+1, +2, +3, ...）？

原因：Hash 冲突聚集（clustering）
  线性探测：
    hash(A) = 5
    hash(B) = 6  } → 形成连续块
    hash(C) = 7

    即使 hash(D) = 8（本应不冲突），也会受之前聚集的影响

  二次探测：
    hash(A) = 5  → +1   → 位置 6
    hash(B) = 6  → +4   → 位置 10
    hash(C) = 7  → +9   → 位置 16

    分散分布，减少聚集问题
```

### C.3 紧凑数组 vs HashMap：为什么 AppendOnlyMap 更适合 Shuffle

#### 内存布局对比

```
【HashMap】
  ┌─────────────┐
  │ Node[0]     │ ← 哈希表数组（可能很稀疏）
  ├─────────────┤
  │ Node[1]     │
  │   ↓ key     │ ← 指向堆上的 Key 对象
  │   ↓ value   │ ← 指向堆上的 Value 对象
  │   ↓ next    │ ← 指向链表中的下一个 Node
  ├─────────────┤
  │ Node[2]     │
  │   ...       │
  ├─────────────┤

  问题：
  - 指针多（key、value、next 各一个，每个 8 字节）
  - 堆上有大量 Node 对象（每个 24-56 字节）
  - L3 Cache 无法容纳整个表的热点数据
  - GC 压力大


【AppendOnlyMap 紧凑数组】
  ┌─────────────┬─────────────┐
  │  Key_0      │  Value_0    │
  ├─────────────┼─────────────┤
  │  Key_1      │  Value_1    │
  ├─────────────┼─────────────┤
  │  Key_2      │  Value_2    │
  ├─────────────┼─────────────┤
  │   null      │   null      │ ← 哈希冲突导致的空位
  ├─────────────┼─────────────┤
  │  Key_3      │  Value_3    │
  ├─────────────┼─────────────┤

  优点：
  - 一块连续内存，L3 Cache 友好
  - 即使 key/value 本身是对象，引用也是紧凑排列的
  - 无额外指针开销（next 指针）
  - GC 只需扫描数组本身，开销小
```

#### Cache 局部性分析

```
场景：迭代 100 万个 KV 对进行聚合

【HashMap】
  for ((key, value) in hashMap.entrySet()) {
    combiner(value)
  }

  执行过程：
  1. 读取哈希表 Node[i]（L3 miss）
  2. 跳转到堆上的 Node 对象（L3 miss）
  3. 读取 key 引用（L3 miss）
  4. 读取 value 引用（L3 miss）
  5. 执行 combiner → 触发 value 对象加载（L3 miss）

  每次迭代 ~5 个 L3 缓存未命中
  总计：~500 万次 L3 miss
  延迟：~150 纳秒/次 miss = 750 毫秒

【AppendOnlyMap】
  for (i in 0 to data.length step 2) {
    if (data(i) != null) {
      combiner(data(i+1))
    }
  }

  执行过程：
  1. 读取 data[i]（L3 hit）- 因为前面刚访问过 data[i-2]
  2. 读取 data[i+1]（L3 hit）
  3. 执行 combiner

  数组顺序访问，L3 缓存预取效果好
  大部分访问都是 L3 hit

  总计：显著减少 miss 次数，3-5 倍性能提升
```

### C.4 Rehash 与扩容

```scala
// AppendOnlyMap 的负载因子管理
def growTable(): Unit = {
  // 触发条件：elementCount > 0.7 * capacity

  val newCapacity = capacity * 2  // 翻倍扩容
  val newData = new Array[AnyRef](2 * newCapacity)

  // Rehash：遍历旧表，根据新 capacity 重新计算位置
  for (i in 0 until data.length by 2) {
    if (data(i) != null) {
      val key = data(i)
      val value = data(i + 1)

      // 在新表中重新定位
      var pos = hashcode(key) & (newCapacity - 1)
      var index = pos << 1

      // 同样使用开放寻址 + 二次探测找空位
      var j = 0
      while (j < newCapacity) {
        if (newData(index) == null) {
          newData(index) = key
          newData(index + 1) = value
          j = newCapacity  // 退出
        }
        j += 1
        pos = (pos + j * j) & (newCapacity - 1)
        index = pos << 1
      }
    }
  }

  data = newData
  capacity = newCapacity
}

// 为什么是 0.7 负载因子？
// 理由：
// - 太高（如 0.9）：查询碰撞率高，需要探测多次
// - 太低（如 0.3）：内存浪费
// - 0.7：二次探测平衡点，期望探测次数 ~1.5-2
```

### C.5 原地排序：从 AppendOnlyMap 到 SortedIterator

#### 排序前的数据整理

```scala
def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)] = {
  destroyed = true

  // Step 1: 把所有 KV 对压实到数组前端
  var newIndex = 0
  for (i in 0 until data.length by 2) {
    if (data(i) != null) {
      data(newIndex) = data(i)           // key 移到前面
      data(newIndex + 1) = data(i + 1)   // value 移到前面
      newIndex += 2
    }
  }

  // 现在 data 前 newIndex 个元素是紧凑的 KV 对
  // 后面的元素全是 null
  //
  // 示例（5 个 KV 对）：
  // 排序前：[k0, v0, null, null, k1, v1, k2, v2, null, null, k3, v3, k4, v4]
  // 排序后：[k0, v0, k1, v1, k2, v2, k3, v3, k4, v4, null, null, null, null]

  // Step 2: 就地排序，只排序前 newIndex 个元素
  new Sorter(new KVArraySortDataFormat[K, AnyRef])
    .sort(data, 0, newIndex, keyComparator)

  // Step 3: 返回迭代器（只读前 newIndex 个元素）
  new Iterator[(K, V)] {
    private var i = 0
    override def hasNext: Boolean = i < newIndex
    override def next(): (K, V) = {
      val k = data(i)
      val v = data(i + 1)
      i += 2
      (k, v)
    }
  }
}
```

#### 为什么支持原地排序

```
【本质原因】
排序只涉及元素的移动和比较，不需要额外数据结构

【具体分析】
比如 TimSort 算法（Spark 使用的）：

插入排序阶段（排序小 run）：
  for (i = 1; i < n; i++) {
    val key = array[i]
    j = i - 1
    while (j >= 0 && array[j] > key) {
      array[j+1] = array[j]  // 原地交换
      j -= 1
    }
    array[j+1] = key
  }

  ✓ 只涉及读、写、比较
  ✓ 不创建新数组
  ✓ 不使用栈（递归）
  ✓ 空间复杂度 O(1)

【与 HashMap 的区别】
HashMap 的排序困难：
  - HashMap 的数据分散在堆上
  - 每个 entry 通过指针链接
  - 排序需要遍历链表，操作复杂
  - 无法原地交换（交换需要修改链接关系，不止是数组元素移动）

AppendOnlyMap 的排序简单：
  - 数据已经在紧凑数组中
  - 排序就是数组元素重排
  - 交换只涉及数组操作
  - 完全可以原地进行

【实际应用】
假设 AppendOnlyMap 有 100 万个 KV 对：

排序前：
  data 数组大小：~200 万个元素
  占用内存：~16MB（假设每个引用 8 字节）

排序过程：
  TimSort 的临时空间：只需要 ~100KB 的 run buffer
  额外内存：< 1MB

排序后：
  data 数组大小：不变，仍然是 ~16MB

对比：
  ❌ 如果用新数组排序：需要再分配 16MB = 32MB 总占用
  ✅ 原地排序：始终保持 16MB，额外开销 < 1MB
```

### C.6 紧凑数组的缺点与不适用场景

#### 主要缺点

```
【1. 固定容量扩容成本】
  问题：需要 rehash 所有元素

  HashMap：
    private Node<K,V>[] table;
    // 扩容：旧数组 → 新数组
    // 成本：O(n)，但相对较低（只是重新链接）

  AppendOnlyMap：
    private AnyRef[] data;
    // 扩容：重新计算每个元素的位置
    // 成本：O(n × collision_factor)
    // 如果哈希冲突多，二次探测会很慢

  实测：AppendOnlyMap 扩容可能比 HashMap 慢 2-3 倍


【2. 删除操作困难】
  问题：不支持删除，因为删除会破坏开放寻址链

  假设：
    hash(A) = 5 → 5 (命中)
    hash(B) = 5 → 8 (二次探测)

  删除 A：
    data[5] = null  // 标记为删除
    data[8] = B     // 但查询 B 时会停在 data[5]（认为查询失败）
    ↓
    B 无法找到！

  解决方案：
    ✗ 标记删除（占用空间）
    ✓ 实现 Tombstone 机制（Apache Spark 不这样做）
    ✓ 定期 rebuild（太复杂）

  结论：AppendOnlyMap "只增不减"，这是必要的设计约束


【3. 哈希冲突敏感】
  问题：冲突多时，探测次数增加

  示例：
    key 哈希分布不均：
      大部分 key 集中在某个范围
      ↓
      开放寻址冲突率高
      ↓
      查询/插入变成 O(n) 而非 O(1)

  HashMap：
    链表长度可控（Java 8+ 自动转红黑树）
    长链表不会导致扫描速度下降

  AppendOnlyMap：
    二次探测冲突多时，探测序列很长
    性能退化到 O(n)

  现实：
    Spark Shuffle key 通常是 (partitionId, K)
    分布相对均匀，不易集中冲突


【4. 空间浪费（负载因子约束）】
  AppendOnlyMap 维持 0.7 负载因子：
    实际使用空间 = 70%
    浪费空间 = 30%

  HashMap 可以维持更高（如 0.75）：
    因为链表可以无限长
    冲突不会导致查询时间爆炸

  例：100 万个 KV，AppendOnlyMap 可能需要 200 万个槽位（1400 万元素）
       HashMap 可能只需要 133 万个槽位（因为链表处理冲突）

  结果：AppendOnlyMap 多占用 5-10% 内存


【5. 不支持并发修改】
  AppendOnlyMap 没有并发控制

  原因：
    ✓ Shuffle Map Task 单线程执行
    ✓ 没有并发需求
    ✗ 设计上放弃了锁机制，减少开销
```

#### 不适用场景

```
【场景 1：需要支持删除操作】
  例：LRU Cache、实时数据流处理中需要移除过期数据

  ✗ AppendOnlyMap 不行（只增不减）
  ✓ HashMap、ConcurrentHashMap 更合适


【场景 2：高并发读写】
  例：多线程缓存、共享状态字典

  ✗ AppendOnlyMap 无锁，unsafe（不支持并发）
  ✓ ConcurrentHashMap、Collections.synchronizedMap 更合适


【场景 3：键空间巨大但实际存储稀疏】
  例：100 亿个可能的 key，实际只存 100 万个

  ✗ AppendOnlyMap：
    - 需要预分配 capacity（哈希表大小）
    - 预分配太小 → 频繁 rehash
    - 预分配太大 → 内存浪费

  ✓ HashMap 更灵活（可动态调整）


【场景 4：键频繁变化，需要频繁扩容】
  例：写入速率不确定，数据量会大幅波动

  ✗ AppendOnlyMap：
    - 每次 rehash 成本 O(n × collision)
    - 频繁扩容，总成本可能很高

  ✓ HashMap 扩容成本更低


【场景 5：需要有序迭代（持续维护）】
  例：实时排行榜，需要随时获取排序结果

  ✗ AppendOnlyMap：
    - destructiveSortedIterator 调用一次后 destroyed = true
    - 无法继续插入或再次排序

  ✓ TreeMap、SortedMap 支持持续有序维护
```

#### 为什么 Spark Shuffle 还是选择 AppendOnlyMap

```
尽管有这些缺点，Spark 仍然选择 AppendOnlyMap，原因：

【优点足够强】
1. 内存效率：
   - 紧凑存储，L3 Cache 友好
   - 无额外指针开销
   - 排序时不需要额外空间

2. 排序性能：
   - 支持原地排序（空间 O(1)）
   - 与聚合一体，无额外阶段

3. 小对象优化：
   - 减少 GC 压力
   - 大数据量时性能优于 HashMap ~2-3 倍

【约束完全匹配 Shuffle 需求】
1. Shuffle Map Task 是单线程 → 无并发问题
2. 数据只需"写入→聚合→排序→输出" → 无需删除
3. 最终必须排序 → 原地排序优势显著
4. 内存有限 → 紧凑存储是必要的
5. 数据量可预估 → 初始 capacity 可合理设置，扩容不频繁

结论：
  AppendOnlyMap 不是"最通用"的数据结构
  而是"最优化"的针对 Shuffle 聚合场景的设计
```

### C.7 一句话总结

> **AppendOnlyMap 支持原地排序的本质**：因为数据存储在连续的数组中，排序只涉及数组元素的位置移动，不需要创建新的数据结构或分配额外的内存空间，空间复杂度为 O(1)。

---

## 附录 D：关键问题答疑总结

本附录汇总了对 Spark Shuffle 机制理解过程中的常见疑问，基于源码分析和实际测试。

### D.1 为什么 Sort Shuffle 每个 Task 只产生 2 个文件，而 Hash Shuffle 产生 R 个？

**Hash Shuffle 的思路（已废弃）**：

```
每个 MapTask 直接为每个 Reducer 分别创建一个文件
└─ Task 0 产生：file_0_0, file_0_1, file_0_2, ... file_0_{R-1}
└─ Task 1 产生：file_1_0, file_1_1, file_1_2, ... file_1_{R-1}
└─ ...
└─ Task M-1 产生：file_{M-1}_0, file_{M-1}_1, ..., file_{M-1}_{R-1}

总文件数：M × R

ReduceTask 0 需要读取：file_0_0, file_1_0, file_2_0, ..., file_{M-1}_0
ReduceTask 1 需要读取：file_0_1, file_1_1, file_2_1, ..., file_{M-1}_1
...

问题：
✗ 文件数量与 partition 数成正比，大数据场景文件爆炸
✗ Reducer 需要连接多个小文件，网络和 I/O 压力大
```

**Sort Shuffle 的改进**：

```
每个 MapTask 产生 1 个大的 data file + 1 个 index file
└─ Task 0 产生：shuffle_0_0.data, shuffle_0_0.index
└─ Task 1 产生：shuffle_0_1.data, shuffle_0_1.index
└─ ...
└─ Task M-1 产生：shuffle_0_{M-1}.data, shuffle_0_{M-1}.index

总文件数：2 × M

ReduceTask 0 需要读取：
  ├─ shuffle_0_0.data 中的 [offset_0, offset_1) 范围
  ├─ shuffle_0_1.data 中的 [offset_0, offset_1) 范围
  ├─ ...
  └─ shuffle_0_{M-1}.data 中的 [offset_0, offset_1) 范围

优势：
✓ 文件数只与 MapTask 数相关，与 Reducer 数无关
✓ Reducer 从同一 data file 中读取多个 Partition 的数据，充分利用顺序读性能
✓ Index file 提供了精确的位置定位，避免扫描小文件
```

### D.2 "Partition" 在 Shuffle 中具体指什么？

**常见混淆**：Partition 可能指"分区数"（数量）或"分区"（逻辑概念）。

**在 Shuffle 上下文中**：

```
spark.shuffle.sort.bypassMergeThreshold = 200

这里的 "200" 指的是：Shuffle 的 Partition 数量 = Reducer Task 的数量

例子：
rdd.repartition(500).reduceByKey(...)
             ↑         ↑
        产生 500 个 Partition

这个 Shuffle 的 partition 数 = 500
→ 需要 500 个 ReduceTask
→ Bypass 不可用（500 > 200）

关键理解：
├─ MapTask 数 = 上一个 Stage 的 Partition 数
├─ Reducer 数 = 当前 Shuffle 的 Partition 数
├─ Shuffle 写方案的选择基于：**当前 Shuffle 的 Partition 数**
└─ 不是基于 MapTask 数（MapTask 多时只是产生更多的临时文件，但文件本身不受限制）
```

### D.3 Partition 限制与 Map Task 数量的关系

**问题**：如果 MapTask 数量很多（如 1000 个），会不会也导致文件过多？

**回答**：**不会被 Bypass 限制，但有不同的影响**。

```
场景 1：1000 个 MapTask，Partition = 50（BypassMergeSort 可用）
├─ Bypass 会产生 1000 个 shuffle_x_0.data + shuffle_x_0.index 文件
├─ 虽然文件多，但这是 Spark 的设计（每个 Task 产生独立文件）
├─ Sort Shuffle 本来就不受 Partition 数限制（只受 MapTask 数限制）
└─ BypassMergeSort 也不受 MapTask 数限制

场景 2：100 个 MapTask，Partition = 500（BypassMergeSort 不可用）
├─ BypassMergeSort 被限制（500 > 200）
├─ 改用 SerializedShuffleHandle（如果 serializer 支持）
├─ MapTask 数（100）没有限制它的选择
└─ Partition 数（500）是决定性因素

限制原因对比：
├─ BypassMergeSort 的 200 限制：
│  ├─ 源于同时打开 partition 个数的文件缓冲
│  ├─ 200 个文件的缓冲（6.4MB）是可接受的
│  └─ 与每个文件的大小和 MapTask 数无关
│
├─ Sort Shuffle 没有 Partition 限制：
│  ├─ 因为只产生 2 个文件（data + index）
│  └─ 与 Partition 数无关
```

### D.4 为什么说"同一 core 连续执行的 task 可以共用文件"是 Hash Shuffle 特性，而 Sort Shuffle 与 core 无关？

**Hash Shuffle 时代**：

```
4 个 cores，4 个 tasks，3 个 partitions

无优化（12 个文件）：
├─ Core 1: Task 0 → 产生 file_0_0, file_0_1, file_0_2
├─ Core 2: Task 1 → 产生 file_1_0, file_1_1, file_1_2
├─ Core 3: Task 2 → 产生 file_2_0, file_2_1, file_2_2
└─ Core 4: Task 3 → 产生 file_3_0, file_3_1, file_3_2

FileConsolidation 优化（6 个文件）：
├─ Core 1 的文件：
│  ├─ Task 0 追加 partition 0 数据 → FileSegment 0
│  ├─ Task 0 追加 partition 1 数据 → FileSegment 1
│  └─ Task 0 追加 partition 2 数据 → FileSegment 2
│  ├─ Task 4 追加 partition 0 数据 → FileSegment 3（继续追加到同一文件）
│  ├─ Task 4 追加 partition 1 数据 → FileSegment 4
│  └─ Task 4 追加 partition 2 数据 → FileSegment 5
│  结果：core1_consolidated_file （包含 Task 0 和 Task 4 的所有数据）
│
└─ 类似地，Core 2、3、4 各产生一个 consolidated file

优势：
✓ 同一 core 内的 tasks 顺序执行（无并发竞争）
✓ 可以共用文件句柄
✓ 文件数量从 M×R 降至 cores×R
```

**Sort Shuffle 时代**：

```
无论多少 cores，无论 tasks 如何调度分配到 cores：

每个 Task 产生 shuffle_x_y.data + shuffle_x_y.index
└─ Task 0 → shuffle_0_0.data + shuffle_0_0.index
└─ Task 1 → shuffle_0_1.data + shuffle_0_1.index
└─ ...

优势：
✓ 完全避免 core 的概念
✓ 每个 task 有独立的文件，无需共用和锁竞争
✓ 文件数只与 task 数相关：2 × M
✓ 可扩展性最强
```

### D.5 BypassMergeSort、BaseShuffleHandle、SerializedShuffleHandle 是否都对应 Sort-based Shuffle？

**回答**：**是的，都是 Sort Shuffle 家族的不同优化方案**。

```
SortShuffleManager
│
├─ BypassMergeSortShuffleHandle ← 快速路径（避免排序）
│  └─ 使用 BypassMergeSortShuffleWriter
│     └─ 不排序，只做分区+拼接
│
├─ SerializedShuffleHandle ← 二进制优化方案
│  └─ 使用 UnsafeShuffleWriter
│     └─ 直接操作序列化二进制数据
│
└─ BaseShuffleHandle ← 通用路径
   └─ 使用 SortShuffleWriter（有聚合）或 UnsafeShuffleWriter（无聚合）
      └─ 支持聚合、排序等所有功能
```

**三者的共同点**：
- 都产生 1 个 data file + 1 个 index file
- 都由 IndexShuffleBlockResolver 负责文件管理
- 都支持 Reduce 端精确位置定位

**三者的区别**：

| 特性 | Bypass | Serialized | Base |
|-----|--------|-----------|------|
| **排序** | ❌ 无 | ✓ 有（二进制） | ✓ 有（对象） |
| **聚合** | ❌ 无 | ❌ 无 | ✓ 有 |
| **Spill** | 可能（缓冲满） | ✓ 通常需要 | ✓ 通常需要 |
| **适用场景** | Partition ≤ 200，无聚合 | Partition > 200，无聚合，无 relocation serializer | 其他所有情况 |

### D.6 SerializedShuffleHandle 中的"16M partition 限制"从何而来？

**源码定位**：PackedRecordPointer.java

```java
// 结构：[24 bit partition ID][13 bit page number][27 bit offset]
//       ← 最多支持 2^24 - 1 = 16,777,215 个 partition

private static final long PARTITION_ID_MASK = (1L << 24) - 1;
private static final long PAGE_NUMBER_MASK = (1L << 13) - 1;
private static final long OFFSET_IN_PAGE_MASK = (1L << 27) - 1;

// 如果 partitionId > 16M，会溢出，导致数据损坏
if (partitionId > PARTITION_ID_MASK) {
    throw new IllegalArgumentException(
        "too many partitions for Serialized shuffle: " + partitionId);
}
```

**为什么设计成 24 bit？**

```
目标：压缩指针占用空间
├─ 总空间：64 bit（一个 Long）
├─ 分配方案：
│  ├─ Partition ID：24 bit（最多 1600 万个 partition）
│  ├─ Page number：13 bit（最多 8192 页）
│  └─ Offset：27 bit（每页最多 128MB）
│
├─ 优势：
│  ├─ 8 字节压缩指针 vs 原本的 2-3 个对象引用（24-56 字节）
│  ├─ 更快的排序（只排序 8 字节指针，不排序 records）
│  └─ 更好的缓存局部性
│
└─ 折衷：
   └─ Partition 数量有上限（16M）
      └─ 在实际应用中很少超过，因为 partition 太多会导致其他问题
```

### D.7 "map task 数多也会产生大量临时文件"为什么不是 BypassMergeSort 的限制？

**理解限制的本质**：

```
BypassMergeSort 的限制是"同时打开的文件数"：

200 个 partition → 同时打开 200 个缓冲
                └─ 每个缓冲 32KB
                └─ 总计 6.4MB 内存
                └─ 可以接受

2000 个 map task × 3 个 partition → 产生 6000 个临时文件
                                   └─ 但同时打开的仍然只有 3 个
                                   └─ 每个缓冲 32KB
                                   └─ 总计 96KB 内存
                                   └─ 完全没问题

关键区别：
├─ BypassMergeSort 的"200 限制"指的是 Partition 数（reducer 数）
├─ 不是指 MapTask 数（mapper 数）
└─ Map task 再多也只是产生更多 data file，但不影响写阶段的并发缓冲数
```

### D.8 三种模式的本质流程对比

这个对比深入展示为什么说"Bypass 是特例，Unsafe/Base 是主流"。

#### 流程图对比

```
【Bypass 模式】

写入阶段：
  record (k1, v1, partition 5)
    ↓
  计算 partition ID = 5
    ↓
  直接写入 partition_5.tmp 的缓冲
    ↓
  record (k2, v2, partition 1)
    ↓
  计算 partition ID = 1
    ↓
  直接写入 partition_1.tmp 的缓冲

磁盘文件：
  partition_0.tmp: [所有partition 0的数据]
  partition_1.tmp: [所有partition 1的数据（含 k2,v2）]
  ...
  partition_5.tmp: [所有partition 5的数据（含 k1,v1）]

合并阶段（简单拼接）：
  output.data = partition_0.tmp + partition_1.tmp + ... + partition_5.tmp
                  [partition 0]    [partition 1]      [partition 5]


【Base/Unsafe 模式】

写入阶段：
  record (k1, v1, partition 5)
    ↓
  序列化 (Base会额外将对象存在内存)
    ↓
  写入内存（Base: HashMap 或 Buffer / Unsafe: Tungsten 堆外内存）
    ↓（内存满 → Spill）
    │
    ├─ 对内存中的数据排序（Base: 对象排序 / Unsafe: 指针排序）
    │
    ├─ 写入 spill 文件（已按 partitionId 排序）
    │
    └─ 清空内存，继续处理新数据

  Spill 文件内容（已排序）：
    spill_0: [partition 0的数据] [partition 1的数据] [partition 5的数据(k1,v1)]
    spill_1: [partition 0的数据] [partition 1的数据] [...]
    spill_2: [...]

合并阶段（多路归并）：
  for partition = 0 to N:
      for spill_file in [spill_0, spill_1, ...]:
          读取 spill_file 中 partition 对应的数据
          写入 output.data 的 partition 位置

  output.data = [所有partition 0] [所有partition 1] ... [所有partition 5]
```

#### 文件数量对比

```
假设：2000 个 map tasks，500 个 partitions

【Bypass】
  产生临时文件数：500（每个 partition 一个）
  最终输出文件：2（data + index）

  特点：直接产生分散的小文件

【Base/Unsafe】
  产生 spill 文件数：取决于内存（可能 5-20 个）
  最终输出文件：2（data + index）

  特点：先积累再溅出，文件数较少

关键对比：
  Bypass：
    写入：500 个缓冲并发写 → I/O 分散
    合并：读 500 个文件流式拼接 → 简单但 I/O 随机

  Base/Unsafe：
    写入：1 个缓冲顺序写 → I/O 集中
    溅出：多个已排序的 spill 文件 → 合并友好
    合并：多路归并 spill 文件 → 可用 NIO transferTo 零拷贝
```

#### 为什么 Base/Unsafe 在大数据量时更高效？

```
1. 写入效率：
   Bypass（500 分区）：
   - 需要维护 500 个文件缓冲
   - 对每条记录进行分区计算和缓冲查找
   - 内存页缓存污染严重（随机访问）

   Base/Unsafe：
   - 一个顺序写缓冲
   - 数据按分区有序到达内存
   - 缓存利用率高

2. 合并效率：
   Bypass（读 500 个文件）：
   - 文件系统频繁切换
   - 磁盘随机寻址
   - 文件描述符压力大

   Base/Unsafe（读 10 个 spill）：
   - 文件较少，寻址次数少
   - 支持 NIO transferTo 零拷贝
   - 多路归并算法优化空间大

3. 内存效率：
   Bypass：
   - 每个缓冲 32KB（500×32KB = 16MB）

   Unsafe：
   - 指针数组 + Tungsten 页
   - 8 字节指针远小于对象（Base 120+ 字节）
   - GC 压力低
```

#### 总结：三者的设计哲学

```
Bypass（BypassMergeSortShuffleWriter）：
  哲学：回避排序，极限优化小场景
  优点：实现简单，零拷贝拼接
  缺点：分区多时性能差，无聚合支持

Base（SortShuffleWriter + ExternalSorter）：
  哲学：通用方案，支持所有功能
  优点：支持聚合、排序，处理任意大小数据
  缺点：GC 压力大，大数据量时性能不及 Unsafe

Unsafe（UnsafeShuffleWriter + ShuffleExternalSorter）：
  哲学：二进制优化，通用但高性能
  优点：小指针、低 GC、支持 16M 分区
  缺点：无聚合支持，relocation serializer 要求高

结论：
  实际应用中，Base 和 Unsafe 才是主流
  它们都采用"积累-溅出-归并"的通用流程
  Bypass 只在特定场景（<200 分区、无聚合）作为快速路径
```

---