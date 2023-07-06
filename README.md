# TinyKV

TinyKV是什么？
- 利用Raft共识算法实现了一个键值存储系统
- 参考了MIT6.824和TiKV项目
- 实现一个可横向扩展、高可用、支持分布式事务的键值存储服务
- 深入了解TiKV的架构和实现

## 大纲

1. 单机KV
   1. 实现单机存储引擎
   2. 实现KV服务的handlers
2. 基于Raft的KV
   1. 实现基本的Raft算法
   2. 在Raft算法上构建一个容错的KV服务
   3. 在Raft算法添加Raft日志垃圾回收和快照功能
3. 基于Multi-Raft的KV
   1. 在Raft算法添加实现成员变更和领导转移功能
   2. 在RaftStore上实现成员变更和Region分裂
   3. 实现一个基本的调度器
4. 事务
   1. 实现一个MVCC层
   2. 实现 KvGet、KvPrewrite、KvCommit请求的handlers
   3. 实现 KvScan、KvCheckTxnStatus、KvBatchRollback、KvResolveLock请求的handlers

## 架构

![TinyKV架构](./doc/imgs/overview.png)

## 参考资料

- 阅读材料：https://github.com/tangyiheng/TinyKV/blob/course/doc/reading_list.md
- 数据存储设计（TiKV）：
  - https://cn.pingcap.com/blog/tidb-internal-1
  - https://cn.pingcap.com/blog/?tag=TiKV
  - TiKV源码解读：https://cn.pingcap.com/blog/?tag=TiKV%20%E6%BA%90%E7%A0%81%E8%A7%A3%E6%9E%90
- 调度设计（PD）：
  - https://cn.pingcap.com/blog/tidb-internal-3
  - https://cn.pingcap.com/blog/?tag=PD

## 部署

构建：

```bash
cd tinykv
make
```

在bin目录下得到tinykv、tinyscheduler两个服务的二进制可执行文件

## TinyKV + TinySQL

运行服务：

```bash
mkdir -p data
./tinyscheduler-server
./tinykv-server -path=data
./tinysql-server --store=tikv --path="127.0.0.1:2379"
```

使用MySQL客户端连接：

```bash
mysql -u root -h 127.0.0.1 -P 4000
```