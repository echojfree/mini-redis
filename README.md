# Mini Redis

一个用于学习和面试的精简版 Redis 实现，使用 Java 8 开发。

## 项目介绍

Mini Redis 是一个教学目的的 Redis 服务器实现，涵盖了面试中常见的技术知识点：
- 网络编程（Netty）
- 并发编程
- 数据结构实现
- 设计模式
- 持久化机制
- 性能优化

## 功能特性

### 已实现
- [ ] TCP 服务器
- [ ] RESP 协议支持
- [ ] 基础命令（PING、GET、SET 等）
- [ ] 五大数据类型
- [ ] 持久化（RDB/AOF）
- [ ] 事务支持
- [ ] 发布订阅
- [ ] 过期键处理

## 快速开始

### 环境要求
- JDK 8+
- Maven 3.6+

### 编译运行
```bash
# 编译项目
mvn clean compile

# 运行测试
mvn test

# 打包
mvn package

# 运行服务器
java -jar target/mini-redis-1.0-SNAPSHOT.jar
```

### 客户端连接
```bash
# 使用 telnet 连接
telnet localhost 6379

# 或使用 redis-cli
redis-cli -p 6379
```

## 项目结构
```
mini-redis/
├── src/main/java/com/mini/redis/
│   ├── server/          # 服务器核心
│   ├── protocol/        # RESP 协议处理
│   ├── command/         # 命令实现
│   ├── storage/         # 存储引擎
│   ├── network/         # 网络层
│   ├── config/          # 配置管理
│   ├── persistence/     # 持久化
│   └── utils/           # 工具类
├── src/test/java/       # 测试代码
└── pom.xml              # Maven 配置
```

## 开发进度

查看 [plan.md](plan.md) 了解详细的实现计划。

## 版本历史

- v0.1.0 - 基础架构和网络层
- v0.2.0 - 核心存储引擎
- v0.3.0 - 命令系统
- v0.4.0 - 持久化机制
- v0.5.0 - 高级特性

## 许可证

MIT License