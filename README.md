# EasyTier WebSocket Relay for Cloudflare Workers

## 项目简介

该项目是 EasyTier 的第三方服务端实现。EasyTier 是一个去中心化 P2P 组网程序，官方代码使用 Rust 实现。本项目使用 Cloudflare Worker + Durable Object 实现了 JavaScript 版本的 WebSocket 服务端，支持网络转发与 P2P 打洞信息交换。

项目使用 Claude 进行开发，目前处于早期阶段，还存在很多问题，欢迎提交代码或 issue。

> **注意：本项目仅供学习交流使用**

## 技术架构

- 基于 Cloudflare Workers 和 Durable Objects
- 使用 WebSocket 协议进行实时通信
- 采用 Protocol Buffers 进行高效序列化
- 透传 EasyTier 客户端加密的数据（中继层不负责端到端加密）
- 模块化设计，便于扩展和维护

## 开发环境搭建

### 前置要求

- Node.js (>= 22.0.0)
- pnpm (推荐) 或 npm
- Wrangler CLI (Cloudflare Workers 工具链)

### 安装步骤

1. 克隆项目仓库：
```bash
git clone <your-repo-url>
cd easytier-ws-relay
```

2. 安装依赖：
```bash
pnpm install
# 或者使用 npm
npm install
```

3. 安装 Wrangler CLI：
```bash
npm install -g wrangler
```

4. 登录 Cloudflare：
```bash
wrangler login
```

## 本地开发

### 启动开发服务器

```bash
# 启动本地开发服务器
pnpm run dev
# 或者
wrangler dev --ip 0.0.0.0
```

### 直接启动（不监听文件变化）

```bash
pnpm run start
# 或者
wrangler dev
```

## 部署到 Cloudflare

### 部署命令

```bash
# 部署到 Cloudflare Workers
wrangler deploy
```

### 配置说明

项目使用 [wrangler.toml](./wrangler.toml) 文件进行配置，主要配置项包括：

- [name](./wrangler.toml): Worker 名称
- [main](./wrangler.toml): 入口文件路径
- [compatibility_date](./wrangler.toml): 兼容性日期
- Durable Objects 配置
- 环境变量配置

## 项目结构

```
easytier-ws-relay/
├── protos/                 # Protocol Buffers 定义
│   ├── google
│   │   └── protobuf
│   │       └── timestamp.proto
│   ├── common.proto        # 通用协议定义
│   ├── error.proto         # 错误协议定义
│   └── peer_rpc.proto      # 对等节点 RPC 协议定义
├── src/
│   ├── worker/             # Worker 实现
│   │   ├── core/           # Worker 核心功能
│   │   │   ├── basic_handlers.js   # 基础处理器
│   │   │   ├── compress.js         # 压缩功能
│   │   │   ├── constants.js        # 常量定义
│   │   │   ├── crypto.js           # 加密功能
│   │   │   ├── packet.js           # 数据包处理
│   │   │   ├── peer_manager.js     # 对等节点管理
│   │   │   ├── protos.js           # Protobuf 相关功能
│   │   │   ├── protos_generated.js # Protobuf 生成的代码
│   │   │   └── rpc_handler.js      # RPC 处理器
│   │   └── relay_room.js           # 中继房间实现
│   └── worker.js                   # Worker 入口文件
├── package.json            # 项目配置
├── wrangler.toml           # Cloudflare Workers 配置
└── README.md               # 项目说明
```

## 功能特性

- WebSocket 双向通信中继
- 基于 Room 的连接管理
- 使用 Protobuf 进行高效序列化
- 透传 EasyTier 客户端加密的数据（中继层不负责端到端加密）
- 客户端状态管理与心跳维持
- RPC 请求/响应处理机制

## 纯 P2P 模式

在 `wrangler.toml` 的 `[vars]` 中配置：
- `EASYTIER_DISABLE_RELAY`: `"1"` 开启纯 P2P，默认 `"0"`
- RPC 仅支持未压缩报文，协商 `None`；收到 Zstd 会明确关闭连接，要求客户端改用未压缩 RPC。

修改完配置后按正常方式运行 `wrangler dev` 或部署即可生效。

## Durable Object 地区配置

Durable Object 默认会根据请求来源自动选择最近的地区部署。如需指定地区，可在 `wrangler.toml` 的 `[vars]` 中配置：

- `LOCATION_HINT`: Durable Object 的位置提示，可选值如下：

| 参数 | 地区 |
|------|------|
| `wnam` | 西部地区（北美） |
| `enam` | 东部地区（北美） |
| `sam` | 南美洲 |
| `weur` | 西欧 |
| `eeur` | 东欧 |
| `apac` | 亚太地区（默认） |
| `oc` | 大洋洲 |
| `afr` | 非洲 |
| `me` | 中东 |

> 详细说明请参考 [Cloudflare 官方文档](https://developers.cloudflare.com/durable-objects/reference/data-location/#supported-locations-1)

修改完配置后按正常方式运行 `wrangler dev` 或部署即可生效。

## 客户端连接说明

部署后，EasyTier 客户端连接地址需要添加路径 `/ws`。

默认情况下，WebSocket路径为`/ws`，该路径可以在`wrangler.toml`中通过`WS_PATH`变量进行自定义。

easytier中端口号使用0为使用协议默认端口，ws对应80，wss对应443。

开发模式:
```
ws://your-network-ip:0/ws
```
部署后:
```
wss://your-deployment.workers.dev:0/ws
```

## 接入、资源限制与测试

通过 `room` 参数将网络稳定分配到不同房间，例如 `wss://your-deployment.workers.dev:0/ws?room=my-network`。不传参数时使用 `default` 房间；每个房间都有独立的节点、摘要注册和 RPC 状态。

需要限制接入时，将 `EASYTIER_RELAY_TOKEN` 保存为 Cloudflare Secret：

```bash
pnpm exec wrangler secret put EASYTIER_RELAY_TOKEN
```

客户端可通过 `Authorization: Bearer <token>` 或 URL 参数 `token` 提交令牌，例如 `wss://your-deployment.workers.dev:0/ws?room=my-network&token=<URL编码后的令牌>`。请使用 WSS，并避免在日志中记录完整连接 URL。未配置 Secret 时为公共接入模式。这个共享令牌用于中继接入，不验证单个节点身份或 EasyTier 网络密钥所有权；网络摘要只用于分组和检查同名网络的一致性。

握手后，报文来源 Peer ID 必须等于当前连接注册的 Peer ID；本地 RPC 内层 ID 和业务请求 ID 也会校验。当前支持直接连接的节点，经过其他节点转来的不同来源 ID 会被拒绝。重复 Peer ID 的新连接会被拒绝，旧连接进入关闭状态后才允许重连。

`EASYTIER_DISABLE_RELAY="1"` 时仅转发 RPC、Ping 和 Pong 信令，丢弃 Data、KCP 和不透明的 ForeignNetworkPacket。默认 `"0"` 保留数据中继。纯 P2P 模式下不支持封装在 ForeignNetworkPacket 中的信令。

RPC 仅协商 `None`，大响应也不会使用 Gzip。发给中继自身的 Zstd RPC、分片 RPC、加密或包级压缩控制报文会明确关闭连接，客户端必须使用未压缩的单片控制报文。节点之间的加密或压缩数据保持原样透传，原始长度与线上的载荷长度可以不同。

可通过 Worker `env` 配置以下限制；非法值回退到默认值，超出上限的值会被截断：

| 变量 | 默认值 | 上限 / 范围 |
| --- | --- | --- |
| `EASYTIER_MAX_CONNECTIONS` | 128 个连接 / 房间 | 不超过每组路由节点上限 |
| `EASYTIER_MAX_ROUTE_PEERS` | 256 个节点 / 网络组 | 1024 |
| `EASYTIER_MAX_TOTAL_ROUTE_PEERS` | 1024 条节点记录 / 房间 | 4096 |
| `EASYTIER_MAX_PEER_INFO_BYTES` | 4096 字节 / 节点记录 | 16384 |
| `EASYTIER_MAX_FRAME_BYTES` | 1048576 字节 / 入站帧 | 1048576 |
| `EASYTIER_MAX_MESSAGES_PER_SECOND` | 1000 帧 / 连接 / 秒 | 10000 |
| `EASYTIER_MAX_BYTES_PER_SECOND` | 8388608 字节 / 连接 / 秒 | 33554432 |
| `EASYTIER_ROUTE_BROADCAST_INTERVAL_MS` | 100 毫秒 | 1–1000 |

节点变更会合并广播，已确认会话使用增量 PeerInfo；连接矩阵变化时仍发送完整矩阵。连接元数据在握手和会话变更后保存到 WebSocket Attachment，休眠恢复会重建节点映射并使用新会话 ID 请求完整路由。旧版本缺失身份的 Attachment 会要求重连。

```bash
npm ci
npm test
npm exec wrangler deploy -- --dry-run
```

本地 workerd 冒烟验证（另开终端运行 `wrangler dev --ip 127.0.0.1 --port 18783`）：

```bash
npm run test:runtime
```

回归测试与验证边界见 [修复说明](./docs/review-fixes.md)。

## 贡献

欢迎提交 Issue 和 Pull Request 来改进本项目。

## 许可证

[MIT License](./LICENSE)

## 免责声明

本项目仅供学习交流使用，请勿用于任何商业用途或非法用途。使用本项目代码造成的任何后果，原作者概不负责。
