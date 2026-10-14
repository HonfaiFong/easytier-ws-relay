# 第四轮审查修复（2026-10-14）

基于 `456fff10aeb4c80a92f39e9a6e242f8b1e445966`，先同步官方 EasyTier `v2.6.4` 的协议定义，再修改中继行为。

## 官方协议基线

上游 [v2.6.4 Release](https://github.com/EasyTier/EasyTier/releases/tag/v2.6.4) 对应提交 `8428a89d2dabc94c97d370ec607c6ca142473626`。三个 `.proto` 文件原本已在该基线上，实际仅有空白差异；重新同步并生成后 Protobuf 代码没有语义变更。下载文件只规范化换行和行尾空白，原始与规范化后的 SHA-256 均记录在 `protos/upstream.json`。

新增 `npm run sync:upstream` 与 `npm run check:upstream`。同步脚本从锁定提交取得协议与 Rust RPC 代码生成器，检查其 `idx + 1` 编号规则，再按 `.proto` 中的方法声明顺序生成 `src/worker/core/rpc_methods.js`，最后重新生成 Protobuf 代码。离线检查验证协议哈希和方法定义。整个项目仍是 JavaScript/Cloudflare 中继实现，同步范围是协议和所实现的 RPC 行为。

除了审查指出的方法编号，还核对了 [官方 RPC 客户端](https://github.com/EasyTier/EasyTier/blob/8428a89d2dabc94c97d370ec607c6ca142473626/easytier/src/proto/rpc_impl/client.rs)：其 `proto_name` 使用服务的 Proto 名称，例如 `PeerCenterRpc`，而不是包名 `peer_rpc`。现在本地处理器接受官方描述符，测试默认也使用该格式。

## 修复结果

| 问题 | 处理 |
| --- | --- |
| PeerCenter RPC 方法编号 | `ReportPeers=1`、`GetGlobalPeerMap=2`；路由同步使用生成的 `SyncRouteInfo=1`。移除零编号兼容分支和 `EASYTIER_OSPF_ROUTE_METHOD_INDEX` 覆盖，避免偏离官方协议。测试和冒烟脚本同步修正。 |
| 恢复连接逐包重复生成路由 | 入站包只在尚无待重试意图时启动完整同步；已排队的连接继续由每秒定时器从最新拓扑生成。500 个连续 Data 包只生成一次同步，窗口恢复后再生成一次并送达。 |
| 握手响应不可交付留下注册连接 | 房间构造时按实际握手响应大小校验帧、连接速率、房间速率与缓冲容量。暂时限流仍入队；永久不可发送以 1011 关闭并清理 Peer/路由映射；控制队列无法容纳握手时以 1013 关闭并清理。 |
| 全局地图重复计算 | 在构建地图前检查房间总配额；房间饱和丢弃当前请求，由客户端按 RPC 超时重试，保留健康连接。每组缓存最多一份响应体，最长 1 秒；报告、成员变更、连接替换及 TTL 到期使缓存失效。缓存不含事务 ID，响应封装仍逐请求生成。超出响应大小限制只保留标记，避免重复编码或长期保留大缓冲。报告更新本身只使缓存失效，不再立即生成全局地图。 |
| Pong 挤占控制资源 | 本地 Pong 限制载荷、连接/房间速率和连接/房间字节数；超额丢弃，避免入队。正常 Pong 保留已有控制预算和队列机制，端到端转发的 Ping/Pong 仍按普通透传预算处理。 |

## 配置与边界

新增限制均可调低，超上限截断到下表默认值，非法值回退到默认值。

| 变量 | 默认值 / 上限 |
| --- | --- |
| `EASYTIER_MAX_ROOM_GLOBAL_MAP_REQUESTS_PER_SECOND` | 32 次 / 房间 / 秒 |
| `EASYTIER_MAX_PONG_PAYLOAD_BYTES` | 4096 字节 |
| `EASYTIER_MAX_PONGS_PER_SECOND` | 4 次 / 连接 / 秒 |
| `EASYTIER_MAX_ROOM_PONGS_PER_SECOND` | 256 次 / 房间 / 秒 |
| `EASYTIER_MAX_PONG_BYTES_PER_SECOND` | 16384 字节 / 连接 / 秒 |
| `EASYTIER_MAX_ROOM_PONG_BYTES_PER_SECOND` | 65536 字节 / 房间 / 秒 |

Pong 字节配额包含 16 字节外层头，并在入队前计费。原有出站总预算仍在实际发送时检查；新增限制不提供严格的控制报文优先级调度。上游普通 Ping 载荷是 4 字节序号，默认限制保留其正常行为。

原有单连接全局地图限额仍为 4 次/秒，主动超额的来源以 1008 关闭。房间配额适用于所有网络组和缓存命中请求，以限制响应封装与发送开销。协议能力边界仍包含：本地控制 RPC 只接收未加密、未压缩的单片报文，普通节点间编码数据继续透明转发。

## 验证

- `npm run check:upstream`：协议来源与生成的 RPC 方法定义校验通过。
- `npm test`：81 项通过，包含官方方法编号、真实描述符及 int64 事务回显、恢复连接连续 500 包、握手容量与清理、跨网络组房间配额、缓存复用/失效/TTL、Pong 载荷/速率/房间字节限制。
- `npm exec wrangler deploy -- --dry-run`：打包通过。
- `npm run test:runtime`：workerd 握手、数据/RPC 透传、Ping、重复 ID、房间隔离及伪造身份验证通过。
- `npm run test:egress`：8192 字节/连接/秒、16384 字节/房间/秒的 workerd 实例验证汇聚限流、延迟 Pong/RPC 与路由最终同步通过。
- 官方 `easytier-core 2.6.4-8428a89d`：两个 Windows 客户端以无 TUN、禁用普通 P2P 的方式连接同一 workerd 房间，均看到对方的两跳路由。通过客户端 A 的 SOCKS5 和客户端 B 的子网映射完成 4300 字节 TCP 回显；重启 B 后再次完成相同回显。客户端 CLI 的 PeerCenter 输出中也能看到实际节点报告。官方 Windows 发布包 SHA-256 已与 GitHub Release 元数据核对：`27af91e270e554709b048bd32327fefd2dfce5062ae1e8701af7550c6f525f84`。

可复现真实客户端验证（先启动端口 18783 的 workerd，再指定官方 core/cli 所在目录）：

```bash
npm run test:client -- ./tmp/easytier-v2.6.4/easytier-windows-x86_64
```

脚本校验客户端版本，使用本机 19881/19882 管理端口、19891 SOCKS5 和 19895 回显端口，结束时清理自己启动的客户端和连接。日志与 CLI 快照保存在忽略的 `tmp/` 中。客户端参数含 `--no-tun true`、`--bind-device false`、`--disable-p2p true`、`--disable-upnp true`、禁用 STUN 和加密，用于隔离并验证纯 WebSocket 中继路径。该结果不代表加密、P2P 或跨公网部署已验证。

未部署到 Cloudflare；实际线上休眠、公网压力与跨 NAT 打洞仍需要独立验证。
