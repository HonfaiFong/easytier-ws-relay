# 第三轮审查修复（2026-10-14）

基于 `3461f467972433ea15fe91b9f60ccec1c116fda9`，修复出站限流与控制同步的相互影响。

| 审查问题 | 处理结果 |
| --- | --- |
| P1：汇聚流量误踢接收方 | 连接、房间出站速率超限时丢弃本次透传帧，不关闭发送方或接收方。出站帧过大也不归咎于接收方。实际传输发送失败关闭对应目的连接；缓冲区连续两次观察满载且间隔至少一秒才按慢消费者关闭。 |
| P1：路由发送失败后不恢复 | 每个物理连接最多保留一个待更新意图，限流后每秒重试，并从最新拓扑重新生成帧。只有发送成功才推进 PeerInfo、连接位图和 ForeignNetworkInfo 缓存，清除 `routeResyncPending`。关闭及替换连接时删除旧意图。 |
| P1：合法 RPC 被误判为协议错误 | `OutboundError`、`RateLimitError`、`SocketClosedError` 与请求校验错误分开处理。握手响应、Pong、RPC 响应在瞬时限流后进入有界队列；队列满或响应超出永久容量时不关闭合法请求方。客户端仍需按 RPC 超时重试。主动超额请求全局地图的来源按 1008 关闭。 |
| P2：缺失明文 RPC 身份 | 未加密、未包级压缩的转发 RPC 必须能解析为标准 RpcPacket，显式包含 `fromPeer`、`toPeer`，并与外层报文一致。无法解析的历史明文格式不再无条件透传。加密、包级压缩的端到端 RPC 仍原样转发。 |
| P2：单侧 PeerCenter 链路 | 默认保留上游有向报告语义。可配置 `EASYTIER_PEER_CENTER_REQUIRE_MUTUAL="1"`，只返回两个当前在线节点均报告且未过期的相互连接；保留各自方向的延迟。断线、同 ID 替换及报告过期后撤销旧链路。 |

## 控制预算与重试边界

控制保留额度包含在原有总上限内：单连接每秒保留 `min(64 KiB, 总预算 / 8)`，房间每秒保留 `min(256 KiB, 总预算 / 8)`，除法向下取整。普通透传流量只能使用扣除保留额度后的预算；中继自身生成的握手、Pong、RPC 响应和路由更新可使用完整预算。客户端转发的 RPC 仍使用普通透传预算，防止通过包类型占用保留额度。

本地控制队列每连接最多 32 帧、1 MiB，房间最多 4 MiB。按连接轮转发送，在预算恢复后重试；关闭连接时释放队列。队列满时丢弃新控制帧，RPC 客户端需要超时重试。数据透传不入队，超额直接丢包；目前没有按来源分配公平数据带宽，也没有改变 EasyTier 的传输重试机制。

重试采用短期 `setTimeout`：有待发送控制报文或路由意图时保留活动计时器，会推迟 Durable Object 休眠。待发送内容清空后取消重试计时器。未持久化响应队列或计时器；进程重启后按已有 Attachment 重建路由会话，RPC 响应由客户端重试。该机制保证可恢复限流后的重新尝试，未增加路由 ACK 重传协议。配置必须容纳单个控制帧；永久超出帧、缓冲或每秒容量的帧不无限重试。

上游 [v2.6.4 PeerCenterServer](https://github.com/EasyTier/EasyTier/blob/8428a89d2dabc94c97d370ec607c6ca142473626/easytier/src/peer_center/server.rs) 按 `src → dst` 保存单侧报告，不要求双向确认。因此本项目将双向确认设为可选部署策略，默认 `0`，启用后可能暂时隐藏只完成单向上报的正常连接。相互上报是可信度加固，不是链路密码学证明，无法防止两个恶意节点串通。

## 验证

- `npm test`：66 个 Node 测试通过。新增 22 个测试，包含自动计时器重试、最新拓扑合并、休眠 resync 失败保留、合法 RPC 限流恢复、握手响应顺序、队列边界及轮转、关闭/重连清理、明文 RPC 缺失字段和解码失败、双向报告撤销和过期。
- `npm exec wrangler deploy -- --dry-run`：本地打包通过。
- `npm run test:runtime`：workerd 上原有 HTTP/WebSocket 握手、数据/RPC 转发、Ping、重复 ID、房间隔离和伪造来源验证通过。
- `npm run test:egress`：使用下列独立 workerd 实例验证汇聚流量限流、接收方保持在线、Pong/RPC 延迟响应、新节点路由最终同步和明文身份拒绝。

```bash
npm exec wrangler dev -- --ip 127.0.0.1 --port 18784 --inspector-port 19294 --var EASYTIER_MAX_OUTBOUND_BYTES_PER_SECOND:8192 --var EASYTIER_MAX_ROOM_OUTBOUND_BYTES_PER_SECOND:16384
# 另开终端
npm run test:egress
```

上述 workerd 测试使用合成 EasyTier 报文。未执行真实 EasyTier v2.6.4 客户端互通、实际 NAT 打洞、Cloudflare 在线休眠或公网压力测试，未部署上线。
