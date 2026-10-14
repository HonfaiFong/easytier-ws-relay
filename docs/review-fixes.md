# 2026-10-14 审查修复说明

基于提交 `79150fcfd0f0a99abadc4d4ef5d1d2fb39aea0c5`，修复已确认的逻辑问题。

| 报告问题 | 处理结果 |
| --- | --- |
| 1. 休眠身份丢失 | 握手和会话角色变化后更新 Attachment；唤醒恢复节点索引并更换路由会话 ID。升级到本版本时，旧 Attachment 缺少身份的连接需要重连。 |
| 2. 摘要冒充认证 | 明确摘要只是分组依据，增加可选 Worker Secret 接入令牌。令牌不证明网络密钥所有权，也不提供每个节点的密码学身份认证。未设置令牌仍为公共接入模式。 |
| 3. 跨房间全局状态 | PeerManager、摘要注册和 PeerCenter 状态均改为房间实例持有。最后一个连接关闭后清理该组状态。 |
| 4. Gzip/Zstd 错配 | 只协商、生成未压缩 RPC；本地 Zstd 请求和响应明确拒绝，避免继续错误解码。节点之间的 RPC 仍保持原样转发。 |
| 5. 重复 Peer ID | 拒绝已有 OPEN 连接的重复 ID；旧连接关闭回调只删除仍指向自身的映射。 |
| 6. 纯 P2P | 转发层仅允许 RPC、Ping、Pong；数据、KCP 和 ForeignNetworkPacket 被阻止。 |
| 7. 配置读取 | 所有处理器使用房间注入的 env，移除 process.env 回退；兼容日期保持不变。 |
| 8. 报文校验 | 校验状态、来源、包类型、明文长度、RPC 内层和业务 ID；增加帧、消息率、字节率限制；修复 TypedArray/DataView 的偏移处理。 |
| 9. 广播开销 | 限制每组和全房间路由数量及单条 PeerInfo 大小，合并广播，使用已有会话版本缓存发送增量 PeerInfo；位图比较使用 SHA-256。完整连接矩阵的算法复杂度仍然存在，首次同步和拓扑变更仍需全量发送。 |
| 10. 工程维护 | 移除摘要和完整载荷日志，增加可运行的 Node 测试，提交 npm 锁文件，取消忽略 pnpm 锁文件，修复 README 本地链接，使用标准 ESM 导入并添加本地 Wrangler 开发依赖。 |

## 协议边界

EasyTier 的包级压缩保留原始长度，压缩后的实际载荷会更短；加密会增加尾部。因此只对明文报文检查 `header.len === payload.length`。不透明的透传包检查帧大小、原始长度上限及非空载荷，不尝试解密或解压。参考 [EasyTier 上游包压缩实现](https://github.com/EasyTier/EasyTier/blob/main/easytier-core/src/packet/compressor.rs)。

目前只接受来源 ID 与直连连接相同的报文，未实现多跳来源授权。纯 P2P 模式只保留原生 RPC 信令，未实现 ForeignNetworkPacket 的安全递归解包。本地 RPC 尚不支持 Zstd、分片、包级压缩或加密控制报文，这些请求会明确关闭连接。

共享令牌是可选的接入门槛，持有者仍可首次声明任意未占用 Peer ID。若需要验证网络密钥所有权或防止可信客户端冒用其他节点的身份，需要客户端支持的挑战响应或公钥认证协议。当前未实现 HMAC 挑战、防重放、按 IP 限流及持久化的节点凭据。

## 验证范围

`npm test` 覆盖握手、重复连接、关闭回调、两种休眠时机、恢复转发及路由重协商、房间和网络隔离、来源伪造、配额、TypedArray 偏移、P2P 限制、大响应、64 位事务 ID、压缩拒绝、广播合并和 PeerCenter 摘要刷新。

本次验证：34 个 Node 回归测试通过，`wrangler deploy --dry-run` 打包通过。本地 workerd 下的 `npm run test:runtime` 通过 HTTP 健康检查、真实 WebSocket 握手、数据与 RPC 转发、Ping、重复 ID 拒绝、跨房间同 ID 和伪造来源拒绝；使用的是合成协议报文。

休眠回归测试使用模拟 WebSocket，保留 Attachment 后重新构造房间及 JS 连接对象；不能代替 Cloudflare 在线休眠测试。未部署到线上，未进行真实 EasyTier 客户端、NAT 打洞或 100/500/1000 节点压力验证。

运行命令及部署配置见 [README](../README.md)。
