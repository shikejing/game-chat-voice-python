# 配置项

所有参数集中在 `game_chat/config.py` 的 `RoomSettings`，构造 `RoomService` /
`create_app(settings=...)` 时可覆盖。默认值偏向单房间中等规模的游戏频道。

| 参数 | 默认值 | 含义 |
|---|---|---|
| `offline_grace_seconds` | `30.0` | 断线后保留房间/频道成员身份的宽限期（秒）。`<=0` 表示断线立即驱逐。 |
| `channel_message_retention` | `500` | 每个文本频道保留用于回放/补拉的最近消息条数（超出滚动丢弃）。 |
| `replay_max_messages` | `500` | 单次重连回放 / `fetch_history` 最多返回的消息条数。 |
| `client_send_queue_max` | `1024` | **每连接服务端→客户端**有界发送队列容量。堆满即判定慢消费者并断开驱逐。 |
| `client_send_quota` | `40` | **每连接客户端→服务端**令牌桶容量（也是稳态每秒请求数）。 |
| `client_send_window_seconds` | `1.0` | 令牌桶窗口长度；稳态速率 = `quota / window`（请求/秒），允许突发最多 `quota`。 |
| `slow_consumer_drain_seconds` | `5.0` | 慢消费者相关容忍参数（预留；当前实现以队列容量为准即时判定）。 |
| `dedup_cache_size` | `4096` | 每频道 `client_msg_id` 去重索引的最大条数，超出按保留窗口淘汰。 |
| `voice_participant_limit` | `16` | 单个语音频道的通话参与者上限。 |
| `room_member_limit` | `1000` | 单房间成员数上限。 |

## 限流（入站）

- 每个 WebSocket 连接一个令牌桶。正常请求（含心跳、消息、信令）消耗 1 个令牌，
  桶按 `client_send_quota / client_send_window_seconds` 每秒持续补充。
- 超限时服务端**不断连**，而是回一帧
  `{"type":"error","code":"rate_limited", ...}`，message 中带建议退避秒数；
  客户端应退避后重试。该帧不计入服务端扇出。

## 背压（出站）

- 每个连接一个有界 `asyncio.Queue`。服务端所有推送（消息、成员事件、回放、快照）
  都走"非阻塞入队"：
  - 入队成功：由独立写任务顺序写出，保证单个连接上的事件顺序。
  - 入队失败（队列满）：该连接被标记 `slow_consumer_send_queue_full`，
    服务端立即断开它、把玩家移出房间与所有频道，并向其余成员广播权威集合，
    **处理过程不等待慢连接**，因此慢玩家不会阻塞整个房间。
- 调大 `client_send_queue_max` 可容忍更突发/更慢的客户端；调小则更快隔离慢消费者。

## 并发模型

- 每个房间一把 `asyncio.Lock`：成员变更、频道变更、消息取号与扇出入队、
  权限校验与 ACK 水位推进都在持锁期间原子完成。
- 扇出本身只是非阻塞的内存入队，锁内不做网络写，因此临界区很短、不会被慢客户端拉长。
- 多房间互不影响；水平扩展时按 `room_id` 把同一房间路由到同一实例即可。

## 内存与媒体边界

- 状态仅在单进程内存，不做持久化；文本消息只保留上述有界窗口。
- 语音频道只转发协商信令（SDP/ICE 等小 JSON），**不接触、不缓存、不落盘**媒体流；
  媒体由客户端之间（WebRTC P2P）或外部媒体服务器承载。
