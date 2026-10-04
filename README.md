# game-chat-voice

面向游戏房间的实时文本聊天与语音频道服务（FastAPI + WebSocket）。

服务端**只做状态协调与信令转发**：维护频道、成员、消息序号与语音通话参与方集合，
转发客户端之间不透明的 SDP/ICE 信令；**不处理、不转发媒体流，也不落盘任何媒体**。
消息日志为内存结构（见“消息投递语义”）。

## 运行

```bash
uv sync
uv run python main.py            # 监听 127.0.0.1:8000
uv run pytest                    # 运行全部测试
```

- 健康检查：`GET /health`
- WebSocket：`/ws/rooms/{room_id}/players/{player_id}`
  房间在首次连接时自动创建；同一玩家可同时持有多个连接（多端/重连竞态）。

## 架构

| 模块 | 职责 |
| --- | --- |
| `game_chat/connections.py` | 在线连接注册表；每个连接一个有界出站队列，提供限流背压 |
| `game_chat/messages.py` | 每频道 append-only 消息日志：密集序号、幂等去重、断线补取 |
| `game_chat/voice.py` | 单个语音频道的通话参与方集合（join/leave 幂等） |
| `game_chat/rooms.py` | 房间核心：频道、成员、权限检查、语音协调、补取、令牌桶限流；单房间单锁串行化所有变更 |
| `game_chat/protocol.py` | JSON 帧协议与校验 |
| `game_chat/app.py` | FastAPI 装配、WebSocket 会话 reader/writer 循环 |

### 并发模型

- 每个房间一把 `asyncio.Lock`，所有变更操作都在锁内完成
  **“权限校验 → 状态变更 → 收集出站事件”**，因此并发操作不可能在校验通过后
  被另一个操作改变状态（越权与竞态关闭/踢人不会被绕过）。
- 事件投递在锁外执行；慢连接只会阻塞它自己的 writer 协程，不会阻塞房间。
- 出站队列满时先让出一次事件循环给本连接 writer 排空；仍满则执行配置的背压策略。

## WebSocket 协议

所有帧均为 JSON：`{"event": "...", "data": {...}, "req_id": "可选"}`。
客户端请求可带 `req_id`，服务端在对应响应/错误帧中原样回带，便于客户端关联重试。

### 客户端 → 服务端

| event | data | 说明 |
| --- | --- | --- |
| `channel.create` | `{"type": "text"\|"voice"}` | 创建频道，创建者即 owner/admin |
| `channel.close` | `{"channel_id"}` | 关闭频道（仅 owner） |
| `channel.join` / `channel.leave` | `{"channel_id"}` | 加入/退出，幂等 |
| `channel.kick` | `{"channel_id", "target_id"}` | 移除成员（owner/admin） |
| `message.send` | `{"channel_id", "body", "client_msg_id"}` | 发消息，`client_msg_id` 为客户端幂等键 |
| `message.ack` | `{"channel_id", "seq"}` | 确认已收到序号 |
| `voice.join` / `voice.leave` | `{"channel_id"}` | 加入/离开语音通话，幂等 |
| `voice.signal` | `{"channel_id", "target_id", "payload"}` | 向单个目标转发不透明信令 |
| `state.snapshot` | `{}` | 立即取一次房间状态快照 |
| `session.resume` | `{"channels": [{"channel_id", "last_seq"}]}` | 重连后补取 `last_seq` 之后的消息 |

### 服务端 → 客户端

`ready`、`state.snapshot`、`channel.created`、`channel.closed`、
`member.joined`、`member.left`、`message`（补发的带 `"replayed": true`）、
`message.ack`（发送确认，含服务端分配的 `id`/`seq`）、
`voice.participant_joined`、`voice.participant_left`、`voice.signal`、
`resume.complete`、`ok`、`error`。

错误帧：`{"event":"error","data":{"code":"...","message":"..."},"req_id":...}`。

## 消息投递语义

1. **稳定总序**：每个文本频道一条 append-only 日志。服务端在房间锁内分配
   从 1 开始、稠密、单调递增的 `seq`。无论请求到达时如何乱序或交错，
   一旦提交，所有玩家看到的都是同一个序列，不丢号、不重号。
2. **稳定唯一标识**：每条消息同时具有服务端全局唯一 `id` 与频道内 `seq`，
   供客户端去重。
3. **至少一次 + 幂等**：发送必须带客户端生成的 `client_msg_id`。
   网络重传/重复发送同一个键时，服务端返回**原始消息**（相同 `id`/`seq`），
   不会产生第二条记录；客户端可按 `id` 去重，按 `seq` 排序。
4. **迟到消息**：序号在提交时确定；客户端收到旧序号（乱序到达）按 `seq`
   插入即可，状态不会分叉。历史频道日志按 `channel_history` 截断，
   截断后剩余消息保留原始序号。
5. **断线补取（resume）**：重连后发送 `session.resume` 并携带各频道已收到的
   `last_seq`，服务端只回放严格大于该序号且仍在历史窗口内的消息
   （带 `replayed: true`），随后推送一次 `state.snapshot`，再返回
   `resume.complete`。回放会推进该成员水位，因此**已确认消息不会重复投递**。
   频道成员身份在断线期间保留；语音参与身份在最后一个连接断开时释放，
   重连后需重新 `voice.join`，状态连续可解释。

文本消息只投递给**该频道当前成员**；成员进出、语音参与事件只投递给频道/通话
相关玩家；语音信令只投递给 `target_id` 指定的单一目标。

## 语音频道与信令

- 语音通话仅存在于 `type=voice` 的频道；加入频道后还需 `voice.join` 才进入通话。
- `voice.join`/`voice.leave` 幂等：重复加入不会出现重复成员，重复离开不会
  让集合进入错误状态；参与方变更时事件携带当前完整 `participants` 名单，
  各端最终收敛到一致集合。
- 信令目标在房间锁内根据**当前**参与方快照校验：发送方和目标方都必须仍在
  通话中，且不允许自发自收。已离开（含断线被清理）的参与方绝不可能再收到
  信令，对其发送返回 `not_call_participant`。
- 最后一人离开后通话自动销毁，下次 `voice.join` 开启新通话。

## 权限模型

每个频道有三种角色：`owner`（创建者）> `admin` > `member`。

| 操作 | 要求 | 拒绝原因码（可区分） |
| --- | --- | --- |
| 发言 / 进出频道 / 语音信令 | 频道成员 | `permission_denied.not_member` |
| `voice.*` 在文本频道 | 语音频道 | `permission_denied.not_voice_channel` |
| `channel.kick` | owner/admin | `permission_denied.not_member`（非成员）/ `.not_admin`（普通成员） |
| admin 踢 admin | 仅 owner 可踢 admin | `permission_denied.target_higher_role` |
| 任何人踢 owner | 不允许 | `permission_denied.target_is_owner` |
| `channel.close` | 仅 owner | `permission_denied.not_owner` |
| 信令自发自收 | 目标≠自己 | `permission_denied.self_target` |

其他错误码：`channel_not_found`、`not_call_participant`、
`voice_call_not_found`、`stale_operation`（如非法 ack 序号）、
`rate_limited.messages|signaling|actions`、`protocol_error`。

HTTP 状态码在直接 API 调用时通过异常的 `http_status` 提供（403/404/409/429）。

## 限流与背压配置

可编程构造 `Settings`，或用环境变量（前缀 `GAME_CHAT_`）覆盖默认值：

| 环境变量 | 默认 | 含义 |
| --- | --- | --- |
| `GAME_CHAT_RATE_MESSAGES_PER_SECOND` | `5.0` | 每玩家消息令牌桶速率 |
| `GAME_CHAT_RATE_SIGNALING_PER_SECOND` | `10.0` | 每玩家语音信令速率 |
| `GAME_CHAT_RATE_ACTIONS_PER_SECOND` | `5.0` | 每玩家频道操作（建/关/进出/踢/语音进出）速率 |
| `GAME_CHAT_RATE_BURST` | `10` | 令牌桶突发容量 |
| `GAME_CHAT_BACKPRESSURE_QUEUE_SIZE` | `256` | 每连接出站队列容量 |
| `GAME_CHAT_BACKPRESSURE_SLOW_CONSUMER_BEHAVIOR` | `drop_oldest` | 队列持续满时：`drop_oldest`（丢弃最旧事件，适合在线状态类事件，保最新）或 `disconnect`（断开慢连接） |
| `GAME_CHAT_CHANNEL_HISTORY` | `500` | 每频道内存保留的消息条数（断线补取窗口） |

限流按玩家独立计算；被限流的请求被拒绝且**不占用消息序号**。

## 测试

```bash
uv run pytest
```

覆盖场景：

- 消息序号稠密有序、重复 `client_msg_id` 幂等、乱序并发发送后序列一致；
- 断线重连补取离线消息、已确认消息不重复投递、语音状态断线释放与重连收敛；
- 8 连接并发进出与 80 条并发发言无丢失、成员无重复、序号稠密；
- 越权关闭/踢人（非成员、普通成员、平级 admin、踢 owner）返回不同原因码；
- 令牌桶限流（拒绝不占序号）、`drop_oldest` 与 `disconnect` 两种背压策略；
- 信令仅达目标、离开后无法再被投递。
