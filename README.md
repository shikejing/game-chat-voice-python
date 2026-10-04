# game-chat-voice-python

面向游戏房间的**实时文本聊天**与**语音频道信令**服务。服务端只做状态协调与信令转发，
不处理、不落盘任何语音媒体（媒体走 WebRTC 等 P2P/媒体服务器通道）。

## 能力概览

- 房间内创建/关闭多个**文本频道**与**语音频道**；加入、退出、移除成员实时同步。
  所有成员关系都是集合语义，重复加入/重复退出幂等，不会产生重复或残留状态。
- 文本频道消息按频道内单调递增的 `seq` 投递，并带全局唯一 `message_id`；
  支持客户端幂等键 `client_msg_id` 去重。在线扇出 + 离线回放都保证按 `seq` 顺序。
- 语音信令（`offer` / `answer` / `ice_candidate` / `custom`）只能投递给当前通话参与者；
  参与者加入/离开时，其余参与者通过 `voice_participants` 事件收敛到一致集合。
  已离开者**收不到也发不出**信令。
- 断线重连：在离线宽限期内重连自动回放错过的文本消息（先回放、后快照，顺序确定），
  客户端用 `ack` 维护确认水位，已确认的消息不会重复投递。
- 频道级权限：只有频道管理员或房主能关闭频道/移除成员；越权返回**可区分的错误码**；
  所有"检查 + 修改"在每房间单把锁内原子完成，并发下无法绕过。
- 并发安全与背压：每房间串行化变更；每连接有界发送队列 + 入站令牌桶限流；
  慢消费者会被断开并驱逐，不会拖垮房间。

更多细节见：

- [`docs/protocol.md`](docs/protocol.md)：WebSocket 协议与全部事件
- [`docs/messaging.md`](docs/messaging.md)：消息投递语义、顺序、去重、断线重连
- [`docs/permissions.md`](docs/permissions.md)：权限模型与错误码
- [`docs/configuration.md`](docs/configuration.md)：限流 / 背压 / 保留策略配置项

## 快速开始

```bash
uv sync
uv run uvicorn main:app --host 127.0.0.1 --port 8000
# 健康检查
curl http://127.0.0.1:8000/health
```

连接 WebSocket（query 里带玩家身份）：

```
ws://127.0.0.1:8000/ws/rooms/{room_id}?player_id=alice&name=Alice
```

房间在首个玩家连接时自动创建，首个玩家成为房主。

```jsonc
// 客户端 -> 服务端
{"type": "create_channel", "ref": "1", "name": "general", "kind": "text"}
{"type": "join_channel", "ref": "2", "channel_id": "ch_xxx"}
{"type": "message", "ref": "3", "channel_id": "ch_xxx",
 "body": "hello", "client_msg_id": "uuid-generated-by-client"}
{"type": "ack", "channel_id": "ch_xxx", "seq": 12}
{"type": "voice_signal", "channel_id": "ch_yyy",
 "signal_type": "offer", "target_id": "bob", "payload": {"sdp": "..."}}
```

## 运行测试

```bash
uv run pytest
```

测试覆盖：乱序 / 重复 / 迟到消息、断线重连回放、并发进出、并发发言顺序、
越权操作、语音参与者收敛、限流与慢消费者驱逐，以及真实 WebSocket 端到端用例。

## 模块结构

```
game_chat/
  config.py      # 可调参数（宽限期、保留数、队列、限流……）
  errors.py      # 领域异常与稳定错误码
  models.py      # Room / Channel / Member / StoredMessage
  connection.py  # 有界发送队列 + 令牌桶入站限流
  state.py       # 房间注册表 + 每房间 asyncio.Lock
  service.py     # 核心编排：成员、频道、消息、回放、语音信令
  protocol.py    # JSON 协议解析与请求分发
  app.py         # FastAPI / WebSocket 端点
```

> 说明：当前状态保存在单进程内存中，适合作为房间协调层水平分片（按 room_id 路由）。
> 不持久化媒体；文本消息仅在配置的有界窗口内保留以支持断线回放。
