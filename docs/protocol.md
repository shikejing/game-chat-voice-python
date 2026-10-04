# WebSocket 协议

连接：`GET /ws/rooms/{room_id}?player_id={id}&name={显示名}`（WebSocket 升级）

- 同一 `player_id` 同时只允许一个活跃连接：新连接会替换并关闭旧连接
  （旧连接收到关闭原因 `replaced_by_new_session`），因此重复登录不会产生重复成员状态。
- 缺少 `player_id` 时以 HTTP WS 关闭码 `4401` 拒绝；房间人数超限时以 `4403` 拒绝。

所有报文都是 JSON 文本帧。客户端请求可选带 `ref`，服务端对应回包原样回填，便于关联。

## 客户端 → 服务端

| `type` | 关键字段 | 说明 |
|---|---|---|
| `ping` | `ref?` | 心跳，回 `pong` |
| `create_channel` | `name`, `kind`(`text`/`voice`), `channel_id?` | 创建频道，创建者自动加入并成为频道管理员 |
| `close_channel` | `channel_id` | 关闭频道（仅管理员/房主） |
| `join_channel` | `channel_id` | 加入频道（语音频道同时加入通话，幂等） |
| `leave_channel` | `channel_id` | 离开频道（语音频道同时退出通话，幂等） |
| `remove_member` | `channel_id`, `target_id` | 移除成员（仅管理员/房主） |
| `message` | `channel_id`, `body`, `client_msg_id?` | 发送文本消息 |
| `ack` | `channel_id`, `seq` | 确认已收到该频道 `seq` 及之前的消息（单调水位） |
| `fetch_history` | `channel_id`, `after_seq?`, `limit?` | 按需拉取历史消息（有界） |
| `voice_signal` | `channel_id`, `signal_type`, `target_id`, `payload?` | 转发语音协商信令 |
| `leave_room` | — | 立即退出房间（不经过离线宽限） |

允许的 `signal_type`：`offer`、`answer`、`ice_candidate`、`custom`。

请求成功统一回：

```json
{"type": "ack", "ref": "3", "data": {"message_id": "msg_...", "seq": 1, "deduped": false}}
```

请求失败统一回（错误码见 [`permissions.md`](permissions.md)）：

```json
{"type": "error", "ref": "3", "code": "permission_denied_not_channel_admin",
 "message": "channel_admin or room_owner required"}
```

## 服务端 → 客户端（推送事件）

| `type` | 关键字段 | 触发时机 |
|---|---|---|
| `connected` | `replayed`, `new_member` | 连接建立成功（在回放与快照之前） |
| `snapshot` | `members`, `channels` | 连接/重连后的权威全量状态，**永远在回放消息之后** |
| `message` | `message_id`, `client_msg_id?`, `seq`, `sender_id`, `body`, `created_at`, `deduped` | 新文本消息（实时或离线回放） |
| `presence` | `player_id`, `connected`, `rejoined?` | 房间成员上/下线 |
| `channel_created` | `channel` | 新频道 |
| `channel_closed` | `channel_id` | 频道被关闭 |
| `channel_snapshot` | `channel` | 加入频道时的单频道快照 |
| `channel_members` | `channel_id`, `members`, `voice_participants?` | 频道成员集合变化（权威全量集合） |
| `voice_participants` | `channel_id`, `participants` | 通话参与者集合变化（权威全量集合） |
| `voice_signal` | `channel_id`, `signal_type`, `from`, `target_id`, `payload` | 语音信令，**只投给 `target_id`** |
| `removed_from_channel` | `channel_id`, `by` | 自己被管理员移出频道 |
| `room_member_left` | `player_id`, `reason?` | 成员永久离开房间（主动离开或宽限期到期 / 慢消费者驱逐） |
| `pong` / `error` / `ack` | — | 心跳响应 / 错误 / 请求应答 |

`channel` / 频道快照结构：

```json
{
  "channel_id": "ch_...",
  "name": "general",
  "kind": "text",
  "owner_id": "alice",
  "closed": false,
  "members": ["alice", "bob"],
  "admins": ["alice"],
  "next_seq": 12
}
```

语音频道额外含 `voice_participants`（当前通话参与者）。

## 连接关闭码

| 码 | 含义 |
|---|---|
| 1000 | 正常关闭（含主动离开、被新会话替换） |
| 4401 | 缺少 `player_id` |
| 4403 | 连接被拒（房间已满等） |
| 4500 | 服务端内部错误 |

慢消费者会在发送队列持续堆满时被服务端关闭，关闭原因 `slow_consumer_send_queue_full`，
并广播 `room_member_left`（`reason="slow_consumer"`）。
