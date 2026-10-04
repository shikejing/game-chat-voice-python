# 权限模型与错误码

## 角色层级

| 角色 | 获得方式 | 能力 |
|---|---|---|
| 房主 `room owner` | 房间的首个连接者（`Room.owner_id`） | 对房间内**所有**频道拥有最高权限（可关闭任意频道、移除任意成员包括频道管理员） |
| 频道管理员 `channel admin` | 创建频道者自动成为；也可由房主/管理员内部授予（`admins`） | 关闭该频道、移除该频道普通成员 |
| 频道成员 `channel member` | `join_channel` | 在该文本频道发言、拉取该频道历史；在语音频道参与通话与收发信令 |
| 房间成员 `room member` | 连接即加入房间 | 创建频道、收到房间/频道状态事件 |

规则汇总：

1. **发言 / 读历史**：必须是对应文本频道的成员，否则
   `permission_denied_not_channel_member`。
2. **关闭频道 / 移除成员**：必须是该频道的管理员或房主，校验与状态修改在**同一把
   房间锁内原子完成**，TOCTOU 类并发绕过不可能发生。
3. 管理员**不能**移除同为管理员的成员（错误码
   `permission_denied_admin_target`）；房主可以。
4. 已关闭频道不能再发言/加入（`invalid_state`），但管理员对其重复关闭会被明确拒绝
   （同样 `invalid_state`）。
5. 语音信令是"通话参与者之间"的特权：频道成员但不在通话中 → 发送被拒
   `permission_denied_not_voice_participant`；目标不在通话中 → `not_found`。
   这保证已离开通话的人**既收不到也发不出**信令。

## 成员状态一致性

- 所有成员集合都是 Python `set`：`join` 用 `add`、`leave` 用 `discard`，
  因此重复加入不重复、重复退出不报错也不留错状态。
- 每次集合变更都广播**权威全量集合**（`channel_members` / `voice_participants`），
  而不是增量；客户端直接覆盖本地集合即可收敛，天然免疫重复/乱序事件导致的偏差。
- 断线在宽限期内只翻转 `connected`；超时或慢消费者则彻底移出并广播全量集合。

## 错误码对照表

| `code` | HTTP/WS 场景 | 含义 / 可区分用途 |
|---|---|---|
| `not_found` | WS error / WS 4403 | 房间、频道、目标成员不存在（含语音信令目标已不在通话中） |
| `already_exists` | WS error | 房间/频道 ID 已存在 |
| `conflict` | WS error / WS 4403 | 与当前状态冲突（如房间人数上限） |
| `invalid_request` | WS error | 参数缺失/类型错误、非法信令类型、空消息体 |
| `invalid_state` | WS error | 频道已关闭、对已关闭频道重复操作 |
| `permission_denied_not_channel_member` | WS error | 不是频道成员却发言/读历史 |
| `permission_denied_not_channel_admin` | WS error | 非管理员尝试关闭频道或移除成员 |
| `permission_denied_admin_target` | WS error | 管理员尝试移除另一个管理员 |
| `permission_denied_not_voice_participant` | WS error | 不在语音通话中却发送信令 |
| `rate_limited` | WS error | 超出每连接入站请求速率（令牌桶），message 含建议退避时间 |
| `unknown_request` | WS error | 不认识的请求 `type` |

所有错误帧形如：

```json
{"type": "error", "ref": "客户端原始ref", "code": "permission_denied_not_channel_admin",
 "message": "channel_admin or room_owner required"}
```

客户端应按 `code`（而不是 message 文本）做分支处理。
