# tv-datafeed 接口文档

基于 REA 对 TradingView 桌面版 41.7.1 / Web 端实际抓包证据实现的 K 线数据客户端。

- 协议端点：`wss://data.tradingview.com/socket.io/websocket?from=chart%2F&type=chart&auth=sessionid`
- 帧格式：`~m~<字节数>~m~<JSON文本>`，一帧 TCP 消息可包含多帧
- 心跳：服务端发送 `~h~<N>`，客户端必须**原样回显**（本模块自动处理）
- 代理：自动读取 `HTTPS_PROXY` / `HTTP_PROXY` 环境变量（国内网络通常需要）

## 安装

```bash
cd tv-datafeed
npm install
```

依赖：`ws`、`https-proxy-agent`。源码为 ESM（`.mjs`），调用方建议使用 `import` 语法。

---

## 高层接口：`loadBars(options)`

加载某品种某周期的历史 K 线。内部自动完成 连接 → 匿名认证 → 建会话 → 解析品种 → 创建序列 → 收数据 → 断开 的全流程。

```js
import { loadBars } from './index.mjs';

const bars = await loadBars({
  symbol: 'BINANCE:BTCUSDT',
  resolution: 'D',
  barsCount: 300,
  onBarUpdate: (bar) => console.log('增量更新', bar),
});
```

### 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `symbol` | `string` | 必填 | TradingView 品种代码，如 `BINANCE:BTCUSDT`、`NASDAQ:AAPL` |
| `resolution` | `string` | `'D'` | 周期，TradingView 标准写法：`1`/`5`/`15`/`30`/`60`/`120`/`240`（分钟）、`D`、`W`、`M`，原样透传 |
| `barsCount` | `number` | `300` | 请求的 K 线根数 |
| `onBarUpdate` | `(bar: Bar) => void` | 可选 | 每收到一根 K 线（含实时更新覆盖）时的回调 |

### 返回值

`Promise<Bar[]>`，按 `time` 升序排列。`time` 为 Unix 秒级时间戳。

```ts
interface Bar {
  time: number;    // Unix 秒
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}
```

### 行为说明与限制

- 内部在 `symbol_resolved` 后等待首批数据到齐（按 `barsCount` 判断，15 秒超时）再断开。
- 品种代码错误时服务端回 `symbol_error`，仅在控制台打印错误，`loadBars` 不会 reject，返回的 `bars` 为空数组。
- 每次调用新建独立连接，不可并发复用同一连接。

---

## 高层接口：`loadBarsPaged(options)` — 历史分页

模拟图表向左滚动，通过 `modify_series` 按"回看时长"逐页扩展历史（2026-10 真实抓包证据：`rea-analysis-output/capture-pagination.json`）。

```js
import { loadBarsPaged } from './index.mjs';

const bars = await loadBarsPaged({
  symbol: 'BINANCE:BTCUSDT',
  resolution: '5',
  initialCount: 200,
  pages: 3,   // 依次请求 12M → 24M → 48M
  onLog: console.log,
});
```

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `symbol` / `resolution` / `onBarUpdate` | 同 `loadBars` | — | `resolution` 默认 `'5'` |
| `initialCount` | `number` | `300` | 首批 `create_series` 请求的根数 |
| `pages` | `number` | `3` | 分页次数（每页时长 `12M/24M/48M/96M/192M` 逐级递增） |
| `onLog` | `(msg: string) => void` | `console.log` | 分页进度日志 |

行为说明：`modify_series` 响应是**整个区间的全量重发**（不是增量），模块按时间去重合并；分钟线受服务端历史深度限制（如币安 5m 只有约 5500 根），到达尽头时提前结束并返回。

---

## 高层接口：`switchResolution(options)` — 同会话切换周期

验证真实 TV 客户端行为：切换周期**不重建会话**，直接发 `modify_series` 带新 resolution + 时长（真实帧：点 5Y 按钮 → `["1W","60M"]`）。

```js
import { switchResolution } from './index.mjs';

const r = await switchResolution({ symbol: 'BINANCE:BTCUSDT', from: '5', to: 'D', barsCount: 200 });
// r = { from: { resolution, range, bars }, to: { resolution, range, bars } }
```

---

## 底层接口

### `encodeFrame(payload: string): string`

将一段文本编码为 TradingView 传输帧：`~m~<Buffer.byteLength(payload)>~m~<payload>`。

### `decodeFrames(text: string): Generator<string>`

从一段原始文本中按帧协议解码，逐帧 yield 出 payload。遇到非法帧头即停止解析。

### `class TradingViewFeed`

WebSocket 客户端封装，负责帧编解码、心跳回显、消息分发。

#### 构造函数

```js
const tv = new TradingViewFeed((...args) => console.log('[tv]', ...args));
```

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `log` | `(...args) => void` | 空函数 | 每次调用 `send()` 时记录出站消息 |

#### 属性

| 属性 | 类型 | 说明 |
| --- | --- | --- |
| `session` | `object` | 连接后服务端下发的会话信息（含 `session_id`），握手消息到达后自动填充 |

#### 方法

| 方法 | 签名 | 说明 |
| --- | --- | --- |
| `connect()` | `() => Promise<void>` | 建立 WebSocket 连接（自动带 `Origin: https://www.tradingview.com` 头与浏览器 UA；读取 `HTTPS_PROXY`/`HTTP_PROXY` 走代理）。open 时 resolve，出错 reject |
| `on(type, handler)` | `(string, (params) => void) => void` | 注册消息处理器。`params` 为服务端消息的 `p` 字段。特殊类型 `_session` 对应会话握手消息 |
| `send(type, params)` | `(string, any[]) => void` | 发送 `{ m: type, p: params }` 消息（自动加帧） |
| `newId(prefix)` | `(string) => string` | 生成会话内唯一 ID，如 `cs_1`、`cs_2`（计数器为 36 进制） |
| `waitFor(type, timeoutMs?)` | `(string, number=30000) => Promise<params>` | 一次性等待某类型消息，收到即自动注销监听并 resolve；超时 reject。**注意：先 `waitFor` 再 `send`，避免响应早于监听注册** |
| `close()` | `() => void` | 关闭 WebSocket 连接 |
| `createChartSeries({symbol, resolution, barsCount})` | `async` | 认证 → 建会话 → 解析品种 → `create_series`，等待 `symbol_resolved` 后返回 `chartSession` id（后续 modify 系列方法复用） |
| `modifySeries(chartSession, resolution, duration)` | `(string, string, string) => void` | 历史分页帧：`[cs, 'sds_1', 's1', 'sds_sym_1', resolution, duration]`。**`duration` 是回看时长字符串**（`'12M'`=12 个月、`'24M'`、`'60M'`…），不是时间戳——传数字时间戳会报 `critical_error: invalid series resolution`。响应为全量区间重发（`timescale_update`/`du`） |
| `changeResolution(chartSession, newResolution, duration?)` | `(string, string, string='24M') => void` | 同会话切换周期（`modify_series` + 新 resolution；真实帧 seriesId 会 +1，但沿用原 id 服务端也接受） |

#### 构造选项（登录态）

```js
// 匿名（默认）: set_auth_token ['unauthorized_user_token']
const tv = new TradingViewFeed(log, { authToken: 'unauthorized_user_token' });
// 登录态: 传服务端下发的 sessionid（WS URL 本身也带 ?auth=sessionid）
const tv2 = new TradingViewFeed(log, { authToken: sessionId });
```

#### 事件（服务端消息类型 `m`）

| 消息类型 | 方向 | 说明 |
| --- | --- | --- |
| `qsd` / 含 `session_id` 的消息 | 下行 | 会话握手，模块自动存入 `this.session` 并触发 `_session` 事件 |
| `symbol_resolved` | 下行 | `resolve_symbol` 的响应，`p` 形如 `[cs, 'sds_sym_1', symbolInfo]` |
| `timescale_update` | 下行 | 历史数据首批回包，`p` 形如 `['cs_xxx', { sds_1: { s: [{i, v: [t,o,h,l,c,v]}] } }]` |
| `du` | 下行 | 增量/实时更新，结构与 `timescale_update` 相同 |
| `symbol_error` | 下行 | 品种解析失败 |
| `critical_error` | 下行 | 协议级严重错误 |
| `~h~<N>` | 下行 | 心跳，模块自动原样回显，上层无需处理 |

### 典型底层调用序列（与抓包证据帧一致）

```js
const tv = new TradingViewFeed(console.log);
await tv.connect();

tv.send('set_auth_token', ['unauthorized_user_token']); // 匿名认证
tv.send('set_locale', ['en', 'US']);

const cs = tv.newId('cs');
tv.send('chart_create_session', [cs, '']);
tv.send('switch_timezone', [cs, 'Etc/UTC']);

// 品种参数为 DSL 字符串，前缀 '='
const symbolDsl = '=' + JSON.stringify({ adjustment: 'splits', symbol: 'BINANCE:BTCUSDT' });
const resolved = tv.waitFor('symbol_resolved');
tv.send('resolve_symbol', [cs, 'sds_sym_1', symbolDsl]);
tv.send('create_series', [cs, 'sds_1', 's1', 'sds_sym_1', 'D', 300, '']);

await resolved;
// 后续监听 timescale_update / du 收取 K 线
tv.close();
```

---

## CLI 用法

```bash
node index.mjs [symbol] [resolution] [barsCount] [--paged N] [--switch RES]

# 基础加载
node index.mjs BINANCE:BTCUSDT D 300   # 币安 BTCUSDT 日线 300 根
node index.mjs NASDAQ:AAPL 60 100      # 苹果 60 分钟线 100 根

# 历史分页（modify_series，时长逐页翻倍 12M→24M→48M…）
node index.mjs BINANCE:BTCUSDT 5 300 --paged 3

# 同会话切换周期（先建 5 分钟序列，再 modify_series 切到日线）
node index.mjs BINANCE:BTCUSDT 5 300 --switch D
```

| 位置参数 | 默认值 | 说明 |
| --- | --- | --- |
| 1 | `BINANCE:BTCUSDT` | 品种代码 |
| 2 | `D` | 周期 |
| 3 | `300` | K 线根数 |

| 标志 | 默认值 | 说明 |
| --- | --- | --- |
| `--paged N` | 关闭 | 历史分页页数，每页回看时长 `12M/24M/48M/96M/192M` 逐级递增；到达服务端历史尽头提前结束 |
| `--switch RES` | 关闭 | 建立序列后切到 `RES` 周期（如 `D`、`60`），输出切换前后两个周期的区间与根数 |

输出：共加载的 K 线总数与最近 5 根的 ISO 时间及 OHLCV。

---

## 环境变量

| 变量 | 说明 |
| --- | --- |
| `HTTPS_PROXY` / `HTTP_PROXY` | WebSocket 出站代理地址，如 `http://127.0.0.1:7890`；未设置则直连 |
| `TV_DEBUG` | 设置任意值后，每条收到的消息打印 `[recv] <m> <p截断160字符>` 调试日志 |
