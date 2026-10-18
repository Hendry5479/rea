// TradingView data feed client — 基于 REA 对 TradingView 桌面版 41.7.1 / Web 端的实际抓包证据实现
// 协议证据: wss://data.tradingview.com/socket.io/websocket?from=chart%2F&date=...&type=chart&auth=sessionid
// 帧格式: ~m~<len>~m~<json>；服务端心跳 ~h~<N> 需原样回显
// 代理: 自动读取 HTTPS_PROXY 环境变量（国内网络通常需要）
import WebSocket from 'ws';
import { HttpsProxyAgent } from 'https-proxy-agent';

const TV_WS_URL =
  'wss://data.tradingview.com/socket.io/websocket?from=chart%2F&type=chart&auth=sessionid';

// ---- 帧编解码 ----
export function encodeFrame(payload) {
  return `~m~${Buffer.byteLength(payload)}~m~${payload}`;
}

export function* decodeFrames(text) {
  let i = 0;
  while (i < text.length) {
    const marker = text.indexOf('~m~', i);
    if (marker !== i) break;
    const lenStart = marker + 3;
    const lenEnd = text.indexOf('~m~', lenStart);
    if (lenEnd === -1) break;
    const len = parseInt(text.slice(lenStart, lenEnd), 10);
    const payloadStart = lenEnd + 3;
    yield text.slice(payloadStart, payloadStart + len);
    i = payloadStart + len;
  }
}

// ---- 客户端 ----
export class TradingViewFeed {
  #ws;
  #nextId = 0;
  #listeners = new Map();
  session = {};

  /**
   * @param log 日志函数
   * @param options.authToken 认证 token；匿名固定为 'unauthorized_user_token'，
   *        登录态传服务端下发的 sessionid（协议证据: WS URL ?auth=sessionid + set_auth_token 帧）
   */
  constructor(log = () => {}, { authToken = 'unauthorized_user_token' } = {}) {
    this.log = log;
    this.authToken = authToken;
  }

  on(messageType, handler) {
    if (!this.#listeners.has(messageType)) this.#listeners.set(messageType, []);
    this.#listeners.get(messageType).push(handler);
  }

  #emit(messageType, params) {
    for (const h of this.#listeners.get(messageType) ?? []) h(params);
  }

  connect() {
    const proxy = process.env.HTTPS_PROXY ?? process.env.HTTPS_PROXY ?? process.env.HTTP_PROXY;
    const agent = proxy ? new HttpsProxyAgent(proxy) : undefined;
    return new Promise((resolve, reject) => {
      this.#ws = new WebSocket(TV_WS_URL, {
        agent,
        headers: {
          Origin: 'https://www.tradingview.com',
          'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36',
        },
      });
      this.#ws.on('open', () => resolve());
      this.#ws.on('error', (e) => reject(new Error(`WebSocket error: ${e.message}`)));
      this.#ws.on('message', (data) => this.#handleRaw(data.toString('utf8')));
    });
  }

  #handleRaw(raw) {
    for (const payload of decodeFrames(raw)) {
      if (payload.startsWith('~h~')) {
        // 心跳: 服务端发 ~h~N，客户端必须原样回显
        this.#sendRaw(payload);
        continue;
      }
      let msg;
      try {
        msg = JSON.parse(payload);
      } catch {
        continue;
      }
      if (msg.session_id) {
        this.session = msg;
        this.#emit('_session', msg);
        continue;
      }
      if (msg.m) {
        if (process.env.TV_DEBUG) console.log('[recv]', msg.m, JSON.stringify(msg.p).slice(0, 160));
        this.#emit(msg.m, msg.p);
      }
    }
  }

  #sendRaw(text) {
    this.#ws.send(encodeFrame(text));
  }

  send(messageType, params) {
    const payload = JSON.stringify({ m: messageType, p: params });
    this.log('→', payload);
    this.#sendRaw(payload);
  }

  newId(prefix) {
    return `${prefix}_${(++this.#nextId).toString(36)}`;
  }

  waitFor(messageType, timeoutMs = 30000) {
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error(`等待 ${messageType} 超时`)), timeoutMs);
      const h = (p) => {
        clearTimeout(t);
        this.#listeners.set(
          messageType,
          (this.#listeners.get(messageType) ?? []).filter((x) => x !== h),
        );
        resolve(p);
      };
      this.on(messageType, h);
    });
  }

  close() {
    this.#ws?.close();
  }

  /**
   * 高层辅助：建会话 → 匿名/登录认证 → 解析品种 → 建立序列，等待 symbol_resolved。
   * 返回 chartSession id，之后可用 modifySeries/changeResolution 在同一会话内继续操作。
   */
  async createChartSeries({ symbol, resolution = 'D', barsCount = 300 }) {
    this.send('set_auth_token', [this.authToken]);
    this.send('set_locale', ['en', 'US']);

    const chartSession = this.newId('cs');
    this.send('chart_create_session', [chartSession, '']);
    this.send('switch_timezone', [chartSession, 'Etc/UTC']);

    const symbolDsl = '=' + JSON.stringify({ adjustment: 'splits', symbol });
    const resolved = this.waitFor('symbol_resolved', 30000);
    const errored = this.waitFor('symbol_error', 30000).then((p) => {
      throw new Error(`invalid symbol: ${p[2]}`);
    });
    this.send('resolve_symbol', [chartSession, 'sds_sym_1', symbolDsl]);
    this.send('create_series', [chartSession, 'sds_1', 's1', 'sds_sym_1', resolution, barsCount, '']);
    await Promise.race([resolved, errored]);
    return chartSession;
  }

  /**
   * 历史分页（图表向左滚动/范围切换时的帧，真实抓包证据见 rea-analysis-output/capture-pagination.json）：
   *   modify_series [cs, sds_1, s1, sds_sym_1, resolution, duration]
   * - resolution: 与 create_series 同格式（'5'/'D' 均可，真实帧亦见 '1D'/'1W' 后缀形式）
   * - duration:   回看时长字符串（'12M'=12个月、'24M'、'60M'…），不是时间戳！
   *   传数字时间戳会报 critical_error "invalid series resolution"。
   * 服务端响应为整个区间的全量重发（timescale_update/du），按时间去重即可。
   */
  modifySeries(chartSession, resolution, duration) {
    this.send('modify_series', [chartSession, 'sds_1', 's1', 'sds_sym_1', resolution, duration]);
  }

  /** 同会话切换周期：modify_series 传入新 resolution + 时长（真实帧: 5Y按钮 → ["1W","60M"]，seriesId 会+1但服务端也接受原id） */
  changeResolution(chartSession, newResolution, duration = '24M') {
    this.send('modify_series', [chartSession, 'sds_1', 's1', 'sds_sym_1', newResolution, duration]);
  }
}

// ---- bar 收集器：按时间去重（实时 du 覆盖旧值、分页负索引补历史）----
function makeBarCollector(onBarUpdate) {
  const barMap = new Map();
  const handle = (payload) => {
    const [, data] = payload;
    for (const key of Object.keys(data)) {
      const entry = data[key];
      if (Array.isArray(entry?.s)) {
        for (const item of entry.s) {
          const [t, o, h, l, c, v] = item.v;
          barMap.set(t, { time: t, open: o, high: h, low: l, close: c, volume: v });
          onBarUpdate?.(barMap.get(t));
        }
      } else if (entry?.s === 'error') {
        console.error('series 数据错误:', key, entry);
      }
    }
  };
  return { barMap, handle };
}

// 周期 → 秒（modify_series 需要按周期步进往前翻页）
export const RESOLUTION_SECONDS = {
  '1': 60, '3': 180, '5': 300, '15': 900, '30': 1800, '45': 2700,
  '60': 3600, '120': 7200, '180': 10800, '240': 14400, '480': 28800,
  D: 86400, W: 604800, M: 2592000,
};

function sortedBars(barMap) {
  return [...barMap.values()].sort((a, b) => a.time - b.time);
}

/** 等待条件成立（barMap 尺寸/时间条件），超时返回 false */
async function waitForBars(barMap, predicate, timeoutMs) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    if (predicate()) return true;
    await new Promise((r) => setTimeout(r, 250));
  }
  return false;
}

// ---- 高层: 加载某品种某周期的 K 线 ----
export async function loadBars({ symbol, resolution = 'D', barsCount = 300, onBarUpdate }) {
  const tv = new TradingViewFeed((...a) => console.log('[tv]', ...a));
  await tv.connect();
  tv.log('← 会话', tv.session.session_id);

  const { barMap, handle } = makeBarCollector(onBarUpdate);
  tv.on('timescale_update', handle);
  tv.on('du', handle);

  const chartSession = await tv.createChartSeries({ symbol, resolution, barsCount });
  await waitForBars(barMap, () => barMap.size >= barsCount, 15000);

  tv.close();
  return sortedBars(barMap);
}

// ---- 高层: 历史分页加载（模拟图表向左滚动 → modify_series + 时长字符串）----
export async function loadBarsPaged({
  symbol, resolution = '5', initialCount = 300, pages = 3, onBarUpdate, onLog = console.log,
}) {
  const tv = new TradingViewFeed((...a) => console.log('[tv]', ...a));
  await tv.connect();
  tv.on('symbol_error', (p) => console.error('symbol_error', p));
  tv.on('critical_error', (p) => console.error('critical_error', p));

  const { barMap, handle } = makeBarCollector(onBarUpdate);
  tv.on('timescale_update', handle);
  tv.on('du', handle);

  const chartSession = await tv.createChartSeries({ symbol, resolution, barsCount: initialCount });
  await waitForBars(barMap, () => barMap.size >= initialCount, 15000);
  onLog(`首批 ${barMap.size} 根`);

  // 时长逐页翻倍: 12M → 24M → 48M …（服务端对分钟线受可用历史深度限制）
  const DURATIONS = ['12M', '24M', '48M', '96M', '192M'];
  for (let p = 0; p < Math.min(pages, DURATIONS.length); p++) {
    const duration = DURATIONS[p];
    tv.modifySeries(chartSession, resolution, duration);
    const before = barMap.size;
    const beforeEarliest = Math.min(...barMap.keys());
    const ok = await waitForBars(barMap, () => barMap.size > before, 20000);
    if (!ok) {
      onLog(`第 ${p + 1} 页(${duration})超时/无更多历史`);
      break;
    }
    const earliest = Math.min(...barMap.keys());
    if (earliest === beforeEarliest) {
      onLog(`第 ${p + 1} 页(${duration})已到服务端历史尽头（${barMap.size} 根）`);
      break;
    }
    onLog(`第 ${p + 1} 页(${duration}) → 共 ${barMap.size} 根（最早已到 ${new Date(earliest * 1000).toISOString()}）`);
  }

  tv.close();
  return sortedBars(barMap);
}

// ---- 高层: 同会话切换周期（modify_series 新 resolution + 时长）----
export async function switchResolution({ symbol, from = '5', to = 'D', barsCount = 300, duration = '24M' }) {
  const tv = new TradingViewFeed((...a) => console.log('[tv]', ...a));
  await tv.connect();

  const { barMap, handle } = makeBarCollector();
  tv.on('timescale_update', handle);
  tv.on('du', handle);

  const chartSession = await tv.createChartSeries({ symbol, resolution: from, barsCount });
  await waitForBars(barMap, () => barMap.size >= barsCount, 15000);
  const fromBars = barMap.size;
  const fromRange = [Math.min(...barMap.keys()), Math.max(...barMap.keys())];

  barMap.clear();
  tv.changeResolution(chartSession, to);
  await waitForBars(barMap, () => barMap.size >= 100, 20000);
  const toBars = barMap.size;
  const toRange = [Math.min(...barMap.keys()), Math.max(...barMap.keys())];

  tv.close();
  return {
    from: { resolution: from, range: fromRange.map((t) => new Date(t * 1000).toISOString()), bars: fromBars },
    to: { resolution: to, range: toRange.map((t) => new Date(t * 1000).toISOString()), bars: toBars },
  };
}

// ---- CLI 入口 ----
// node index.mjs BINANCE:BTCUSDT D 300              # 基础加载
// node index.mjs BINANCE:BTCUSDT 5 300 --paged 3    # 历史分页（modify_series）
// node index.mjs BINANCE:BTCUSDT 5 300 --switch D   # 同会话切换周期
import { pathToFileURL } from 'node:url';
if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  const args = process.argv.slice(2);
  const symbol = args[0] ?? 'BINANCE:BTCUSDT';
  const resolution = args[1] ?? 'D';
  const count = Number(args[2] ?? 300);
  const flagIdx = args.indexOf('--paged');
  const switchIdx = args.indexOf('--switch');

  if (switchIdx !== -1) {
    const to = args[switchIdx + 1] ?? 'D';
    const r = await switchResolution({ symbol, from: resolution, to, barsCount: count });
    console.log(`同会话切周期验证:\n  ${JSON.stringify(r.from)}\n  → ${JSON.stringify(r.to)}`);
  } else if (flagIdx !== -1) {
    const pages = Number(args[flagIdx + 1] ?? 3);
    const bars = await loadBarsPaged({ symbol, resolution, initialCount: count, pages });
    console.log(`\n共 ${bars.length} 根K线，最早3根:`);
    for (const b of bars.slice(0, 3)) {
      console.log(
        new Date(b.time * 1000).toISOString(),
        `O=${b.open} H=${b.high} L=${b.low} C=${b.close} V=${b.volume}`,
      );
    }
  } else {
    const bars = await loadBars({ symbol, resolution, barsCount: count });
    console.log(`\n共 ${bars.length} 根K线，最近5根:`);
    for (const b of bars.slice(-5)) {
      console.log(
        new Date(b.time * 1000).toISOString(),
        `O=${b.open} H=${b.high} L=${b.low} C=${b.close} V=${b.volume}`,
      );
    }
  }
}
