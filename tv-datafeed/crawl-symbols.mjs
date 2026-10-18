// TradingView 品种目录爬虫 — 抓取 symbol-search 公开接口，按类型全量落盘
//
// 数据源: https://symbol-search.tradingview.com/symbol_search/v3/
//   ?text=&start=<offset>&hl=1&lang=en&search_type=<type>&domain=production
// - 空 text 返回该类型全部品种（热门度排序，每页 50 条，接口上限 10000 条/查询）
// - 需要代理时与 index.mjs 一致：读取 HTTPS_PROXY / HTTP_PROXY，
//   未设置则探测本机常见代理端口（国内直连该接口不通）
//
// 用法:
//   node crawl-symbols.mjs [--force] [--types=crypto,stocks]
//   --force        已落盘的类型也重新抓取
//   --types=a,b    只抓指定类型（默认全部 8 类）
//
// 输出: data/symbols-<type>.json
//   { type, updatedAt, count, capped, items: [{ code, symbol, description, exchange, type, currency }] }
// code 为 charting-library / tv-datafeed 可用的品种代码（PREFIX:SYMBOL）。
import https from 'node:https';
import zlib from 'node:zlib';
import fs from 'node:fs';
import path from 'node:path';
import net from 'node:net';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { HttpsProxyAgent } from 'https-proxy-agent';

const SYMBOL_SEARCH_HOST = 'symbol-search.tradingview.com';
// 类型 → 中文标签（与前端 symbolDirectory.ts 保持一致）
export const SYMBOL_TYPES = [
  { id: 'crypto', label: '加密货币' },
  { id: 'stocks', label: '股票' },
  { id: 'forex', label: '外汇' },
  { id: 'futures', label: '期货' },
  { id: 'index', label: '指数' },
  { id: 'bond', label: '债券' },
  { id: 'funds', label: '基金' },
  { id: 'cfd', label: '差价合约' },
];
const PAGE_SIZE = 50;
const MAX_START = 10000; // 接口对单查询的总量上限
const PAGE_DELAY_MS = 150;
const TYPE_DELAY_MS = 500;
const PAGE_TIMEOUT_MS = 20000;

const BROWSER_UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36';

// ---- 代理（与前端 tvDatafeedLoader.ensureProxyEnv 行为一致）----
const CANDIDATE_PROXY_PORTS = [7897, 7890, 7891, 10809, 1080];

function isPortOpen(port, timeoutMs = 250) {
  return new Promise((resolve) => {
    const socket = net.connect({ port, host: '127.0.0.1' });
    const finish = (open) => {
      socket.destroy();
      resolve(open);
    };
    socket.setTimeout(timeoutMs, () => finish(false));
    socket.once('connect', () => finish(true));
    socket.once('error', () => finish(false));
  });
}

async function ensureProxyAgent() {
  const explicit = process.env.HTTPS_PROXY ?? process.env.HTTP_PROXY;
  let proxy = explicit?.trim() || null;
  if (!proxy) {
    for (const port of CANDIDATE_PROXY_PORTS) {
      if (await isPortOpen(port)) {
        proxy = `http://127.0.0.1:${port}`;
        console.log(`[crawl] 未配置代理，自动使用本机代理 ${proxy}`);
        break;
      }
    }
  }
  return proxy ? new HttpsProxyAgent(proxy) : undefined;
}

// ---- GET JSON（gzip 自动解压）----
function getJson(url, agent) {
  return new Promise((resolve, reject) => {
    const req = https.get(
      url,
      {
        agent,
        headers: {
          Origin: 'https://www.tradingview.com',
          Referer: 'https://www.tradingview.com/',
          Accept: 'application/json',
          'Accept-Encoding': 'gzip',
          'User-Agent': BROWSER_UA,
        },
        timeout: PAGE_TIMEOUT_MS,
      },
      (res) => {
        if (res.statusCode !== 200) {
          res.resume();
          reject(new Error(`HTTP ${res.statusCode}: ${url}`));
          return;
        }
        let stream = res;
        if (res.headers['content-encoding'] === 'gzip') {
          stream = res.pipe(zlib.createGunzip());
        }
        const chunks = [];
        stream.on('data', (c) => chunks.push(c));
        stream.on('end', () => {
          try {
            resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
          } catch (e) {
            reject(e);
          }
        });
        stream.on('error', reject);
      },
    );
    req.on('timeout', () => req.destroy(new Error('请求超时')));
    req.on('error', reject);
  });
}

const stripEm = (s) => (typeof s === 'string' ? s.replace(/<\/?em>/g, '') : '');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---- 单类型抓取：空 text 热门度排序全量分页（网络异常时返回已收集的部分数据）----
async function crawlType(typeId, agent) {
  const seen = new Map();
  let start = 0;
  let capped = false;
  let error = null;

  while (start <= MAX_START) {
    const url =
      `https://${SYMBOL_SEARCH_HOST}/symbol_search/v3/` +
      `?text=&start=${start}&hl=1&lang=en&search_type=${typeId}&domain=production`;
    let body;
    try {
      for (let attempt = 1; attempt <= 3; attempt++) {
        try {
          body = await getJson(url, agent);
          break;
        } catch (e) {
          if (attempt === 3) throw e;
          console.warn(`[crawl] ${typeId} start=${start} 第 ${attempt} 次失败: ${e.message}，重试`);
          await sleep(1000 * attempt);
        }
      }
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
      break;
    }

    const rows = Array.isArray(body?.symbols) ? body.symbols : [];
    if (rows.length === 0) break;

    for (const row of rows) {
      const symbol = stripEm(row.symbol);
      if (!symbol) continue;
      // 期货等衍生品：主条目是裸代码（如 MNQ），无法直接 resolve（invalid symbol）。
      // 可交易的合约在 contracts 数组中，取首个 continuous 合约（如 CME_MINI:MNQ1!）。
      let outSymbol = symbol;
      let code = row.prefix ? `${stripEm(row.prefix)}:${symbol}` : symbol;
      const contracts = Array.isArray(row.contracts) ? row.contracts : [];
      if (contracts.length > 0) {
        const continuous =
          contracts.find(
            (c) => Array.isArray(c?.typespecs) && c.typespecs.includes('continuous'),
          ) ?? contracts[0];
        const cSymbol = stripEm(continuous?.symbol);
        // 合约项可能不带 prefix（如 COMEX 的 GC1!），用主条目 source_id
        // （TV 真实交易所 id，如 COMEX/NYMEX/CME_MINI）兜底
        const cPrefix =
          stripEm(continuous?.prefix) ||
          (typeof row.source_id === 'string' ? stripEm(row.source_id) : '') ||
          (typeof row.exchange === 'string' ? stripEm(row.exchange) : '');
        if (cSymbol && cPrefix) {
          outSymbol = cSymbol;
          code = `${cPrefix}:${cSymbol}`;
        }
      }
      if (seen.has(code)) continue;
      seen.set(code, {
        code,
        symbol: outSymbol,
        description: stripEm(row.description),
        exchange: typeof row.exchange === 'string' ? row.exchange : '',
        type: typeof row.type === 'string' ? row.type : typeId,
        currency: typeof row.currency_code === 'string' ? row.currency_code : '',
      });
    }

    if (typeof body?.symbols_remaining === 'number' && body.symbols_remaining <= 0) break;
    if (start + rows.length > MAX_START) {
      capped = true;
      break;
    }
    start += rows.length;
    if (start % 1000 === 0) console.log(`[crawl] ${typeId}: 已翻到 ${start}，收集 ${seen.size} 条`);
    await sleep(PAGE_DELAY_MS);
  }

  capped = capped || seen.size >= MAX_START;
  return { items: [...seen.values()], capped, error };
}

function writeSymbolFile(file, id, items, capped, error = null) {
  fs.writeFileSync(
    file,
    JSON.stringify({
      type: id,
      updatedAt: new Date().toISOString(),
      count: items.length,
      capped,
      ...(error ? { error } : {}),
      items,
    }),
  );
}

// ---- CLI ----
if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  const args = process.argv.slice(2);
  const force = args.includes('--force');
  const typesArg = args.find((a) => a.startsWith('--types'));
  const typeFilter = typesArg
    ? typesArg.split('=')[1]?.split(',').map((s) => s.trim()).filter(Boolean)
    : null;

  const outDir = path.join(path.dirname(fileURLToPath(import.meta.url)), 'data');
  fs.mkdirSync(outDir, { recursive: true });

  const agent = await ensureProxyAgent();
  if (!agent) console.log('[crawl] 未检测到代理，尝试直连（国内网络通常失败）');

  const types = SYMBOL_TYPES.filter((t) => !typeFilter || typeFilter.includes(t.id));
  for (const { id } of types) {
    const file = path.join(outDir, `symbols-${id}.json`);
    if (!force && fs.existsSync(file)) {
      let prev = { count: 0 };
      try {
        prev = JSON.parse(fs.readFileSync(file, 'utf8'));
      } catch {}
      // 上次部分完成（带 error 标记）的类型重跑时自动续抓
      if (!prev.error) {
        console.log(`[crawl] 跳过 ${id}（已存在 ${prev.count} 条，--force 可重抓）`);
        continue;
      }
      console.log(`[crawl] ${id} 上次部分完成（${prev.count} 条），重新抓取 …`);
    }
    console.log(`[crawl] 开始抓取 ${id} …`);
    const t0 = Date.now();
    try {
      const { items, capped, error } = await crawlType(id, agent);
      writeSymbolFile(file, id, items, capped, error);
      if (error) {
        console.error(`[crawl] ${id} 部分完成: ${items.length} 条后中断（${error}），重跑本脚本可续抓`);
      } else {
        console.log(
          `[crawl] ${id} 完成: ${items.length} 条（${capped ? '命中接口 10000 上限' : '全量'}），耗时 ${((Date.now() - t0) / 1000).toFixed(0)}s`,
        );
      }
    } catch (e) {
      console.error(`[crawl] ${id} 异常: ${e instanceof Error ? e.message : String(e)}`);
    }
    await sleep(TYPE_DELAY_MS);
  }
  console.log('[crawl] 全部完成 →', outDir);
}
