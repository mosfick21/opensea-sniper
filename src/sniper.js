// OpenSea listing sniper: buys any listing in a collection priced at or below the cap, on any chain.
// Listings arrive from the OpenSea stream (websocket) with API polling as a backup.
import { readFileSync, writeFileSync } from "node:fs";
import { createInterface } from "node:readline/promises";
import { Wallet, JsonRpcProvider, Interface, parseEther, formatEther, parseUnits, id } from "ethers";

if (Number(process.versions.node.split(".")[0]) < 22) {
  console.error(`Node.js 22 or newer is needed (you have ${process.version}). Download it from https://nodejs.org`);
  process.exit(1);
}

// .env sits next to src/, so the bot works from any working directory.
try { process.loadEnvFile(new URL("../.env", import.meta.url).pathname.replace(/^\/(\w:)/, "$1")); } catch {}

const env = (k, d = "") => (process.env[k] ?? "").trim() || d;

const CFG = {
  apiKey: env("OPENSEA_API_KEY"),
  privateKey: env("PRIVATE_KEY"),
  collection: "",
  chain: "",
  chainId: 0n,
  rpcUrls: [],
  maxPriceUsd: null,
  maxPriceEth: null,
  maxBuys: 1,
  maxSpend: env("MAX_SPEND") ? parseEther(env("MAX_SPEND")) : null,
  useStream: env("USE_STREAM", "true") === "true",
  pollMs: Number(env("POLL_MS", "2000")),
  dryRun: env("DRY_RUN", "true") !== "false",
};

const API = "https://api.opensea.io/api/v2";
// OpenSea chain slug -> chain id, public RPC, native coin (and its CoinGecko id for the USD rate).
// The collection's chain is read from OpenSea; add RPC_<CHAIN> in .env for a faster private RPC.
const CHAINS = {
  ethereum: { id: 1, rpc: "https://ethereum-rpc.publicnode.com", native: "ETH", cg: "ethereum" },
  base: { id: 8453, rpc: "https://mainnet.base.org", native: "ETH", cg: "ethereum" },
  arbitrum: { id: 42161, rpc: "https://arb1.arbitrum.io/rpc", native: "ETH", cg: "ethereum" },
  optimism: { id: 10, rpc: "https://mainnet.optimism.io", native: "ETH", cg: "ethereum" },
  unichain: { id: 130, rpc: "https://mainnet.unichain.org", native: "ETH", cg: "ethereum" },
  zora: { id: 7777777, rpc: "https://rpc.zora.energy", native: "ETH", cg: "ethereum" },
  blast: { id: 81457, rpc: "https://rpc.blast.io", native: "ETH", cg: "ethereum" },
  shape: { id: 360, rpc: "https://mainnet.shape.network", native: "ETH", cg: "ethereum" },
  abstract: { id: 2741, rpc: "https://api.mainnet.abs.xyz", native: "ETH", cg: "ethereum" },
  soneium: { id: 1868, rpc: "https://rpc.soneium.org", native: "ETH", cg: "ethereum" },
  b3: { id: 8333, rpc: "https://mainnet-rpc.b3.fun", native: "ETH", cg: "ethereum" },
  matic: { id: 137, rpc: "https://polygon-bor-rpc.publicnode.com", native: "POL", cg: "polygon-ecosystem-token" },
  avalanche: { id: 43114, rpc: "https://api.avax.network/ext/bc/C/rpc", native: "AVAX", cg: "avalanche-2" },
  ape_chain: { id: 33139, rpc: "https://rpc.apechain.com", native: "APE", cg: "apecoin" },
  ronin: { id: 2020, rpc: "https://api.roninchain.com/rpc", native: "RON", cg: "ronin" },
  berachain: { id: 80094, rpc: "https://rpc.berachain.com", native: "BERA", cg: "berachain-bera" },
  sei: { id: 1329, rpc: "https://evm-rpc.sei-apis.com", native: "SEI", cg: "sei-network" },
  flow: { id: 747, rpc: "https://mainnet.evm.nodes.onflow.org", native: "FLOW", cg: "flow" },
  hyperevm: { id: 999, rpc: "https://rpc.hyperliquid.xyz/evm", native: "HYPE", cg: "hyperliquid" },
  monad: { id: 143, rpc: "https://rpc.monad.xyz", native: "MON", cg: "monad" },
  somnia: { id: 5031, rpc: "https://api.infra.mainnet.somnia.network", native: "SOMI", cg: "somnia" },
};
// Native coin of the collection's chain; set in main().
let SYM = "ETH";
let CG_ID = "ethereum";
// Gas is picked live: top priority fee paid in recent blocks, plus a margin, within these bounds.
const GAS_LIMIT = 400_000n;
const PRIORITY_MIN = parseUnits("0.05", "gwei");
const PRIORITY_MAX = parseUnits("5", "gwei");
const FEE_BLOCKS = 10;
// Max fulfillment requests in flight, so a burst of listings can't get the API key rate-limited.
const MAX_FETCHING = 3;
// Stream replays old events on join; those are covered by polling, not the fast path.
const MAX_STREAM_AGE_MS = 10 * 60_000;
const STREAM = "wss://stream.openseabeta.com/socket/websocket";
const ZERO = "0x0000000000000000000000000000000000000000";
// Seaport 1.6 and 1.5. A fulfillment pointing anywhere else is refused.
const SEAPORT = new Set(["0x0000000000000068f116a894984e2db1123eb395", "0x00000000000000adc04c56bf30ac9d3c0aaf14dc"]);
const DEFAULT_PROTOCOL = "0x0000000000000068F116a894984e2DB1123eB395";

const OFFER = "tuple(uint8 itemType,address token,uint256 identifierOrCriteria,uint256 startAmount,uint256 endAmount)";
const CONS = "tuple(uint8 itemType,address token,uint256 identifierOrCriteria,uint256 startAmount,uint256 endAmount,address recipient)";
const PARAMS = `tuple(address offerer,address zone,${OFFER}[] offer,${CONS}[] consideration,uint8 orderType,uint256 startTime,uint256 endTime,bytes32 zoneHash,uint256 salt,bytes32 conduitKey,uint256 totalOriginalConsiderationItems)`;
const BASIC = "tuple(address considerationToken,uint256 considerationIdentifier,uint256 considerationAmount,address offerer,address zone,address offerToken,uint256 offerIdentifier,uint256 offerAmount,uint8 basicOrderType,uint256 startTime,uint256 endTime,bytes32 zoneHash,uint256 salt,bytes32 offererConduitKey,bytes32 fulfillerConduitKey,uint256 totalOriginalAdditionalRecipients,tuple(uint256 amount,address recipient)[] additionalRecipients,bytes signature)";
const seaport = new Interface([
  `function fulfillBasicOrder(${BASIC} parameters) payable returns (bool)`,
  `function fulfillBasicOrder_efficient_6GL6yc(${BASIC} parameters) payable returns (bool)`,
  `function fulfillOrder(tuple(${PARAMS} parameters,bytes signature) order,bytes32 fulfillerConduitKey) payable returns (bool)`,
  `function fulfillAdvancedOrder(tuple(${PARAMS} parameters,uint120 numerator,uint120 denominator,bytes signature,bytes extraData) advancedOrder,tuple(uint256 orderIndex,uint8 side,uint256 index,uint256 identifier,bytes32[] criteriaProof)[] criteriaResolvers,bytes32 fulfillerConduitKey,address recipient) payable returns (bool)`,
]);

// Order hashes OpenSea still serves but that can't be filled; remembered across restarts.
const DEAD_FILE = new URL("../dead-orders.json", import.meta.url);
const dead = new Set((() => { try { return JSON.parse(readFileSync(DEAD_FILE, "utf8")); } catch { return []; } })());
function markDead(hash) {
  if (dead.has(hash)) return;
  dead.add(hash);
  try { writeFileSync(DEAD_FILE, JSON.stringify([...dead])); } catch {}
}

const C = { reset: "\x1b[0m", bold: "\x1b[1m", red: "\x1b[31m", green: "\x1b[32m", yellow: "\x1b[33m", blue: "\x1b[34m", magenta: "\x1b[35m", cyan: "\x1b[36m", gray: "\x1b[90m" };
const TAGS = {
  INFO: C.reset, STATUS: C.gray, LISTED: C.cyan, CHEAP: C.bold + C.magenta, SOLD: C.gray,
  HIT: C.bold + C.yellow, SENT: C.blue, BOUGHT: C.bold + C.green, FAILED: C.red, SKIP: C.red,
  DRYRUN: C.magenta, ERROR: C.bold + C.red,
};
// Three columns: time | tag | message
const log = (tag, msg) => {
  const color = TAGS[tag] ?? C.reset;
  console.log(`${C.gray}${new Date().toISOString().slice(11, 23)}${C.reset}  ${color}${tag.padEnd(7)}  ${msg}${C.reset}`);
};
const usdOf = (wei) => (state.ethUsd ? ` ($${(Number(formatEther(wei)) * state.ethUsd).toFixed(2)})` : "");
const eth = (wei) => Number(formatEther(wei)).toFixed(6);

if (!CFG.apiKey) throw new Error("OPENSEA_API_KEY missing in .env");
if (!CFG.privateKey) throw new Error("PRIVATE_KEY missing in .env");

// RPC providers are built once the collection's chain is known (main).
let providers = [];
const wallet = new Wallet(CFG.privateKey);
const me = wallet.address.toLowerCase();

const state = {
  ethUsd: null,
  baseFee: null,
  priorityFee: PRIORITY_MIN,
  nonce: null,
  bought: 0,
  pending: 0,
  spent: 0n,
  pendingSpend: 0n,
  fetching: 0,
  streamSeen: 0,
  pollCheapest: null,
  attempted: new Set(),
};

// ---------- OpenSea API ----------

async function os(path, body, timeoutMs = 5000) {
  const res = await fetch(API + path, {
    method: body ? "POST" : "GET",
    headers: { "x-api-key": CFG.apiKey, accept: "application/json", ...(body && { "content-type": "application/json" }) },
    body: body && JSON.stringify(body),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await res.text();
  if (!res.ok) {
    const err = new Error(`OpenSea ${res.status} ${path}: ${text.slice(0, 300)}`);
    err.status = res.status;
    throw err;
  }
  return JSON.parse(text);
}

// ---------- Price cap ----------

function capWei() {
  let cap = CFG.maxPriceEth;
  if (CFG.maxPriceUsd) {
    if (!state.ethUsd) return cap; // USD cap unknown until the coin rate loads
    const usdCap = parseEther((CFG.maxPriceUsd / state.ethUsd).toFixed(18));
    cap = cap === null || usdCap < cap ? usdCap : cap;
  }
  return cap;
}

// USD rate of the native coin: Coinbase first, CoinGecko as a fallback.
async function refreshEthUsd() {
  const sources = [
    async () => Number((await fetch(`https://api.coinbase.com/v2/prices/${SYM}-USD/spot`, { signal: AbortSignal.timeout(4000) }).then((r) => r.json()))?.data?.amount),
    async () => Number((await fetch(`https://api.coingecko.com/api/v3/simple/price?ids=${CG_ID}&vs_currencies=usd`, { signal: AbortSignal.timeout(4000) }).then((r) => r.json()))?.[CG_ID]?.usd),
  ];
  for (const src of sources) {
    try {
      const p = await src();
      if (p > 0) { state.ethUsd = p; return; }
    } catch {}
  }
  log("ERROR", `${SYM}/USD price unavailable`);
}

// Priority fee = highest tip anyone paid in the last FEE_BLOCKS blocks, +25%, so our buy sorts first.
async function refreshFees() {
  try {
    const h = await providers[0].send("eth_feeHistory", [`0x${FEE_BLOCKS.toString(16)}`, "latest", [99]]);
    state.baseFee = BigInt(h.baseFeePerGas.at(-1));
    let top = 0n;
    for (const r of h.reward ?? []) if (BigInt(r[0]) > top) top = BigInt(r[0]);
    let tip = (top * 125n) / 100n;
    if (tip < PRIORITY_MIN) tip = PRIORITY_MIN;
    if (tip > PRIORITY_MAX) tip = PRIORITY_MAX;
    state.priorityFee = tip;
  } catch (e) {
    log("ERROR", `fee refresh failed: ${e.message}`);
  }
}
const gwei = (wei) => `${(Number(wei) / 1e9).toFixed(3)} gwei`;

// ---------- Buying ----------

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const retryable = (e) => e.name === "TimeoutError" || e.status === 429 || e.status >= 500;

// Fulfillment is the slowest step. If the first request is slow, a second one races it;
// transient failures (timeout, 429, 5xx) are retried quickly instead of dropping the listing.
async function fetchFulfillment(l) {
  const body = {
    listing: { hash: l.hash, chain: CFG.chain, protocol_address: l.protocol },
    fulfiller: { address: wallet.address },
  };
  for (let attempt = 1; ; attempt++) {
    let done = false;
    try {
      const first = os("/listings/fulfillment_data", body).finally(() => { done = true; });
      const hedge = sleep(500).then(() => (done ? first : os("/listings/fulfillment_data", body)));
      return await Promise.any([first, hedge]);
    } catch (e) {
      const err = e.errors?.find((x) => !retryable(x)) ?? e.errors?.[0] ?? e;
      if (attempt >= 3 || !retryable(err)) throw err;
      await sleep(150);
    }
  }
}

function encodeFulfillment(tx) {
  const name = tx.function.slice(0, tx.function.indexOf("("));
  const frag = seaport.getFunction(name);
  if (!frag) throw new Error(`unknown Seaport function ${tx.function}`);
  if (id(tx.function).slice(0, 10) !== frag.selector) throw new Error(`selector mismatch for ${tx.function}`);
  const args = frag.inputs.map((i) => {
    if (!(i.name in tx.input_data)) throw new Error(`input_data missing ${i.name}`);
    return tx.input_data[i.name];
  });
  return seaport.encodeFunctionData(frag, args);
}

// Sends to every RPC at once and resolves on the first acceptance.
async function broadcast(raw) {
  try {
    return await Promise.any(providers.map((p) => p.send("eth_sendRawTransaction", [raw])));
  } catch (e) {
    throw new Error(e.errors.map((r) => r?.shortMessage || r?.message).join(" | "));
  }
}

// Fetching fulfillment data takes no buy slot, so a stale listing can't block a fresh one.
// The slot (how many to buy / MAX_SPEND) is taken only right before signing.
function hasRoom(value) {
  if (state.bought + state.pending >= CFG.maxBuys) return false;
  return CFG.maxSpend === null || state.spent + state.pendingSpend + value <= CFG.maxSpend;
}

async function buy(l) {
  const t0 = performance.now();
  let slot = 0n;
  state.fetching++;
  try {
    let fd;
    try {
      fd = await fetchFulfillment(l);
    } finally {
      state.fetching--;
    }
    const tx = fd.fulfillment_data.transaction;
    const value = BigInt(String(tx.value));
    const cap = capWei();
    if (!SEAPORT.has(tx.to.toLowerCase())) throw new Error(`refusing: tx.to ${tx.to} is not Seaport`);
    if (cap === null || value > cap) throw new Error(`refusing: value ${eth(value)} ${SYM} is above cap`);
    const data = encodeFulfillment(tx);
    const tFetch = performance.now();

    if (CFG.dryRun) {
      let sim = "ok";
      try {
        await providers[0].call({ from: wallet.address, to: tx.to, value, data });
      } catch (e) {
        sim = `revert (${e.shortMessage || e.message})`.slice(0, 160);
      }
      log("DRYRUN", `#${l.tokenId}  ${eth(value)} ${SYM}  opensea ${Math.round(tFetch - t0)}ms  simulate: ${sim}`);
      return;
    }

    // Polled listings are often dead orders OpenSea still serves; simulate those first so they
    // don't burn gas. Fresh stream listings skip this to stay fast.
    if (l.source === "poll") {
      try {
        await providers[0].call({ from: wallet.address, to: tx.to, value, data });
      } catch (e) {
        markDead(l.hash);
        throw new Error(`simulation reverted, dead order (${e.shortMessage || e.message})`.slice(0, 160));
      }
    }

    if (!hasRoom(value)) throw new Error("buy limit reached by another pending buy");
    slot = value;
    state.pending++;
    state.pendingSpend += value;

    const base = state.baseFee ?? 0n;
    const tip = state.priorityFee;
    const nonce = state.nonce++;
    const raw = await wallet.signTransaction({
      type: 2,
      chainId: CFG.chainId,
      nonce,
      to: tx.to,
      value,
      data,
      gasLimit: GAS_LIMIT,
      maxPriorityFeePerGas: tip,
      maxFeePerGas: base * 3n + tip,
    });
    let hash;
    try {
      hash = await broadcast(raw);
    } catch (e) {
      state.nonce = await providers[0].getTransactionCount(wallet.address, "pending");
      throw e;
    }
    log("SENT", `#${l.tokenId}  ${eth(value)} ${SYM}  gas ${gwei(tip)}  opensea ${Math.round(tFetch - t0)}ms + send ${Math.round(performance.now() - tFetch)}ms  ${hash}`);

    // No timeout: an unconfirmed tx keeps its buy slot, so a late-mining tx can never push past MAX_BUYS.
    const slow = setTimeout(() => log("STATUS", `still waiting for tx ${hash}`), 30_000);
    const receipt = await providers[0].waitForTransaction(hash, 1).finally(() => clearTimeout(slow));
    if (receipt?.status === 1) {
      state.bought++;
      state.spent += value;
      log("BOUGHT", `#${l.tokenId}  ${eth(value)} ${SYM}${usdOf(value)}  (${state.bought}/${CFG.maxBuys})`);
    } else {
      markDead(l.hash);
      log("FAILED", `#${l.tokenId}  reverted, someone was faster  ${hash}`);
    }
  } catch (e) {
    if (e.status === 400 && /not valid/i.test(e.message)) markDead(l.hash);
    log("SKIP", `#${l.tokenId}  ${e.message.replace(/\s+/g, " ").slice(0, 160)}`);
  } finally {
    if (slot) {
      state.pending--;
      state.pendingSpend -= slot;
    }
    if (state.bought >= CFG.maxBuys) {
      log("INFO", `done: bought ${state.bought}, spent ${eth(state.spent)} ${SYM}`);
      process.exit(0);
    }
  }
}

function consider(l) {
  if (state.attempted.has(l.hash) || dead.has(l.hash)) return;
  if (!l.native || l.maker === me) return;
  const cap = capWei();
  if (cap === null || l.priceWei > cap) return;
  if (!hasRoom(l.priceWei)) return;
  if (state.fetching >= MAX_FETCHING) return; // not marked attempted: polling brings it back
  state.attempted.add(l.hash);
  log("HIT", `#${l.tokenId}  ${eth(l.priceWei)} ${SYM}${usdOf(l.priceWei)}  via ${l.source}`);
  buy(l);
}

// ---------- Listing sources ----------

// Reconnect delay grows on repeated failures (0.3s → 10s) so OpenSea does not block the key.
let streamFails = 0;
function startStream() {
  // Phoenix v2 frames: [join_ref, ref, topic, event, payload]
  const ws = new WebSocket(`${STREAM}?token=${CFG.apiKey}&vsn=2.0.0`);
  let ref = 0;
  let heartbeat;
  const send = (joinRef, topic, event) => ws.send(JSON.stringify([joinRef, String(++ref), topic, event, {}]));

  ws.onopen = () => {
    send("1", `collection:${CFG.collection}`, "phx_join");
    heartbeat = setInterval(() => send(null, "phoenix", "heartbeat"), 30_000);
    streamFails = 0;
    log("INFO", "stream connected, watching live");
  };
  ws.onmessage = (m) => {
    let frame;
    try { frame = JSON.parse(m.data); } catch { return; }
    const [, , , event, payload] = frame;
    if (event === "phx_reply" && payload?.status === "error") log("ERROR", `stream join: ${JSON.stringify(payload)}`);
    if (event === "item_sold") {
      const p = payload?.payload;
      const wei = p?.sale_price ? BigInt(p.sale_price) : null;
      log("SOLD", `#${p?.item?.nft_id?.split("/").pop()}  ${wei === null ? "?" : `${eth(wei)} ${SYM}${usdOf(wei)}`}`);
      return;
    }
    if (event !== "item_listed") return;
    state.streamSeen++;
    const p = payload?.payload;
    if (!p?.order_hash || p.is_private) return;
    if ((p.chain ?? p.item?.chain?.name) !== CFG.chain) return;
    const token = p.payment_token ?? {};
    const native = !token.address || token.address.toLowerCase() === ZERO;
    const listedAt = Date.parse(p.event_timestamp) || null;
    if (listedAt && Date.now() - listedAt > MAX_STREAM_AGE_MS) return;
    const wei = BigInt(p.base_price);
    const c = capWei();
    const cheap = native && c !== null && wei <= c;
    log(cheap ? "CHEAP" : "LISTED", `#${p.item?.nft_id?.split("/").pop()}  ${eth(wei)} ${SYM}${usdOf(wei)}  ${Date.now() - listedAt}ms ago${native ? "" : `  (not ${SYM})`}`);
    consider({
      source: "stream",
      listedAt,
      hash: p.order_hash,
      protocol: p.protocol_address || DEFAULT_PROTOCOL,
      priceWei: BigInt(p.base_price),
      native,
      maker: p.maker?.address?.toLowerCase(),
      tokenId: p.item?.nft_id?.split("/").pop(),
    });
  };
  ws.onerror = () => {};
  ws.onclose = () => {
    clearInterval(heartbeat);
    const wait = Math.min(300 * 2 ** streamFails++, 10_000);
    log("ERROR", `stream closed, reconnecting in ${(wait / 1000).toFixed(1)}s (backup check still running)`);
    setTimeout(startStream, wait);
  };
}

async function poll() {
  let delay = CFG.pollMs;
  try {
    const r = await os(`/listings/collection/${CFG.collection}/best?limit=50`);
    let cheapest = null;
    for (const l of r.listings ?? []) {
      if (l.protocol_data?.parameters?.consideration?.[0]?.token === ZERO && !dead.has(l.order_hash)) {
        const v = BigInt(l.price.current.value);
        if (cheapest === null || v < cheapest) cheapest = v;
      }
      const price = l.price?.current;
      const params = l.protocol_data?.parameters;
      if (!price || !params) continue;
      consider({
        source: "poll",
        hash: l.order_hash,
        protocol: l.protocol_address || DEFAULT_PROTOCOL,
        priceWei: BigInt(price.value),
        native: params.consideration?.[0]?.token === ZERO,
        maker: params.offerer?.toLowerCase(),
        tokenId: params.offer?.[0]?.identifierOrCriteria,
      });
    }
    state.pollCheapest = cheapest;
  } catch (e) {
    if (e.status === 429) delay = Math.max(delay * 2, 10_000);
    log("ERROR", `poll: ${e.message.replace(/\s+/g, " ").slice(0, 160)}`);
  }
  setTimeout(poll, delay);
}

// ---------- Start ----------

// Asks collection, price and count at start.
async function ask() {
  // Line iterator buffers input, so pasted/piped answers are not lost between prompts.
  const rl = createInterface({ input: process.stdin });
  const lines = rl[Symbol.asyncIterator]();
  const q = async (label, def) => {
    const hint = def === undefined ? "" : ` ${C.gray}[${def}]${C.reset}`;
    process.stdout.write(`${C.bold}${label}${C.reset}${hint}: `);
    const { value, done } = await lines.next();
    if (done) throw new Error("no input");
    const v = value.trim();
    return v || (def === undefined ? "" : String(def));
  };
  // Accepts a slug or a full collection URL.
  while (!CFG.collection) CFG.collection = (await q("Collection (slug or OpenSea link)")).replace(/^.*\/collection\//, "").split(/[/?#]/)[0];
  await setupChain();
  while (!(CFG.maxPriceUsd > 0)) CFG.maxPriceUsd = Number(await q("Max price USD"));
  for (;;) {
    const v = Number(await q("How many to buy", 1));
    if (Number.isInteger(v) && v > 0) { CFG.maxBuys = v; break; }
  }
  rl.close();
}

// Reads the collection's chain from OpenSea and connects to it.
// For a chain not in CHAINS, set RPC_<CHAIN>, CHAIN_ID_<CHAIN> and NATIVE_<CHAIN> in .env.
async function setupChain() {
  const col = await os(`/collections/${CFG.collection}`);
  CFG.collectionName = col.name;
  CFG.chain = col.contracts?.[0]?.chain;
  if (!CFG.chain) throw new Error(`could not read the chain of ${CFG.collection}`);
  const key = CFG.chain.toUpperCase();
  const known = CHAINS[CFG.chain] ?? {};
  const id = env(`CHAIN_ID_${key}`) || known.id;
  CFG.rpcUrls = [...env(`RPC_${key}`).split(","), known.rpc].map((u) => u?.trim()).filter(Boolean);
  if (!id || !CFG.rpcUrls.length) throw new Error(`chain "${CFG.chain}" is not built in: set RPC_${key}, CHAIN_ID_${key} and NATIVE_${key} in .env`);
  CFG.chainId = BigInt(id);
  SYM = env(`NATIVE_${key}`) || known.native || "ETH";
  CG_ID = known.cg ?? "";
  // batchMaxCount 1: send each RPC call immediately instead of ethers' 10ms batching delay
  providers = CFG.rpcUrls.map((u) => new JsonRpcProvider(u, Number(CFG.chainId), { staticNetwork: true, batchMaxCount: 1 }));
  const net = await providers[0].send("eth_chainId", []);
  if (BigInt(net) !== CFG.chainId) throw new Error(`RPC is on chain ${BigInt(net)}, expected ${CFG.chainId} for ${CFG.chain}`);
  await refreshEthUsd();
  log("INFO", `${col.name} on ${CFG.chain} (${SYM}${state.ethUsd ? ` $${state.ethUsd}` : ""})`);
}

async function main() {
  await ask();
  const [balance, nonce] = await Promise.all([
    providers[0].getBalance(wallet.address),
    providers[0].getTransactionCount(wallet.address, "pending"),
  ]);
  state.nonce = nonce;
  await Promise.all([refreshEthUsd(), refreshFees()]);
  setInterval(refreshEthUsd, 30_000);
  setInterval(refreshFees, 2_000);

  const cap = capWei();
  log("INFO", `wallet ${wallet.address}  balance ${eth(balance)} ${SYM}`);
  log("INFO", `buy <= $${CFG.maxPriceUsd} (${cap === null ? "?" : eth(cap)} ${SYM})  x${CFG.maxBuys}  gas auto (now ${gwei(state.priorityFee)})`);
  log("INFO", CFG.dryRun ? "DRY RUN: nothing will be sent" : "LIVE: will buy");

  setInterval(() => {
    const cheap = state.pollCheapest === null ? "?" : `${eth(state.pollCheapest)} ${SYM} (~$${(Number(formatEther(state.pollCheapest)) * (state.ethUsd ?? 0)).toFixed(2)})`;
    const c = capWei();
    log("STATUS", `cap ${c === null ? "?" : eth(c)} ${SYM} | cheapest ${cheap} | gas ${gwei(state.priorityFee)} | seen ${state.streamSeen} | bought ${state.bought}/${CFG.maxBuys}`);
  }, 30_000);

  if (CFG.useStream) startStream();
  poll();
}

main().catch((e) => {
  log("ERROR", e.message);
  process.exit(1);
});
