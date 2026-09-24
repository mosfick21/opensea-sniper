# OpenSea Sniper

Buys an NFT on OpenSea as soon as someone lists it at or below your price. Works on any OpenSea chain.

## What you need

| What | Where to get it |
|---|---|
| Node.js 22 or newer | https://nodejs.org (download the LTS version and install) |
| OpenSea API key | https://opensea.io → Profile → Settings → Developer → Create API key |
| Wallet private key | MetaMask → ⋮ → Account details → Show private key. **Use a new wallet with only the coins you plan to spend.** |
| Coins on the chain | Send the chain’s coin (ETH, POL, APE...) to that wallet, on the same chain as the collection |
| RPC URL (optional, faster) | https://alchemy.com → Create app → pick the chain → copy the HTTPS URL |

## Setup (one time)

```
git clone <this repo link>
cd <repo folder>
npm install
```

Copy the example settings file:

- Windows: `copy .env.example .env`
- Mac/Linux: `cp .env.example .env`

Open `.env` and fill in:

```
OPENSEA_API_KEY=your_opensea_key
PRIVATE_KEY=your_wallet_private_key
```

## Run

```
npm start
```

The bot asks 3 things:

```
Collection (slug or OpenSea link): https://opensea.io/collection/your-collection
Max price USD: 20
How many to buy [1]: 2
```

Stop the bot with `Ctrl + C`.

## Test first, then buy for real

In `.env`:

- `DRY_RUN=true`: the bot finds listings but buys nothing (safe test).
- `DRY_RUN=false`: the bot really buys.

## What the log means

| Tag | Meaning |
|---|---|
| `LISTED` | New listing, above your price |
| `CHEAP` | New listing at or below your price |
| `HIT` | Bot is trying to buy |
| `SENT` | Buy transaction sent |
| `BOUGHT` | You got it |
| `FAILED` | Someone else was faster (gas is still paid) |
| `SKIP` | Listing can't be bought (already sold or cancelled). No gas spent. |
| `SOLD` | Someone bought an item from the collection |
| `STATUS` | Every 30s: price cap, cheapest listing, current gas |

## Chains

The bot finds the collection’s chain by itself. Nothing to set.

Built in: Ethereum, Base, Arbitrum, Optimism, Unichain, Zora, Blast, Shape, Abstract, Soneium, B3, Polygon, Avalanche, ApeChain, Ronin, Berachain, Sei, Flow, HyperEVM, Monad, Somnia.

Only listings priced in the chain’s own coin are bought (ETH on Base, POL on Polygon, and so on).

**Faster (optional):** add your own RPC in `.env` as `RPC_<CHAIN>`:

```
RPC_BASE=https://base-mainnet.g.alchemy.com/v2/your_key
RPC_UNICHAIN=https://unichain-mainnet.g.alchemy.com/v2/your_key
```

**Chain not in the list?** Add these 3 lines to `.env` (example for a chain called `newchain`):

```
RPC_NEWCHAIN=https://rpc.newchain.xyz
CHAIN_ID_NEWCHAIN=12345
NATIVE_NEWCHAIN=ETH
```

## Other settings (`.env`)

| Setting | What it does |
|---|---|
| `MAX_SPEND` | Total coins the bot may spend. Empty = no limit |
| `USE_STREAM` | `true` = watch OpenSea live (keep this on) |
| `POLL_MS` | Backup check every X milliseconds |

Gas is picked automatically.

## Tips

- Never share `.env`. It holds your private key.
- A failed buy still costs a little gas.
- For the best speed, run it on a VPS in the US (close to OpenSea's servers).
