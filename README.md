# redis-sliding-window

Distributed rate limiting for Node.js using the **Sliding Window Log** algorithm on Redis, with a ready-to-use **Fastify** plugin.

- **Exact**: no burst at window boundaries, unlike fixed window counters
- **Atomic**: the whole decision runs in one Lua script, safe across any number of app instances
- **One round trip** per request, sent as `EVALSHA`
- **Clock-skew free**: timestamps come from the Redis server, not from each app instance
- **Typed**: written in TypeScript, works with `ioredis` standalone and Cluster clients

```ts
import Fastify from "fastify";
import Redis from "ioredis";
import rateLimit from "redis-sliding-window/fastify";

const app = Fastify();

await app.register(rateLimit, {
  redis: new Redis(process.env.REDIS_URL),
  limit: 100,
  windowMs: 60_000, // 100 requests per minute, per IP
});
```

## Installation

```bash
npm install github:wallaceluis/redis-sliding-window ioredis
```

The package is installed from GitHub and compiled on install.

`ioredis` (>= 5) is a peer dependency. `fastify` (>= 4) is only needed if you use the plugin. Requires Redis 5 or newer.

## Why sliding window log?

A fixed window counter resets at fixed boundaries, so a client can send `limit` requests at the end of one window and `limit` more at the start of the next: twice the limit in a short burst. The sliding window log stores the timestamp of each accepted request and counts the ones inside the last `windowMs`, measured from *now*. The limit holds for every possible window, not just the aligned ones.

| Algorithm              | Accuracy                        | Memory per client | Redis work per request |
| ---------------------- | ------------------------------- | ----------------- | ---------------------- |
| Fixed window           | Up to 2x the limit at boundaries | O(1)              | O(1)                   |
| Sliding window counter | Approximation                   | O(1)              | O(1)                   |
| **Sliding window log** | **Exact**                       | **O(limit)**      | **O(log limit)**       |
| Token bucket           | Exact, allows bursts by design  | O(1)              | O(1)                   |

The trade-off is memory: this algorithm keeps one sorted set entry per accepted request. See [Performance](#performance) for the numbers and for when to choose something else.

## Performance

### What happens on each request

`consume()` sends a single command to Redis. The script behind it runs these steps atomically:

| Step                                   | Command            | Complexity        |
| -------------------------------------- | ------------------ | ----------------- |
| Drop entries older than the window     | `ZREMRANGEBYSCORE` | O(log N + M)      |
| Count what is left                     | `ZCARD`            | O(1)              |
| Record the request (only when allowed) | `ZADD` + `PEXPIRE` | O(log N)          |
| Read the oldest entry for `resetMs`    | `ZRANGE 0 0`       | O(log N)          |

`N` is the number of entries in the client's window (at most `limit`) and `M` the entries that expired since the previous call. Each entry is removed exactly once, so the cost of `M` is amortized O(1) per request.

### Design decisions

- **One network round trip.** Latency of a rate limit check is dominated by the network, not by Redis. Doing the read-decide-write cycle in Lua costs one round trip instead of the three or four of a `MULTI` based approach, and removes the race condition between the count and the write.
- **`EVALSHA`, not `EVAL`.** The scripts are registered with `defineCommand`, so `ioredis` sends the 40-character SHA instead of the script body and falls back to `EVAL` only on a cache miss.
- **Rejected requests are not stored.** A client that is over the limit costs a `ZREMRANGEBYSCORE`, a `ZCARD` and a `ZRANGE`, and adds nothing to the log. Memory is bounded by `limit` entries per client no matter how hard it is hammered, and a blocked client cannot extend its own block.
- **Keys expire on their own.** Every accepted request refreshes a `PEXPIRE` of `windowMs`, so idle clients are evicted by Redis without a cleanup job.
- **Server clock.** By default the script reads `TIME` inside Redis. App instances with drifting clocks cannot corrupt the ordering of the log.

### Memory

Each accepted request is one sorted set member of about 14 bytes (`<8-char instance id>:<sequence>`) plus its score. Sorted sets of up to 128 entries use Redis' compact listpack encoding (`zset-max-listpack-entries`); larger ones are promoted to a skiplist, which costs noticeably more per entry.

Worst case memory is `active clients x limit` entries. As a rule of thumb:

| Limit per window | Fit                                                                           |
| ---------------- | ----------------------------------------------------------------------------- |
| up to ~100       | Ideal. Stays in the listpack encoding, a few KB per active client at most     |
| 100 to ~10,000   | Fine, budget memory for `active clients x limit` skiplist entries             |
| above ~10,000    | Prefer a sliding window counter or token bucket; exactness is rarely worth it |

Measure with your own traffic: `redis-cli MEMORY USAGE rl:<id>` reports the real size of a client's log.

### Redis Cluster

Each check touches a single key, so it runs on a Cluster without hash tags and load is spread across shards by client id.

## Fastify plugin

```ts
import rateLimit from "redis-sliding-window/fastify";

await app.register(rateLimit, {
  redis,
  limit: 100,
  windowMs: 60_000,
  keyGenerator: (request) => request.headers["x-api-key"]?.toString() ?? request.ip,
  skip: (request) => request.url === "/health",
});
```

The check runs in an `onRequest` hook, before body parsing and validation, so rejected requests are as cheap as possible.

### Options

| Option          | Type                                    | Default         | Description                                                        |
| --------------- | --------------------------------------- | --------------- | ------------------------------------------------------------------ |
| `redis`         | `Redis \| Cluster`                      | required        | `ioredis` client                                                   |
| `limit`         | `number`                                | required        | Maximum requests per window                                        |
| `windowMs`      | `number`                                | required        | Window size in milliseconds                                        |
| `prefix`        | `string`                                | `"rl"`          | Namespace of the Redis keys                                        |
| `keyGenerator`  | `(request) => string \| Promise<string>` | `request.ip`    | Identifies the client being limited                                |
| `skip`          | `(request) => boolean \| Promise<boolean>` | —            | Return `true` to let a request through without counting it         |
| `failOpen`      | `boolean`                               | `true`          | When Redis is unreachable: `true` allows requests, `false` answers 503 |
| `headers`       | `boolean`                               | `true`          | Send the `X-RateLimit-*` headers                                   |
| `errorResponse` | `(request, result) => unknown`          | —               | Custom body for the 429 response                                   |
| `now`           | `() => number`                          | Redis clock     | Custom clock in milliseconds (useful in tests)                     |

Behind a reverse proxy, enable Fastify's [`trustProxy`](https://fastify.dev/docs/latest/Reference/Server/#trustproxy) so `request.ip` is the client address and not the proxy's.

### Per-route rules

```ts
// Stricter limit, counted in its own window
app.post("/login", { config: { rateLimit: { limit: 5, windowMs: 60_000 } } }, handler);

// No rate limit
app.get("/health", { config: { rateLimit: false } }, handler);
```

### Response

Every limited response carries:

| Header                  | Value                                               |
| ----------------------- | --------------------------------------------------- |
| `X-RateLimit-Limit`     | Requests allowed per window                         |
| `X-RateLimit-Remaining` | Requests left in the current window                 |
| `X-RateLimit-Reset`     | Seconds until the oldest request leaves the window  |
| `Retry-After`           | Seconds to wait (only on `429`)                     |

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 8

{ "statusCode": 429, "error": "Too Many Requests", "message": "Rate limit exceeded, retry in 8 seconds" }
```

The limiter is also available as `app.rateLimiter` for manual checks.

## API reference

The core class has no dependency on Fastify and can be used with any framework, queue consumer or job.

### `new SlidingWindowRateLimiter(redis, options)`

```ts
import Redis from "ioredis";
import { SlidingWindowRateLimiter } from "redis-sliding-window";

const limiter = new SlidingWindowRateLimiter(new Redis(), {
  limit: 10,
  windowMs: 1000,
});
```

| Option     | Type           | Default     | Description                                 |
| ---------- | -------------- | ----------- | ------------------------------------------- |
| `limit`    | `number`       | required    | Maximum requests per window                 |
| `windowMs` | `number`       | required    | Window size in milliseconds                 |
| `prefix`   | `string`       | `"rl"`      | Keys are stored as `<prefix>:<id>`          |
| `now`      | `() => number` | Redis clock | Custom clock in milliseconds                |

Throws a `RangeError` when `limit` or `windowMs` is not a positive integer.

### `limiter.consume(id, override?): Promise<RateLimitResult>`

Registers one request for `id` and reports whether it is allowed. `override` takes a partial `{ limit, windowMs }` for this call only; use a different `id` for each rule so they do not share a log.

```ts
const result = await limiter.consume(`user:${userId}`);
if (!result.allowed) {
  throw new TooManyRequests(result.retryAfterMs);
}
```

### `limiter.peek(id, override?): Promise<RateLimitResult>`

Same result as `consume`, without registering a request. Read-only.

### `limiter.reset(id): Promise<void>`

Deletes the log of `id`, for example after a successful login.

### `RateLimitResult`

| Field          | Type      | Description                                                              |
| -------------- | --------- | ------------------------------------------------------------------------ |
| `allowed`      | `boolean` | Whether the request is within the limit                                  |
| `limit`        | `number`  | The limit that was applied                                               |
| `remaining`    | `number`  | Requests left in the current window                                      |
| `resetMs`      | `number`  | Milliseconds until the oldest request leaves the window and frees a slot |
| `retryAfterMs` | `number`  | Milliseconds to wait before retrying. `0` when allowed                   |

## Development

```bash
npm install
npm test          # Jest
npm run typecheck
npm run build     # emits dist/
```

The test suite runs the real Lua scripts against [`ioredis-mock`](https://github.com/stipsan/ioredis-mock), so it needs no Redis server.

## License

[MIT](LICENSE)
