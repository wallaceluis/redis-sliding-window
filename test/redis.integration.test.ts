/**
 * Runs against a real Redis (the unit tests use ioredis-mock). Skipped unless
 * REDIS_URL is set; CI starts a Redis service for it.
 *
 *   REDIS_URL=redis://localhost:6379 npm test
 */
import Redis from "ioredis";
import { SlidingWindowRateLimiter } from "../src";

const url = process.env.REDIS_URL;
const describeIfRedis = url ? describe : describe.skip;

describeIfRedis("against a real Redis server", () => {
  let redis: Redis;
  const prefix = `it-${process.pid}-${Date.now()}`;

  beforeAll(() => {
    redis = new Redis(url as string);
  });

  afterAll(async () => {
    const keys = await redis.keys(`${prefix}*`);
    if (keys.length) await redis.del(...keys);
    await redis.quit();
  });

  it("uses the Redis server clock when no `now` is given", async () => {
    const limiter = new SlidingWindowRateLimiter(redis, { limit: 2, windowMs: 60_000, prefix });

    const first = await limiter.consume("clock");
    const second = await limiter.consume("clock");
    const third = await limiter.consume("clock");

    expect([first.allowed, second.allowed, third.allowed]).toEqual([true, true, false]);
    expect(third.retryAfterMs).toBeGreaterThan(59_000);
    expect(third.retryAfterMs).toBeLessThanOrEqual(60_000);
  });

  it("never lets concurrent requests from several app instances exceed the limit", async () => {
    // Three "app instances", each with its own connection, hammering the same key.
    const clients = [new Redis(url as string), new Redis(url as string), new Redis(url as string)];
    try {
      const limiters = clients.map((c) => new SlidingWindowRateLimiter(c, { limit: 25, windowMs: 60_000, prefix }));

      const results = await Promise.all(
        Array.from({ length: 150 }, (_, i) => limiters[i % limiters.length]!.consume("shared")),
      );

      expect(results.filter((r) => r.allowed)).toHaveLength(25);
      expect(await redis.zcard(`${prefix}:shared`)).toBe(25);
    } finally {
      await Promise.all(clients.map((c) => c.quit()));
    }
  });

  it("frees slots as requests slide out of the window", async () => {
    const limiter = new SlidingWindowRateLimiter(redis, { limit: 1, windowMs: 150, prefix });

    expect((await limiter.consume("slide")).allowed).toBe(true);
    expect((await limiter.consume("slide")).allowed).toBe(false);
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect((await limiter.consume("slide")).allowed).toBe(true);
  });

  it("sets a TTL so idle keys expire on their own", async () => {
    const limiter = new SlidingWindowRateLimiter(redis, { limit: 5, windowMs: 10_000, prefix });
    await limiter.consume("ttl");

    const ttl = await redis.pttl(`${prefix}:ttl`);
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(10_000);
  });
});
