import RedisMock from "ioredis-mock";
import { type RedisClient, SlidingWindowRateLimiter } from "../src";

const redis = new RedisMock() as unknown as RedisClient;

function setup(options: { limit?: number; windowMs?: number; prefix?: string } = {}) {
  const clock = { now: 1_000_000 };
  const limiter = new SlidingWindowRateLimiter(redis, {
    limit: options.limit ?? 3,
    windowMs: options.windowMs ?? 1000,
    prefix: options.prefix,
    now: () => clock.now,
  });
  return { redis, clock, limiter };
}

afterEach(async () => {
  await redis.flushall();
});

describe("SlidingWindowRateLimiter", () => {
  it("allows requests up to the limit and counts down", async () => {
    const { limiter } = setup();

    const results = [await limiter.consume("u1"), await limiter.consume("u1"), await limiter.consume("u1")];

    expect(results.map((r) => r.allowed)).toEqual([true, true, true]);
    expect(results.map((r) => r.remaining)).toEqual([2, 1, 0]);
    expect(results[0]).toMatchObject({ limit: 3, retryAfterMs: 0 });
  });

  it("rejects requests over the limit with the time to wait", async () => {
    const { limiter, clock } = setup();
    await limiter.consume("u1");
    clock.now += 400;
    await limiter.consume("u1");
    await limiter.consume("u1");

    clock.now += 100;
    const rejected = await limiter.consume("u1");

    // The first request leaves the window 1000 ms after it was made, 500 ms from now.
    expect(rejected).toEqual({ allowed: false, limit: 3, remaining: 0, resetMs: 500, retryAfterMs: 500 });
  });

  it("slides: a slot frees when the oldest request expires, not at a fixed boundary", async () => {
    const { limiter, clock } = setup();
    await limiter.consume("u1"); // t = 0
    clock.now += 600;
    await limiter.consume("u1"); // t = 600
    await limiter.consume("u1"); // t = 600

    clock.now += 399; // t = 999
    expect((await limiter.consume("u1")).allowed).toBe(false);

    clock.now += 1; // t = 1000: the request from t = 0 is out
    const allowed = await limiter.consume("u1");
    expect(allowed.allowed).toBe(true);
    expect(allowed.remaining).toBe(0);

    // The two from t = 600 are still inside the window.
    expect((await limiter.consume("u1")).allowed).toBe(false);
  });

  it("does not let a burst straddle two windows (the fixed window flaw)", async () => {
    const { limiter, clock } = setup({ limit: 5, windowMs: 1000 });
    clock.now += 900;
    for (let i = 0; i < 5; i++) await limiter.consume("u1");

    clock.now += 200; // a fixed window would have reset here
    expect((await limiter.consume("u1")).allowed).toBe(false);
  });

  it("does not store rejected requests", async () => {
    const { limiter, clock, redis } = setup({ limit: 1 });
    await limiter.consume("u1");
    for (let i = 0; i < 10; i++) await limiter.consume("u1");

    expect(await redis.zcard("rl:u1")).toBe(1);

    clock.now += 1000;
    expect((await limiter.consume("u1")).allowed).toBe(true);
  });

  it("tracks each id independently", async () => {
    const { limiter } = setup({ limit: 1 });

    expect((await limiter.consume("a")).allowed).toBe(true);
    expect((await limiter.consume("a")).allowed).toBe(false);
    expect((await limiter.consume("b")).allowed).toBe(true);
  });

  it("is atomic under concurrent requests", async () => {
    const { limiter } = setup({ limit: 10 });

    const results = await Promise.all(Array.from({ length: 50 }, () => limiter.consume("u1")));

    expect(results.filter((r) => r.allowed)).toHaveLength(10);
  });

  it("expires the key so idle clients do not leak memory", async () => {
    const { limiter, redis } = setup({ windowMs: 5000 });
    await limiter.consume("u1");

    const ttl = await redis.pttl("rl:u1");
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(5000);
  });

  it("uses the configured prefix", async () => {
    const { limiter, redis } = setup({ prefix: "api" });
    await limiter.consume("u1");

    expect(await redis.exists("api:u1")).toBe(1);
  });

  it("peek reports the state without consuming", async () => {
    const { limiter, clock } = setup({ limit: 2 });

    expect(await limiter.peek("u1")).toMatchObject({ allowed: true, remaining: 2 });

    await limiter.consume("u1");
    await limiter.peek("u1");
    await limiter.peek("u1");
    expect(await limiter.peek("u1")).toMatchObject({ allowed: true, remaining: 1, resetMs: 1000 });

    await limiter.consume("u1");
    clock.now += 250;
    expect(await limiter.peek("u1")).toEqual({
      allowed: false,
      limit: 2,
      remaining: 0,
      resetMs: 750,
      retryAfterMs: 750,
    });

    clock.now += 750;
    expect(await limiter.peek("u1")).toMatchObject({ allowed: true, remaining: 2 });
  });

  it("reset clears the window", async () => {
    const { limiter } = setup({ limit: 1 });
    await limiter.consume("u1");

    await limiter.reset("u1");

    expect((await limiter.consume("u1")).allowed).toBe(true);
  });

  it("accepts a per-call rule", async () => {
    const { limiter } = setup({ limit: 1 });
    await limiter.consume("u1");

    expect((await limiter.consume("u1", { limit: 2 })).allowed).toBe(true);
    await expect(limiter.consume("u1", { limit: 0 })).rejects.toThrow(RangeError);
  });

  it("falls back to the Redis server clock", async () => {
    const limiter = new SlidingWindowRateLimiter(redis, { limit: 1, windowMs: 60_000 });

    expect((await limiter.consume("u1")).allowed).toBe(true);
    const rejected = await limiter.consume("u1");
    expect(rejected.allowed).toBe(false);
    expect(rejected.retryAfterMs).toBeGreaterThan(59_000);
    expect(rejected.retryAfterMs).toBeLessThanOrEqual(60_000);
  });

  it("validates its options", () => {
    expect(() => new SlidingWindowRateLimiter(redis, { limit: 0, windowMs: 1000 })).toThrow(RangeError);
    expect(() => new SlidingWindowRateLimiter(redis, { limit: 1.5, windowMs: 1000 })).toThrow(RangeError);
    expect(() => new SlidingWindowRateLimiter(redis, { limit: 1, windowMs: -1 })).toThrow(RangeError);
  });
});
