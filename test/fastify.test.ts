import Fastify, { type FastifyInstance } from "fastify";
import RedisMock from "ioredis-mock";
import type { RedisClient } from "../src";
import rateLimit, { type FastifyRateLimitOptions } from "../src/fastify";

const redis = new RedisMock() as unknown as RedisClient;

let app: FastifyInstance;
let clock: { now: number };

async function build(options: Partial<FastifyRateLimitOptions> = {}) {
  clock = { now: 1_000_000 };
  app = Fastify();
  await app.register(rateLimit, {
    redis,
    limit: 2,
    windowMs: 10_000,
    now: () => clock.now,
    ...options,
  });
  app.get("/", async () => ({ ok: true }));
  app.get("/open", { config: { rateLimit: false } }, async () => ({ ok: true }));
  app.get("/strict", { config: { rateLimit: { limit: 1 } } }, async () => ({ ok: true }));
  await app.ready();
  return app;
}

afterEach(async () => {
  await app.close();
  await redis.flushall();
});

describe("fastify plugin", () => {
  it("sets rate limit headers on allowed requests", async () => {
    await build();

    const res = await app.inject({ url: "/" });

    expect(res.statusCode).toBe(200);
    expect(res.headers["x-ratelimit-limit"]).toBe("2");
    expect(res.headers["x-ratelimit-remaining"]).toBe("1");
    expect(res.headers["x-ratelimit-reset"]).toBe("10");
    expect(res.headers["retry-after"]).toBeUndefined();
  });

  it("answers 429 with Retry-After once the limit is reached", async () => {
    await build();
    await app.inject({ url: "/" });
    clock.now += 2500;
    await app.inject({ url: "/" });

    const res = await app.inject({ url: "/" });

    expect(res.statusCode).toBe(429);
    expect(res.headers["retry-after"]).toBe("8");
    expect(res.headers["x-ratelimit-remaining"]).toBe("0");
    expect(res.json()).toEqual({
      statusCode: 429,
      error: "Too Many Requests",
      message: "Rate limit exceeded, retry in 8 seconds",
    });
  });

  it("limits each client separately", async () => {
    await build({ keyGenerator: (request) => String(request.headers["x-api-key"]) });
    const as = (key: string) => app.inject({ url: "/", headers: { "x-api-key": key } });

    await as("a");
    await as("a");

    expect((await as("a")).statusCode).toBe(429);
    expect((await as("b")).statusCode).toBe(200);
  });

  it("skips routes with rateLimit: false", async () => {
    await build();

    for (let i = 0; i < 5; i++) {
      const res = await app.inject({ url: "/open" });
      expect(res.statusCode).toBe(200);
      expect(res.headers["x-ratelimit-limit"]).toBeUndefined();
    }
  });

  it("applies per-route rules in their own window", async () => {
    await build();

    expect((await app.inject({ url: "/strict" })).statusCode).toBe(200);
    const res = await app.inject({ url: "/strict" });
    expect(res.statusCode).toBe(429);
    expect(res.headers["x-ratelimit-limit"]).toBe("1");

    // The global window was not touched by the /strict requests.
    expect((await app.inject({ url: "/" })).headers["x-ratelimit-remaining"]).toBe("1");
  });

  it("honours skip", async () => {
    await build({ skip: (request) => request.headers["x-internal"] === "1" });

    for (let i = 0; i < 5; i++) {
      expect((await app.inject({ url: "/", headers: { "x-internal": "1" } })).statusCode).toBe(200);
    }
  });

  it("supports a custom error response and hiding headers", async () => {
    await build({
      limit: 1,
      headers: false,
      errorResponse: (_request, result) => ({ code: "RATE_LIMITED", retryAfterMs: result.retryAfterMs }),
    });
    await app.inject({ url: "/" });

    const res = await app.inject({ url: "/" });

    expect(res.statusCode).toBe(429);
    expect(res.json()).toEqual({ code: "RATE_LIMITED", retryAfterMs: 10_000 });
    expect(res.headers["x-ratelimit-limit"]).toBeUndefined();
    expect(res.headers["retry-after"]).toBe("10");
  });

  describe("when Redis is down", () => {
    function brokenRedis(): RedisClient {
      const broken = new RedisMock() as unknown as RedisClient;
      broken.slidingWindowConsume = (() => Promise.reject(new Error("ECONNREFUSED"))) as never;
      return broken;
    }

    it("fails open by default", async () => {
      await build({ redis: brokenRedis() });

      expect((await app.inject({ url: "/" })).statusCode).toBe(200);
    });

    it("answers 503 with failOpen: false", async () => {
      await build({ redis: brokenRedis(), failOpen: false });

      expect((await app.inject({ url: "/" })).statusCode).toBe(503);
    });
  });

  it("exposes the limiter on the instance", async () => {
    await build();
    await app.inject({ url: "/" });

    expect(await app.rateLimiter.peek("127.0.0.1")).toMatchObject({ remaining: 1 });
  });
});
