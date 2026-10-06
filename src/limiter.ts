import { randomBytes } from "node:crypto";
import type { Cluster, Redis, Result } from "ioredis";
import { CONSUME_SCRIPT, PEEK_SCRIPT } from "./script";

declare module "ioredis" {
  interface RedisCommander<Context> {
    slidingWindowConsume(
      key: string,
      windowMs: number,
      limit: number,
      member: string,
      now: number | "",
    ): Result<[number, number, number], Context>;
    slidingWindowPeek(
      key: string,
      windowMs: number,
      limit: number,
      member: string,
      now: number | "",
    ): Result<[number, number, number], Context>;
  }
}

export type RedisClient = Redis | Cluster;

export interface RateLimitRule {
  /** Maximum number of requests allowed inside the window. */
  limit: number;
  /** Size of the window in milliseconds. */
  windowMs: number;
}

export interface RateLimiterOptions extends RateLimitRule {
  /** Namespace of the keys written to Redis. Default: `"rl"`. */
  prefix?: string;
  /**
   * Clock used to timestamp requests, in milliseconds. By default the Redis
   * server clock is used, so every app instance agrees on the time.
   */
  now?: () => number;
}

export interface RateLimitResult {
  allowed: boolean;
  limit: number;
  /** Requests left in the current window. */
  remaining: number;
  /** Milliseconds until the oldest request leaves the window and frees a slot. */
  resetMs: number;
  /** Milliseconds to wait before retrying. `0` when the request was allowed. */
  retryAfterMs: number;
}

function assertRule({ limit, windowMs }: RateLimitRule): void {
  if (!Number.isInteger(limit) || limit <= 0) {
    throw new RangeError(`limit must be a positive integer, got ${limit}`);
  }
  if (!Number.isInteger(windowMs) || windowMs <= 0) {
    throw new RangeError(`windowMs must be a positive integer, got ${windowMs}`);
  }
}

export class SlidingWindowRateLimiter {
  private readonly rule: RateLimitRule;
  private readonly prefix: string;
  private readonly now?: () => number;
  // Members must be unique per request, even across processes in the same millisecond.
  private readonly instanceId = randomBytes(6).toString("base64url");
  private sequence = 0;

  constructor(
    private readonly redis: RedisClient,
    options: RateLimiterOptions,
  ) {
    assertRule(options);
    this.rule = { limit: options.limit, windowMs: options.windowMs };
    this.prefix = options.prefix ?? "rl";
    this.now = options.now;

    // defineCommand caches the script in Redis and calls it with EVALSHA,
    // so the Lua source is sent over the wire only once per connection.
    if (typeof redis.slidingWindowConsume !== "function") {
      redis.defineCommand("slidingWindowConsume", { numberOfKeys: 1, lua: CONSUME_SCRIPT });
      redis.defineCommand("slidingWindowPeek", { numberOfKeys: 1, lua: PEEK_SCRIPT });
    }
  }

  /**
   * Registers one request for `id` and reports whether it is allowed.
   * Rejected requests are not stored, so a blocked client cannot extend its own block.
   */
  async consume(id: string, override?: Partial<RateLimitRule>): Promise<RateLimitResult> {
    const rule = this.resolveRule(override);
    this.sequence = (this.sequence + 1) % Number.MAX_SAFE_INTEGER;
    const member = `${this.instanceId}:${this.sequence}`;

    const reply = await this.redis.slidingWindowConsume(
      this.key(id),
      rule.windowMs,
      rule.limit,
      member,
      this.now ? this.now() : "",
    );
    return toResult(reply, rule);
  }

  /** Reports the current state for `id` without consuming a request. */
  async peek(id: string, override?: Partial<RateLimitRule>): Promise<RateLimitResult> {
    const rule = this.resolveRule(override);
    const reply = await this.redis.slidingWindowPeek(
      this.key(id),
      rule.windowMs,
      rule.limit,
      "",
      this.now ? this.now() : "",
    );
    return toResult(reply, rule);
  }

  /** Forgets every request registered for `id`. */
  async reset(id: string): Promise<void> {
    await this.redis.del(this.key(id));
  }

  private key(id: string): string {
    return `${this.prefix}:${id}`;
  }

  private resolveRule(override?: Partial<RateLimitRule>): RateLimitRule {
    if (!override) return this.rule;
    const rule = { ...this.rule, ...override };
    assertRule(rule);
    return rule;
  }
}

function toResult([allowed, remaining, resetMs]: [number, number, number], rule: RateLimitRule): RateLimitResult {
  const isAllowed = allowed === 1;
  return {
    allowed: isAllowed,
    limit: rule.limit,
    remaining,
    resetMs,
    retryAfterMs: isAllowed ? 0 : resetMs,
  };
}
