import type { FastifyPluginAsync, FastifyReply, FastifyRequest } from "fastify";
import fp from "fastify-plugin";
import {
  type RateLimitResult,
  type RateLimitRule,
  type RedisClient,
  SlidingWindowRateLimiter,
} from "./limiter";

export interface FastifyRateLimitOptions extends RateLimitRule {
  redis: RedisClient;
  /** Namespace of the keys written to Redis. Default: `"rl"`. */
  prefix?: string;
  /** Identifies the client being limited. Default: the request IP. */
  keyGenerator?: (request: FastifyRequest) => string | Promise<string>;
  /** Return `true` to let a request through without counting it. */
  skip?: (request: FastifyRequest) => boolean | Promise<boolean>;
  /**
   * What to do when Redis is unreachable. `true` (default) lets requests
   * through, `false` answers 503. Pick `false` when the limit is a security control.
   */
  failOpen?: boolean;
  /** Send `X-RateLimit-*` headers. Default: `true`. */
  headers?: boolean;
  /** Body of the 429 response. */
  errorResponse?: (request: FastifyRequest, result: RateLimitResult) => unknown;
  /** Custom clock in milliseconds. Defaults to the Redis server clock. */
  now?: () => number;
}

declare module "fastify" {
  interface FastifyInstance {
    rateLimiter: SlidingWindowRateLimiter;
  }
  interface FastifyContextConfig {
    /** Per-route rule, or `false` to disable rate limiting on the route. */
    rateLimit?: Partial<RateLimitRule> | false;
  }
}

function setHeaders(reply: FastifyReply, result: RateLimitResult): void {
  reply.header("X-RateLimit-Limit", result.limit);
  reply.header("X-RateLimit-Remaining", result.remaining);
  reply.header("X-RateLimit-Reset", Math.ceil(result.resetMs / 1000));
}

const plugin: FastifyPluginAsync<FastifyRateLimitOptions> = async (fastify, options) => {
  const {
    redis,
    keyGenerator = (request) => request.ip,
    skip,
    failOpen = true,
    headers = true,
    errorResponse,
  } = options;

  const limiter = new SlidingWindowRateLimiter(redis, options);
  fastify.decorate("rateLimiter", limiter);

  fastify.addHook("onRequest", async (request, reply) => {
    const routeRule = request.routeOptions.config?.rateLimit;
    if (routeRule === false) return;
    if (skip && (await skip(request))) return;

    const client = await keyGenerator(request);
    // Routes with their own rule get their own window instead of sharing the global one.
    const id = routeRule ? `${request.routeOptions.url ?? request.url}:${client}` : client;

    let result: RateLimitResult;
    try {
      result = await limiter.consume(id, routeRule);
    } catch (error) {
      request.log.error({ err: error }, "rate limiter unavailable");
      if (failOpen) return;
      return reply.code(503).send({
        statusCode: 503,
        error: "Service Unavailable",
        message: "Rate limiter unavailable",
      });
    }

    if (headers) setHeaders(reply, result);
    if (result.allowed) return;

    const retryAfter = Math.ceil(result.retryAfterMs / 1000);
    reply.header("Retry-After", retryAfter);
    return reply.code(429).send(
      errorResponse
        ? errorResponse(request, result)
        : {
            statusCode: 429,
            error: "Too Many Requests",
            message: `Rate limit exceeded, retry in ${retryAfter} second${retryAfter === 1 ? "" : "s"}`,
          },
    );
  });
};

/**
 * Fastify plugin that applies a sliding window log rate limit to every route.
 *
 * ```ts
 * await app.register(rateLimit, { redis, limit: 100, windowMs: 60_000 });
 * ```
 */
export const rateLimit = fp(plugin, {
  fastify: ">=4",
  name: "redis-sliding-window",
});

export default rateLimit;
