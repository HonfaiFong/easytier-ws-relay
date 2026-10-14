import { timingSafeEqual } from 'node:crypto';
import { Buffer } from 'node:buffer';

export function authorizeRelay(request, env = {}) {
  const token = env.EASYTIER_RELAY_TOKEN;
  // An unset secret preserves the public-relay deployment mode.
  if (!token) return true;
  const authorization = request.headers.get('Authorization');
  const supplied = authorization?.startsWith('Bearer ')
    ? authorization.slice(7)
    : new URL(request.url).searchParams.get('token');
  if (!supplied) return false;
  const expected = Buffer.from(String(token));
  const actual = Buffer.from(supplied);
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

export function positiveLimit(env, name, fallback, maximum = fallback) {
  const value = Number(env?.[name] ?? fallback);
  return Math.min(Number.isSafeInteger(value) && value > 0 ? value : fallback, maximum);
}
