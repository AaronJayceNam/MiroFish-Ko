// ── 베스트에포트 IP 레이트리밋 (in-memory) ───────────────────────
// 주의: 서버리스 인스턴스마다 메모리가 분리/휘발됨 → MVP 남용 방지용 1차 방어선.
// 프로덕션은 Upstash Redis 등으로 교체 (UPSTASH_* env, 아래 주석 참고).

const WINDOW_MS = 60_000
const MAX_PER_WINDOW = 20 // 분당 라운드/턴 요청 상한
const buckets = new Map()

export function clientIp(req) {
  const fwd = req.headers['x-forwarded-for']
  if (typeof fwd === 'string' && fwd.length) return fwd.split(',')[0].trim()
  return req.socket?.remoteAddress || 'unknown'
}

// true = 허용, false = 차단
export function allow(ip) {
  const now = Date.now()
  const b = buckets.get(ip)
  if (!b || now - b.start > WINDOW_MS) {
    buckets.set(ip, { start: now, count: 1 })
    return true
  }
  b.count += 1
  // 가벼운 청소
  if (buckets.size > 5000) buckets.clear()
  return b.count <= MAX_PER_WINDOW
}

// 프로덕션 교체 예시:
// import { Ratelimit } from '@upstash/ratelimit'
// import { Redis } from '@upstash/redis'
// const rl = new Ratelimit({ redis: Redis.fromEnv(), limiter: Ratelimit.slidingWindow(20, '60 s') })
// const { success } = await rl.limit(ip)
