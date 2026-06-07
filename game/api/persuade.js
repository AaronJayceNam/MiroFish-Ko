// ── POST /api/persuade — 한 턴 설득 판정 ──────────────────────────
// 입력: { seed, message, guard, turn, history:[{role,text}] }
// 출력: { npc_reply, mood, delta, guard, verdict, reason, clip_caption, turn, maxTurns }
// 보안: API 키 서버 전용 · 입력 격리/길이제한 · 게이지 서버 클램프 · IP 레이트리밋.
import { scenarioFromSeed } from './_lib/scenarios.js'
import { judgeTurn } from './_lib/anthropic.js'
import { MAX_INPUT_CHARS } from './_lib/prompt.js'
import { allow, clientIp } from './_lib/ratelimit.js'

const MAX_TURNS = Number(process.env.MAX_TURNS || 6)

function clamp(n, lo, hi) {
  n = Number.isFinite(n) ? n : lo
  return Math.max(lo, Math.min(hi, Math.round(n)))
}

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    res.status(405).json({ error: 'method_not_allowed' })
    return
  }
  if (!process.env.ANTHROPIC_API_KEY) {
    res.status(500).json({ error: 'server_misconfigured', message: 'ANTHROPIC_API_KEY 미설정' })
    return
  }
  if (!allow(clientIp(req))) {
    res.status(429).json({ error: 'rate_limited', message: '잠시 후 다시 시도해주세요.' })
    return
  }

  try {
    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body || {}
    const { seed, message } = body
    if (!seed || typeof message !== 'string' || !message.trim()) {
      res.status(400).json({ error: 'bad_request', message: 'seed·message 필요' })
      return
    }

    const scenario = scenarioFromSeed(seed)
    const turn = clamp(body.turn ?? 1, 1, MAX_TURNS)
    const incomingGuard = clamp(body.guard ?? scenario.difficulty.startGuard, 0, 100)
    const history = Array.isArray(body.history) ? body.history.slice(-10) : []
    const trimmed = message.slice(0, MAX_INPUT_CHARS)

    const out = await judgeTurn({
      scenario,
      history,
      message: trimmed,
      guard: incomingGuard,
      turn,
      maxTurns: MAX_TURNS,
    })

    // ── 서버 권위 클램프: 모델 출력 신뢰하되 게이지는 서버가 최종 결정 ──
    const delta = clamp(out.delta ?? 0, -25, 25)
    const guard = clamp(incomingGuard + delta, 0, 100)
    const success = guard >= 100 || (out.goal_met === true && guard >= 90)
    const verdict = success ? 'success' : turn >= MAX_TURNS ? 'fail' : 'in_progress'

    res.status(200).json({
      npc_reply: String(out.npc_reply || '...'),
      mood: out.mood || 'suspicious',
      delta,
      guard,
      verdict,
      goal_met: success,
      reason: String(out.reason || ''),
      clip_caption: String(out.clip_caption || ''),
      turn,
      maxTurns: MAX_TURNS,
    })
  } catch (err) {
    console.error('persuade error', err?.message)
    res.status(502).json({ error: 'upstream_error', message: '판정 중 오류가 발생했어요.' })
  }
}
