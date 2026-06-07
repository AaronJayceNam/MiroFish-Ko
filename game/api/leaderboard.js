// ── /api/leaderboard — 일일 리더보드 (스텁) ───────────────────────
// GET  : 오늘의 상위 기록 반환
// POST : { name, score, seed } 기록 제출
// 주의: in-memory → 서버리스에서 휘발/인스턴스별 분리. 프로덕션은 Supabase/Upstash 교체.
//   예) Supabase: insert into scores(name,score,seed,day) / select ... order by score desc limit 20

const store = globalThis.__lb || (globalThis.__lb = { day: today(), rows: [] })

function today() {
  return new Date().toISOString().slice(0, 10)
}
function rollover() {
  const d = today()
  if (store.day !== d) {
    store.day = d
    store.rows = []
  }
}

export default function handler(req, res) {
  rollover()
  if (req.method === 'GET') {
    const top = [...store.rows].sort((a, b) => b.score - a.score).slice(0, 20)
    res.status(200).json({ day: store.day, top })
    return
  }
  if (req.method === 'POST') {
    try {
      const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body || {}
      const name = String(body.name || '익명').slice(0, 16)
      const score = Math.max(0, Math.min(100, Math.round(Number(body.score) || 0)))
      const seed = String(body.seed || '').slice(0, 32)
      store.rows.push({ name, score, seed, at: Date.now() })
      if (store.rows.length > 2000) store.rows = store.rows.slice(-1000)
      const rank = [...store.rows].sort((a, b) => b.score - a.score).findIndex((r) => r.at === store.rows[store.rows.length - 1].at) + 1
      res.status(200).json({ ok: true, rank, day: store.day })
    } catch {
      res.status(400).json({ error: 'bad_request' })
    }
    return
  }
  res.status(405).json({ error: 'method_not_allowed' })
}
