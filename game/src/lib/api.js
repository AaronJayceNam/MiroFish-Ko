// ── 클라이언트 → 서버리스 함수 호출 (Claude는 서버에서만) ───────────

export async function persuade({ seed, message, guard, turn, history }) {
  const res = await fetch('/api/persuade', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ seed, message, guard, turn, history }),
  })
  const data = await res.json().catch(() => ({}))
  if (!res.ok) throw new Error(data.message || data.error || '요청 실패')
  return data
}

export async function getLeaderboard() {
  const res = await fetch('/api/leaderboard')
  if (!res.ok) return { top: [] }
  return res.json()
}

export async function submitScore({ name, score, seed }) {
  try {
    const res = await fetch('/api/leaderboard', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ name, score, seed }),
    })
    return res.json()
  } catch {
    return { ok: false }
  }
}
