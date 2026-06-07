// ── 익명 세션 + 일일 무료 라운드 관리 ────────────────────────────
// 영속화 전 단계: localStorage 기반. 이후 Supabase 익명 세션으로 승격 가능.

const ID_KEY = 'pa_session_id'
const FREE_KEY = 'pa_free_state'
const UNLIMITED_KEY = 'pa_unlimited'

export const FREE_ROUNDS_PER_DAY = Number(import.meta.env.VITE_FREE_ROUNDS_PER_DAY || 5)

export function sessionId() {
  let id = localStorage.getItem(ID_KEY)
  if (!id) {
    id = 'u_' + Math.random().toString(36).slice(2) + Date.now().toString(36)
    localStorage.setItem(ID_KEY, id)
  }
  return id
}

function today() {
  return new Date().toISOString().slice(0, 10)
}

function readFree() {
  try {
    const s = JSON.parse(localStorage.getItem(FREE_KEY) || '{}')
    if (s.day !== today()) return { day: today(), used: 0, bonus: 0 }
    return { day: s.day, used: s.used || 0, bonus: s.bonus || 0 }
  } catch {
    return { day: today(), used: 0, bonus: 0 }
  }
}

function writeFree(s) {
  localStorage.setItem(FREE_KEY, JSON.stringify(s))
}

export function isUnlimited() {
  return localStorage.getItem(UNLIMITED_KEY) === '1'
}
export function grantUnlimited() {
  localStorage.setItem(UNLIMITED_KEY, '1')
}

// 남은 무료 라운드 (보너스 포함)
export function roundsLeft() {
  if (isUnlimited()) return Infinity
  const s = readFree()
  return Math.max(0, FREE_ROUNDS_PER_DAY + s.bonus - s.used)
}

export function canPlay() {
  return roundsLeft() > 0
}

// 라운드 1회 소모
export function consumeRound() {
  if (isUnlimited()) return
  const s = readFree()
  s.used += 1
  writeFree(s)
}

// 보상형 광고/공유 보너스로 추가 라운드 지급
export function addBonusRounds(n = 1) {
  const s = readFree()
  s.bonus += n
  writeFree(s)
}
