// ── 바이럴 공유: 챌린지 링크 + OG 카드 + 원탭 공유 ─────────────────
import { track } from './analytics.js'
import { addBonusRounds } from './session.js'

function baseUrl() {
  return import.meta.env.VITE_PUBLIC_BASE_URL || window.location.origin
}

// "이 캐릭터 너도 뚫어봐" 챌린지 링크 (seed 포함 → 같은 판 재현)
export function challengeUrl(seed) {
  return `${baseUrl()}/?c=${encodeURIComponent(seed)}`
}

// 결과 OG 이미지 URL (동적 카드)
export function ogImageUrl({ caption, score, verdict, char }) {
  const q = new URLSearchParams({ caption, score: String(score), verdict, char })
  return `${baseUrl()}/api/og?${q.toString()}`
}

// 원탭 공유 (Web Share API → 미지원 시 클립보드 복사)
export async function shareResult({ seed, caption, score, verdict, char }) {
  const url = challengeUrl(seed)
  const text = `“${caption}”\n설득 게이지 ${score}/100 — 너도 이 캐릭터(${char}) 뚫어봐!`
  track('share_click', { verdict, score })
  try {
    if (navigator.share) {
      await navigator.share({ title: '설득 AI', text, url })
      addBonusRounds(1) // 공유 보너스 라운드 → 공유가 곧 UA
      track('share_success', { method: 'web_share' })
      return { shared: true }
    }
  } catch {
    /* 사용자가 취소 → 폴백 */
  }
  try {
    await navigator.clipboard.writeText(`${text}\n${url}`)
    addBonusRounds(1)
    track('share_success', { method: 'clipboard' })
    return { shared: true, copied: true }
  } catch {
    return { shared: false }
  }
}

// 챌린지 링크로 진입했는지 확인 (?c=seed)
export function incomingChallengeSeed() {
  const c = new URLSearchParams(window.location.search).get('c')
  if (c) track('challenge_open', { seed: c })
  return c || null
}
