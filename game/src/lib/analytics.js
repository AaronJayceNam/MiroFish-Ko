// ── 분석 이벤트 훅 (KPI 트래킹 지점) ─────────────────────────────
// MVP: window.dataLayer + console. 실연동 시 GA4/Amplitude/자체 /api 로 교체.
// 산출 지표: D1/D7 리텐션, ARPDAU, 광고 시청률, 공유 전환율 (docs/MONETIZATION_KPI.md)

const RETAIN_KEY = 'pa_visit_days'

export function track(event, props = {}) {
  const payload = { event, ts: Date.now(), ...props }
  window.dataLayer = window.dataLayer || []
  window.dataLayer.push(payload)
  if (import.meta.env.DEV) console.debug('[analytics]', event, props)
  // 실연동 예: navigator.sendBeacon('/api/track', JSON.stringify(payload))
}

// 방문일 기록 → D1/D7 리텐션 산출용. 신규 방문일에 retain 이벤트 발화.
export function trackVisit() {
  let days = []
  try {
    days = JSON.parse(localStorage.getItem(RETAIN_KEY) || '[]')
  } catch {
    days = []
  }
  const today = new Date().toISOString().slice(0, 10)
  if (!days.includes(today)) {
    const first = days[0]
    days.push(today)
    localStorage.setItem(RETAIN_KEY, JSON.stringify(days))
    if (first) {
      const diff = Math.round((Date.parse(today) - Date.parse(first)) / 86400000)
      if (diff === 1) track('retain_d1')
      if (diff >= 6 && diff <= 8) track('retain_d7')
    }
    track('visit', { day: today, totalDays: days.length })
  }
}
