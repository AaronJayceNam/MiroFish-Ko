// ── 시나리오 엔진 (서버·클라 공용 단일 소스) ──────────────────────
// 순수 ESM, Node 전용 API 미사용 → Vercel 함수와 Vite 클라 양쪽에서 import.
// seed(문자열) → 결정적으로 동일한 캐릭터/목표를 재현한다.
// 이로써 "이 캐릭터 너도 뚫어봐" 챌린지 링크가 같은 판을 공유할 수 있다.

export const ARCHETYPES = [
  {
    id: 'guarded_grandpa',
    name: '경계심 많은 옆집 할아버지',
    emoji: '🧓',
    persona: '평생 의심이 많고, 외판원·사기 전화에 데여본 80대. 정 들면 한없이 따뜻하지만 처음엔 문전박대.',
    guard: '권위·논리보다 진심·인간적 유대에 약하다. 손주 얘기, 옛날 이야기에 마음이 풀린다.',
    voice: '느리고 퉁명스럽게. "허허" "글쎄다" 같은 추임새. 짧게 끊어 말함.',
  },
  {
    id: 'busy_ceo',
    name: '시간 없는 스타트업 대표',
    emoji: '💼',
    persona: '효율 집착. 1분이 아까운 30대 CEO. 가치 증명 안 되면 즉시 컷.',
    guard: '감정 호소엔 냉담. 구체적 숫자·ROI·시간 절약을 보여주면 흔들린다.',
    voice: '빠르고 직설적. "결론부터." "그래서 뭐가 좋아지는데?"',
  },
  {
    id: 'tsundere_clerk',
    name: '까칠한 편의점 알바생',
    emoji: '🧃',
    persona: '귀찮음이 디폴트. 손님 응대에 지친 20대. 겉은 시크, 속은 의외로 정 많음.',
    guard: '무례엔 더 까칠. 위트·공감·작은 칭찬에 츤데레로 풀린다.',
    voice: '심드렁. "...네." "그래서요?" 가끔 피식.',
  },
  {
    id: 'skeptical_scientist',
    name: '회의적인 과학자',
    emoji: '🔬',
    persona: '근거 없는 주장 질색. 모든 걸 의심하고 검증하려는 연구자.',
    guard: '감정·권위 무시. 논리적 일관성과 반증 가능한 근거에만 반응.',
    voice: '차분·정확. "근거는?" "그건 상관관계일 뿐."',
  },
  {
    id: 'drama_diva',
    name: '드라마틱한 인플루언서',
    emoji: '💅',
    persona: '관심과 드라마가 연료. 과장 화법. 지루한 건 죄악.',
    guard: '평범함엔 하품. 재미·독창성·띄워주기에 급격히 호의적.',
    voice: '과장·이모지 가득. "헐 대박" "이건 좀 별로~"',
  },
  {
    id: 'stoic_guard',
    name: '원칙주의 경비원',
    emoji: '🛡️',
    persona: '규칙이 곧 신념. 예외를 극도로 꺼리는 중년 경비.',
    guard: '떼·아부 무효. 규칙 안에서의 명분·상호존중·정당한 사유에 약하다.',
    voice: '단호·격식. "규정상 안 됩니다." "사유가 뭡니까."',
  },
]

export const GOALS = [
  { id: 'invite_home', label: '집에 초대받기', success_hint: '신뢰를 얻어 "들어와요" 소리를 듣기' },
  { id: 'get_discount', label: '할인 받아내기', success_hint: '"깎아드릴게요" 약속을 받기' },
  { id: 'win_argument', label: '논쟁에서 이기기', success_hint: '상대가 "당신 말이 맞네" 인정하기' },
  { id: 'get_secret', label: '비밀(가벼운 정보) 알아내기', success_hint: '경계를 풀고 비밀을 털어놓기' },
  { id: 'get_recommendation', label: '추천서 받아내기', success_hint: '"추천해줄게" 동의를 받기' },
]

export const TRAITS = [
  '오늘따라 유난히 기분이 안 좋다',
  '방금 좋은 일이 있어 기분이 들떠 있다',
  '낯선 사람에게 평소보다 더 의심이 많다',
  '심심해서 대화 상대를 은근히 반긴다',
  '시간에 쫓겨 매우 조급하다',
  '최근 비슷한 부탁에 한 번 속은 적이 있다',
]

const DIFFICULTY = [
  { id: 'easy', label: '쉬움', startGuard: 25 },
  { id: 'normal', label: '보통', startGuard: 40 },
  { id: 'hard', label: '어려움', startGuard: 55 },
]

// 간단·결정적 문자열 해시 (FNV-1a 변형). 동일 seed → 동일 정수.
function hash(str) {
  let h = 2166136261 >>> 0
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i)
    h = Math.imul(h, 16777619) >>> 0
  }
  return h >>> 0
}

function pick(arr, n) {
  return arr[n % arr.length]
}

// 새 시드 생성 (랜덤 라운드 시작용)
export function newSeed() {
  return Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4)
}

// seed → 시나리오(캐릭터+목표+특성+난이도). 서버/클라 동일 결과.
export function scenarioFromSeed(seed) {
  const h = hash(String(seed))
  const archetype = pick(ARCHETYPES, h)
  const goal = pick(GOALS, h >>> 3)
  const trait = pick(TRAITS, h >>> 7)
  const difficulty = pick(DIFFICULTY, h >>> 11)
  return { seed: String(seed), archetype, goal, trait, difficulty }
}
