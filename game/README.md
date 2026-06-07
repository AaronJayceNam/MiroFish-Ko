# 설득 AI · Persuade AI (MVP)

무작위로 생성되는, 개성·경계심을 가진 **LLM NPC를 대화로 설득**해 목표를 달성하고,
그 예측불가한 반응을 **짤/클립으로 공유**하는 바이럴 웹 게임 MVP.

> 2026 트렌드 반영: AI-네이티브(없으면 게임 불성립) · 2차 시청성(보는 재미) · 공유 루프=UA · 하이브리드 캐주얼 수익화(루트박스/가챠 없음).
> **현실 점검**: 이건 "빠르게 검증할 MVP"이지 보장된 수익이 아닙니다. 검증 지표(공유 전환율·D1/D7·ARPDAU)를 먼저 띄우고 데이터로 키웁니다.

## 핵심 루프 (한 판 60~120초)
진입 → 캐릭터/목표 제시 → 대화로 설득(최대 6턴) → **LLM 구조화 판정**(게이지·이유) → 성공/실패 → 결과 카드 → 공유/재도전

## 기술 스택
- **프론트**: React + Vite + TailwindCSS (모바일 우선)
- **백엔드**: Vercel Functions — Anthropic(Claude) 호출. **API 키는 서버에만**, 클라는 `/api/*`만 호출.
- **AI 판정**: 구조화 출력 강제 `{npc_reply, mood, delta, persuasion, goal_met, verdict, reason, clip_caption}`
- **저장**: 익명 세션(localStorage). 리더보드/영속화는 스텁 → Supabase/Upstash 교체 지점 주석.
- **배포**: Vercel 원클릭.

## 디렉터리
```
game/
├─ api/                     # Vercel 서버리스 (서버 전용)
│  ├─ persuade.js           #  POST /api/persuade — 한 턴 설득 판정
│  ├─ leaderboard.js        #  GET/POST /api/leaderboard — 일일 리더보드(스텁)
│  ├─ og.jsx                #  GET /api/og — 결과 공유 OG 이미지(Edge)
│  └─ _lib/                 #  anthropic / prompt / scenarios / ratelimit
├─ src/
│  ├─ state/useGame.js      # 코어 루프 상태
│  ├─ lib/                  # api / session / monetization / share / analytics / scenarios
│  └─ components/           # Start · Game · Result · Paywall · Leaderboard …
└─ docs/                    # M0 컨셉표 · PRD/리스크 · 수익화/KPI 메모
```

## 로컬 실행
```bash
cd game
npm install
cp .env.example .env            # ANTHROPIC_API_KEY 등 채우기
```
- **권장(풀스택)**: `npm i -g vercel` 후 `vercel dev` → 프론트 + /api 함수 동시 구동.
- **프론트만**: `npm run dev` (이 경우 /api 호출은 `vercel dev`를 별도로 띄우고
  `API_PROXY=http://localhost:3000 npm run dev`로 프록시).

## 배포 (Vercel)
1. 이 레포를 GitHub에 푸시.
2. [vercel.com](https://vercel.com) → New Project → 레포 선택 → **Root Directory: `game`**.
3. **Environment Variables**에 `ANTHROPIC_API_KEY`(필수), 필요시 `MODEL`, `MAX_TURNS`, `VITE_PUBLIC_BASE_URL` 등록.
4. Deploy. (`vercel.json`이 빌드/함수 설정 포함)
5. **도메인 연결**: Project → Settings → Domains → 도메인 추가 후 안내된 A/CNAME 레코드 설정.
   연결 후 `VITE_PUBLIC_BASE_URL`을 그 도메인으로 설정하면 공유 링크/OG가 절대경로로 생성됩니다.

### 계정/서비스 연결 가이드
| 서비스 | 용도 | 연결 지점 |
|---|---|---|
| Anthropic | LLM 판정 | `ANTHROPIC_API_KEY` (서버 env) |
| Supabase | 익명세션·리더보드 영속화 | `SUPABASE_*` env, `api/leaderboard.js` 주석 |
| Upstash | 분산 레이트리밋 | `UPSTASH_*` env, `api/_lib/ratelimit.js` 주석 |
| Stripe(테스트) | IAP 결제 | `STRIPE_SECRET_KEY`, `src/lib/monetization.js#purchase` |
| 광고 SDK | 보상형 광고 | `VITE_AD_NETWORK_ID`, `monetization.js#watchRewardedAd` |

## 비용(COGS) & 모델
- 기본 모델 `claude-opus-4-8`(품질 우선). 트래픽 확장 시 `MODEL=claude-haiku-4-5`로 교체해 COGS 절감 검토.
- 통제 레버: 프롬프트 캐싱 · 히스토리 6턴 상한 · `max_tokens` 캡 · 무료횟수 제한.
- 자세한 단가/지표: `docs/MONETIZATION_KPI.md`.

## 보안
- API 키 **서버 전용**. 클라이언트 번들에 키 없음.
- 프롬프트 인젝션 방어: 사용자 입력 `<player_message>` 격리 + 시스템 규칙 고정 + **게이지 서버 클램프**.
- 입력 길이 제한(500자) · IP 레이트리밋(베스트에포트).

## 문서
- `docs/M0_CONCEPTS.md` — 컨셉 3개 점수표 & 확정 근거
- `docs/PRD.md` — PRD + 리스크 1장
- `docs/MONETIZATION_KPI.md` — 수익화·KPI·COGS 설계
