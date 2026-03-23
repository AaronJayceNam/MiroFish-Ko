"""
ETF Buzz Monitor → MiroFish 시드 문서 자동 생성기

Supabase DB에서 대시보드 분석 데이터를 가져와
MiroFish에 투입할 분석 리포트 형태의 시드 문서를 생성한다.

Usage:
    python generate_etf_seed.py
    python generate_etf_seed.py --output ./my_seed.md
"""

import os
import sys
import json
import argparse
from collections import defaultdict
from datetime import datetime, timedelta

import psycopg2

# ── DB 연결 ──
DB_URL = os.environ.get(
    "SUPABASE_DB_URL",
    "postgresql://postgres:aceetf4947!!@db.xlkfxhiaofgjvkrydpjm.supabase.co:5432/postgres"
)


def get_conn():
    return psycopg2.connect(DB_URL)


def query(conn, sql, params=None):
    c = conn.cursor()
    c.execute(sql, params or ())
    cols = [d[0] for d in c.description]
    return [dict(zip(cols, row)) for row in c.fetchall()]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 섹션 생성 함수들
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def section_psychology(conn):
    """섹션 1: 현재 투자자 심리 진단 (psychology_reports)"""
    rows = query(conn, """
        SELECT report_json, period_from, period_to
        FROM psychology_reports ORDER BY created_at DESC LIMIT 1
    """)
    if not rows:
        return ""

    r = rows[0]
    rj = json.loads(r["report_json"]) if isinstance(r["report_json"], str) else r["report_json"]

    lines = [
        f"## 1. 현재 투자자 심리 진단 ({r['period_from']} ~ {r['period_to']})",
        "",
        f"시장 온도: {rj.get('market_temperature', '?')}/100 ({rj.get('temperature_label', '')})",
        "",
        f"### 핵심 진단",
        rj.get("core_narrative", ""),
        "",
    ]

    # 소스별 분석
    for sa in rj.get("source_analyses", [])[:5]:
        lines.append(f"### {sa.get('source_name', '')} 분석")
        lines.append(f"- 감성 방향: {sa.get('sentiment_direction', '')}")
        lines.append(f"- 핵심 키워드: {', '.join(sa.get('top_keywords', []))}")
        if sa.get("anomaly_signals"):
            lines.append(f"- 이상 신호: {sa['anomaly_signals']}")
        if sa.get("detail"):
            lines.append(sa["detail"][:800])
        lines.append("")

    # 투자자 세그먼트
    for seg in rj.get("investor_segments", [])[:4]:
        lines.append(f"### 투자자 세그먼트: {seg.get('segment_name', '')}")
        lines.append(f"- 현재 관심: {seg.get('current_interest', '')}")
        lines.append(f"- 행동 패턴: {seg.get('behavior_pattern', '')}")
        if seg.get("etf_preference"):
            lines.append(f"- ETF 선호: {seg['etf_preference']}")
        lines.append("")

    # ACE ETF 포커스
    ace = rj.get("ace_etf_focus", "")
    if ace:
        lines.append("### ACE ETF 전략 포인트")
        lines.append(str(ace)[:1500])
        lines.append("")

    return "\n".join(lines)


def section_forecast(conn):
    """섹션 2: 미래 수요 예측 (forecast_reports)"""
    rows = query(conn, """
        SELECT report_json, period_from, period_to
        FROM forecast_reports ORDER BY created_at DESC LIMIT 1
    """)
    if not rows:
        return ""

    rj = json.loads(rows[0]["report_json"]) if isinstance(rows[0]["report_json"], str) else rows[0]["report_json"]

    lines = [
        f"## 2. 미래 투자자 수요 예측 ({rows[0]['period_from']} ~ {rows[0]['period_to']})",
        "",
    ]

    # 단기/중기/장기
    for term_key, label in [("short_term", "단기"), ("mid_term", "중기"), ("long_term", "장기")]:
        t = rj.get(term_key, {})
        if not t:
            continue
        lines.append(f"### {label} 전망 ({t.get('period', '')})")
        if t.get("main_driver"):
            lines.append(t["main_driver"][:600])
        if t.get("key_inflection"):
            lines.append(f"핵심 변곡점: {t['key_inflection'][:400]}")
        if t.get("structural_change"):
            lines.append(f"구조 변화: {t['structural_change'][:400]}")
        lines.append("")

    # 섹터 전망
    sf = rj.get("sector_forecast", {})
    if sf.get("short_term_top3"):
        lines.append("### 단기 유망 섹터 TOP 3")
        for s in sf["short_term_top3"]:
            lines.append(f"- {s.get('rank', '')}위 {s.get('sector_type', '')}: {s.get('rationale', '')[:300]}")
        lines.append("")

    # 신규 ETF 기회
    neo = rj.get("new_etf_opportunities", "")
    if neo:
        lines.append("### 신규 ETF 런칭 기회")
        lines.append(str(neo)[:1500])
        lines.append("")

    # 실행 포인트
    ap = rj.get("action_points", {})
    if ap.get("prepare_now"):
        lines.append("### 즉시 실행 포인트")
        lines.append(ap["prepare_now"][:800])
        lines.append("")

    return "\n".join(lines)


def section_weekly_strategy(conn):
    """섹션 3: 주간 전략 리포트 (weekly_strategy_reports)"""
    rows = query(conn, """
        SELECT report_json, period_from, period_to
        FROM weekly_strategy_reports ORDER BY created_at DESC LIMIT 1
    """)
    if not rows:
        return ""

    rj = json.loads(rows[0]["report_json"]) if isinstance(rows[0]["report_json"], str) else rows[0]["report_json"]

    lines = [
        f"## 3. 주간 전략 리포트 ({rows[0]['period_from']} ~ {rows[0]['period_to']})",
        "",
        "### 핵심 결론",
        rj.get("executive_summary", "")[:2000],
        "",
    ]

    # 전략 카드
    for card in rj.get("cards", [])[:6]:
        lines.append(f"### {card.get('title', '')}")
        lines.append(card.get("description", "")[:800])
        lines.append("")

    # 에이전트 토론 (1라운드만)
    disc = rj.get("agent_discussions", {})
    r1 = disc.get("round1", {})
    if r1:
        lines.append("### 멀티에이전트 전략 토론")
        for agent, opinion in list(r1.items())[:4]:
            lines.append(f"**{agent}**: {opinion[:500]}")
            lines.append("")

    return "\n".join(lines)


def section_buzz_ranking(conn):
    """섹션 4: 버즈 랭킹 & 인기 ETF"""
    rows = query(conn, """
        SELECT b.ticker, e.name, e.short_name, e.category,
            SUM(b.buzz_score) as total_buzz,
            SUM(b.naver_post_count) as naver,
            SUM(b.youtube_video_count) as youtube,
            SUM(b.blog_post_count) as blog,
            SUM(b.news_count) as news,
            AVG(b.naver_avg_sentiment) as avg_sentiment
        FROM buzz_daily b JOIN etf_registry e ON b.ticker=e.ticker
        WHERE b.date::date >= (CURRENT_DATE - INTERVAL '7 days')
        GROUP BY b.ticker, e.name, e.short_name, e.category
        ORDER BY total_buzz DESC LIMIT 25
    """)

    lines = [
        "## 4. 최근 1주간 버즈 랭킹",
        "",
        "온라인 커뮤니티, 블로그, 유튜브, 뉴스에서 가장 많이 언급된 ETF 순위:",
        "",
    ]

    for i, r in enumerate(rows, 1):
        sent = f", 감성 {r['avg_sentiment']:.2f}" if r["avg_sentiment"] else ""
        lines.append(
            f"{i}. **{r['name']}** ({r['category'] or '-'}) — "
            f"버즈 {r['total_buzz']:.0f}점 "
            f"(네이버 {r['naver']}건, 유튜브 {r['youtube']}건, "
            f"블로그 {r['blog']}건, 뉴스 {r['news']}건{sent})"
        )
    lines.append("")

    # 브랜드별 합산
    brand_rows = query(conn, """
        SELECT brand, SUM(total_buzz) as total_buzz, COUNT(*) as etf_count FROM (
            SELECT
                CASE
                    WHEN e.name LIKE 'ACE%%' THEN 'ACE'
                    WHEN e.name LIKE 'KODEX%%' THEN 'KODEX'
                    WHEN e.name LIKE 'TIGER%%' THEN 'TIGER'
                    WHEN e.name LIKE 'SOL%%' THEN 'SOL'
                    WHEN e.name LIKE 'KBSTAR%%' THEN 'KBSTAR'
                    WHEN e.name LIKE 'HANARO%%' THEN 'HANARO'
                    ELSE '기타'
                END as brand,
                b.ticker,
                SUM(b.buzz_score) as total_buzz
            FROM buzz_daily b JOIN etf_registry e ON b.ticker=e.ticker
            WHERE b.date::date >= (CURRENT_DATE - INTERVAL '7 days')
            GROUP BY e.name, b.ticker
        ) sub GROUP BY brand ORDER BY total_buzz DESC
    """)

    lines.append("### 브랜드별 버즈 합산")
    for br in brand_rows:
        lines.append(f"- {br['brand']}: 총 버즈 {br['total_buzz']:.0f} ({br['etf_count']}개 ETF)")
    lines.append("")

    return "\n".join(lines)


def section_investor_flow(conn):
    """섹션 5: 자금 흐름 패턴"""
    rows = query(conn, """
        SELECT t.ticker, e.name, e.category,
            SUM(t.individual_net_buy) as ind_net,
            SUM(t.institutional_net_buy) as inst_net,
            SUM(t.foreign_net_buy) as for_net
        FROM etf_investor_trading t JOIN etf_registry e ON t.ticker=e.ticker
        WHERE t.date::date >= (CURRENT_DATE - INTERVAL '7 days')
        GROUP BY t.ticker, e.name, e.category
        ORDER BY ind_net DESC LIMIT 15
    """)

    lines = [
        "## 5. 최근 1주간 자금 흐름",
        "",
        "### 개인순매수 상위 ETF",
    ]

    for r in rows:
        ind = float(r["ind_net"] or 0)
        inst = float(r["inst_net"] or 0)
        lines.append(
            f"- **{r['name']}**: 개인 순매수 {ind/1e6:+,.0f}백만원, "
            f"기관 {inst/1e6:+,.0f}백만원"
        )
    lines.append("")

    # 개인순매도 상위
    sell_rows = query(conn, """
        SELECT t.ticker, e.name,
            SUM(t.individual_net_buy) as ind_net
        FROM etf_investor_trading t JOIN etf_registry e ON t.ticker=e.ticker
        WHERE t.date::date >= (CURRENT_DATE - INTERVAL '7 days')
        GROUP BY t.ticker, e.name
        ORDER BY ind_net ASC LIMIT 10
    """)

    lines.append("### 개인순매도 상위 ETF (이탈 신호)")
    for r in sell_rows:
        lines.append(f"- **{r['name']}**: 개인 순매도 {float(r['ind_net'] or 0)/1e6:,.0f}백만원")
    lines.append("")

    return "\n".join(lines)


def section_sentiment(conn):
    """섹션 6: 커뮤니티 감성 분석"""
    rows = query(conn, """
        SELECT n.ticker, e.name,
            AVG(n.sentiment_score) as avg_sent,
            COUNT(*) as post_count,
            SUM(CASE WHEN n.sentiment_label='positive' THEN 1 ELSE 0 END) as pos,
            SUM(CASE WHEN n.sentiment_label='negative' THEN 1 ELSE 0 END) as neg
        FROM naver_stk_community n JOIN etf_registry e ON n.ticker=e.ticker
        WHERE n.collected_at::date >= (CURRENT_DATE - INTERVAL '7 days')
          AND n.sentiment_score IS NOT NULL
        GROUP BY n.ticker, e.name
        HAVING COUNT(*) >= 3
        ORDER BY post_count DESC LIMIT 15
    """)

    lines = [
        "## 6. 커뮤니티 감성 분석 (네이버 종목토론실)",
        "",
    ]

    for r in rows:
        total = r["pos"] + r["neg"]
        pos_pct = r["pos"] / total * 100 if total > 0 else 0
        lines.append(
            f"- **{r['name']}**: 감성 {r['avg_sent']:.2f} "
            f"(게시글 {r['post_count']}건, 긍정 {pos_pct:.0f}%)"
        )
    lines.append("")

    return "\n".join(lines)


def section_trend_analysis(conn):
    """섹션 7: 메가트렌드 심층 분석"""
    rows = query(conn, """
        SELECT category, analysis_text, keywords_json, analysis_date
        FROM trend_analyses
        WHERE analysis_date = (SELECT MAX(analysis_date) FROM trend_analyses)
        ORDER BY category
    """)

    lines = [
        "## 7. 메가트렌드 분석 (카테고리별 전문가 분석)",
        "",
    ]

    for r in rows:
        lines.append(f"### {r['category']} 트렌드 ({r['analysis_date']})")
        if r["analysis_text"]:
            lines.append(r["analysis_text"][:1200])
        if r["keywords_json"]:
            try:
                kws = json.loads(r["keywords_json"])
                lines.append(f"\n핵심 키워드: {', '.join(kws[:8])}")
            except:
                pass
        lines.append("")

    return "\n".join(lines)


def section_success_cases(conn):
    """섹션 8: 최근 성공한 ETF 사례"""
    # AUM 증가율 상위
    aum_rows = query(conn, """
        WITH latest AS (
            SELECT ticker, aum, date,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) as rn
            FROM etf_aum WHERE aum IS NOT NULL AND aum > 0
        ),
        prev AS (
            SELECT ticker, aum, date,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) as rn
            FROM etf_aum WHERE aum IS NOT NULL AND aum > 0
              AND date::date <= (CURRENT_DATE - INTERVAL '30 days')
        )
        SELECT l.ticker, e.name, e.category,
               l.aum as current_aum, p.aum as prev_aum,
               CASE WHEN p.aum > 0 THEN (l.aum - p.aum) / p.aum * 100 ELSE 0 END as growth_pct
        FROM latest l
        JOIN prev p ON l.ticker = p.ticker AND p.rn = 1
        JOIN etf_registry e ON l.ticker = e.ticker
        WHERE l.rn = 1 AND l.aum > 5000000000
        ORDER BY growth_pct DESC LIMIT 10
    """)

    lines = [
        "## 8. 최근 성공한 ETF 사례 (AUM 성장률 기준)",
        "",
    ]

    if aum_rows:
        for r in aum_rows:
            lines.append(
                f"- **{r['name']}** ({r['category']}): "
                f"AUM {r['current_aum']/1e8:,.0f}억원, "
                f"1개월 성장률 {r['growth_pct']:+.1f}%"
            )
    else:
        lines.append("(AUM 데이터 부족으로 분석 불가)")
    lines.append("")

    return "\n".join(lines)


def section_competition(conn):
    """섹션 9: 경쟁 환경 분석"""
    brand_rows = query(conn, """
        SELECT brand, SUM(cnt) as etf_count,
               SUM(overseas) as overseas, SUM(domestic) as domestic, SUM(bond) as bond
        FROM (
            SELECT
                CASE
                    WHEN name LIKE 'ACE%%' THEN 'ACE'
                    WHEN name LIKE 'KODEX%%' THEN 'KODEX'
                    WHEN name LIKE 'TIGER%%' THEN 'TIGER'
                    WHEN name LIKE 'SOL%%' THEN 'SOL'
                    WHEN name LIKE 'KBSTAR%%' THEN 'KBSTAR'
                    WHEN name LIKE 'HANARO%%' THEN 'HANARO'
                    WHEN name LIKE 'ARIRANG%%' THEN 'ARIRANG'
                    ELSE '기타'
                END as brand,
                1 as cnt,
                CASE WHEN category='해외주식' THEN 1 ELSE 0 END as overseas,
                CASE WHEN category='국내주식' THEN 1 ELSE 0 END as domestic,
                CASE WHEN category='채권' THEN 1 ELSE 0 END as bond
            FROM etf_registry
        ) sub GROUP BY brand ORDER BY etf_count DESC
    """)

    lines = [
        "## 9. 경쟁 환경 분석",
        "",
        "### 브랜드별 ETF 라인업",
    ]

    for br in brand_rows[:8]:
        lines.append(
            f"- **{br['brand']}**: {br['etf_count']}개 "
            f"(해외 {br['overseas']}, 국내 {br['domestic']}, 채권 {br['bond']})"
        )
    lines.append("")

    return "\n".join(lines)


def section_latest_news(conn):
    """섹션 10: 최신 트렌드 뉴스"""
    rows = query(conn, """
        SELECT title, source, category, summary, published_at
        FROM trend_news
        WHERE published_at::date >= (CURRENT_DATE - INTERVAL '14 days')
        ORDER BY published_at DESC LIMIT 15
    """)

    lines = [
        "## 10. 최신 트렌드 뉴스 (최근 2주)",
        "",
    ]

    for r in rows:
        lines.append(f"### [{r['category']}] {r['title']}")
        lines.append(f"출처: {r['source']} | {r['published_at']}")
        if r["summary"]:
            lines.append(r["summary"][:400])
        lines.append("")

    return "\n".join(lines)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 메인
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_seed(output_path: str):
    conn = get_conn()
    today = datetime.now().strftime("%Y-%m-%d")

    header = f"""# 한국 ETF 시장 투자자 심리 및 트렌드 종합 분석 리포트
분석 기준일: {today}
데이터 소스: ETF Buzz Monitor (aceetf.streamlit.app)

이 리포트는 네이버 커뮤니티, 블로그, 유튜브, 뉴스, 토스, 레딧 등
다채널 데이터를 종합 분석한 결과입니다.
새로운 ETF 런칭 시 시장 반응을 예측하기 위한 기초 자료로 활용됩니다.

---
"""

    sections = [
        header,
        section_psychology(conn),
        section_forecast(conn),
        section_weekly_strategy(conn),
        section_buzz_ranking(conn),
        section_investor_flow(conn),
        section_sentiment(conn),
        section_trend_analysis(conn),
        section_success_cases(conn),
        section_competition(conn),
        section_latest_news(conn),
    ]

    content = "\n".join(s for s in sections if s)
    conn.close()

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"시드 문서 생성 완료: {output_path}")
    print(f"  크기: {len(content):,} chars ({len(content.encode('utf-8')):,} bytes)")
    print(f"  섹션: {content.count('## ')}개")
    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETF Buzz Monitor → MiroFish 시드 문서 생성")
    parser.add_argument("--output", "-o",
                        default="./backend/uploads/etf_market_analysis_seed.md",
                        help="출력 파일 경로")
    args = parser.parse_args()
    generate_seed(args.output)
