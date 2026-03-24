"""
신규 ETF 런칭 시뮬레이션을 위한 시드 문서 + 프롬프트 자동 생성기

Usage:
    python generate_new_etf_seed.py \
        --theme "K-수출" \
        --etf-name "ACE K-수출핵심TOP10산업액티브" \
        --description "한국 수출 핵심 기업 10종목에 집중 투자하는 액티브 ETF" \
        --holdings "삼성전자,현대차,LG에너지솔루션,아모레퍼시픽,CJ제일제당" \
        --keywords "K-수출,K뷰티투자,한류ETF,수출기업주가,K푸드관련주"

    # 간단 버전 (theme만 입력하면 LLM이 나머지를 자동 생성)
    python generate_new_etf_seed.py --theme "2차전지 리사이클링"
"""

import os
import sys
import json
import argparse
import time
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from urllib.parse import quote_plus

# ── 설정 ──
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
SUPABASE_DB_URL = os.environ.get("SUPABASE_DB_URL", "")
NAVER_CLIENT_ID = os.environ.get("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.environ.get("NAVER_CLIENT_SECRET", "")


def load_env():
    """MiroFish .env에서 키 로드"""
    global OPENAI_API_KEY, SUPABASE_DB_URL, NAVER_CLIENT_ID, NAVER_CLIENT_SECRET
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        env_map = {
            "LLM_API_KEY": "OPENAI_API_KEY",
            "OPENAI_API_KEY": "OPENAI_API_KEY",
            "SUPABASE_DB_URL": "SUPABASE_DB_URL",
            "NAVER_CLIENT_ID": "NAVER_CLIENT_ID",
            "NAVER_CLIENT_SECRET": "NAVER_CLIENT_SECRET",
        }
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    k, v = k.strip(), v.strip()
                    if k in env_map:
                        target = env_map[k]
                        if target == "OPENAI_API_KEY" and not OPENAI_API_KEY:
                            OPENAI_API_KEY = v
                        elif target == "SUPABASE_DB_URL" and not SUPABASE_DB_URL:
                            SUPABASE_DB_URL = v
                        elif target == "NAVER_CLIENT_ID" and not NAVER_CLIENT_ID:
                            NAVER_CLIENT_ID = v
                        elif target == "NAVER_CLIENT_SECRET" and not NAVER_CLIENT_SECRET:
                            NAVER_CLIENT_SECRET = v


load_env()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 0. DB 연결 헬퍼
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _get_db_conn():
    """Supabase DB 연결을 반환. 실패 시 None."""
    if not SUPABASE_DB_URL:
        return None
    try:
        import psycopg2
        return psycopg2.connect(SUPABASE_DB_URL)
    except Exception as e:
        print(f"  DB 연결 실패: {e}")
        return None


def _build_ilike_params(column: str, keywords: List[str], prefix: str = "kw"):
    """SQL ILIKE 조건과 파라미터를 안전하게 생성."""
    conditions = []
    params = {}
    for i, kw in enumerate(keywords):
        key = f"{prefix}_{i}"
        conditions.append(f"{column} ILIKE %({key})s")
        params[key] = f"%{kw}%"
    return " OR ".join(conditions), params


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. 웹 데이터 수집 (네이버/구글 검색)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def search_naver_blog(query: str, display: int = 20) -> List[Dict]:
    """네이버 블로그 검색 API"""
    import urllib.request
    client_id = NAVER_CLIENT_ID
    client_secret = NAVER_CLIENT_SECRET
    url = f"https://openapi.naver.com/v1/search/blog.json?query={quote_plus(query)}&display={display}&sort=date"
    req = urllib.request.Request(url)
    req.add_header("X-Naver-Client-Id", client_id)
    req.add_header("X-Naver-Client-Secret", client_secret)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("items", [])
    except Exception as e:
        print(f"  네이버 블로그 검색 실패 ({query}): {e}")
        return []


def search_naver_news(query: str, display: int = 20) -> List[Dict]:
    """네이버 뉴스 검색 API"""
    import urllib.request
    client_id = NAVER_CLIENT_ID
    client_secret = NAVER_CLIENT_SECRET
    url = f"https://openapi.naver.com/v1/search/news.json?query={quote_plus(query)}&display={display}&sort=date"
    req = urllib.request.Request(url)
    req.add_header("X-Naver-Client-Id", client_id)
    req.add_header("X-Naver-Client-Secret", client_secret)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("items", [])
    except Exception as e:
        print(f"  네이버 뉴스 검색 실패 ({query}): {e}")
        return []


def search_naver_cafearticle(query: str, display: int = 10) -> List[Dict]:
    """네이버 카페 검색 API"""
    import urllib.request
    client_id = NAVER_CLIENT_ID
    client_secret = NAVER_CLIENT_SECRET
    url = f"https://openapi.naver.com/v1/search/cafearticle.json?query={quote_plus(query)}&display={display}&sort=date"
    req = urllib.request.Request(url)
    req.add_header("X-Naver-Client-Id", client_id)
    req.add_header("X-Naver-Client-Secret", client_secret)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("items", [])
    except Exception as e:
        print(f"  네이버 카페 검색 실패 ({query}): {e}")
        return []


def strip_html(text: str) -> str:
    """HTML 태그 제거"""
    return re.sub(r"<[^>]+>", "", text or "")


def collect_theme_data(theme: str, keywords: List[str]) -> Dict:
    """테마 관련 데이터를 네이버 API로 수집"""
    print(f"\n{'='*50}")
    print(f"테마 데이터 수집: {theme}")
    print(f"검색 키워드: {keywords}")
    print(f"{'='*50}")

    all_blogs = []
    all_news = []
    all_cafe = []

    for kw in keywords:
        print(f"\n  검색 중: '{kw}'")

        # 투자 관점 키워드 조합
        invest_queries = [
            kw,
            f"{kw} 투자",
            f"{kw} ETF",
        ]

        for q in invest_queries:
            blogs = search_naver_blog(q, 10)
            all_blogs.extend(blogs)
            time.sleep(0.2)

            news = search_naver_news(q, 10)
            all_news.extend(news)
            time.sleep(0.2)

        # 카페는 기본 키워드만
        cafes = search_naver_cafearticle(f"{kw} 투자", 5)
        all_cafe.extend(cafes)
        time.sleep(0.2)

    # 중복 제거 (title 기준)
    seen_titles = set()
    unique_blogs, unique_news, unique_cafe = [], [], []
    for b in all_blogs:
        t = strip_html(b.get("title", ""))
        if t and t not in seen_titles:
            seen_titles.add(t)
            unique_blogs.append(b)
    for n in all_news:
        t = strip_html(n.get("title", ""))
        if t and t not in seen_titles:
            seen_titles.add(t)
            unique_news.append(n)
    for c in all_cafe:
        t = strip_html(c.get("title", ""))
        if t and t not in seen_titles:
            seen_titles.add(t)
            unique_cafe.append(c)

    print(f"\n  수집 결과: 블로그 {len(unique_blogs)}건, 뉴스 {len(unique_news)}건, 카페 {len(unique_cafe)}건")

    return {
        "blogs": unique_blogs[:30],
        "news": unique_news[:30],
        "cafe": unique_cafe[:15],
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. 기존 DB에서 관련 데이터 추출
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_existing_market_context(theme: str, keywords: List[str]) -> str:
    """기존 Supabase DB에서 테마 관련 데이터 추출"""
    conn = _get_db_conn()
    if not conn:
        return ""
    c = conn.cursor()

    lines = []

    # 1. 관련 트렌드 뉴스
    try:
        cond, params = _build_ilike_params("title", keywords[:5], "tn")
        c.execute(f"""
            SELECT title, source, category, summary, published_at
            FROM trend_news WHERE ({cond})
            ORDER BY published_at DESC LIMIT 15
        """, params)
        rows = c.fetchall()
        if rows:
            lines.append("### 관련 트렌드 뉴스 (기존 DB)")
            for r in rows:
                lines.append(f"- [{r[2]}] {r[0]} ({r[1]}, {r[4]})")
                if r[3]:
                    lines.append(f"  {r[3][:200]}")
            lines.append("")
    except Exception:
        pass

    # 2. 관련 ETF 버즈
    try:
        cond, params = _build_ilike_params("e.name", keywords[:5], "eb")
        c.execute(f"""
            SELECT e.name, e.category, SUM(b.buzz_score) as buzz
            FROM buzz_daily b JOIN etf_registry e ON b.ticker=e.ticker
            WHERE ({cond}) AND b.date::date >= (CURRENT_DATE - INTERVAL '30 days')
            GROUP BY e.name, e.category ORDER BY buzz DESC LIMIT 10
        """, params)
        rows = c.fetchall()
        if rows:
            lines.append("### 관련 기존 ETF 버즈 (최근 30일)")
            for r in rows:
                lines.append(f"- {r[0]} ({r[1]}): 버즈 {float(r[2]):.0f}")
            lines.append("")
    except Exception:
        pass

    # 3. 관련 트렌드 분석
    try:
        cond, params = _build_ilike_params("analysis_text", keywords[:3], "ta")
        c.execute(f"""
            SELECT category, analysis_text, analysis_date
            FROM trend_analyses WHERE ({cond})
            ORDER BY analysis_date DESC LIMIT 3
        """, params)
        rows = c.fetchall()
        if rows:
            lines.append("### 관련 전문가 분석 (기존 DB)")
            for r in rows:
                lines.append(f"**{r[0]}** ({r[2]}):")
                lines.append(r[1][:600])
                lines.append("")
    except Exception:
        pass

    # 4. 시장 심리 요약 (최신)
    try:
        c.execute("""
            SELECT report_json FROM psychology_reports
            ORDER BY created_at DESC LIMIT 1
        """)
        row = c.fetchone()
        if row:
            rj = json.loads(row[0]) if isinstance(row[0], str) else row[0]
            lines.append("### 현재 시장 심리 요약")
            lines.append(f"시장 온도: {rj.get('market_temperature', '?')}/100 ({rj.get('temperature_label', '')})")
            lines.append(rj.get("core_narrative", "")[:500])
            lines.append("")
    except Exception:
        pass

    conn.close()
    return "\n".join(lines)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2-B. 확장 DB 추출 (etf-buzz-monitor)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_etf_launch_cases(keywords: List[str], limit: int = 5) -> str:
    """과거 유사 테마 ETF의 런칭 사례를 DB에서 추출 (성과+버즈+AUM+투자자수급)"""
    conn = _get_db_conn()
    if not conn:
        return ""
    c = conn.cursor()
    lines = []

    try:
        # 키워드와 매칭되는 ETF 찾기
        cond, params = _build_ilike_params("e.name", keywords[:5], "lc")
        c.execute(f"""
            SELECT DISTINCT e.ticker, e.name, e.category
            FROM etf_registry e
            WHERE ({cond})
            ORDER BY e.name LIMIT {limit}
        """, params)
        etfs = c.fetchall()

        if not etfs:
            conn.close()
            return ""

        lines.append("### 과거 유사 ETF 런칭 사례")
        lines.append("")

        for ticker, name, category in etfs:
            lines.append(f"#### {name} ({ticker}, {category})")

            # 상장 초기 30일 성과
            c.execute("""
                SELECT date, close, volume
                FROM etf_performance
                WHERE ticker = %(tk)s
                ORDER BY date ASC LIMIT 30
            """, {"tk": ticker})
            perf_rows = c.fetchall()
            if perf_rows:
                first_date = perf_rows[0][0]
                first_close = perf_rows[0][1]
                last_close = perf_rows[-1][1]
                total_volume = sum(r[2] or 0 for r in perf_rows)
                avg_volume = total_volume // len(perf_rows) if perf_rows else 0
                price_change = ((last_close - first_close) / first_close * 100) if first_close else 0
                lines.append(f"- 첫 거래일: {first_date}")
                lines.append(f"- 초기 30일 가격 변동: {price_change:+.1f}%")
                lines.append(f"- 일평균 거래량: {avg_volume:,}주")

            # 상장 초기 AUM 변화
            c.execute("""
                SELECT date, aum FROM etf_aum
                WHERE ticker = %(tk)s
                ORDER BY date ASC LIMIT 30
            """, {"tk": ticker})
            aum_rows = c.fetchall()
            if aum_rows:
                first_aum = aum_rows[0][1] or 0
                last_aum = aum_rows[-1][1] or 0
                aum_growth = ((last_aum - first_aum) / first_aum * 100) if first_aum else 0
                lines.append(f"- 초기 AUM: {first_aum / 1e8:,.0f}억원 → {last_aum / 1e8:,.0f}억원 ({aum_growth:+.0f}%)")

            # 상장 초기 버즈
            c.execute("""
                SELECT date, buzz_score, youtube_total_views,
                       cafe_post_count, blog_post_count
                FROM buzz_daily
                WHERE ticker = %(tk)s
                ORDER BY date ASC LIMIT 30
            """, {"tk": ticker})
            buzz_rows = c.fetchall()
            if buzz_rows:
                avg_buzz = sum(r[1] or 0 for r in buzz_rows) / len(buzz_rows)
                total_yt_views = sum(r[2] or 0 for r in buzz_rows)
                total_cafe = sum(r[3] or 0 for r in buzz_rows)
                total_blog = sum(r[4] or 0 for r in buzz_rows)
                lines.append(f"- 초기 30일 평균 버즈: {avg_buzz:.1f}")
                lines.append(f"- 유튜브 총 조회수: {total_yt_views:,}, 카페 {total_cafe}건, 블로그 {total_blog}건")

            # 투자자 수급 (초기)
            c.execute("""
                SELECT SUM(individual_net_buy), SUM(institutional_net_buy),
                       SUM(foreign_net_buy)
                FROM etf_investor_trading
                WHERE ticker = %(tk)s
                ORDER BY date ASC LIMIT 30
            """, {"tk": ticker})
            inv_row = c.fetchone()
            if inv_row and any(v is not None for v in inv_row):
                ind = inv_row[0] or 0
                inst = inv_row[1] or 0
                frgn = inv_row[2] or 0
                lines.append(f"- 초기 순매수: 개인 {ind:+,}주, 기관 {inst:+,}주, 외국인 {frgn:+,}주")

            # 성공/실패 판단
            if perf_rows and aum_rows:
                if (last_aum or 0) > 500e8 and price_change > -5:
                    lines.append(f"- **판정: 성공** (AUM {last_aum/1e8:,.0f}억 달성)")
                elif (last_aum or 0) < 50e8 or price_change < -15:
                    lines.append(f"- **판정: 부진** (AUM {last_aum/1e8:,.0f}억, 수익률 {price_change:+.1f}%)")
                else:
                    lines.append(f"- **판정: 보통**")

            lines.append("")

    except Exception as e:
        print(f"  런칭 사례 추출 실패: {e}")

    conn.close()
    return "\n".join(lines)


def get_youtube_channel_influence(keywords: List[str]) -> str:
    """ETF 관련 유튜브 채널 영향력 데이터 추출"""
    conn = _get_db_conn()
    if not conn:
        return ""
    c = conn.cursor()
    lines = []

    try:
        # 전체 ETF 유튜브 채널 영향력 Top 15
        c.execute("""
            SELECT d.channel_title, d.estimated_age_group, d.content_style,
                   d.video_count, d.total_views, d.ace_video_count, d.ace_view_count
            FROM youtube_channel_demographics d
            WHERE d.total_views > 0
            ORDER BY d.total_views DESC LIMIT 15
        """)
        rows = c.fetchall()
        if rows:
            lines.append("### 유튜브 채널 영향력 Top 15 (ETF 콘텐츠)")
            lines.append("")
            lines.append("| 채널 | 추정 시청 연령 | 콘텐츠 스타일 | 영상 수 | 총 조회수 | ACE 영상 | ACE 조회수 |")
            lines.append("|------|--------------|-------------|---------|----------|---------|----------|")
            for r in rows:
                ch = r[0] or "?"
                age = r[1] or "?"
                style = (r[2] or "?")[:20]
                vcnt = r[3] or 0
                views = r[4] or 0
                ace_v = r[5] or 0
                ace_vw = r[6] or 0
                lines.append(f"| {ch[:20]} | {age} | {style} | {vcnt} | {views:,} | {ace_v} | {ace_vw:,} |")
            lines.append("")

        # 키워드 관련 유튜브 영상 최신 10건
        if keywords:
            cond, params = _build_ilike_params("v.title", keywords[:3], "yt")
            c.execute(f"""
                SELECT v.title, v.channel_title, v.view_count,
                       v.comment_count, v.sentiment_label, v.published_at
                FROM youtube_videos v
                WHERE ({cond})
                ORDER BY v.published_at DESC LIMIT 10
            """, params)
            vrows = c.fetchall()
            if vrows:
                lines.append("### 테마 관련 최신 유튜브 영상")
                lines.append("")
                for r in vrows:
                    title = (r[0] or "")[:60]
                    ch = r[1] or "?"
                    views = r[2] or 0
                    comments = r[3] or 0
                    sent = r[4] or "?"
                    date = (r[5] or "")[:10]
                    lines.append(f"- **{title}** ({ch}, {date})")
                    lines.append(f"  조회 {views:,}, 댓글 {comments}, 감성: {sent}")
                lines.append("")

    except Exception as e:
        print(f"  유튜브 채널 데이터 추출 실패: {e}")

    conn.close()
    return "\n".join(lines)


def get_competing_etf_details(keywords: List[str]) -> str:
    """경쟁 ETF 상세 비교표 (보수, AUM, 보유종목)"""
    conn = _get_db_conn()
    if not conn:
        return ""
    c = conn.cursor()
    lines = []

    try:
        cond, params = _build_ilike_params("e.name", keywords[:5], "ce")
        c.execute(f"""
            SELECT e.ticker, e.name, e.category, e.brand,
                   f.total_expense_ratio,
                   (SELECT aum FROM etf_aum WHERE ticker = e.ticker
                    ORDER BY date DESC LIMIT 1) as latest_aum
            FROM etf_registry e
            LEFT JOIN etf_fee f ON e.ticker = f.ticker
            WHERE ({cond})
            ORDER BY latest_aum DESC NULLS LAST
            LIMIT 15
        """, params)
        rows = c.fetchall()

        if rows:
            lines.append("### 경쟁/유사 ETF 비교표")
            lines.append("")
            lines.append("| ETF명 | 브랜드 | 카테고리 | 총보수 | AUM(억) |")
            lines.append("|-------|--------|---------|--------|---------|")
            for r in rows:
                name = r[1] or "?"
                brand = r[3] or "?"
                cat = r[2] or "?"
                fee = f"{r[4]:.2f}%" if r[4] else "N/A"
                aum = f"{r[5]/1e8:,.0f}" if r[5] else "N/A"
                lines.append(f"| {name[:30]} | {brand} | {cat} | {fee} | {aum} |")
            lines.append("")

            # 상위 3개 ETF의 보유종목
            for ticker, name, *_ in rows[:3]:
                c.execute("""
                    SELECT stock_name, weight FROM etf_holdings
                    WHERE ticker = %(tk)s
                    ORDER BY date DESC, rank ASC LIMIT 10
                """, {"tk": ticker})
                hrows = c.fetchall()
                if hrows:
                    holding_str = ", ".join(
                        f"{h[0]}({h[1]:.1f}%)" if h[1] else h[0]
                        for h in hrows
                    )
                    lines.append(f"- **{name}** 주요종목: {holding_str}")
            if rows[:3]:
                lines.append("")

    except Exception as e:
        print(f"  경쟁 ETF 데이터 추출 실패: {e}")

    conn.close()
    return "\n".join(lines)


def get_community_reactions(keywords: List[str]) -> str:
    """커뮤니티 실제 투자자 반응 샘플 (카페, 토스, 블로그)"""
    conn = _get_db_conn()
    if not conn:
        return ""
    c = conn.cursor()
    lines = []

    try:
        # 네이버 카페 반응
        cond, params = _build_ilike_params("title", keywords[:3], "cf")
        c.execute(f"""
            SELECT title, cafe_name, sentiment_label,
                   content, published_at
            FROM naver_cafe_posts
            WHERE ({cond})
            ORDER BY published_at DESC LIMIT 8
        """, params)
        rows = c.fetchall()
        if rows:
            lines.append("### 투자자 커뮤니티 반응 (네이버 카페)")
            lines.append("")
            for r in rows:
                title = (r[0] or "")[:60]
                cafe = r[1] or "?"
                sent = r[2] or "중립"
                snippet = (r[3] or "")[:150].replace("\n", " ")
                date = (r[4] or "")[:10]
                lines.append(f"- **{title}** ({cafe}, {date}, {sent})")
                if snippet:
                    lines.append(f"  > {snippet}")
            lines.append("")

        # 토스 커뮤니티 반응
        cond, params = _build_ilike_params("content", keywords[:3], "ts")
        c.execute(f"""
            SELECT title, content, sentiment_label,
                   like_count, reply_count, published_at
            FROM toss_community_posts
            WHERE ({cond})
            ORDER BY published_at DESC LIMIT 8
        """, params)
        rows = c.fetchall()
        if rows:
            lines.append("### 투자자 커뮤니티 반응 (토스증권)")
            lines.append("")
            for r in rows:
                title = (r[0] or "")[:60]
                snippet = (r[1] or "")[:150].replace("\n", " ")
                sent = r[2] or "중립"
                likes = r[3] or 0
                replies = r[4] or 0
                date = (r[5] or "")[:10]
                lines.append(f"- **{title}** ({date}, {sent}, 좋아요 {likes}, 댓글 {replies})")
                if snippet:
                    lines.append(f"  > {snippet}")
            lines.append("")

        # 블로그 투자 분석
        cond, params = _build_ilike_params("title", keywords[:3], "bg")
        c.execute(f"""
            SELECT title, blog_name, sentiment_label,
                   content, published_at
            FROM naver_blog_posts
            WHERE ({cond})
            ORDER BY published_at DESC LIMIT 5
        """, params)
        rows = c.fetchall()
        if rows:
            lines.append("### 블로그 투자 분석 (DB 수집)")
            lines.append("")
            for r in rows:
                title = (r[0] or "")[:60]
                blog = r[1] or "?"
                sent = r[2] or "중립"
                snippet = (r[3] or "")[:200].replace("\n", " ")
                date = (r[4] or "")[:10]
                lines.append(f"- **{title}** ({blog}, {date}, {sent})")
                if snippet:
                    lines.append(f"  > {snippet}")
            lines.append("")

    except Exception as e:
        print(f"  커뮤니티 반응 추출 실패: {e}")

    conn.close()
    return "\n".join(lines)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. LLM으로 시드 문서 + 프롬프트 자동 생성
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def llm_generate(system: str, user: str, max_tokens: int = 4096) -> str:
    """OpenAI API 호출"""
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.5,
        max_tokens=max_tokens,
    )
    return resp.choices[0].message.content.strip()


def generate_etf_concept(theme: str) -> Dict:
    """테마만 주면 LLM이 ETF 컨셉 자동 생성"""
    system = """너는 한국 ETF 상품 기획 전문가다.
주어진 투자 테마에 대해 신규 ETF 상품 컨셉을 설계해라.
반드시 JSON만 출력하라."""

    user = f"""투자 테마: {theme}

아래 키를 포함한 JSON을 생성해라:
1. etf_name: 상품명 (예: "ACE K-수출핵심TOP10산업액티브")
2. description: 상품 설명 (200자 이내)
3. holdings: 편입 예상 종목 배열 (5~10개)
4. category: 카테고리 (해외주식/국내주식/채권/원자재/기타)
5. fee: 총보수 (예: "0.45%")
6. target_investors: 타겟 투자자 설명
7. search_keywords_ko: 한국어 검색 키워드 배열 (투자자 관심도 파악용, 8~12개)
8. search_keywords_en: 영어 검색 키워드 배열 (4~6개)
9. competing_etfs: 경쟁/유사 ETF 배열
10. differentiation: 차별화 포인트"""

    result = llm_generate(system, user)
    # JSON 파싱
    try:
        # ```json ... ``` 블록 추출
        match = re.search(r"```(?:json)?\s*(.*?)\s*```", result, re.DOTALL)
        if match:
            return json.loads(match.group(1))
        return json.loads(result)
    except:
        print(f"  LLM JSON 파싱 실패, 기본값 사용")
        return {
            "etf_name": f"ACE {theme} ETF",
            "description": f"{theme} 관련 핵심 기업에 투자하는 ETF",
            "holdings": [],
            "search_keywords_ko": [theme, f"{theme} 투자", f"{theme} ETF", f"{theme} 관련주"],
            "search_keywords_en": [theme],
            "competing_etfs": [],
            "differentiation": "",
        }


def generate_analysis(theme: str, etf_concept: Dict, collected_data: Dict, db_context: str) -> str:
    """수집된 데이터를 기반으로 LLM이 투자자 관심도 분석"""
    # 수집 데이터 텍스트화
    data_text = ""

    if collected_data.get("news"):
        data_text += "### 최신 뉴스\n"
        for n in collected_data["news"][:15]:
            data_text += f"- {strip_html(n.get('title', ''))}\n"
            desc = strip_html(n.get("description", ""))
            if desc:
                data_text += f"  {desc[:200]}\n"
        data_text += "\n"

    if collected_data.get("blogs"):
        data_text += "### 블로그 게시글\n"
        for b in collected_data["blogs"][:15]:
            data_text += f"- {strip_html(b.get('title', ''))}\n"
            desc = strip_html(b.get("description", ""))
            if desc:
                data_text += f"  {desc[:150]}\n"
        data_text += "\n"

    if collected_data.get("cafe"):
        data_text += "### 카페/커뮤니티 게시글\n"
        for c in collected_data["cafe"][:10]:
            data_text += f"- {strip_html(c.get('title', ''))}\n"
        data_text += "\n"

    system = """너는 ETF 시장 분석가다.
수집된 온라인 데이터를 기반으로 특정 투자 테마에 대한
투자자 관심도와 시장 반응을 분석하는 리포트를 작성하라.
마크다운 형식으로, 투자자 관점에서 실질적인 인사이트를 제공하라."""

    user = f"""## 분석 대상
테마: {theme}
신규 ETF 컨셉: {etf_concept.get('etf_name', '')}
상품 설명: {etf_concept.get('description', '')}
편입 종목: {', '.join(etf_concept.get('holdings', []))}

## 수집된 온라인 데이터
{data_text}

## 기존 시장 데이터
{db_context[:3000]}

## 분석 요청
아래 항목을 포함한 분석 리포트를 작성해줘:

1. **테마 관심도 진단**: 이 테마에 대한 현재 온라인 관심 수준 (높음/중간/낮음)과 근거
2. **투자자 심리 분석**: 뉴스/블로그/커뮤니티에서 나타나는 투자자들의 태도와 감성
3. **핵심 관심 포인트**: 투자자들이 가장 관심 있어하는 하위 주제 3~5개
4. **잠재적 우려 사항**: 투자자들이 걱정하거나 부정적으로 보는 요소
5. **경쟁 환경**: 유사 테마의 기존 ETF나 투자 수단 현황
6. **수요 예측**: 이 테마의 ETF가 출시되면 투자자 반응 예상
7. **핵심 마케팅 메시지**: 이 ETF를 성공시키기 위한 3가지 핵심 메시지"""

    return llm_generate(system, user, max_tokens=3000)


def generate_simulation_prompt(theme: str, etf_concept: Dict) -> str:
    """MiroFish용 시뮬레이션 프롬프트 자동 생성"""
    system = """너는 MiroFish 소셜 시뮬레이션 시스템의 프롬프트 설계 전문가다.
신규 ETF 런칭 시뮬레이션을 위한 시뮬레이션 요구사항을 작성하라.

MiroFish는 다수의 AI 에이전트가 Twitter/Reddit 같은 소셜 플랫폼에서
상호작용하며 여론을 시뮬레이션하는 시스템이다.

프롬프트는 한국어로, 구체적이고 시뮬레이션 가능한 형태로 작성하라.
500자 이내로 작성하라."""

    user = f"""신규 ETF 정보:
- 상품명: {etf_concept.get('etf_name', '')}
- 설명: {etf_concept.get('description', '')}
- 편입종목: {', '.join(etf_concept.get('holdings', [])[:5])}
- 카테고리: {etf_concept.get('category', '')}
- 타겟 투자자: {etf_concept.get('target_investors', '')}
- 경쟁 ETF: {', '.join(etf_concept.get('competing_etfs', [])[:3])}
- 차별점: {etf_concept.get('differentiation', '')}

위 ETF가 신규 상장된다고 가정하고,
한국 ETF 시장의 다양한 참여자(개인투자자, 증권사, 유튜버, 커뮤니티 등)가
이 ETF에 어떻게 반응할지 시뮬레이션하기 위한 프롬프트를 작성해줘.

특히 다음을 포함해야 해:
1) 투자자들의 초기 반응과 매수 심리
2) 경쟁 ETF 대비 포지셔닝
3) 소셜미디어 확산 패턴
4) 성공/실패 시나리오"""

    return llm_generate(system, user, max_tokens=1000)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. 시드 문서 조합
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def build_seed_document(
    theme: str,
    etf_concept: Dict,
    collected_data: Dict,
    db_context: str,
    launch_cases: str,
    youtube_influence: str,
    competing_details: str,
    community_reactions: str,
    analysis: str,
    simulation_prompt: str,
    output_path: str,
):
    """최종 시드 문서 생성"""
    today = datetime.now().strftime("%Y-%m-%d")
    etf_name = etf_concept.get("etf_name", f"{theme} ETF")

    doc = []
    doc.append(f"# {theme} 테마 ETF 런칭 시뮬레이션 시드 문서")
    doc.append(f"생성일: {today}")
    doc.append(f"대상 ETF: {etf_name}")
    doc.append("")
    doc.append("---")
    doc.append("")

    # Part 1: 신규 ETF 상세 정보
    doc.append("## 1. 신규 ETF 상품 정보")
    doc.append("")
    doc.append(f"### 상품 개요")
    doc.append(f"- 상품명: {etf_name}")
    doc.append(f"- 상품 설명: {etf_concept.get('description', '')}")
    doc.append(f"- 카테고리: {etf_concept.get('category', '')}")
    doc.append(f"- 총보수: {etf_concept.get('fee', 'N/A')}")
    doc.append(f"- 타겟 투자자: {etf_concept.get('target_investors', '')}")
    doc.append(f"- 차별화 포인트: {etf_concept.get('differentiation', '')}")
    doc.append("")

    holdings = etf_concept.get("holdings", [])
    if holdings:
        doc.append("### 편입 예정 종목")
        for i, h in enumerate(holdings, 1):
            doc.append(f"{i}. {h}")
        doc.append("")

    competing = etf_concept.get("competing_etfs", [])
    if competing:
        doc.append("### 경쟁/유사 ETF")
        for c in competing:
            doc.append(f"- {c}")
        doc.append("")

    # Part 2: 테마 관심도 실시간 데이터
    doc.append("## 2. 테마 관심도 실시간 데이터 (온라인 수집)")
    doc.append("")

    if collected_data.get("news"):
        doc.append(f"### 최신 뉴스 ({len(collected_data['news'])}건)")
        for n in collected_data["news"][:20]:
            title = strip_html(n.get("title", ""))
            desc = strip_html(n.get("description", ""))
            doc.append(f"**{title}**")
            if desc:
                doc.append(desc[:300])
            doc.append("")

    if collected_data.get("blogs"):
        doc.append(f"### 블로그 투자 분석 ({len(collected_data['blogs'])}건)")
        for b in collected_data["blogs"][:20]:
            title = strip_html(b.get("title", ""))
            desc = strip_html(b.get("description", ""))
            doc.append(f"**{title}**")
            if desc:
                doc.append(desc[:300])
            doc.append("")

    if collected_data.get("cafe"):
        doc.append(f"### 커뮤니티 토론 ({len(collected_data['cafe'])}건)")
        for c in collected_data["cafe"][:10]:
            title = strip_html(c.get("title", ""))
            desc = strip_html(c.get("description", ""))
            doc.append(f"- {title}")
            if desc:
                doc.append(f"  {desc[:200]}")
        doc.append("")

    # Part 3: 기존 시장 데이터
    if db_context:
        doc.append("## 3. 기존 시장 데이터 (ETF Buzz Monitor)")
        doc.append("")
        doc.append(db_context)
        doc.append("")

    # Part 4: 과거 ETF 런칭 사례
    if launch_cases:
        doc.append("## 4. 과거 유사 ETF 런칭 사례")
        doc.append("")
        doc.append(launch_cases)
        doc.append("")

    # Part 5: 유튜브 채널 영향력
    if youtube_influence:
        doc.append("## 5. 유튜브 채널 영향력 데이터")
        doc.append("")
        doc.append(youtube_influence)
        doc.append("")

    # Part 6: 경쟁 ETF 상세 비교
    if competing_details:
        doc.append("## 6. 경쟁 ETF 상세 비교")
        doc.append("")
        doc.append(competing_details)
        doc.append("")

    # Part 7: 커뮤니티 실제 반응
    if community_reactions:
        doc.append("## 7. 투자자 커뮤니티 실제 반응")
        doc.append("")
        doc.append(community_reactions)
        doc.append("")

    # Part 8: AI 분석
    doc.append("## 8. 테마 투자자 관심도 AI 분석")
    doc.append("")
    doc.append(analysis)
    doc.append("")

    # 시드 문서 저장
    content = "\n".join(doc)
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)

    # 시뮬레이션 프롬프트 별도 저장
    prompt_path = output_path.replace(".md", "_prompt.txt")
    with open(prompt_path, "w", encoding="utf-8") as f:
        f.write(simulation_prompt)

    print(f"\n{'='*50}")
    print(f"생성 완료!")
    print(f"  시드 문서: {output_path} ({len(content):,} chars)")
    print(f"  시뮬레이션 프롬프트: {prompt_path}")
    print(f"{'='*50}")
    print(f"\nMiroFish 사용법:")
    print(f"  1. http://localhost:3000 접속")
    print(f"  2. 시드 문서 업로드: {os.path.basename(output_path)}")
    print(f"  3. 시뮬레이션 요구사항에 아래 프롬프트 붙여넣기:")
    print(f"\n--- 프롬프트 ---")
    print(simulation_prompt)
    print(f"--- 끝 ---")

    return output_path, prompt_path


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 메인
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    parser = argparse.ArgumentParser(
        description="신규 ETF 런칭 시뮬레이션용 시드 문서 + 프롬프트 자동 생성"
    )
    parser.add_argument("--theme", "-t", required=True,
                        help="투자 테마 (예: 'K-수출', '2차전지 리사이클링', '우주항공')")
    parser.add_argument("--etf-name", help="ETF 상품명 (미입력 시 LLM이 자동 생성)")
    parser.add_argument("--description", help="ETF 설명")
    parser.add_argument("--holdings", help="편입 종목 (쉼표 구분)")
    parser.add_argument("--keywords", help="추가 검색 키워드 (쉼표 구분)")
    parser.add_argument("--output", "-o", help="출력 파일 경로")
    args = parser.parse_args()

    theme = args.theme
    safe_theme = re.sub(r"[^a-zA-Z0-9가-힣_-]", "_", theme)
    output_path = args.output or f"./backend/uploads/seed_{safe_theme}_{datetime.now().strftime('%Y%m%d')}.md"

    print(f"\n{'='*60}")
    print(f"  신규 ETF 시드 문서 생성기")
    print(f"  테마: {theme}")
    print(f"{'='*60}")

    # Step 1: ETF 컨셉 설계
    if args.etf_name:
        etf_concept = {
            "etf_name": args.etf_name,
            "description": args.description or f"{theme} 관련 핵심 기업 투자 ETF",
            "holdings": args.holdings.split(",") if args.holdings else [],
            "search_keywords_ko": args.keywords.split(",") if args.keywords else [theme],
            "search_keywords_en": [],
            "competing_etfs": [],
        }
    else:
        print("\n[Step 1/8] LLM으로 ETF 컨셉 자동 설계 중...")
        etf_concept = generate_etf_concept(theme)
        print(f"  → {etf_concept.get('etf_name', '?')}")

    # 검색 키워드 결정
    keywords = etf_concept.get("search_keywords_ko", [theme])
    if args.keywords:
        keywords = args.keywords.split(",") + keywords
    keywords = list(dict.fromkeys(keywords))  # 중복 제거, 순서 유지

    # Step 2: 웹 데이터 수집
    print("\n[Step 2/8] 온라인 데이터 수집 중...")
    collected_data = collect_theme_data(theme, keywords[:8])

    # Step 3: 기존 DB 데이터 추출
    print("\n[Step 3/8] 기존 DB에서 관련 데이터 추출 중...")
    db_context = get_existing_market_context(theme, keywords[:5])

    # Step 4: 과거 ETF 런칭 사례
    print("\n[Step 4/8] 과거 유사 ETF 런칭 사례 추출 중...")
    launch_cases = get_etf_launch_cases(keywords[:5])

    # Step 5: 유튜브 채널 영향력
    print("\n[Step 5/8] 유튜브 채널 영향력 데이터 추출 중...")
    youtube_influence = get_youtube_channel_influence(keywords[:5])

    # Step 6: 경쟁 ETF 상세 비교
    print("\n[Step 6/8] 경쟁 ETF 상세 비교 데이터 추출 중...")
    competing_details = get_competing_etf_details(keywords[:5])

    # Step 6.5: 커뮤니티 실제 반응
    print("\n[Step 6.5/8] 투자자 커뮤니티 반응 추출 중...")
    community_reactions = get_community_reactions(keywords[:5])

    # Step 7: AI 분석
    print("\n[Step 7/8] AI 투자자 관심도 분석 중...")
    analysis = generate_analysis(theme, etf_concept, collected_data, db_context)

    # Step 8: 시뮬레이션 프롬프트 생성
    print("\n[Step 8/8] MiroFish 시뮬레이션 프롬프트 생성 중...")
    simulation_prompt = generate_simulation_prompt(theme, etf_concept)

    # 최종 문서 생성
    build_seed_document(
        theme=theme,
        etf_concept=etf_concept,
        collected_data=collected_data,
        db_context=db_context,
        launch_cases=launch_cases,
        youtube_influence=youtube_influence,
        competing_details=competing_details,
        community_reactions=community_reactions,
        analysis=analysis,
        simulation_prompt=simulation_prompt,
        output_path=output_path,
    )


if __name__ == "__main__":
    main()
