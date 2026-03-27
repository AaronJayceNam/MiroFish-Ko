"""
K-휴머노이드 ETF 편입종목 네이버 종목토론방 + 토스 커뮤니티 수집 & 분석
"""
import re
import time
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# ── 편입 종목 ──
HOLDINGS = {
    "005380": ("현대차", 20.0),
    "108860": ("로보티즈", 20.0),
    "277810": ("레인보우로보틱스", 17.0),
    "058610": ("에스피지", 12.2),
    "454910": ("두산로보틱스", 2.5),
    "012330": ("현대모비스", 4.4),
}

NAVER_PAGES = 5
TOSS_PAGES = 3

# ── 네이버 종목토론실 수집 ──
def scrape_naver_discussion(ticker: str, name: str, pages: int = 5):
    """네이버 금융 종목토론실에서 게시글 수집"""
    session = requests.Session()
    session.verify = False
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Referer": "https://finance.naver.com/",
    })

    posts = []
    auto_alert = re.compile(r"\d+\s*%\s*이상\s*(상승했어요|하락했어요)")

    for page in range(1, pages + 1):
        url = f"https://finance.naver.com/item/board.naver?code={ticker}&page={page}"
        try:
            resp = session.get(url, timeout=10)
            resp.encoding = resp.apparent_encoding or "utf-8"
            soup = BeautifulSoup(resp.text, "html.parser")

            rows = soup.select("table.type2 tr")
            for row in rows:
                cells = row.select("td")
                if len(cells) < 6:
                    continue
                title_tag = cells[1].select_one("a")
                if not title_tag:
                    continue
                title = title_tag.get_text(strip=True)
                if auto_alert.search(title):
                    continue

                date_text = cells[0].get_text(strip=True)
                views = cells[3].get_text(strip=True).replace(",", "")
                pos = cells[4].get_text(strip=True).replace(",", "")
                neg = cells[5].get_text(strip=True).replace(",", "")

                posts.append({
                    "title": title,
                    "date": date_text,
                    "views": int(views) if views.isdigit() else 0,
                    "pos": int(pos) if pos.isdigit() else 0,
                    "neg": int(neg) if neg.isdigit() else 0,
                })
            time.sleep(0.5)
        except Exception as e:
            print(f"  [네이버] {name}({ticker}) page {page} 에러: {e}")

    print(f"  [네이버] {name}({ticker}): {len(posts)}건 수집")
    return posts


# ── 토스 커뮤니티 수집 ──
def _isin_check_digit(isin_no_check: str) -> int:
    digits = ""
    for c in isin_no_check:
        if c.isdigit():
            digits += c
        else:
            digits += str(ord(c) - 55)
    total = 0
    for i, d in enumerate(reversed(digits)):
        n = int(d)
        if i % 2 == 0:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return (10 - (total % 10)) % 10


def ticker_to_isin(ticker: str) -> str:
    ticker = ticker.lstrip("A").zfill(6)
    nsin = f"7{ticker}00"
    isin_no_check = f"KR{nsin}"
    check = _isin_check_digit(isin_no_check)
    return f"{isin_no_check}{check}"


def scrape_toss_community(ticker: str, name: str, pages: int = 3):
    """토스증권 종토방에서 커뮤니티 글 수집"""
    isin = ticker_to_isin(ticker)
    posts = []

    headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)",
        "Accept": "application/json",
    }

    hosts = [
        "https://wts-cert-api.tossinvest.com",
        "https://wts-info-api.tossinvest.com",
    ]

    for host in hosts:
        last_comment_id = None
        try:
            for page in range(pages):
                params = {
                    "subjectType": "STOCK",
                    "subjectId": isin,
                    "commentSortType": "POPULAR",
                    "size": 20,
                }
                if last_comment_id:
                    params["lastCommentId"] = last_comment_id

                resp = requests.get(
                    f"{host}/api/v4/comments",
                    params=params,
                    headers=headers,
                    timeout=10,
                )
                if resp.status_code != 200:
                    break

                data = resp.json()
                comments = data if isinstance(data, list) else data.get("contents", data.get("data", []))
                if not comments:
                    break

                for c in comments:
                    body = c.get("message", c.get("body", c.get("content", "")))
                    likes = c.get("likeCount", c.get("sympathyCount", 0))
                    created = c.get("createdAt", c.get("registerDate", ""))
                    posts.append({
                        "body": body[:300] if body else "",
                        "likes": likes or 0,
                        "created": str(created)[:19] if created else "",
                    })

                # pagination
                if isinstance(comments, list) and comments:
                    last_id = comments[-1].get("commentId", comments[-1].get("id"))
                    if last_id:
                        last_comment_id = last_id
                    else:
                        break
                else:
                    break

                time.sleep(0.5)

            if posts:
                break  # success with this host
        except Exception as e:
            print(f"  [토스] {name}({ticker}) {host} 에러: {e}")
            continue

    print(f"  [토스] {name}({ticker}): {len(posts)}건 수집")
    return posts


# ── 분석 ──
def analyze_posts(naver_posts, toss_posts, name):
    """수집된 게시글 분석"""
    result = {"name": name, "naver": {}, "toss": {}}

    # 네이버 분석
    if naver_posts:
        total = len(naver_posts)
        avg_views = sum(p["views"] for p in naver_posts) / total if total else 0
        total_pos = sum(p["pos"] for p in naver_posts)
        total_neg = sum(p["neg"] for p in naver_posts)
        sentiment_ratio = total_pos / (total_pos + total_neg) if (total_pos + total_neg) > 0 else 0.5

        # 인기글 (조회수 상위 5)
        top_by_views = sorted(naver_posts, key=lambda x: x["views"], reverse=True)[:5]
        # 긍정 반응 높은 글
        top_by_pos = sorted(naver_posts, key=lambda x: x["pos"], reverse=True)[:3]

        result["naver"] = {
            "total_posts": total,
            "avg_views": round(avg_views),
            "total_pos": total_pos,
            "total_neg": total_neg,
            "sentiment_ratio": round(sentiment_ratio, 2),
            "sentiment_label": "긍정 우위" if sentiment_ratio > 0.6 else "부정 우위" if sentiment_ratio < 0.4 else "중립",
            "top_by_views": [{"title": p["title"], "views": p["views"], "pos": p["pos"], "neg": p["neg"]} for p in top_by_views],
            "top_by_pos": [{"title": p["title"], "views": p["views"], "pos": p["pos"]} for p in top_by_pos],
        }

    # 토스 분석
    if toss_posts:
        total = len(toss_posts)
        avg_likes = sum(p["likes"] for p in toss_posts) / total if total else 0
        top_by_likes = sorted(toss_posts, key=lambda x: x["likes"], reverse=True)[:5]

        result["toss"] = {
            "total_posts": total,
            "avg_likes": round(avg_likes, 1),
            "top_by_likes": [{"body": p["body"][:150], "likes": p["likes"]} for p in top_by_likes],
        }

    return result


# ── Main ──
if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore")

    all_results = {}

    for ticker, (name, weight) in HOLDINGS.items():
        print(f"\n{'='*50}")
        print(f"수집 중: {name} ({ticker}) — 비중 {weight}%")
        print(f"{'='*50}")

        naver = scrape_naver_discussion(ticker, name, NAVER_PAGES)
        toss = scrape_toss_community(ticker, name, TOSS_PAGES)
        analysis = analyze_posts(naver, toss, name)
        analysis["weight"] = weight
        analysis["ticker"] = ticker
        all_results[name] = analysis

    # 결과 저장
    output_path = "/Users/yongsoonam/project/MiroFish-Ko/prompts/k휴머노이드_종목토론_분석.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    print(f"\n✅ 분석 결과 저장: {output_path}")

    # 마크다운 요약 출력
    print("\n" + "="*60)
    print("📊 종목토론방 분석 요약")
    print("="*60)
    for name, data in all_results.items():
        n = data.get("naver", {})
        t = data.get("toss", {})
        print(f"\n### {name} ({data['ticker']}) — 비중 {data['weight']}%")
        if n:
            print(f"  [네이버] {n['total_posts']}건 | 평균 조회 {n['avg_views']} | 감성: {n['sentiment_label']} ({n['sentiment_ratio']})")
            print(f"  인기글:")
            for p in n.get("top_by_views", [])[:3]:
                print(f"    - {p['title'][:50]}... (조회 {p['views']}, 👍{p['pos']} 👎{p['neg']})")
        if t:
            print(f"  [토스] {t['total_posts']}건 | 평균 좋아요 {t['avg_likes']}")
            for p in t.get("top_by_likes", [])[:3]:
                print(f"    - \"{p['body'][:60]}...\" (좋아요 {p['likes']})")
