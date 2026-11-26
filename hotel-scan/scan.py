#!/usr/bin/env python3
"""广东酒店源扫描：只找翡翠台 (txiptv /iptv/live/1000.json?key=txiptv)。

流程：种子 -> /24 扩展 -> 扫活 IP -> 活 IP 取 JSON -> 只留名字含「翡翠台」的频道
（排除「华丽翡翠台」）-> 结果写 candidates.json。

跑在 GitHub Actions 上（runner 直连网络）；种子见 seeds.txt（2026-11-26 收集）。
"""

import concurrent.futures
import json
import re
import threading
import time
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent
SEEDS_FILE = BASE_DIR / "seeds.txt"
OUTPUT_FILE = BASE_DIR / "results" / "candidates.json"

TIMEOUT = 1.0            # IP 扫描超时（秒）
JSON_TIMEOUT = 3.0       # 频道 JSON 拉取超时（秒）
MAX_WORKERS = 500
RETRY_TIMES = 1
RETRY_INTERVAL = 0.02
IP_SEGMENT_RANGE = range(1, 256)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Connection": "keep-alive",
}

TARGET = "翡翠台"       # 目标频道名（子串匹配）
EXCLUDE = "华丽翡翠台"  # 排除：不是正主

session = requests.Session()
session.headers.update(HEADERS)

scan_counter = 0
valid_counter = 0
lock = threading.Lock()


def request_with_retry(url, timeout=TIMEOUT, **kwargs):
    kwargs.setdefault("headers", HEADERS)
    kwargs.setdefault("timeout", timeout)
    for _ in range(RETRY_TIMES + 1):
        try:
            resp = session.request("GET", url, **kwargs)
            if resp.status_code in (200, 302):
                return resp
        except requests.exceptions.RequestException:
            time.sleep(RETRY_INTERVAL)
    return None


def load_seeds():
    seeds = []
    for line in SEEDS_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r"^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d+)$", line)
        if not m:
            print(f"【种子跳过】格式不对：{line}")
            continue
        seeds.append(line)
    return sorted(set(seeds))


def expand_seed(seed):
    """一个种子 -> 同 /24 的 255 个探测地址。"""
    ip, port = seed.split(":")
    prefix = ".".join(ip.split(".")[:3])
    return [f"http://{prefix}.{i}:{port}/iptv/live/1000.json?key=txiptv"
            for i in IP_SEGMENT_RANGE]


def is_url_accessible(url):
    global scan_counter, valid_counter
    resp = request_with_retry(url, timeout=TIMEOUT)
    with lock:
        scan_counter += 1
        if scan_counter % 1000 == 0:
            print(f"【IP扫描进度】已完成 {scan_counter} 个，当前可用 {valid_counter} 个")
    if resp is not None:
        with lock:
            valid_counter += 1
        return url
    return None


def extract_jade(url):
    """活 IP 取一次 JSON，只留名字含翡翠台（排除华丽翡翠台）的频道。"""
    resp = request_with_retry(url, timeout=JSON_TIMEOUT)
    if resp is None:
        return []
    try:
        data = resp.json()
    except ValueError:
        return []
    if data.get("count", 0) == 0:
        return []
    # 频道 URL 拼装用的基址：http://IP:端口
    proto_end = url.find("://") + 3
    slash = url.find("/", proto_end)
    base = url[:slash] if slash != -1 else url
    out = []
    for item in data.get("data", []):
        if not isinstance(item, dict):
            continue
        name = item.get("name", "")
        urlx = item.get("url", "")
        if not name or not urlx:
            continue
        if TARGET not in name or EXCLUDE in name:
            continue
        chid = item.get("chid", "")
        srcid = item.get("srcid", "")
        if urlx.startswith("http"):
            final_url = urlx.strip()
        elif urlx.startswith("udp"):
            final_url = (f"{base}/tsfile/live/{chid}_{srcid}.m3u8"
                         f"?key=txiptv&playlive=1&down=1")
        else:
            final_url = f"{base}{urlx}".strip()
        out.append({
            "name": name,
            "url": final_url,
            "chid": chid,
            "srcid": srcid,
            "source_ip": base,
            "json_url": url,
        })
    return out


def main():
    seeds = load_seeds()
    print(f"【种子】{len(seeds)} 个")
    tasks = []
    for seed in seeds:
        tasks.extend(expand_seed(seed))
    print(f"【扫描任务】{len(tasks)} 个（{len(seeds)} 种子 x 255）")

    t0 = time.time()
    valid_urls = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for result in ex.map(is_url_accessible, tasks):
            if result:
                valid_urls.append(result)
    print(f"【IP扫描完成】耗时 {time.time() - t0:.1f}s | 可用IP {len(valid_urls)} 个")

    candidates = []
    seen = set()
    for idx, url in enumerate(valid_urls, 1):
        if idx % 10 == 0:
            print(f"【频道提取】{idx}/{len(valid_urls)}，已命中 {len(candidates)} 条")
        for c in extract_jade(url):
            if c["url"] in seen:
                continue
            seen.add(c["url"])
            candidates.append(c)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(
        json.dumps(candidates, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"【完成】翡翠台候选 {len(candidates)} 条 -> {OUTPUT_FILE}")
    for c in candidates:
        print(f"  {c['name']} | {c['url']}")


if __name__ == "__main__":
    main()
