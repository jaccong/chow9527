#!/usr/bin/env python3
"""广东酒店源扫描：只找翡翠台 (txiptv /iptv/live/1000.json?key=txiptv)。

流程：种子 -> /24 扩展 -> 扫活 IP -> 活 IP 取 JSON -> 只留名字含「翡翠台」的频道
（排除「华丽翡翠台」）-> 结果写 candidates.json。

跑在 GitHub Actions 上（runner 直连网络）。种子两路合并：
本地 seeds.txt + 远程 https://877622.xyz/ip/ips.txt（每次跑现拉，拉不到就只用本地）。
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
STATS_FILE = BASE_DIR / "results" / "scan_stats.json"

IPS_TXT_URL = "https://877622.xyz/ip/ips.txt"
REMOTE_SEED_TIMEOUT = 15   # 拉 ips.txt 超时（秒）；失败不致命，只用本地种子

TIMEOUT = 2.5            # IP 扫描超时（秒；海外 runner 到国内酒店网关 RTT 大，1.0s 会漏活口）
JSON_TIMEOUT = 6.0       # 频道 JSON 拉取超时（秒）
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

# 过程统计：跑完写进 scan_stats.json 一起提交回仓库，没扫到也能定位卡在哪一步。
stats = {
    "seeds": 0,
    "seeds_local": 0,
    "seeds_remote": 0,
    "tasks": 0,
    "valid_ips": 0,
    "valid_by_seed": {},
    "json_ok": 0,
    "json_fail": 0,
    "json_parse_fail": 0,
    "items_seen": 0,
    "jade_hits": 0,
    "duration_s": 0,
}


def seed_key_of(url):
    """从探测 URL 反推它属于哪个种子（/24 + 端口），用于按种子统计可用数。"""
    host = url.split("://", 1)[1].split("/", 1)[0]
    ip, _, port = host.partition(":")
    prefix = ".".join(ip.split(".")[:3]) + ".0/24"
    return f"{prefix}:{port}"


def bump(key, n=1):
    with lock:
        stats[key] += n


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


SEED_RE = re.compile(r"^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d+)$")


def load_remote_seeds():
    """现拉 https://877622.xyz/ip/ips.txt，提 IP:端口。失败返回空集（不致命）。"""
    try:
        resp = session.get(IPS_TXT_URL, timeout=REMOTE_SEED_TIMEOUT)
        if resp.status_code != 200:
            print(f"【远程种子】拉取失败：HTTP {resp.status_code}，只用本地种子")
            return set()
        found = {m.group(0) for m in re.finditer(
            r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+", resp.text)}
        print(f"【远程种子】{IPS_TXT_URL} -> {len(found)} 个")
        return found
    except requests.exceptions.RequestException as e:
        print(f"【远程种子】拉取异常（{e}），只用本地种子")
        return set()


def load_seeds():
    local = set()
    for line in SEEDS_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        if not SEED_RE.match(line):
            print(f"【种子跳过】格式不对：{line}")
            continue
        local.add(line)
    remote = load_remote_seeds()
    stats["seeds_local"] = len(local)
    stats["seeds_remote"] = len(remote)
    return sorted(local | remote)


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
            key = seed_key_of(url)
            stats["valid_by_seed"][key] = stats["valid_by_seed"].get(key, 0) + 1
        return url
    return None


def extract_jade(url):
    """活 IP 取一次 JSON，只留名字含翡翠台（排除华丽翡翠台）的频道。"""
    resp = request_with_retry(url, timeout=JSON_TIMEOUT)
    if resp is None:
        bump("json_fail")
        return []
    try:
        data = resp.json()
    except ValueError:
        bump("json_parse_fail")
        return []
    bump("json_ok")
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
        bump("items_seen")
        name = item.get("name", "")
        urlx = item.get("url", "")
        if not name or not urlx:
            continue
        if TARGET not in name or EXCLUDE in name:
            continue
        bump("jade_hits")
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
    print(f"【种子】{len(seeds)} 个（本地 {stats['seeds_local']} + 远程 {stats['seeds_remote']}）")
    tasks = []
    for seed in seeds:
        tasks.extend(expand_seed(seed))
    tasks = sorted(set(tasks))  # 多个种子同 /24 同端口会扩出重复地址，去重
    print(f"【扫描任务】{len(tasks)} 个（去重后）")

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

    stats["seeds"] = len(seeds)
    stats["tasks"] = len(tasks)
    stats["valid_ips"] = len(valid_urls)
    stats["duration_s"] = round(time.time() - t0, 1)
    stats["timeouts"] = {"probe_s": TIMEOUT, "json_s": JSON_TIMEOUT}
    STATS_FILE.write_text(
        json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"【统计】{json.dumps(stats, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
