#!/usr/bin/env python3
"""广东酒店源扫描：只找翡翠台 (txiptv /iptv/live/1000.json?key=txiptv)。

流程：种子 -> /24 扩展 -> 扫活 IP -> 活 IP 取 JSON -> 只留名字含「翡翠台」的频道
（排除「华丽翡翠台」）-> 结果写 candidates.json。

跑在 GitHub Actions 上（runner 直连网络）。种子三路合并：
本地 seeds.txt + 远程 https://877622.xyz/ip/ips.txt（每次跑现拉，拉不到就只用本地）
+ ip-library.json 里的已知网关（T1/T2 扩 /24，T3 只探本机）。
跑完把结果写回 ip-library.json：活着的更新 last_alive/频道/分级，死的计 fail_streak
（连死 3 次进 archived），新发现的带翡翠台网关加为 T1。
"""

import concurrent.futures
import json
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent
SEEDS_FILE = BASE_DIR / "seeds.txt"
LIBRARY_FILE = BASE_DIR / "ip-library.json"
OUTPUT_FILE = BASE_DIR / "results" / "candidates.json"
STATS_FILE = BASE_DIR / "results" / "scan_stats.json"

IPS_TXT_URL = "https://877622.xyz/ip/ips.txt"
REMOTE_SEED_TIMEOUT = 15   # 拉 ips.txt 超时（秒）；失败不致命，只用本地种子

TIMEOUT = 2.5            # IP 扫描超时（秒；海外 runner 到国内酒店网关 RTT 大，1.0s 会漏活口）
JSON_TIMEOUT = 6.0       # 频道 JSON 拉取超时（秒）
MAX_WORKERS = 500
RETRY_TIMES = 1
ARCHIVE_AFTER_FAILS = 3  # 连死几次进 archived
MAX_CHANNELS_STORED = 100  # 库里每个网关最多记多少个频道名
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
    "library_seeds": 0,
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


# ---------------- IP 库 ----------------

TIER_ORDER = {"T1": 0, "T2": 1, "T3": 2, "archived": 3}


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def gateway_url(ip, port):
    return f"http://{ip}:{port}/iptv/live/1000.json?key=txiptv"


def load_library():
    """读 ip-library.json；没有或坏了就返回空库（不致命，照常用种子扫）。"""
    if not LIBRARY_FILE.exists():
        print("【IP库】无 ip-library.json，只用种子扫")
        return {"version": 1, "gateways": []}
    try:
        lib = json.loads(LIBRARY_FILE.read_text(encoding="utf-8"))
        rows = lib.get("gateways", [])
        print(f"【IP库】读入 {len(rows)} 行 "
              f"(T1 {sum(1 for r in rows if r.get('tier') == 'T1')} / "
              f"T2 {sum(1 for r in rows if r.get('tier') == 'T2')} / "
              f"T3 {sum(1 for r in rows if r.get('tier') == 'T3')} / "
              f"archived {sum(1 for r in rows if r.get('tier') == 'archived')})")
        return lib
    except (ValueError, OSError) as e:
        print(f"【IP库】读库失败（{e}），只用种子扫")
        return {"version": 1, "gateways": []}


def library_probe_tasks(lib):
    """库里的网关转成探测任务：T1/T2 扩 /24，T3 只探本机，archived 跳过。"""
    tasks = []
    for row in lib.get("gateways", []):
        tier = row.get("tier")
        if tier in ("T1", "T2"):
            tasks.extend(expand_seed(f"{row['ip']}:{row['port']}"))
        elif tier == "T3":
            tasks.append(gateway_url(row["ip"], row["port"]))
    stats["library_seeds"] = len(lib.get("gateways", []))
    return tasks


def build_stream_url(base, item):
    """把 JSON 里一条频道拼成最终流地址（与 extract_jade 同一套规则）。"""
    urlx = item.get("url", "")
    chid = item.get("chid", "")
    srcid = item.get("srcid", "")
    if urlx.startswith("http"):
        return urlx.strip()
    if urlx.startswith("udp"):
        return (f"{base}/tsfile/live/{chid}_{srcid}.m3u8"
                f"?key=txiptv&playlive=1&down=1")
    return f"{base}{urlx}".strip()


def classify_gateway(row, valid_set):
    """给库里一个网关定级：活着就拉一次 JSON 记频道、重算分级；死了计 fail_streak。"""
    url = gateway_url(row["ip"], row["port"])
    if url not in valid_set:
        row["alive_streak"] = 0
        row["fail_streak"] = row.get("fail_streak", 0) + 1
        if row["fail_streak"] >= ARCHIVE_AFTER_FAILS and row.get("tier") != "archived":
            row["tier"] = "archived"
            print(f"【IP库】{row['ip']}:{row['port']} 连死 {row['fail_streak']} 次 -> archived")
        return "dead"
    # 活着：拉 JSON 记频道
    row["last_alive"] = now_iso()
    row["alive_streak"] = row.get("alive_streak", 0) + 1
    row["fail_streak"] = 0
    resp = request_with_retry(url, timeout=JSON_TIMEOUT)
    if resp is None:
        print(f"【IP库】{row['ip']}:{row['port']} 探活但 JSON 拉不到，频道/分级保持原样")
        return "alive_nojson"
    try:
        data = resp.json()
    except ValueError:
        print(f"【IP库】{row['ip']}:{row['port']} JSON 解析失败，频道/分级保持原样")
        return "alive_nojson"
    names = []
    jade_streams = []
    base = f"http://{row['ip']}:{row['port']}"
    for item in data.get("data", []):
        if not isinstance(item, dict):
            continue
        name = item.get("name", "")
        if not name:
            continue
        if name not in names:
            names.append(name)
        if TARGET in name and EXCLUDE not in name:
            jade_streams.append(build_stream_url(base, item))
    row["channels"] = names[:MAX_CHANNELS_STORED]
    row["jade_streams"] = jade_streams
    has_gd = any("广东" in n for n in names)
    old_tier = row.get("tier")
    if jade_streams:
        row["tier"] = "T1"
    elif has_gd:
        row["tier"] = "T2"
    else:
        row["tier"] = "T3"
    if row["tier"] != old_tier:
        print(f"【IP库】{row['ip']}:{row['port']} {old_tier} -> {row['tier']} "
              f"（频道 {len(names)} 个，翡翠台 {len(jade_streams)} 条）")
    return "alive"


def update_library(lib, valid_set, candidates):
    """跑完写回库：已知行定级/衰减 + 新发现的带翡翠台网关加为 T1。"""
    rows = lib.get("gateways", [])
    known = {(r["ip"], r["port"]) for r in rows}
    counts = {"alive": 0, "alive_nojson": 0, "dead": 0}
    for row in rows:
        counts[classify_gateway(row, valid_set)] += 1
    # 新 T1：候选的 source_ip 不在库里 -> 加行（JSON 列出，未做 HEAD 验流）
    new_t1 = []
    for c in candidates:
        base = c.get("source_ip", "")
        m = re.match(r"http://([^/:]+)(?::(\d+))?$", base)
        if not m:
            continue
        key = (m.group(1), int(m.group(2) or 9901))
        if key in known:
            continue
        known.add(key)
        new_t1.append({
            "ip": key[0], "port": key[1], "tier": "T1", "region": "广东",
            "first_seen": now_iso()[:10], "source": "scan",
            "last_alive": now_iso(), "alive_streak": 1, "fail_streak": 0,
            "channels": ["翡翠台"], "jade_streams": [c["url"]],
            "stream_verified": False,
            "verified_by": "scan.py：JSON 列出翡翠台，未做 HEAD 验流",
        })
        print(f"【IP库】新 T1：{key[0]}:{key[1]}（{c['name']}）")
    rows.extend(new_t1)
    rows.sort(key=lambda r: (TIER_ORDER.get(r.get("tier"), 9), r["ip"], r["port"]))
    lib["gateways"] = rows
    lib["updated"] = now_iso()
    lib.setdefault("version", 1)
    try:
        LIBRARY_FILE.write_text(
            json.dumps(lib, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"【IP库】写回 {len(rows)} 行（新 T1 {len(new_t1)} 个，"
              f"活 {counts['alive']} / 活但无JSON {counts['alive_nojson']} / 死 {counts['dead']}）")
    except OSError as e:
        print(f"【IP库】写回失败（{e}），结果文件不受影响")
    return {
        "rows": len(rows),
        "t1": sum(1 for r in rows if r.get("tier") == "T1"),
        "t2": sum(1 for r in rows if r.get("tier") == "T2"),
        "t3": sum(1 for r in rows if r.get("tier") == "T3"),
        "archived": sum(1 for r in rows if r.get("tier") == "archived"),
        "new_t1": [f"{r['ip']}:{r['port']}" for r in new_t1],
        "alive": counts["alive"],
        "dead": counts["dead"],
    }


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
        out.append({
            "name": name,
            "url": build_stream_url(base, item),
            "chid": chid,
            "srcid": srcid,
            "source_ip": base,
            "json_url": url,
        })
    return out


def main():
    lib = load_library()
    seeds = load_seeds()
    print(f"【种子】{len(seeds)} 个（本地 {stats['seeds_local']} + 远程 {stats['seeds_remote']}）")
    tasks = []
    for seed in seeds:
        tasks.extend(expand_seed(seed))
    tasks.extend(library_probe_tasks(lib))
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

    # 写回 IP 库（不影响上面的结果文件）
    lib_stats = update_library(lib, set(valid_urls), candidates)

    stats["seeds"] = len(seeds)
    stats["tasks"] = len(tasks)
    stats["valid_ips"] = len(valid_urls)
    stats["duration_s"] = round(time.time() - t0, 1)
    stats["timeouts"] = {"probe_s": TIMEOUT, "json_s": JSON_TIMEOUT}
    stats["library"] = lib_stats
    # 匹配度 4 个数（库的目标就是让它们涨）
    alive = len(valid_urls)
    stats["metrics"] = {
        "alive_rate": round(alive / len(tasks), 4) if tasks else 0,
        "jade_rate": round(lib_stats["t1"] / alive, 4) if alive else 0,
        "gd_coverage": None,  # 下轮：从库里 T1+T2 占比算
        "stream_ok_rate": None,  # scan.py 只验 JSON 列表，不验流；test.py 那套才有
    }
    STATS_FILE.write_text(
        json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"【统计】{json.dumps(stats, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
