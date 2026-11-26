#!/usr/bin/env python3
"""对 candidates.json 里的翡翠台候选拉流抽帧，供人工看台标判断。

每条候选录 9 秒，抽第 2/5/8 秒三帧（缩到 960 宽），再把每帧的左上、右上
各裁 320x180，存到 results/frames/<序号>/。台标在右上角还是左上角，看裁图就知道。
"""

import json
import subprocess
from pathlib import Path

BASE = Path(__file__).resolve().parent
CAND = BASE / "results" / "candidates.json"
OUT = BASE / "results" / "frames"
MAX_CANDIDATES = 10


def run(cmd, timeout=90):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode == 0
    except subprocess.TimeoutExpired:
        return False


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    if not CAND.exists():
        print("没有 candidates.json，跳过抽帧")
        return
    cands = json.loads(CAND.read_text(encoding="utf-8"))
    print(f"候选 {len(cands)} 条，最多抽 {MAX_CANDIDATES} 条")
    for idx, c in enumerate(cands[:MAX_CANDIDATES], 1):
        d = OUT / f"{idx:02d}"
        d.mkdir(parents=True, exist_ok=True)
        (d / "info.txt").write_text(
            f"{c['name']}\n{c['url']}\n来源 {c.get('source_ip', '')}\n",
            encoding="utf-8")
        rec = d / "rec.ts"
        ok = run(["ffmpeg", "-y", "-v", "error",
                  "-i", c["url"], "-t", "9", "-c", "copy", str(rec)],
                 timeout=60)
        if not ok or not rec.exists() or rec.stat().st_size == 0:
            print(f"[{idx}] 拉流失败：{c['url']}")
            continue
        for t in (2, 5, 8):
            frame = d / f"f{t}.png"
            if not run(["ffmpeg", "-y", "-v", "error",
                        "-ss", str(t), "-i", str(rec),
                        "-frames:v", "1",
                        "-vf", "scale=960:-1", str(frame)], timeout=30):
                continue
            for corner, x in (("tl", "0"), ("tr", "iw-320")):
                crop = d / f"f{t}_{corner}.png"
                run(["ffmpeg", "-y", "-v", "error",
                     "-i", str(frame),
                     "-vf", f"crop=320:180:{x}:0", str(crop)], timeout=30)
        print(f"[{idx}] 完成：{c['name']} | {c['url']}")
    print("抽帧完成 -> results/frames/")


if __name__ == "__main__":
    main()
