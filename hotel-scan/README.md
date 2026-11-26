# 广东酒店源扫描：只找翡翠台

在 GitHub Actions 上跑（runner 直连网络），目标只有一个：找到能播的翡翠台酒店源。

## 怎么跑

- 手动：仓库 Actions 页 → hotel-scan → Run workflow。
- 自动：改 `hotel-scan/scan.py`、`seeds.txt`、`frames.py` 或工作流文件并 push 到 main，就会跑一次。

## 流程

1. `scan.py`：26 个种子各扩一个 /24（共 6630 个探测地址），扫 txiptv 接口
   `/iptv/live/1000.json?key=txiptv`，只留名字含「翡翠台」的频道（排除「华丽翡翠台」），
   结果写 `results/candidates.json`。
2. `frames.py`：对每个候选拉流 9 秒，抽第 2/5/8 秒三帧，再裁左上、右上两个角，
   存到 `results/frames/<序号>/`，供人工看台标判断。
3. 结果（`results/candidates.json` 和 `results/frames/`）会提交回仓库，
   全量也存一份在 Actions 的 artifacts 里。

## 验证标准

看抽帧图的左上角和右上角有没有台标。翡翠台的台标在右上角
（TVB 彩色标志加白色「翡翠台」三字），左上角没有。

## 注意

- runner 在海外机房，扫到的只能算候选：能播不等于在佛山联通家里能播，
  最终要回家在盒子上实测；扫不到也不等于家里播不了。
- 种子是 2026-11-26 收集的（Guovin 广东分支、ssili126、alantang 几个公开结果
  加两个已知广东电信酒店段），酒店 IP 死得快，种子要定期换。
- 仓库里原来的东西（README.md、SKILL.md）没动，这个扫描是另外加的。
