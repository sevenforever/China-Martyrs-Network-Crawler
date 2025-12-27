#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
中华英烈网 · 流式全量爬虫  v1.3.3
-----------------------------------------------------
★ 新增
    · column_explanations  —— 统一的「英 -> 中文」字段映射
    · 保存前 .rename(...)  —— 写 CSV 时自动用中文表头
"""

from __future__ import annotations
import os, sys, json, time, random, hashlib, pickle, pathlib, argparse, logging
from typing import Dict, Any, List, Set

import requests
import pandas as pd
from tqdm.auto import tqdm

# ───────────────────────────────── 0. CLI
p = argparse.ArgumentParser(description="中华英烈网爬虫 · Break-Point + Retry + 中文表头")
p.add_argument("--chunks",       type=int,   default=1,  help="每多少页写 1 个 CSV")
p.add_argument("--pages",        type=int,   default=None, help="只抓前 N 页 (调试)")
p.add_argument("--sleep-base",   type=float, default=0.1, help="每页固定休眠秒")
p.add_argument("--sleep-jitter", type=float, default=0.1, help="附加抖动 (0~x) 秒")
p.add_argument("--cool-every",   type=int,   default=200,   help="每 N 页再额外休眠")
p.add_argument("--cool-time",    type=float, default=0.1,  help="额外休眠秒数")
p.add_argument("--verbose", action="store_true")
args = p.parse_args()

# ───────────────────────────────── 1. 常量
BASE          = r"E:\爬虫实战\中华英烈网"               # ← 自行修改
AREA_FILE     = os.path.join(BASE, "000000000000.json")
TMP_DIR       = os.path.join(BASE, "chunks")
ID_CACHE      = os.path.join(BASE, "seen_ids.pkl")
PAGE_FILE     = os.path.join(BASE, "current_page.txt")

UA            = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                 "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36")

PAGE_SIZE     = 10
TOTAL_PAGES   = 187_629
CHUNK_PAGES   = args.chunks

TIMEOUT       = 30000
MAX_RETRY     = 500000

pathlib.Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

# ───────────────────────────────── 2. 日志
level = logging.DEBUG if args.verbose else logging.INFO
logging.basicConfig(level=level,
                    format="%(asctime)s %(levelname)-8s %(message)s",
                    datefmt="%H:%M:%S",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger("martyrs")

# ───────────────────────────────── 3. 行政区划 → 地址解析
with open(AREA_FILE, "r", encoding="utf-8") as f:
    raw = json.load(f)

AREA: Dict[str, str] = {}
def _dfs(nodes):
    for n in nodes:
        AREA[n["orgId"]] = n["deptName"]
        _dfs(n.get("children", []))
_dfs(raw if isinstance(raw, list) else [raw])
log.info("行政区划 %d 条", len(AREA))

def addr(code: str) -> str:
    if not code:
        return ""
    return "".join(AREA.get(seg, "") for seg in
                   (code[:2] + "000000000000",
                    code[:4] + "00000000",
                    code[:6] + "000000",
                    code))

# ───────────────────────────────── 4. 字段中文映射
column_explanations: Dict[str, str] = {
    "ID": "唯一标识符",
    "status": "状态", "createBy": "创建者", "createTime": "创建时间",
    "updateBy": "更新者", "updateTime": "更新时间", "updateDate": "更新日期",
    "remark": "备注", "delFlag": "删除标记", "searchValue": "搜索值",
    "dataScope": "数据范围",

    "mmdrGuid": "烈士ID",
    "mmdrName": "姓名", "mmdrAsname": "别名",
    "mmdrSexId": "性别ID", "mmdrSex": "性别",

    "mmdrShengId": "省ID", "mmdrSheng": "省",
    "mmdrShiId": "市ID",   "mmdrShi": "市",
    "mmdrXianId": "县ID",  "mmdrXian": "县",
    "mmdrZhenId": "镇ID",  "mmdrZhen": "镇",
    "mmdrCunId": "村ID",   "mmdrCun": "村",

    "mmdrDeathJg": "籍贯",
    "mmdrBirthYear": "出生年份", "mmdrBirthMonth": "出生月份", "mmdrBirthDay": "出生日期",
    "mmdrRdYear": "入党年份",   "mmdrRdMonth": "入党月份",   "mmdrRdDay": "入党日期",
    "mmdrWorkYear": "参工年份", "mmdrWorkMonth": "参工月份", "mmdrWorkDay": "参工日期",

    "mmdrZzmmId": "政治面貌ID", "mmdrZzmm": "政治面貌",
    "mmdrUnit": "所在单位", "mmdrJob": "职务",

    "mmdrDeathYear": "牺牲年份", "mmdrDeathMonth": "牺牲月份", "mmdrDeathDay": "牺牲日期",
    "mmdrDeathPlace": "牺牲地点",

    "mmdrBuryCode": "安葬地代码", "mmdrBury": "安葬地",
    "mmdrCemeteryId": "陵园ID",  "mmdrCemetery": "陵园名称",
    "mmdrBuryPlace": "安葬地址",

    "mmdrDeeds": "主要事迹", "mmdrDeathCause": "牺牲原因", "mmdrHonor": "荣誉称号",

    "mmdrSbdwShengId": "申报省ID",  "mmdrSbdwSheng": "申报省",
    "mmdrSbdwShiId": "申报市ID",    "mmdrSbdwShi": "申报市",
    "mmdrSbdwXianId": "申报县ID",   "mmdrSbdwXian": "申报县",

    # 其余较少用字段可继续在这里补充……
    "page_num": "来源页码"
}

# ───────────────────────────────── 5. 接口签名 & HTTP
TOKEN_URL  = "https://yinglie.chinamartyrs.gov.cn/web-api/getToken"
SEARCH_URL = "https://yinglie.chinamartyrs.gov.cn/web-api/api/martyrs/search"

md5 = lambda s: hashlib.md5(s.encode()).hexdigest().upper()
def autograph(payload: Dict[str, Any], ts: int) -> str:
    kv = "&".join(f"{k}={payload[k]}" for k in sorted(payload) if payload[k])
    return md5(kv + f"&key=B28C665759654EF6A923F18888888888&timestamp={ts}")

def hdr(token: str | None = None,
        sign : str | None = None,
        ts   : int  | None = None,
        nosig: bool = False) -> Dict[str, str]:
    h = {"User-Agent": UA,
         "Origin": "https://www.chinamartyrs.gov.cn",
         "Referer": "https://www.chinamartyrs.gov.cn/",
         "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
    if token: h.update({"authorization": token, "user-token": token})
    if sign:  h["autograph"] = sign
    if ts:    h["timestamp"] = str(ts)
    if nosig: h["noSignature"] = "true"
    return h

# ---------- get_token ----------
def get_token(sess: requests.Session) -> str:
    for i in range(1, MAX_RETRY + 1):
        try:
            js = sess.post(TOKEN_URL, headers=hdr(nosig=True),
                           timeout=TIMEOUT).json()
            if js.get("code") == 200:
                return js["data"]["token"]
            log.warning("get_token 返回异常：%s", js)
        except Exception as e:
            log.warning("get_token 第 %d/%d 次失败：%s", i, MAX_RETRY, e)
        time.sleep(min(2 ** (i - 1), args.cool_time))
    raise RuntimeError("get_token 连续失败")

# ---------- fetch_page ----------
def fetch_page(sess: requests.Session, token: str, page: int) -> Dict[str, Any]:
    payload = {"mmdrName": "", "pageNum": str(page), "pageSize": str(PAGE_SIZE),
               "Params": json.dumps({"beginTime": "", "endTime": ""}, ensure_ascii=False),
               "mmdrShengId": ""}
    ts   = int(time.time() * 1000)
    sign = autograph(payload, ts)

    for i in range(1, MAX_RETRY + 1):
        try:
            js = sess.post(SEARCH_URL, headers=hdr(token, sign, ts),
                           data=payload, timeout=TIMEOUT).json()
            if js.get("code") == 200:
                return js
            log.warning("page %d 返回异常：%s", page, js)
        except Exception as e:
            log.warning("page %d 第 %d/%d 次失败：%s", page, i, MAX_RETRY, e)
        time.sleep(min(2 ** (i - 1), args.cool_time))
    raise RuntimeError(f"page {page} 连续失败")

# ───────────────────────────────── 6. 断点 / 去重 / 分片
def load_ids() -> Set[str]:
    return pickle.load(open(ID_CACHE, "rb")) if os.path.exists(ID_CACHE) else set()

def save_ids(ids: Set[str]) -> None:
    pickle.dump(ids, open(ID_CACHE, "wb"))

def read_page() -> int:
    try:
        return max(1, int(open(PAGE_FILE, encoding="utf-8").read().strip()))
    except Exception:
        return 1

def write_page(next_page: int) -> None:
    open(PAGE_FILE, "w", encoding="utf-8").write(str(next_page))

def cpath(idx: int) -> str:
    return os.path.join(TMP_DIR, f"chunk_{idx:05d}.csv")

# ───────────────────────────────── 7. 主流程
def crawl() -> None:
    seen         = load_ids()
    chunk_rows   = []

    start_page   = read_page()
    total_pages  = min(args.pages, TOTAL_PAGES) if args.pages else TOTAL_PAGES
    cidx         = (start_page - 1) // CHUNK_PAGES

    with requests.Session() as sess:
        token = get_token(sess)
        log.info("token ok · 从第 %d 页开始，共需抓 %d 页", start_page, total_pages)

        bar = tqdm(total=total_pages, initial=start_page - 1,
                   unit="页", ncols=90, colour="cyan")

        for page in range(start_page, total_pages + 1):

            js = fetch_page(sess, token, page)
            for r in js["rows"]:
                gid = r["mmdrGuid"]
                if gid in seen:
                    continue
                seen.add(gid)

                for k in ("mmdrSheng", "mmdrShi", "mmdrXian"):
                    if not r.get(k) and r.get(k + "Id"):
                        r[k] = addr(r[k + "Id"])
                r["ID"]       = gid
                r["page_num"] = page
                chunk_rows.append(r)

            # 限速
            if page % args.cool_every == 0:
                time.sleep(args.cool_time)
            else:
                time.sleep(args.sleep_base + random.random() * args.sleep_jitter)

            # 进度 & 断点
            bar.update(1)
            write_page(page + 1)

            # 分片
            if page % CHUNK_PAGES == 0 or page == total_pages:
                if chunk_rows:
                    df  = pd.DataFrame(chunk_rows)
                    df  = df.rename(columns=column_explanations, errors="ignore")
                    df.to_csv(cpath(cidx), index=False, encoding="utf-8-sig")
                    log.info("📦 CSV 分片 %-18s | %d 行", f"chunk_{cidx:05d}.csv", len(df))
                chunk_rows.clear()
                cidx += 1
                save_ids(seen)

        bar.close()
        log.info("✨ 全部完成！抓取 %d 页，去重后 %d 记录", total_pages, len(seen))
        write_page(1)

if __name__ == "__main__":
    try:
        crawl()
    except KeyboardInterrupt:
        log.warning("⏹️ 手动中断，保存进度…")
        save_ids(load_ids())
        sys.exit(0)
