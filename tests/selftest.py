# -*- coding: utf-8 -*-
"""派单核查平台自测：核心增量逻辑、只读保证、快照一致性、HTTP 流程。

仅依赖标准库 + 被测 app（Flask 仅用于 test_client 流程测试）。
运行：/workspace/.venv/bin/python -I /workspace/tests/selftest.py
"""
import importlib.util
import io
import os
import sqlite3
import sys
import tempfile
from datetime import datetime

WORKSPACE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PATH = os.path.join(WORKSPACE, "app.py")

TMPDIR = tempfile.mkdtemp(prefix="qc_selftest_")
os.environ["QC_DB_PATH"] = os.path.join(TMPDIR, "test.db")
os.environ["QC_SECRET_KEY"] = "test-secret"

spec = importlib.util.spec_from_file_location("qc_app", APP_PATH)
qc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qc)

PASS, FAIL = 0, 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ok  {name}")
    else:
        FAIL += 1
        print(f"FAIL  {name}  {detail}")


def open_con():
    con = sqlite3.connect(os.environ["QC_DB_PATH"], timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=10000")
    return con


def snapshot(con, who):
    """数据库实际三类图号集合。"""
    out = {1: set(), 2: set(), 3: set()}
    for r in con.execute(
            "SELECT image_no, category FROM qc_results WHERE annotator=?",
            (who,)):
        out[r["category"]].add(r["image_no"])
    return out


def reference(ann, dispatch):
    """纯 Python 参考模型。"""
    keys = set(ann)
    missed = set(dispatch) - keys
    reshoot = {i for i in dispatch if ann.get(i) == 3}
    wrong = keys - set(dispatch)
    return {1: missed, 2: reshoot, 3: wrong}


def save(con, who, pairs):
    con.execute("BEGIN IMMEDIATE")
    try:
        qc.save_annotations(con, who, dict(pairs))
        con.commit()
    except Exception:
        con.rollback()
        raise


def replace(con, who, images):
    con.execute("BEGIN IMMEDIATE")
    try:
        info = qc.replace_dispatch(con, who, sorted(images))
        con.commit()
        return info
    except Exception:
        con.rollback()
        raise


def stamps(con, who):
    return {r["image_no"]: r["updated_at"] for r in con.execute(
        "SELECT image_no, updated_at FROM qc_results WHERE annotator=?", (who,))}


# ============================================================ 1. CSV 解析
print("[1] CSV 解析")
pairs = qc.parse_csv("图号,标记\n1,有坡道\n2,无坡道\n3,看不清\n\n1,无坡道\n")
check("标注 CSV 表头跳过+同号取最后", pairs == {1: 2, 2: 2, 3: 3}, str(pairs))
imgs = qc.parse_dispatch_csv("图号\n5\n1\n5\n\n3\n")
check("派单 CSV 去重升序", imgs == [1, 3, 5], str(imgs))
for bad, needle in [
    ("", "没有有效"), ("x\n", "没有有效"), ("70000\n", "超出"),
    ("图号\nx\n", "不是整数"),
]:
    try:
        qc.parse_dispatch_csv(bad)
        check(f"非法派单被拒: {bad!r}", False)
    except qc.UploadError as e:
        check(f"非法派单被拒: {bad!r}", needle in str(e), str(e))
try:
    qc.parse_csv("1\n")
    check("缺列标注 CSV 被拒", False)
except qc.UploadError:
    check("缺列标注 CSV 被拒", True)

# ============================================================ 2. 首次派单 + 旧库标注
print("[2] 旧库标注 + 首次派单全量核查")
con = open_con()
# 先有旧库标注（含派单外的错交、派单内的看不清）
save(con, "甲", {10: 1, 20: 3, 30: 2, 999: 1})
d1 = list(range(10, 60))                      # 10..59
info = replace(con, "甲", d1)
ref = reference({10: 1, 20: 3, 30: 2, 999: 1}, set(d1))
got = snapshot(con, "甲")
check("首次派单三类与参考模型一致", got == ref, f"{got} vs {ref}")
check("漏判=47（派50-已标3）", info["missed"] == 47, str(info))
check("重拍=1（20 看不清）", info["reshoot"] == 1)
check("错交=1（999 派单外）", info["wrong"] == 1)
check("首次派单版本 v1", info["version"] == 1)
check("纯派单无标注=全部漏判",
      snapshot(con, "乙") == {1: set(), 2: set(), 3: set()})
info_b = replace(con, "乙", [7, 8, 9])
check("乙首次派单 3 个全漏判",
      snapshot(con, "乙") == {1: {7, 8, 9}, 2: set(), 3: set()})
check("乙派单版本 v1", info_b["version"] == 1)

# ============================================================ 3. 补传增量
print("[3] 补传只重新核验本批图号")
before = stamps(con, "甲")
# SQLite datetime('now') 精度为秒，跨过秒边界才能区分新旧时间戳
import time as _time
_time.sleep(1.1)
# 本批：补两个漏判（40,41）、把 20 更正为有坡道（重拍消失）、
#       派单外新图 1000（错交）、派单内新图看不清 42（重拍）
save(con, "甲", {40: 1, 41: 2, 20: 1, 1000: 3, 42: 3})
ann = {10: 1, 20: 1, 30: 2, 999: 1, 40: 1, 41: 2, 1000: 3, 42: 3}
ref = reference(ann, set(d1))
got = snapshot(con, "甲")
check("补传后三类与参考模型一致", got == ref, f"{got} vs {ref}")
check("20 重拍消失", 20 not in got[2])
check("42 进入重拍", 42 in got[2])
check("40/41 漏判消除", not ({40, 41} & got[1]))
check("1000 错交", 1000 in got[3])
after = stamps(con, "甲")
touched = {1000, 42, 40, 41, 20}
check("核查结果时间戳只动本批图号",
      all(before.get(i) != after.get(i) for i in touched) and
      all(before.get(i) == after.get(i) for i in before if i not in touched),
      f"touched={touched}")
check("本批外漏判一条不少", len(got[1]) == 47 - 3)  # 40,41 补标消除；42 漏判→重拍
# 汇总版本递增且内容等价于全量重打包
row = con.execute("SELECT version, data FROM summaries WHERE annotator='甲'").fetchone()
check("汇总版本 v2", row["version"] == 2)
full_rows = con.execute(
    "SELECT image_no, mark FROM annotations WHERE annotator='甲'").fetchall()
check("补丁后的汇总与全量重打包一致",
      bytes(row["data"]) == qc.pack_summary((r["image_no"], r["mark"]) for r in full_rows))
check("汇总仍为定长 15000 字节", len(row["data"]) == qc.SUMMARY_BYTES)
check("原始标注更正生效", con.execute(
    "SELECT mark FROM annotations WHERE annotator='甲' AND image_no=20").fetchone()[0] == 1)

# ============================================================ 4. 换单增量
print("[4] 换单只核验增减图号")
before = stamps(con, "甲")
d2 = list(range(20, 70))                 # 移除 10..19，新增 60..69
info = replace(con, "甲", d2)
ref = reference(ann, set(d2))
got = snapshot(con, "甲")
check("换单后三类与参考模型一致", got == ref, f"{got} vs {ref}")
check("新增 10、移除 10", info["added"] == 10 and info["removed"] == 10)
check("派单版本升至 v2", info["version"] == 2)
check("移除的 10（有标注）转错交", 10 in got[3])
check("移除的 11..19（无标注）无任何结果",
      not any(i in got[c] for c in got for i in range(11, 20)))
check("新增 60..69 全漏判", got[1] >= set(range(60, 70)))
after = stamps(con, "甲")
changed = set(d2) ^ set(d1)
check("未增减图号的核查结果时间戳不变",
      all(before.get(i) == after.get(i) for i in before if i not in changed))
# 再换一份完全相同的派单：零增减、结果零变动
same_ts = stamps(con, "甲")
info = replace(con, "甲", d2)
check("相同派单零增减", info["added"] == 0 and info["removed"] == 0)
check("相同派单结果零变动", stamps(con, "甲") == same_ts)
check("相同派单版本仍递增 v3", info["version"] == 3)

# ============================================================ 5. 交错操作长序列对拍参考模型
print("[5] 换单与补传交错对拍")
import random
random.seed(42424)
ann2, dis2 = {}, set()
script = []
for step in range(40):
    who = "丙"
    if step % 3 == 0:                                   # 换单
        pool = random.sample(range(1, 400), random.randint(20, 120))
        dis2 = set(pool)
        replace(con, who, sorted(dis2))
        script.append(("dispatch", step))
    else:                                               # 补传
        batch = {i: random.choice([1, 2, 3])
                 for i in random.sample(range(1, 400), random.randint(5, 60))}
        ann2.update(batch)
        save(con, who, batch)
        script.append(("upload", step))
    got = snapshot(con, who)
    ref = reference(ann2, dis2)
    if got != ref:
        check(f"第 {step} 步与参考模型一致", False, f"{got} vs {ref}")
        break
else:
    check("40 步交错操作每步都与参考模型一致", True)
# 汇总全量等价
row = con.execute("SELECT data FROM summaries WHERE annotator='丙'").fetchone()
full_rows = con.execute(
    "SELECT image_no, mark FROM annotations WHERE annotator='丙'").fetchall()
check("丙的汇总补丁始终等价全量重打包",
      bytes(row["data"]) == qc.pack_summary((r["image_no"], r["mark"]) for r in full_rows))

# ============================================================ 6. 分页
print("[6] 分页（升序、每页<=50、总数）")
big = list(range(1000, 1131))                            # 131 个派单
replace(con, "丁", big)
tot = {r["category"]: r["n"] for r in con.execute(
    "SELECT category, count(*) n FROM qc_results WHERE annotator='丁' GROUP BY 1")}
check("丁 131 个全漏判", tot == {1: 131})
pages = []
for p in range(1, 5):
    rows = [r["image_no"] for r in con.execute(
        "SELECT image_no FROM qc_results WHERE annotator='丁' AND category=1 "
        "ORDER BY image_no LIMIT ? OFFSET ?", (50, (p - 1) * 50))]
    pages.append(rows)
check("页数 50+50+31", [len(x) for x in pages] == [50, 50, 31, 0])
flat = [i for pg in pages for i in pg]
check("拼接后升序且完整", flat == sorted(flat) == big)
qp = [r[3] for r in con.execute(
    "EXPLAIN QUERY PLAN SELECT image_no FROM qc_results "
    "WHERE annotator='丁' AND category=1 ORDER BY image_no LIMIT 50 OFFSET 0")]
check("分页走 idx_qc_cat_img 索引",
      any("idx_qc_cat_img" in s and "SEARCH" in s.upper() for s in qp), str(qp))

# ============================================================ 7. 只读保证审计
print("[7] 查一页不读原始标注、不碰派单表")
audit = open_con()

def blocker(kind, arg1, arg2, dbname, source):
    if kind == sqlite3.SQLITE_READ and arg1 in ("annotations", "dispatch"):
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK
audit.set_authorizer(blocker)
try:
    audit.execute("SELECT count(*) FROM qc_results WHERE annotator='丁'").fetchall()
    audit.execute(
        "SELECT category, count(*) FROM qc_results WHERE annotator='丁' GROUP BY 1").fetchall()
    audit.execute(
        "SELECT image_no FROM qc_results WHERE annotator='丁' AND category=1 "
        "ORDER BY image_no LIMIT 50 OFFSET 0").fetchall()
    audit.execute(
        "SELECT annotator FROM summaries UNION SELECT annotator FROM dispatch_meta").fetchall()
    check("核查页全部 SQL 在禁止读 annotations/dispatch 下成功", True)
except sqlite3.DatabaseError as e:
    check("核查页全部 SQL 在禁止读 annotations/dispatch 下成功", False, str(e))
try:
    audit.execute("SELECT count(*) FROM annotations").fetchall()
    check("authorizer 确实拦截 annotations", False)
except sqlite3.DatabaseError:
    check("authorizer 确实拦截 annotations", True)
audit.set_authorizer(None)

# 补传 SQL 审计：追踪语句，凡访问 annotations 必被 tmp_batch 限定
traced = []
con.set_trace_callback(lambda s: traced.append(s))
save(con, "丁", {1050: 3, 1051: 1})
con.set_trace_callback(None)
bad = [s for s in traced if s.lstrip().upper().startswith("SELECT")
       and "annotations" in s.lower() and "tmp_batch" not in s.lower()]
check("补传对 annotations 的读访问全部经 tmp_batch 限定", not bad, str(bad))
check("补传不含逐图 Python 遍历（无循环单图 SELECT）",
      sum(1 for s in traced if "image_no = ?" in s) == 0)

# ============================================================ 8. 快照一致性 + 并发
print("[8] 快照一致性（单读事务 / WAL / 上传期间可查）")
r1, r2 = open_con(), open_con()
r1.execute("BEGIN")
before_total = r1.execute(
    "SELECT count(*) n FROM qc_results WHERE annotator='丁' AND category=1").fetchone()["n"]
# r2 在 r1 读事务期间提交补传（WAL 下不阻塞）；用新批次 1052/1053
r2.execute("BEGIN IMMEDIATE")
qc.save_annotations(r2, "丁", {1052: 3, 1053: 1})
r2.commit()
during = r1.execute(
    "SELECT count(*) n FROM qc_results WHERE annotator='丁' AND category=1").fetchone()["n"]
r1.commit()
after_total = open_con().execute(
    "SELECT count(*) n FROM qc_results WHERE annotator='丁' AND category=1").fetchone()["n"]
check("读事务内不受并发提交影响（同快照）", during == before_total == 129)
check("新事务看到提交后状态", after_total == 127)
check("WAL 模式开启", (con.execute("PRAGMA journal_mode").fetchone()[0]).lower() == "wal")

# 写锁串行化：两个 IMMEDIATE 不破坏数据
w1, w2 = open_con(), open_con()
w1.execute("BEGIN IMMEDIATE")
try:
    w2.execute("BEGIN IMMEDIATE")
    check("第二个写事务应超时/阻塞报错", False)
except sqlite3.OperationalError:
    check("写事务串行（第二个拿锁失败）", True)
w1.rollback()

# ============================================================ 9. 失败沿用
print("[9] 失败沿用原结果")
con.execute(
    "CREATE TRIGGER boom BEFORE INSERT ON annotations "
    "WHEN NEW.image_no=42424 BEGIN SELECT RAISE(ABORT,'人为故障'); END")
state_before = (
    con.execute("SELECT count(*) n FROM annotations").fetchone()["n"],
    con.execute("SELECT count(*) n FROM qc_results").fetchone()["n"],
    con.execute("SELECT version FROM summaries WHERE annotator='丁'").fetchone()["version"],
)
try:
    save(con, "丁", {42424: 1, 1052: 1})
    check("触发器故障时事务回滚", False)
except sqlite3.Error:
    check("触发器故障时事务回滚", True)
con.execute("DROP TRIGGER boom")
state_after = (
    con.execute("SELECT count(*) n FROM annotations").fetchone()["n"],
    con.execute("SELECT count(*) n FROM qc_results").fetchone()["n"],
    con.execute("SELECT version FROM summaries WHERE annotator='丁'").fetchone()["version"],
)
check("回滚后三张表状态完全不变", state_before == state_after, f"{state_before} vs {state_after}")
check("42424 未写入", con.execute(
    "SELECT count(*) FROM annotations WHERE image_no=42424").fetchone()[0] == 0)

# ============================================================ 10. HTTP 流程
print("[10] Flask HTTP 流程")
client = qc.app.test_client()

def upload_csv(who, rows):
    data = "图号,标记\n" + "".join(f"{i},{m}\n" for i, m in rows)
    return client.post("/upload", data={
        "annotator": who,
        "file": (io.BytesIO(data.encode()), "a.csv"),
    }, content_type="multipart/form-data", follow_redirects=True)

def upload_dispatch(who, images, bad=False):
    body = ("图号\n" if not bad else "") + "".join(f"{i}\n" for i in images)
    return client.post("/dispatch", data={
        "annotator": who,
        "file": (io.BytesIO(body.encode()), "d.csv"),
    }, content_type="multipart/form-data", follow_redirects=True)

rv = upload_csv("戊", [(201, "有坡道"), (202, "看不清"), (901, "无坡道")])
check("标注上传 200", rv.status_code == 200 and "补传成功" in rv.get_data(as_text=True))
rv = upload_dispatch("戊", range(200, 260))
check("派单上传 200", rv.status_code == 200 and "首次派单成功" in rv.get_data(as_text=True))
rv = client.get("/qc?annotator=戊&category=1&page=1")
html = rv.get_data(as_text=True)
check("核查页 200 且含三类", rv.status_code == 200 and "漏判" in html and "错交" in html)
check("漏判 58（60 派单-2 已标，901 不在派单）", "漏判" in html and ">58<" in html)
rv = client.get("/qc?annotator=戊&category=3&page=1")
check("错交页含 901", "901" in rv.get_data(as_text=True))
rv = client.get("/qc?annotator=戊&category=2&page=1")
check("重拍页含 202", "202" in rv.get_data(as_text=True))
# 分页：57 条漏判 → 第 2 页 7 条
p2 = client.get("/qc?annotator=戊&category=1&page=2").get_data(as_text=True)
check("第二页显示 1/2 页", "第 2 / 2 页" in p2)
check("非法类别 400", client.get("/qc?annotator=戊&category=9").status_code == 400)
rv = client.get("/qc?annotator=没人", follow_redirects=True)
check("未知标注员跳首页并提示",
      rv.status_code == 200 and "暂无派单或标注数据" in rv.get_data(as_text=True))
# 非法派单失败沿用
rv = client.post("/dispatch", data={
    "annotator": "戊",
    "file": (io.BytesIO("abc\n".encode()), "d.csv"),
}, content_type="multipart/form-data", follow_redirects=True)
check("非法派单报错且沿用", "派单替换失败" in rv.get_data(as_text=True))
v = con.execute("SELECT version FROM dispatch_meta WHERE annotator='戊'").fetchone()["version"]
check("失败派单版本不增", v == 1)
# 缺标注员
rv = client.post("/dispatch", data={
    "annotator": "", "file": (io.BytesIO(b"1\n"), "d.csv")},
    content_type="multipart/form-data", follow_redirects=True)
check("缺标注员被拒", "请填写标注员" in rv.get_data(as_text=True))
# 首页含新派单入口与统计
home = client.get("/").get_data(as_text=True)
check("首页含派单上传与核查入口",
      "/dispatch" in home and "/qc" in home and "替换派单" in home)

# 两人核查回归
upload_csv("己", [(201, "无坡道"), (202, "有坡道"), (203, "有坡道")])
rv = client.get("/compare?a=戊&b=己")
check("两人核查页 200", rv.status_code == 200 and "Cohen" in rv.get_data(as_text=True))
rv = client.get("/disagreements.csv?a=戊&b=己&va=1&vb=1")
check("分歧清单 200（旧版本号失效时 409）",
      rv.status_code in (200, 409))
check("同人比较被拒", "不能是同一人" in
      client.get("/compare?a=戊&b=戊", follow_redirects=True).get_data(as_text=True))

# ============================================================ 11. 大数据量性能味道
print("[11] 60000 规模下的增量代价")
big_con = open_con()
full = list(range(1, 60001))
t0 = datetime.now()
replace(big_con, "庚", full)                     # 首次全量 6 万
t1 = datetime.now()
info = replace(big_con, "庚", full)              # 完全相同的派单
t2 = datetime.now()
# 换单：移除 60000、新增 60000？构造真实增减
info = replace(big_con, "庚", list(range(2, 60000)))                   # 移除 1 和 60000
t3 = datetime.now()
save(big_con, "庚", {60000: 1})                                        # 单图补传
t4 = datetime.now()
check("6 万首次派单可完成", True, f"{(t1-t0).total_seconds():.2f}s")
check("相同派单换单毫秒级",
      (t2 - t1).total_seconds() < 1.0, f"{(t2-t1).total_seconds():.3f}s")
check("2 图增减换单毫秒级",
      (t3 - t2).total_seconds() < 1.0, f"{(t3-t2).total_seconds():.3f}s")
check("单图补传毫秒级",
      (t4 - t3).total_seconds() < 1.0, f"{(t4-t3).total_seconds():.3f}s")
got = snapshot(big_con, "庚")
# 派单 2..59999 全漏判；60000 已标但已移出派单→错交；1 无标注且移出→无结果
check("6 万规模结果正确",
      len(got[1]) == 59998 and got[3] == {60000} and not got[2],
      f"{len(got[1])},{got[3]},{got[2]}")
big_con.close()

con.close()
print(f"\n{'='*50}\n通过 {PASS} 项，失败 {FAIL} 项")
sys.exit(1 if FAIL else 0)
