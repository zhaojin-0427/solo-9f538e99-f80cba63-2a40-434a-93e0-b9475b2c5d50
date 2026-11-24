# -*- coding: utf-8 -*-
"""坡道标注核查平台

功能一：两人一致性核查（原有）
- 标注员上传 CSV（图号 1~60000，标记：有坡道 / 无坡道 / 看不清）。
- SQLite 保存原始标注，同人同图重复上传视为更正。
- 每位标注员维护一份 2 bit/图 的压缩汇总（固定 15000 字节，上限 128 KiB），
  原始标注与汇总在同一事务中生效；补传只就地修补本人汇总，不回读整份原始标注。
- 核查只读汇总：任选两人，仅双方都判读为「有坡道/无坡道」的图参与配对，
  输出四种配对计数与 Cohen's κ，并可下载分歧清单（含双方判读）。

功能二：派单核查（新增）
- 每位标注员可上传派单 CSV（每行一个图号），整份替换原派单，带版本号。
- 三类物化核查结果 qc_results：
  1 漏判：派到但没有任何标注；
  2 重拍：派到且标为「看不清」；
  3 错交：没派到却有标注。
- 旧库已有标注在首次派单时直接参与全量核查。
- 补传只访问本批图号的原始标注，且只重新核验本批图号；
  换单只对新增/移除的图号重新核验（首次派单才全量初始化）。
- 新派单/更正与核查结果在同一事务生效，失败回滚、沿用原结果。
- 分页只读 qc_results 的覆盖索引，不读原始标注、不遍历整份派单；
  WAL 下上传期间仍可查，三类总数与当页图号取自同一已提交快照（单读事务）。
"""

import csv
import io
import os
import sqlite3

from flask import (Flask, Response, abort, flash, g, redirect,
                   render_template, request, url_for)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("QC_DB_PATH", os.path.join(BASE_DIR, "data", "qc.db"))

MAX_IMAGE_NO = 60000
SUMMARY_LIMIT = 128 * 1024                      # 每人汇总上限 128 KiB
SUMMARY_BYTES = (MAX_IMAGE_NO * 2 + 7) // 8     # 2 bit/图 → 15000 字节
PAGE_SIZE = 50                                  # 核查清单每页最多 50 个

MARK_HAS = "有坡道"
MARK_NONE = "无坡道"
MARK_UNKNOWN = "看不清"
MARK_TO_CODE = {MARK_HAS: 1, MARK_NONE: 2, MARK_UNKNOWN: 3}
CODE_TO_MARK = {v: k for k, v in MARK_TO_CODE.items()}
VALID_CODES = (MARK_TO_CODE[MARK_HAS], MARK_TO_CODE[MARK_NONE])

# 核查结果类别
CAT_MISSED = 1    # 漏判：派到未标
CAT_RESHOOT = 2   # 重拍：派到且标为看不清
CAT_WRONG = 3     # 错交：没派到却标了
CAT_NAMES = {CAT_MISSED: "漏判", CAT_RESHOOT: "重拍", CAT_WRONG: "错交"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS annotations (
    annotator  TEXT NOT NULL,
    image_no   INTEGER NOT NULL CHECK (image_no BETWEEN 1 AND 60000),
    mark       INTEGER NOT NULL CHECK (mark IN (1, 2, 3)),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (annotator, image_no)
);
CREATE TABLE IF NOT EXISTS summaries (
    annotator  TEXT PRIMARY KEY,
    version    INTEGER NOT NULL,
    data       BLOB NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS dispatch (
    annotator  TEXT NOT NULL,
    image_no   INTEGER NOT NULL CHECK (image_no BETWEEN 1 AND 60000),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (annotator, image_no)
);
CREATE TABLE IF NOT EXISTS dispatch_meta (
    annotator   TEXT PRIMARY KEY,
    version     INTEGER NOT NULL,
    image_count INTEGER NOT NULL,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS qc_results (
    annotator  TEXT NOT NULL,
    image_no   INTEGER NOT NULL CHECK (image_no BETWEEN 1 AND 60000),
    category   INTEGER NOT NULL CHECK (category IN (1, 2, 3)),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (annotator, image_no)
);
-- 分页与计数的覆盖索引：单条索引区间扫描即可取一页，无需碰其他表
CREATE INDEX IF NOT EXISTS idx_qc_cat_img
    ON qc_results(annotator, category, image_no);
"""

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 8 * 1024 * 1024  # 上传体积上限
app.secret_key = os.environ.get("QC_SECRET_KEY", "qc-dev-secret")


# ---------------------------------------------------------------- 数据库

def init_db():
    parent = os.path.dirname(DB_PATH)
    if parent:
        os.makedirs(parent, exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=10)
    try:
        con.execute("PRAGMA journal_mode=WAL")  # 上传期间仍可查
        con.executescript(SCHEMA)
        con.commit()
    finally:
        con.close()


def get_db():
    if "db" not in g:
        con = sqlite3.connect(DB_PATH, timeout=10)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA busy_timeout=10000")
        g.db = con
    return g.db


@app.teardown_appcontext
def close_db(_exc):
    con = g.pop("db", None)
    if con is not None:
        con.close()


init_db()


# ---------------------------------------------------------------- 汇总编解码

def pack_summary(pairs):
    """(image_no, code) 序列 → 定长 2 bit/图 字节串。"""
    data = bytearray(SUMMARY_BYTES)
    for image_no, code in pairs:
        i = image_no - 1
        data[i >> 2] |= (code & 0x3) << ((i & 0x3) * 2)
    return bytes(data)


def patch_summary(blob, pairs):
    """就地修补汇总：只改本批图号对应的 2 bit，不回读任何原始标注。"""
    data = bytearray(blob if blob is not None else b"\x00" * SUMMARY_BYTES)
    for image_no, code in pairs:
        i = image_no - 1
        shift = (i & 0x3) * 2
        idx = i >> 2
        data[idx] = (data[idx] & ~(0x3 << shift)) | ((code & 0x3) << shift)
    return bytes(data)


def unpack_code(data, image_no):
    i = image_no - 1
    return (data[i >> 2] >> ((i & 0x3) * 2)) & 0x3


# ---------------------------------------------------------------- CSV 解析

class UploadError(Exception):
    pass


def parse_csv(text):
    """解析标注 CSV，返回 {image_no: code}；同图多次出现以最后一行为准。"""
    pairs = {}
    header_checked = False
    for lineno, row in enumerate(csv.reader(io.StringIO(text)), start=1):
        if not row or all(not cell.strip() for cell in row):
            continue
        if not header_checked:
            header_checked = True
            try:
                int(row[0].strip())
            except ValueError:
                continue  # 首行是表头
        if len(row) < 2:
            raise UploadError(f"第 {lineno} 行：至少需要「图号,标记」两列")
        img_text = row[0].strip()
        mark_text = row[1].strip()
        try:
            image_no = int(img_text)
        except ValueError:
            raise UploadError(f"第 {lineno} 行：图号「{img_text}」不是整数")
        if not 1 <= image_no <= MAX_IMAGE_NO:
            raise UploadError(
                f"第 {lineno} 行：图号 {image_no} 超出 1～{MAX_IMAGE_NO} 范围")
        if mark_text not in MARK_TO_CODE:
            raise UploadError(
                f"第 {lineno} 行：标记「{mark_text}」无效，"
                f"应为 有坡道 / 无坡道 / 看不清")
        pairs[image_no] = MARK_TO_CODE[mark_text]
    if not pairs:
        raise UploadError("CSV 中没有有效数据行")
    return pairs


def parse_dispatch_csv(text):
    """解析派单 CSV（每行一个图号），返回去重后升序的图号列表。"""
    images = []
    seen = set()
    header_checked = False
    for lineno, row in enumerate(csv.reader(io.StringIO(text)), start=1):
        if not row or all(not cell.strip() for cell in row):
            continue
        cell = row[0].strip()
        if not header_checked:
            header_checked = True
            try:
                int(cell)
            except ValueError:
                continue  # 首行是表头
        try:
            image_no = int(cell)
        except ValueError:
            raise UploadError(f"第 {lineno} 行：图号「{cell}」不是整数")
        if not 1 <= image_no <= MAX_IMAGE_NO:
            raise UploadError(
                f"第 {lineno} 行：图号 {image_no} 超出 1～{MAX_IMAGE_NO} 范围")
        if image_no not in seen:
            seen.add(image_no)
            images.append(image_no)
    if not images:
        raise UploadError("派单 CSV 中没有有效图号")
    return sorted(images)


# ---------------------------------------------------------------- 写事务

def _create_temp(con, table, images):
    """建临时表容纳一批图号，所有重算 SQL 都经它限定范围。"""
    con.execute(f"DROP TABLE IF EXISTS {table}")
    con.execute(f"CREATE TEMP TABLE {table} (image_no INTEGER PRIMARY KEY)")
    con.executemany(
        f"INSERT INTO {table}(image_no) VALUES (?)",
        [(i,) for i in images])


def save_annotations(con, annotator, pairs):
    """补传/更正一批标注。

    只写本批图号的原始标注；汇总就地打补丁（不回读整份原始标注）；
    核查结果只重新核验本批图号。与调用方同事务，一起提交或一起回滚。
    返回 (本批条数, 汇总版本, 汇总字节数)。
    """
    batch = sorted(pairs)
    _create_temp(con, "tmp_batch", batch)
    # 1) 本批原始标注 upsert —— 对 annotations 表只有写，没有 SELECT
    con.executemany(
        "INSERT INTO annotations (annotator, image_no, mark, updated_at) "
        "VALUES (?, ?, ?, datetime('now')) "
        "ON CONFLICT (annotator, image_no) DO UPDATE SET "
        "mark = excluded.mark, updated_at = excluded.updated_at",
        [(annotator, img, pairs[img]) for img in batch])

    # 2) 汇总就地修补
    row = con.execute(
        "SELECT version, data FROM summaries WHERE annotator = ?",
        (annotator,)).fetchone()
    blob = patch_summary(row["data"] if row is not None else None,
                         ((img, pairs[img]) for img in batch))
    if len(blob) > SUMMARY_LIMIT:
        raise UploadError(f"汇总大小 {len(blob)} 字节超过 128 KiB 上限")
    if row is None:
        version = 1
        con.execute(
            "INSERT INTO summaries (annotator, version, data, updated_at) "
            "VALUES (?, 1, ?, datetime('now'))", (annotator, blob))
    else:
        version = row["version"] + 1
        con.execute(
            "UPDATE summaries SET version = ?, data = ?, "
            "updated_at = datetime('now') WHERE annotator = ?",
            (version, blob, annotator))

    # 3) 只重算本批图号的核查结果（临时表限定，集合 SQL，无逐图 Python 遍历）
    con.execute(
        "DELETE FROM qc_results WHERE annotator = ? "
        "AND image_no IN (SELECT image_no FROM tmp_batch)", (annotator,))
    # 错交：本批有标注但不在派单内
    con.execute(
        "INSERT INTO qc_results (annotator, image_no, category, updated_at) "
        "SELECT an.annotator, an.image_no, 3, datetime('now') "
        "FROM annotations an "
        "LEFT JOIN dispatch d ON d.annotator = an.annotator "
        "AND d.image_no = an.image_no "
        "WHERE an.annotator = ? "
        "AND an.image_no IN (SELECT image_no FROM tmp_batch) "
        "AND d.image_no IS NULL", (annotator,))
    # 重拍：本批在派单内且标为看不清
    con.execute(
        "INSERT INTO qc_results (annotator, image_no, category, updated_at) "
        "SELECT an.annotator, an.image_no, 2, datetime('now') "
        "FROM annotations an "
        "JOIN dispatch d ON d.annotator = an.annotator "
        "AND d.image_no = an.image_no "
        "WHERE an.annotator = ? "
        "AND an.image_no IN (SELECT image_no FROM tmp_batch) "
        "AND an.mark = 3", (annotator,))
    # 漏判：本批在派单内但没有标注行（CSV 未覆盖到的派单图）
    con.execute(
        "INSERT INTO qc_results (annotator, image_no, category, updated_at) "
        "SELECT d.annotator, d.image_no, 1, datetime('now') "
        "FROM dispatch d "
        "WHERE d.annotator = ? "
        "AND d.image_no IN (SELECT image_no FROM tmp_batch) "
        "AND NOT EXISTS (SELECT 1 FROM annotations an "
        "WHERE an.annotator = d.annotator AND an.image_no = d.image_no)",
        (annotator,))
    con.execute("DROP TABLE tmp_batch")
    return len(batch), version, len(blob)


def _full_recompute_qc(con, annotator):
    """首次派单的全量核查（旧库标注直接参与）。按人限定，集合 SQL。"""
    con.execute("DELETE FROM qc_results WHERE annotator = ?", (annotator,))
    # 漏判：派到但没有任何标注
    con.execute(
        "INSERT INTO qc_results (annotator, image_no, category, updated_at) "
        "SELECT d.annotator, d.image_no, 1, datetime('now') "
        "FROM dispatch d "
        "LEFT JOIN annotations an ON an.annotator = d.annotator "
        "AND an.image_no = d.image_no "
        "WHERE d.annotator = ? AND an.image_no IS NULL", (annotator,))
    # 重拍：派到且标为看不清
    con.execute(
        "INSERT INTO qc_results (annotator, image_no, category, updated_at) "
        "SELECT d.annotator, d.image_no, 2, datetime('now') "
        "FROM dispatch d "
        "JOIN annotations an ON an.annotator = d.annotator "
        "AND an.image_no = d.image_no "
        "WHERE d.annotator = ? AND an.mark = 3", (annotator,))
    # 错交：有标注但没派到
    con.execute(
        "INSERT INTO qc_results (annotator, image_no, category, updated_at) "
        "SELECT an.annotator, an.image_no, 3, datetime('now') "
        "FROM annotations an "
        "LEFT JOIN dispatch d ON d.annotator = an.annotator "
        "AND d.image_no = an.image_no "
        "WHERE an.annotator = ? AND d.image_no IS NULL", (annotator,))


def replace_dispatch(con, annotator, images):
    """整份替换派单，只重新核验增减的图号（首次派单全量初始化）。

    与调用方同事务，一起提交或一起回滚。
    返回 dict：version / image_count / added / removed / 三类计数。
    """
    meta = con.execute(
        "SELECT version, image_count FROM dispatch_meta WHERE annotator = ?",
        (annotator,)).fetchone()

    if meta is None:
        # ---- 首次派单：写入整份派单 + 全量核查 ----
        con.executemany(
            "INSERT INTO dispatch (annotator, image_no, updated_at) "
            "VALUES (?, ?, datetime('now'))",
            [(annotator, i) for i in images])
        _full_recompute_qc(con, annotator)
        con.execute(
            "INSERT INTO dispatch_meta (annotator, version, image_count, "
            "updated_at) VALUES (?, 1, ?, datetime('now'))",
            (annotator, len(images)))
        version, added, removed = 1, len(images), 0
    else:
        # ---- 换单：临时表求集合差，只核验新增/移除图号 ----
        _create_temp(con, "tmp_new_dispatch", images)
        con.execute("DROP TABLE IF EXISTS tmp_removed")
        con.execute("DROP TABLE IF EXISTS tmp_added")
        con.execute(
            "CREATE TEMP TABLE tmp_removed AS "
            "SELECT d.image_no FROM dispatch d WHERE d.annotator = ? "
            "AND d.image_no NOT IN (SELECT image_no FROM tmp_new_dispatch)",
            (annotator,))
        con.execute(
            "CREATE TEMP TABLE tmp_added AS "
            "SELECT n.image_no FROM tmp_new_dispatch n "
            "WHERE n.image_no NOT IN "
            "(SELECT image_no FROM dispatch WHERE annotator = ?)",
            (annotator,))
        removed = con.execute(
            "SELECT count(*) AS n FROM tmp_removed").fetchone()["n"]
        added = con.execute(
            "SELECT count(*) AS n FROM tmp_added").fetchone()["n"]

        # 移除图：删掉原漏判/重拍结果；其中若有标注则转为错交
        con.execute(
            "DELETE FROM qc_results WHERE annotator = ? "
            "AND image_no IN (SELECT image_no FROM tmp_removed)", (annotator,))
        con.execute(
            "INSERT INTO qc_results (annotator, image_no, category, updated_at) "
            "SELECT an.annotator, an.image_no, 3, datetime('now') "
            "FROM annotations an WHERE an.annotator = ? "
            "AND an.image_no IN (SELECT image_no FROM tmp_removed)",
            (annotator,))
        # 新增图：删掉旧结果（可能是此前的错交），按当前标注重判
        con.execute(
            "DELETE FROM qc_results WHERE annotator = ? "
            "AND image_no IN (SELECT image_no FROM tmp_added)", (annotator,))
        con.execute(
            "INSERT INTO qc_results (annotator, image_no, category, updated_at) "
            "SELECT ?, a.image_no, 2, datetime('now') FROM tmp_added a "
            "JOIN annotations an ON an.annotator = ? AND an.image_no = a.image_no "
            "WHERE an.mark = 3", (annotator, annotator))
        con.execute(
            "INSERT INTO qc_results (annotator, image_no, category, updated_at) "
            "SELECT ?, a.image_no, 1, datetime('now') FROM tmp_added a "
            "WHERE NOT EXISTS (SELECT 1 FROM annotations an "
            "WHERE an.annotator = ? AND an.image_no = a.image_no)",
            (annotator, annotator))

        # 派单表同样只做差量写入
        con.execute(
            "DELETE FROM dispatch WHERE annotator = ? "
            "AND image_no IN (SELECT image_no FROM tmp_removed)", (annotator,))
        con.executemany(
            "INSERT INTO dispatch (annotator, image_no, updated_at) "
            "VALUES (?, ?, datetime('now'))",
            [(annotator, r["image_no"]) for r in con.execute(
                "SELECT image_no FROM tmp_added")])
        version = meta["version"] + 1
        con.execute(
            "UPDATE dispatch_meta SET version = ?, image_count = ?, "
            "updated_at = datetime('now') WHERE annotator = ?",
            (version, len(images), annotator))
        for t in ("tmp_new_dispatch", "tmp_removed", "tmp_added"):
            con.execute(f"DROP TABLE IF EXISTS {t}")

    counts = {row["category"]: row["n"] for row in con.execute(
        "SELECT category, count(*) AS n FROM qc_results "
        "WHERE annotator = ? GROUP BY category", (annotator,))}
    return {
        "version": version,
        "image_count": len(images),
        "added": added,
        "removed": removed,
        "missed": counts.get(CAT_MISSED, 0),
        "reshoot": counts.get(CAT_RESHOOT, 0),
        "wrong": counts.get(CAT_WRONG, 0),
    }


# ---------------------------------------------------------------- 两人核查统计

def pair_counts(data_a, data_b):
    """只对双方都判读为有/无坡道的图配对，返回四格计数与分歧图号。"""
    n11 = n10 = n01 = n00 = 0
    disagreements = []
    for image_no in range(1, MAX_IMAGE_NO + 1):
        ca = unpack_code(data_a, image_no)
        cb = unpack_code(data_b, image_no)
        if ca not in VALID_CODES or cb not in VALID_CODES:
            continue
        if ca == 1 and cb == 1:
            n11 += 1
        elif ca == 2 and cb == 2:
            n00 += 1
        elif ca == 1:
            n10 += 1
            disagreements.append(image_no)
        else:
            n01 += 1
            disagreements.append(image_no)
    return (n11, n10, n01, n00), disagreements


def cohen_kappa(n11, n10, n01, n00):
    """返回 (kappa, None) 或 (None, 无法比较的原因)。"""
    total = n11 + n10 + n01 + n00
    if total == 0:
        return None, "没有双方都判读为「有坡道/无坡道」的有效配对"
    po = (n11 + n00) / total
    pe = ((n11 + n10) * (n11 + n01) + (n01 + n00) * (n10 + n00)) / (total * total)
    denom = 1.0 - pe
    if denom == 0:
        return None, "κ 分母为零（两人边际分布完全一致）"
    return (po - pe) / denom, None


def fetch_summary(con, annotator):
    return con.execute(
        "SELECT annotator, version, data, length(data) AS size, updated_at "
        "FROM summaries WHERE annotator = ?", (annotator,)).fetchone()


def list_people(con):
    """所有出现过的标注员（有汇总或有派单的并集），升序。"""
    return [r["annotator"] for r in con.execute(
        "SELECT annotator FROM summaries "
        "UNION SELECT annotator FROM dispatch_meta ORDER BY annotator")]


# ---------------------------------------------------------------- 路由

@app.route("/")
def index():
    con = get_db()
    rows = con.execute(
        "WITH people AS ("
        "  SELECT annotator FROM summaries "
        "  UNION SELECT annotator FROM dispatch_meta"
        ") "
        "SELECT p.annotator, "
        "  s.version AS sum_version, s.updated_at AS sum_updated, "
        "  (SELECT count(*) FROM annotations a WHERE a.annotator = p.annotator) AS n, "
        "  m.version AS dispatch_version, m.image_count AS dispatch_count, "
        "  m.updated_at AS dispatch_updated, "
        "  (SELECT count(*) FROM qc_results q WHERE q.annotator = p.annotator "
        "    AND q.category = 1) AS n_missed, "
        "  (SELECT count(*) FROM qc_results q WHERE q.annotator = p.annotator "
        "    AND q.category = 2) AS n_reshoot, "
        "  (SELECT count(*) FROM qc_results q WHERE q.annotator = p.annotator "
        "    AND q.category = 3) AS n_wrong "
        "FROM people p "
        "LEFT JOIN summaries s ON s.annotator = p.annotator "
        "LEFT JOIN dispatch_meta m ON m.annotator = p.annotator "
        "ORDER BY p.annotator").fetchall()
    return render_template("index.html", annotators=rows,
                           max_image=MAX_IMAGE_NO, cat_names=CAT_NAMES)


def _check_annotator(form):
    annotator = (form.get("annotator") or "").strip()
    if not annotator:
        flash("请填写标注员姓名/工号", "error")
        return None
    if len(annotator) > 64:
        flash("标注员姓名/工号过长（不超过 64 字符）", "error")
        return None
    return annotator


def _read_uploaded_file():
    """读上传文件并解码，失败返回 (None, 错误提示)。"""
    file = request.files.get("file")
    if file is None or not file.filename:
        return None, "请选择要上传的 CSV 文件"
    try:
        return file.read().decode("utf-8-sig"), None
    except UnicodeDecodeError:
        return None, "文件不是有效的 UTF-8 编码"


@app.route("/upload", methods=["POST"])
def upload():
    annotator = _check_annotator(request.form)
    if annotator is None:
        return redirect(url_for("index"))
    text, err = _read_uploaded_file()
    if err is not None:
        flash(f"上传失败，已沿用原数据：{err}", "error")
        return redirect(url_for("index"))
    try:
        pairs = parse_csv(text)
    except UploadError as exc:
        flash(f"上传失败，已沿用原数据：{exc}", "error")
        return redirect(url_for("index"))

    con = get_db()
    try:
        # 原始标注、汇总、核查结果在同一事务中生效；只写本人本批数据
        con.execute("BEGIN IMMEDIATE")
        n_written, version, blob_size = save_annotations(con, annotator, pairs)
        con.commit()
    except UploadError as exc:
        con.rollback()
        flash(f"上传失败，已沿用原数据：{exc}", "error")
        return redirect(url_for("index"))
    except sqlite3.Error as exc:
        con.rollback()
        flash(f"上传失败，已沿用原数据：数据库错误（{exc}）", "error")
        return redirect(url_for("index"))

    flash(f"补传成功：{annotator} 本次写入 {n_written} 条，"
          f"汇总 v{version}（{blob_size} 字节），本批图号的核查结果已同步更新",
          "ok")
    return redirect(url_for("index"))


@app.route("/dispatch", methods=["POST"])
def dispatch_upload():
    annotator = _check_annotator(request.form)
    if annotator is None:
        return redirect(url_for("index"))
    text, err = _read_uploaded_file()
    if err is not None:
        flash(f"派单替换失败，已沿用原派单：{err}", "error")
        return redirect(url_for("index"))
    try:
        images = parse_dispatch_csv(text)
    except UploadError as exc:
        flash(f"派单替换失败，已沿用原派单：{exc}", "error")
        return redirect(url_for("index"))

    con = get_db()
    try:
        # 新派单与核查结果在同一事务中生效；失败回滚沿用原结果
        con.execute("BEGIN IMMEDIATE")
        info = replace_dispatch(con, annotator, images)
        con.commit()
    except sqlite3.Error as exc:
        con.rollback()
        flash(f"派单替换失败，已沿用原派单：数据库错误（{exc}）", "error")
        return redirect(url_for("index"))

    if info["version"] == 1:
        flash(f"首次派单成功：{annotator} 共 {info['image_count']} 个图号，"
              f"已全量核查 —— 漏判 {info['missed']}、重拍 {info['reshoot']}、"
              f"错交 {info['wrong']}，派单版本 v1", "ok")
    else:
        flash(f"派单已整份替换：{annotator} 现共 {info['image_count']} 个图号"
              f"（新增 {info['added']}、移除 {info['removed']}），"
              f"仅增减图号重新核验 —— 漏判 {info['missed']}、"
              f"重拍 {info['reshoot']}、错交 {info['wrong']}，"
              f"派单版本 v{info['version']}", "ok")
    return redirect(url_for("index"))


@app.route("/qc")
def qc_view():
    annotator = (request.args.get("annotator") or "").strip()
    category = request.args.get("category", default=CAT_MISSED, type=int)
    if category not in CAT_NAMES:
        abort(400)
    page = request.args.get("page", default=1, type=int)
    if page is None or page < 1:
        page = 1

    con = get_db()
    # 单读事务：三类总数与当页图号取自同一已提交快照
    con.execute("BEGIN")
    try:
        exists = con.execute(
            "SELECT (SELECT count(*) FROM summaries WHERE annotator = ?) "
            "+ (SELECT count(*) FROM dispatch_meta WHERE annotator = ?) AS c",
            (annotator, annotator)).fetchone()["c"]
        if not annotator or exists == 0:
            con.rollback()
            flash("该标注员暂无派单或标注数据", "error")
            return redirect(url_for("index"))

        totals = {CAT_MISSED: 0, CAT_RESHOOT: 0, CAT_WRONG: 0}
        for row in con.execute(
                "SELECT category, count(*) AS n FROM qc_results "
                "WHERE annotator = ? GROUP BY category", (annotator,)):
            totals[row["category"]] = row["n"]

        total = totals[category]
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        if page > total_pages:
            page = total_pages
        # 覆盖索引区间扫描：只读 qc_results，50 行，不碰原始标注与派单表
        images = [r["image_no"] for r in con.execute(
            "SELECT image_no FROM qc_results "
            "WHERE annotator = ? AND category = ? "
            "ORDER BY image_no LIMIT ? OFFSET ?",
            (annotator, category, PAGE_SIZE, (page - 1) * PAGE_SIZE))]

        meta = con.execute(
            "SELECT version, image_count FROM dispatch_meta "
            "WHERE annotator = ?", (annotator,)).fetchone()
        summ = con.execute(
            "SELECT version FROM summaries WHERE annotator = ?",
            (annotator,)).fetchone()
        people = list_people(con)
        con.commit()
    except Exception:
        con.rollback()
        raise

    return render_template(
        "qc.html", annotator=annotator, category=category,
        cat_names=CAT_NAMES, totals=totals, images=images,
        page=page, total_pages=total_pages, page_size=PAGE_SIZE,
        dispatch_version=meta["version"] if meta else None,
        dispatch_count=meta["image_count"] if meta else 0,
        summary_version=summ["version"] if summ else None,
        people=people)


@app.route("/compare")
def compare():
    a = request.args.get("a", "")
    b = request.args.get("b", "")
    if not a or not b:
        flash("请选择两位标注员", "error")
        return redirect(url_for("index"))
    if a == b:
        flash("标注员 A 与 B 不能是同一人", "error")
        return redirect(url_for("index"))

    con = get_db()
    # 单读事务：计数与分歧清单取自同一已提交状态
    con.execute("BEGIN")
    try:
        ra = fetch_summary(con, a)
        rb = fetch_summary(con, b)
        if ra is None or rb is None:
            con.rollback()
            flash("所选标注员还没有汇总数据", "error")
            return redirect(url_for("index"))
        (n11, n10, n01, n00), disagreements = pair_counts(ra["data"], rb["data"])
        kappa, kappa_error = cohen_kappa(n11, n10, n01, n00)
        result = dict(a=a, b=b, va=ra["version"], vb=rb["version"],
                      size_a=ra["size"], size_b=rb["size"],
                      n11=n11, n10=n10, n01=n01, n00=n00,
                      total=n11 + n10 + n01 + n00,
                      kappa=kappa, kappa_error=kappa_error,
                      dis_count=len(disagreements))
        con.commit()
    except Exception:
        con.rollback()
        raise
    return render_template("compare.html", **result)


@app.route("/disagreements.csv")
def disagreements():
    a = request.args.get("a", "")
    b = request.args.get("b", "")
    va = request.args.get("va", type=int)
    vb = request.args.get("vb", type=int)
    if not a or not b or va is None or vb is None:
        return "参数不完整", 400

    con = get_db()
    con.execute("BEGIN")
    try:
        ra = fetch_summary(con, a)
        rb = fetch_summary(con, b)
        if ra is None or rb is None:
            con.rollback()
            return "所选标注员还没有汇总数据", 404
        if ra["version"] != va or rb["version"] != vb:
            con.rollback()
            return "数据已更新，本次核查结果已失效，请重新核查后再下载", 409
        _, images = pair_counts(ra["data"], rb["data"])
        out = io.StringIO()
        writer = csv.writer(out)
        writer.writerow(["图号", f"{a}的判读", f"{b}的判读"])
        for image_no in images:
            writer.writerow([
                image_no,
                CODE_TO_MARK[unpack_code(ra["data"], image_no)],
                CODE_TO_MARK[unpack_code(rb["data"], image_no)],
            ])
        con.commit()
    except Exception:
        con.rollback()
        raise

    return Response(
        out.getvalue().encode("utf-8-sig"),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; "
                                        "filename=disagreements.csv"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)
