#!/usr/bin/env python3
# =============================================================================
# CISC 886 — Cloud Computing | Project Section 4
# PySpark Preprocessing + EDA + Train/Val/Test Split
# AWS EMR | Region: us-east-1 | NetID: 25qgkp
# Dataset: StackMathQA JSONL pre-uploaded to S3
# Pipeline: Load from S3 → Clean → Filter → Dedup → Format → EDA → Split → Save
# =============================================================================

# ── Step 1: Imports ───────────────────────────────────────────────────────────
import re
import unicodedata
import warnings

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")           # headless backend — required on EMR (no display)
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import boto3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

warnings.filterwarnings("ignore")

# SparkSession — on EMR this is pre-created; builder is a safe no-op
spark = (SparkSession.builder
         .appName("25qgkp-preprocessing-eda")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print(f"Spark version : {spark.version}")
print(f"App name      : {spark.sparkContext.appName}")

# ── Step 2: S3 paths and configuration ───────────────────────────────────────
NET_ID = "25qgkp"
BUCKET = f"s3://{NET_ID}-all-data"
REGION = "us-east-1"

S3_INPUT      = f"{BUCKET}/raw/all.jsonl"
S3_CLEAN      = f"{BUCKET}/processed/clean_data"
S3_EDA        = f"{BUCKET}/eda/eda_report.png"
S3_TRAIN      = f"{BUCKET}/splits/train"
S3_VALIDATION = f"{BUCKET}/splits/validation"
S3_TEST       = f"{BUCKET}/splits/test"

PREP_CFG = {
    "min_question_len":   40,
    "min_answer_len":     100,    
    "max_question_len":   1500,
    "max_answer_len":     4000,
    "min_question_score": 5,
}

SPLIT_CFG = {
    "train_ratio": 0.80,
    "val_ratio":   0.10,
    "test_ratio":  0.10,
    "seed":        42,
}

print("Configuration:")
print(f"  Input    : {S3_INPUT}")
print(f"  Clean    : {S3_CLEAN}")
print(f"  Region   : {REGION}")
print(f"  Train    : {S3_TRAIN}")
print(f"  Val      : {S3_VALIDATION}")
print(f"  Test     : {S3_TEST}")

# ── Step 2b: Input requirement ────────────────────────────────────────────────
# IMPORTANT: This EMR job expects the raw dataset to already exist in S3.
# Upload your local JSONL file before running this step:
#
#   aws s3 cp all.jsonl s3://25qgkp-all-data/raw/all.jsonl
#
# The JSONL file must contain fields: Q, A, and optionally meta.
print("Input requirement:")
print(f"  Expected raw JSONL at: {S3_INPUT}")
print("  This job does not download from HuggingFace or install packages inside EMR.")

# =============================================================================
# PHASE 1 — PREPROCESSING
# =============================================================================
print("=" * 55)
print("  PHASE 1 — PREPROCESSING")
print("=" * 55)

# ── Step 3: Load raw JSONL from S3 ───────────────────────────────────────────
print(f"\n[1/7] Loading from {S3_INPUT} ...")
raw_df = spark.read.json(S3_INPUT)
raw_df.printSchema()

required_cols = {"Q", "A"}
missing_cols = required_cols - set(raw_df.columns)
if missing_cols:
    raise ValueError(
        f"Input JSONL is missing required columns: {sorted(missing_cols)}. "
        "Expected Q and A fields."
    )

total_raw = raw_df.count()
print(f"  Loaded : {total_raw:,} raw records")

# Make optional metadata fields safe even if local JSONL omits meta or some nested keys.
if "meta" not in raw_df.columns:
    raw_df = raw_df.withColumn("meta", F.struct(
        F.lit(None).cast(StringType()).alias("url"),
        F.lit(None).cast(StringType()).alias("answer_id"),
        F.lit(None).cast(IntegerType()).alias("question_score"),
        F.lit(None).cast(IntegerType()).alias("answer_count"),
    ))
else:
    meta_fields = (
        set(raw_df.schema["meta"].dataType.fieldNames())
        if hasattr(raw_df.schema["meta"].dataType, "fieldNames")
        else set()
    )
    if "url" not in meta_fields:
        raw_df = raw_df.withColumn(
            "meta", F.struct(F.lit(None).cast(StringType()).alias("url"), F.col("meta.*"))
        )
    if "answer_id" not in meta_fields:
        raw_df = raw_df.withColumn(
            "meta", F.struct(F.lit(None).cast(StringType()).alias("answer_id"), F.col("meta.*"))
        )
    if "question_score" not in meta_fields:
        raw_df = raw_df.withColumn(
            "meta", F.struct(F.lit(None).cast(IntegerType()).alias("question_score"), F.col("meta.*"))
        )
    if "answer_count" not in meta_fields:
        raw_df = raw_df.withColumn(
            "meta", F.struct(F.lit(None).cast(IntegerType()).alias("answer_count"), F.col("meta.*"))
        )

# ── Step 4: Drop empty / null Q or A ─────────────────────────────────────────
print("\n[2/7] Dropping empty or null Q / A ...")
df = raw_df.filter(
    F.col("Q").isNotNull() & (F.trim(F.col("Q")) != "") &
    F.col("A").isNotNull() & (F.trim(F.col("A")) != "")
)
after_drop_empty = df.count()
dropped_empty    = total_raw - after_drop_empty
print(f"  Dropped (empty Q or A) : {dropped_empty:,}")
print(f"  Remaining              : {after_drop_empty:,}")

# ── Step 5: Text-cleaning UDFs ───────────────────────────────────────────────
def _clean_text(text: str) -> str:
    """Normalize encoding, strip HTML tags, collapse excess blank lines."""
    if not text:
        return ""
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r"<[^>]+>", " ", text)          # strip HTML tags
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.rstrip() for ln in text.split("\n")]
    result, blank = [], 0
    for ln in lines:
        if ln == "":
            blank += 1
            if blank <= 2:
                result.append(ln)
        else:
            blank = 0
            result.append(ln)
    return "\n".join(result).strip()


def _normalize_latex(text: str) -> str:
    """Convert LaTeX bracket notation to $ / $$ and strip zero-width chars."""
    if not text:
        return ""
    text = re.sub(r'\\\[', '$$', text)  # $$ is used as both open and close for display math
    text = re.sub(r'\\\]', '$$', text)
    text = re.sub(r'\\\(', '$',  text)
    text = re.sub(r'\\\)', '$',  text)
    text = re.sub(r'[\u200b\u200c\u200d\ufeff]', '', text)
    return text


def _clean_and_normalize(text: str) -> str:
    return _normalize_latex(_clean_text(text))


clean_udf = F.udf(_clean_and_normalize, StringType())
print("UDFs registered: clean_udf")

# ── Step 6: Apply cleaning ───────────────────────────────────────────────────
print("[3/7] Cleaning text and normalizing LaTeX ...")
df = (df
      .withColumn("Q", clean_udf(F.col("Q")))
      .withColumn("A", clean_udf(F.col("A"))))
print("  Text cleaning and LaTeX normalization applied.")

# ── Step 7: Filter by character length ───────────────────────────────────────
print("\n[4/7] Filtering by character length ...")
df = (df
      .withColumn("q_len", F.length(F.col("Q")))
      .withColumn("a_len", F.length(F.col("A"))))

before_len = df.count()
df = df.filter(
    (F.col("q_len") >= PREP_CFG["min_question_len"]) &
    (F.col("q_len") <= PREP_CFG["max_question_len"]) &
    (F.col("a_len") >= PREP_CFG["min_answer_len"])   &
    (F.col("a_len") <= PREP_CFG["max_answer_len"])
)
after_len   = df.count()
dropped_len = before_len - after_len
print(f"  Dropped by length : {dropped_len:,}")
print(f"  Remaining         : {after_len:,}")

# ── Step 8: Filter by question score ─────────────────────────────────────────
print("\n[5/7] Filtering by question score ...")
before_score = after_len
df = df.filter(
    F.coalesce(F.col("meta.question_score").cast(IntegerType()), F.lit(0))
    >= PREP_CFG["min_question_score"]
)
after_score   = df.count()
dropped_score = before_score - after_score
print(f"  min_question_score : {PREP_CFG['min_question_score']}")
print(f"  Dropped by score   : {dropped_score:,}")
print(f"  Remaining          : {after_score:,}")

# ── Step 9: Deduplicate ───────────────────────────────────────────────────────
print("\n[6/7] Deduplicating ...")
before_dedup = df.count()

# Notebook dedup logic: drop if uid OR content_hash seen before (not AND).
# Spark doesn't have a native OR-dedup, so we do two sequential dropDuplicates passes.
df = df.withColumn(
    "uid", # records with null url AND null answer_id all share uid "|" — these are deduped to 1 row.
    F.concat_ws("|",
        F.coalesce(F.col("meta.url"), F.lit("")),
        F.coalesce(F.col("meta.answer_id").cast(StringType()), F.lit(""))
    )
)
df = df.withColumn(
    "content_hash",
    F.md5(F.concat(
        F.substring(F.col("Q"), 1, 200),
        F.substring(F.col("A"), 1, 200)
    ))
)
# Pass 1: drop rows with a duplicate uid
df = df.dropDuplicates(["uid"])
# Pass 2: drop rows with a duplicate content_hash (among uid-deduped rows)
df = df.dropDuplicates(["content_hash"])

after_dedup   = df.count()
dropped_dedup = before_dedup - after_dedup
print(f"  Dropped duplicates : {dropped_dedup:,}")
print(f"  Remaining          : {after_dedup:,}")

# ── Step 10: Format to Alpaca schema ─────────────────────────────────────────
print("\n[7/7] Formatting to Alpaca schema ...")
clean_df = df.select(
    F.col("Q").alias("instruction"),
    F.lit("").alias("input"),
    F.col("A").alias("output"),
    F.col("q_len"),
    F.col("a_len"),
    F.coalesce(F.col("meta.question_score").cast(IntegerType()), F.lit(0)).alias("q_score"),
    F.coalesce(F.col("meta.answer_count").cast(IntegerType()),   F.lit(0)).alias("a_count"),
)

n_clean   = clean_df.count()
retention = 100.0 * n_clean / total_raw if total_raw else 0

(clean_df
 .select("instruction", "input", "output")
 .write.mode("overwrite")
 .json(S3_CLEAN))

print(f"  Final clean rows : {n_clean:,}")
print(f"  Retention rate   : {retention:.1f}%")
print(f"  Saved → {S3_CLEAN}")

# Print preprocessing summary table
print(f"\n{'─'*52}")
print(f"  {'Raw input':<35} {total_raw:>10,}")
print(f"\n  ── Drop empty Q/A ──")
print(f"    dropped: {dropped_empty:,}")
print(f"\n  ── Length filter ──")
print(f"    dropped: {dropped_len:,}")
print(f"\n  ── Quality filter ──")
print(f"    dropped: {dropped_score:,}")
print(f"\n  ── Deduplication ──")
print(f"    dropped: {dropped_dedup:,}")
print(f"\n  {'Final clean rows':<35} {n_clean:>10,}")
print(f"  {'Retention rate':<35} {retention:>9.1f}%")
print(f"{'─'*52}")

# =============================================================================
# PHASE 2 — EDA
# =============================================================================
print("=" * 55)
print("  PHASE 2 — EDA")
print("=" * 55)

# Compute EDA columns in Spark, then collect to Pandas for plotting
eda_spark = (clean_df
    .withColumn("combined", F.concat(F.col("instruction"), F.col("output")))
    .withColumn(
        "latex_total",
        F.greatest(
            F.lit(0),
            F.size(F.split(F.col("combined"), "\\$")) - F.lit(1)
        )
    )
    .withColumn(
        "has_display",
        F.col("combined").rlike("\\$\\$").cast("boolean")
    )
    .select("q_len", "a_len", "q_score", "a_count", "latex_total", "has_display")
)

eda_pd = eda_spark.toPandas()
if eda_pd.empty:
    raise ValueError(
        "No rows remain after preprocessing filters. "
        "Relax PREP_CFG thresholds or check input data."
    )

print(f"  Rows analysed      : {len(eda_pd):,}")
print(f"  Q length — mean: {eda_pd['q_len'].mean():.0f}   median: {eda_pd['q_len'].median():.0f}   max: {eda_pd['q_len'].max():.0f}")
print(f"  A length — mean: {eda_pd['a_len'].mean():.0f}  median: {eda_pd['a_len'].median():.0f}  max: {eda_pd['a_len'].max():.0f}")
print(f"  Q score  — mean: {eda_pd['q_score'].mean():.1f}  median: {eda_pd['q_score'].median():.0f}  max: {eda_pd['q_score'].max():.0f}")
print(f"  LaTeX/row mean : {eda_pd['latex_total'].mean():.1f}  median: {eda_pd['latex_total'].median():.0f}  max: {eda_pd['latex_total'].max():.0f}")
print(f"  Rows no LaTeX  : {(eda_pd['latex_total'] == 0).sum():,}")

# ── Plot palette ──────────────────────────────────────────────────────────────
C = {
    "blue":   "#2563EB", "teal":  "#0D9488",
    "coral":  "#E85D42", "amber": "#D97706",
    "purple": "#7C3AED", "gray":  "#6B7280",
    "pass":   "#16A34A", "fail":  "#DC2626",
    "text":   "#111827", "sub":   "#6B7280",
    "border": "#E5E7EB",
}

plt.rcParams.update({
    "font.family":       "DejaVu Sans",
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "axes.edgecolor":    C["border"],
    "axes.labelcolor":   C["text"],
    "xtick.color":       C["sub"],
    "ytick.color":       C["sub"],
    "grid.color":        C["border"],
    "grid.linestyle":    "--",
    "grid.alpha":        0.6,
    "figure.facecolor":  "white",
    "axes.facecolor":    "white",
})


def _fmt(n):
    return f"{n:,.0f}" if n >= 1000 else f"{n:.0f}"


# ── Plot 1: Text length distribution ─────────────────────────────────────────
def _plot_length(ax, df):
    q99_q = df["q_len"].quantile(0.99)
    q99_a = df["a_len"].quantile(0.99)
    bins  = np.linspace(0, max(q99_q, q99_a), 40)

    ax.hist(df["q_len"].clip(upper=q99_q), bins=bins, alpha=0.65,
            color=C["blue"],  label="Question", edgecolor="white", linewidth=0.3)
    ax.hist(df["a_len"].clip(upper=q99_a), bins=bins, alpha=0.65,
            color=C["coral"], label="Answer",   edgecolor="white", linewidth=0.3)

    for val, col, lbl in [
        (df["q_len"].mean(), C["blue"],  f"Q mean {_fmt(df['q_len'].mean())} ch"),
        (df["a_len"].mean(), C["coral"], f"A mean {_fmt(df['a_len'].mean())} ch"),
    ]:
        ax.axvline(val, color=col, linewidth=1.4, linestyle="--", alpha=0.9)
        ylim = ax.get_ylim()
        ax.text(val + max(bins) * 0.01, (ylim[1] - ylim[0]) * 0.5,
                lbl, color=col, fontsize=7.5, va="center")

    ax.axvspan(4000, bins[-1], alpha=0.07, color=C["fail"], zorder=0)
    ax.text(max(bins) * 0.92, 0, "truncation\nrisk",
            color=C["fail"], fontsize=7, va="bottom", ha="center")

    ax.set_xlabel("text length (characters)", fontsize=9)
    ax.set_ylabel("frequency", fontsize=9)
    ax.set_title("Plot 1 — Text length distribution  (Q vs A)",
                 fontsize=10, fontweight="bold", pad=8)
    ax.legend(fontsize=8.5, frameon=False)
    ax.yaxis.grid(True)
    ax.set_axisbelow(True)


# ── Plot 2: Question score vs data quality proxy ──────────────────────────────
def _plot_score(ax, df):
    max_score  = df["q_score"].max() + 1
    all_bins   = [0, 1, 5, 10, 50, 200, max_score]
    all_labels = ["0", "1-4", "5-9", "10-49", "50-199", "200+"]
    bins = sorted(set(b for b in all_bins if b <= max_score))
    if bins[-1] < max_score:
        bins.append(max_score)
    labels = all_labels[:len(bins) - 1]

    df = df.copy()
    df["score_bin"] = pd.cut(df["q_score"], bins=bins, labels=labels, right=False)
    grouped = (df.groupby("score_bin", observed=False)
               .agg(count=("q_score", "size"), avg_a_len=("a_len", "mean"))
               .reset_index())
    grouped["avg_a_len"] = grouped["avg_a_len"].fillna(0)
    tick_labels = grouped["score_bin"].astype(str).tolist()

    x, w = np.arange(len(grouped)), 0.35
    ax.bar(x - w/2, grouped["avg_a_len"], width=w, color=C["teal"],   alpha=0.85, zorder=2)
    ax2 = ax.twinx()
    ax2.bar(x + w/2, grouped["count"],    width=w, color=C["purple"], alpha=0.60, zorder=2)

    ax.set_ylabel("avg answer length (chars)", color=C["teal"],   fontsize=9)
    ax2.set_ylabel("row count",                color=C["purple"], fontsize=9)
    ax.tick_params(axis="y", colors=C["teal"])
    ax2.tick_params(axis="y", colors=C["purple"])
    ax.set_xticks(x)
    ax.set_xticklabels(tick_labels, fontsize=9)
    ax.set_xlabel("question score bucket", fontsize=9)
    ax.set_title("Plot 2 — Question score vs data quality proxy",
                 fontsize=10, fontweight="bold", pad=8)

    thresh = "5-9"
    if thresh in tick_labels:
        idx = tick_labels.index(thresh)
        ax.axvline(idx - 0.5, color=C["pass"], linewidth=1.5, linestyle="--", zorder=3)
        ax.text(idx - 0.38, ax.get_ylim()[1] * 0.85,
                "applied\nmin score = 5", color=C["pass"], fontsize=7.5)

    handles = [mpatches.Patch(color=C["teal"],   label="avg answer length"),
               mpatches.Patch(color=C["purple"], label="row count")]
    ax.legend(handles=handles, fontsize=8, frameon=False, loc="upper right")
    ax.yaxis.grid(True, alpha=0.4)
    ax.set_axisbelow(True)
    ax2.spines["right"].set_visible(True)
    ax2.spines["right"].set_color(C["border"])


# ── Plot 3: LaTeX density ─────────────────────────────────────────────────────
def _plot_latex(ax, df):
    max_val = df["latex_total"].max()
    bins_e  = [-0.5, 0.5, 2.5, 6.5, 14.5, 30.5, max_val + 1]
    labels  = ["0\n(none)", "1-2\n(minimal)", "3-6\n(light)",
               "7-14\n(moderate)", "15-30\n(heavy)", "30+\n(very heavy)"]
    colors  = [C["gray"], C["teal"], C["blue"], C["amber"], C["coral"], C["fail"]]
    counts  = []
    for i in range(len(labels)):
        lo = bins_e[i]   + 0.5
        hi = bins_e[i+1] + 0.5
        counts.append(int(((df["latex_total"] >= lo) & (df["latex_total"] < hi)).sum()))

    bars = ax.bar(labels, counts, color=colors, edgecolor="white", linewidth=0.4, zorder=2)
    total = len(df)
    for bar, cnt in zip(bars, counts):
        pct = 100 * cnt / total if total else 0
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + total * 0.002,
                f"{_fmt(cnt)}\n({pct:.1f}%)",
                ha="center", va="bottom", fontsize=7.5, color=C["text"])

    ax2 = ax.twinx()
    ax2.set_ylim(0, 100)
    ax2.set_ylabel("(reference)", color=C["purple"], fontsize=8, alpha=0.5)
    pct_display = df["has_display"].mean() * 100
    ax2.axhline(pct_display, color=C["purple"], linewidth=1.5, linestyle=":", alpha=0.7)
    ax2.text(5.4, pct_display + 1,
             f"{pct_display:.0f}% use display-math block equations",
             color=C["purple"], fontsize=7.5, ha="right")
    ax2.tick_params(axis="y", colors=C["purple"], labelsize=7)

    ax.set_ylabel("row count", fontsize=9)
    ax.set_xlabel("LaTeX $ delimiter count per Q+A pair", fontsize=9)
    ax.set_title("Plot 3 — LaTeX density  (tokenizer readiness check)",
                 fontsize=10, fontweight="bold", pad=8)
    ax.yaxis.grid(True, alpha=0.5)
    ax.set_axisbelow(True)


# ── Render and upload EDA ─────────────────────────────────────────────────────
fig, axes = plt.subplots(3, 1, figsize=(13, 18))
fig.suptitle(
    f"EDA Report — Math QA Dataset  |  raw={total_raw:,}  clean={len(eda_pd):,} rows",
    fontsize=13, fontweight="bold", color=C["text"], y=0.998
)
fig.subplots_adjust(hspace=0.45, top=0.97, bottom=0.04, left=0.09, right=0.93)

_plot_length(axes[0], eda_pd)
_plot_score (axes[1], eda_pd)
_plot_latex (axes[2], eda_pd)

LOCAL_EDA = "/tmp/eda_report.png"
fig.savefig(LOCAL_EDA, dpi=160, bbox_inches="tight", facecolor="white")
plt.close(fig)
print(f"  EDA saved locally → {LOCAL_EDA}")

BUCKET_NAME = f"{NET_ID}-all-data"
s3_client   = boto3.client("s3", region_name=REGION)
s3_client.upload_file(LOCAL_EDA, BUCKET_NAME, "eda/eda_report.png")
print(f"  EDA uploaded → s3://{BUCKET_NAME}/eda/eda_report.png")

# =============================================================================
# PHASE 3 — TRAIN / VALIDATION / TEST SPLIT
# =============================================================================
print("=" * 55)
print("  PHASE 3 — TRAIN / VALIDATION / TEST SPLIT")
print("=" * 55)

SEED    = SPLIT_CFG["seed"]
TRAIN_R = SPLIT_CFG["train_ratio"]
VAL_R   = SPLIT_CFG["val_ratio"]
TEST_R  = SPLIT_CFG["test_ratio"]

final_df = clean_df.select("instruction", "input", "output")

train_df, val_df, test_df = final_df.randomSplit(
    [TRAIN_R, VAL_R, TEST_R],
    seed=SEED
)

train_count = train_df.count()
val_count   = val_df.count()
test_count  = test_df.count()
total_split = train_count + val_count + test_count

print(f"\n  Split ratios  : Train={TRAIN_R:.0%}  Val={VAL_R:.0%}  Test={TEST_R:.0%}")
print(f"  Random seed   : {SEED}  (fixed for reproducibility)")
print(f"  Train         : {train_count:,}")
print(f"  Validation    : {val_count:,}")
print(f"  Test          : {test_count:,}")
print(f"  Total         : {total_split:,}")

assert abs(total_split - n_clean) <= 5, \
    f"Split total {total_split:,} drifts from clean count {n_clean:,}"
print(f"\n  Data leakage check PASSED ✅")

# ── Save splits ───────────────────────────────────────────────────────────────
train_df.write.mode("overwrite").json(S3_TRAIN)
print(f"  Train      saved → {S3_TRAIN}  ({train_count:,} rows)")

val_df.write.mode("overwrite").json(S3_VALIDATION)
print(f"  Validation saved → {S3_VALIDATION}  ({val_count:,} rows)")

test_df.write.mode("overwrite").json(S3_TEST)
print(f"  Test       saved → {S3_TEST}  ({test_count:,} rows)")

# =============================================================================
# FINAL SUMMARY
# =============================================================================
print("\n" + "=" * 60)
print("  PIPELINE COMPLETE — FULL SUMMARY")
print("=" * 60)
print(f"  NetID             : {NET_ID}")
print(f"  AWS Region        : {REGION}")
print(f"  EMR App name      : {spark.sparkContext.appName}")
print()
print(f"  ── Preprocessing ──────────────────────────────────────")
print(f"  Raw rows          : {total_raw:,}")
print(f"  Dropped (empty)   : {dropped_empty:,}")
print(f"  Dropped (length)  : {dropped_len:,}")
print(f"  Dropped (score)   : {dropped_score:,}")
print(f"  Dropped (dups)    : {dropped_dedup:,}")
print(f"  Final clean rows  : {n_clean:,}")
print(f"  Retention rate    : {100*n_clean/total_raw:.1f}%")
print()
print(f"  ── S3 Outputs ─────────────────────────────────────────")
print(f"  Clean data        → {S3_CLEAN}")
print(f"  EDA report        → s3://{NET_ID}-all-data/eda/eda_report.png")
print(f"  Train split       → {S3_TRAIN}  ({train_count:,} rows)")
print(f"  Validation split  → {S3_VALIDATION}  ({val_count:,} rows)")
print(f"  Test split        → {S3_TEST}  ({test_count:,} rows)")
print("=" * 60)
