#!/usr/bin/env python3
"""Collect cluster and upstream-table context for a PySpark optimization case.

This script does not require direct cluster access. It inspects the local case
folder, extracts whatever evidence is already available, and prints fallback
queries/commands for anything missing.
"""

from __future__ import annotations

import argparse
import html
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Sequence


RESOURCE_KEYS = [
    "spark.executor.instances",
    "spark.executor.cores",
    "spark.executor.memory",
    "spark.executor.memoryOverhead",
    "spark.dynamicAllocation.enabled",
    "spark.dynamicAllocation.minExecutors",
    "spark.dynamicAllocation.maxExecutors",
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.localShuffleReader.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.default.parallelism",
    "spark.sql.files.maxPartitionBytes",
    "spark.eventLog.dir",
]

TABLE_NAME_PREFIXES = (
    "ads_",
    "cdm_",
    "dim_",
    "dwd_",
    "mbs_",
    "fct_",
    "mid_",
    "ods_",
    "tmp_",
    "bmart_",
    "fmart_",
)


@dataclass
class CaseContext:
    case_dir: str
    summary: str
    spark_resources: Dict[str, str]
    upstream_tables: List[str]
    source_files: List[str]
    spark_ui_files: List[str]
    missing_cluster_keys: List[str]
    cluster_fallbacks: Dict[str, str]
    table_queries: Dict[str, List[str]]


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


def _extract_markdown_table_rows(text: str) -> List[List[str]]:
    rows: List[List[str]] = []
    for raw_line in text.splitlines():
        line = html.unescape(raw_line).strip()
        if not line.startswith("|") or line.count("|") < 2:
            continue
        cells = [_normalize_space(cell) for cell in line.strip("|").split("|")]
        if len(cells) < 2:
            continue
        if all(re.fullmatch(r"[:\-\s]+", cell or "") for cell in cells):
            continue
        rows.append(cells)
    return rows


def _extract_environment_pairs(text: str) -> Dict[str, str]:
    pairs: Dict[str, str] = {}
    html_pairs = re.findall(r"<tr><td>(.*?)</td><td>(.*?)</td></tr>", text, flags=re.S)
    if html_pairs:
        for key, value in html_pairs:
            key_norm = _normalize_space(key)
            value_norm = _normalize_space(re.sub(r"<.*?>", "", value))
            if key_norm:
                pairs[key_norm] = value_norm
        return pairs

    markdown_rows = _extract_markdown_table_rows(text)
    for row in markdown_rows:
        if len(row) < 2:
            continue
        left, right = row[0], row[1]
        if left in {"Key", "Name", "Metric"} and right in {"Value", "Description"}:
            continue
        if left.startswith(("Jobs", "Stages", "Executors", "SQL", "Environment", "Storage")):
            continue
        pairs[left] = right
    if pairs:
        return pairs

    # Browser-copied Spark UI pages are usually tab-delimited or one-row-per-line.
    # Parse any line with at least one tab as a key/value pair.
    for raw_line in text.splitlines():
        line = html.unescape(raw_line).strip()
        if "\t" not in line:
            continue
        left, right = [part.strip() for part in line.split("\t", 1)]
        if not left or not right:
            continue
        if left in {"Name", "Value"}:
            continue
        if left.startswith(("Jobs", "Stages", "Executors", "SQL", "Environment", "Storage")):
            continue
        pairs[left] = right
    return pairs


def _find_files(root: Path, patterns: Sequence[str]) -> List[Path]:
    files: List[Path] = []
    for pattern in patterns:
        files.extend(root.rglob(pattern))
    unique = []
    seen = set()
    for path in sorted(files):
        if path not in seen:
            seen.add(path)
            unique.append(path)
    return unique


def _detect_tables_from_source(source_root: Path) -> List[str]:
    tables = set()
    py_files = list(source_root.rglob("*.py"))
    for path in py_files:
        text = _read_text(path)
        if "self.table" in text:
            for match in re.findall(r"f'\{db_prefix\}([^']+)'", text):
                if _looks_like_table(match):
                    tables.add(match)
            for match in re.findall(r'f"\{db_prefix\}([^"]+)"', text):
                if _looks_like_table(match):
                    tables.add(match)
            for match in re.findall(r"['\"]\w+['\"]\s*:\s*f['\"]\{db_prefix\}([^'\"]+)['\"]", text):
                if _looks_like_table(match):
                    tables.add(match)
        for match in re.findall(r"\bFROM\s+([A-Za-z0-9_\.]+)", text, flags=re.I):
            if _looks_like_table(match):
                tables.add(match)
        for match in re.findall(r"\bJOIN\s+([A-Za-z0-9_\.]+)", text, flags=re.I):
            if _looks_like_table(match):
                tables.add(match)
    return sorted(tables)


def _looks_like_table(name: str) -> bool:
    parts = name.split(".")
    if len(parts) != 2:
        return False
    return parts[1].startswith(TABLE_NAME_PREFIXES)


def _build_table_queries(table: str, source_texts: Iterable[str]) -> List[str]:
    text_joined = "\n".join(source_texts)
    partition_field = "pt_date" if "pt_date" in text_joined else "partition_date"
    queries = [
        f"DESC FORMATTED {table};",
        f"SHOW PARTITIONS {table};",
        f"SELECT COUNT(1) AS row_cnt FROM {table} WHERE {partition_field} BETWEEN '<start>' AND '<end>';",
        f"SELECT COUNT(1) AS row_cnt FROM {table};",
        f"-- After DESC FORMATTED {table}, copy Location and run:\n-- hdfs dfs -du -s -h <location>\n-- hdfs dfs -count -q <location>",
    ]
    return queries


def collect_case_context(case_dir: Path) -> CaseContext:
    source_root = case_dir / "source"
    spark_ui_root = case_dir / "spark_ui"

    source_files = [str(path.relative_to(case_dir)) for path in _find_files(source_root, ["*.py"])]
    spark_ui_text_files = _find_files(spark_ui_root, ["*.html", "*.htm", "*.txt", "*.md", "*.json"])
    spark_ui_files = [str(path.relative_to(case_dir)) for path in spark_ui_text_files]

    source_texts = [_read_text(path) for path in _find_files(source_root, ["*.py"])]
    upstream_tables = _detect_tables_from_source(source_root)

    spark_resources: Dict[str, str] = {}
    for path in spark_ui_text_files:
        pairs = _extract_environment_pairs(_read_text(path))
        for key, value in pairs.items():
            if key in RESOURCE_KEYS:
                spark_resources[key] = value

    missing_cluster_keys = [key for key in RESOURCE_KEYS if key not in spark_resources]
    cluster_fallbacks = {
        key: f"grep -R \"{key}\" <spark-submit-log-or-dag-config>"
        for key in missing_cluster_keys
    }

    table_queries = {table: _build_table_queries(table, source_texts) for table in upstream_tables}

    summary = (
        "诊断前先整理集群资源形态和上游表上下文。"
        "优先使用 Spark 界面 Environment 中已有的数据；缺失指标则输出可直接执行的 SQL / HDFS 兜底命令。"
    )

    return CaseContext(
        case_dir=str(case_dir),
        summary=summary,
        spark_resources={k: spark_resources[k] for k in RESOURCE_KEYS if k in spark_resources},
        upstream_tables=upstream_tables,
        source_files=source_files,
        spark_ui_files=spark_ui_files,
        missing_cluster_keys=missing_cluster_keys,
        cluster_fallbacks=cluster_fallbacks,
        table_queries=table_queries,
    )


def render_markdown(ctx: CaseContext) -> str:
    lines: List[str] = []
    lines.append("# 上下文采集报告")
    lines.append("")
    lines.append(f"Case: `{Path(ctx.case_dir).name}`")
    lines.append("")
    lines.append("## 总结")
    lines.append("")
    lines.append(ctx.summary)
    lines.append("")
    lines.append("## 已提取的集群资源")
    lines.append("")
    if ctx.spark_resources:
        lines.append("| Key | Value |")
        lines.append("|---|---|")
        for key, value in ctx.spark_resources.items():
            lines.append(f"| `{key}` | `{value}` |")
    else:
        lines.append("未从 Spark 界面 Environment 数据中提取到集群资源。")
    lines.append("")
    lines.append("## 缺失资源的查询命令")
    lines.append("")
    if ctx.missing_cluster_keys:
        for key in ctx.missing_cluster_keys:
            lines.append(f"- `{key}` -> `{ctx.cluster_fallbacks[key]}`")
    else:
        lines.append("- None")
    lines.append("")
    lines.append("## 上游表")
    lines.append("")
    if ctx.upstream_tables:
        for table in ctx.upstream_tables:
            lines.append(f"- `{table}`")
    else:
        lines.append("- 源码中未检测到上游表")
    lines.append("")
    lines.append("## 查询模板")
    lines.append("")
    if ctx.table_queries:
        for table, queries in ctx.table_queries.items():
            lines.append(f"### `{table}`")
            lines.append("")
            for query in queries:
                lines.append("```sql" if query.endswith(";") and not query.startswith("--") else "```text")
                lines.append(query)
                lines.append("```")
            lines.append("")
    else:
        lines.append("未检测到上游表。")
    lines.append("## 源文件")
    lines.append("")
    for rel in ctx.source_files:
        lines.append(f"- `{rel}`")
    lines.append("")
    lines.append("## Spark 界面文件")
    lines.append("")
    for rel in ctx.spark_ui_files:
        lines.append(f"- `{rel}`")
    lines.append("")
    lines.append("## 原始 JSON")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(asdict(ctx), indent=2, ensure_ascii=False))
    lines.append("```")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect case context for PySpark optimization.")
    parser.add_argument("case_dir", type=Path, help="Path to input/<case_name>")
    parser.add_argument("--output", type=Path, help="Optional markdown output path")
    args = parser.parse_args()

    ctx = collect_case_context(args.case_dir)
    report = render_markdown(ctx)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
    else:
        print(report, end="")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
