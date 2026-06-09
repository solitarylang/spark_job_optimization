#!/usr/bin/env python3
"""Collect Spark UI pages from an authenticated Chrome session.

This helper is meant for Spark UI links that are reachable only through the
user's logged-in Chrome session. It opens the target Spark UI pages in Chrome,
extracts the page DOM, preserves table-like blocks in Markdown where possible,
and saves the result as reusable artifacts under the case folder.

The collection strategy is intentionally simple and reproducible:

1. Open the Spark UI base URL in Chrome.
2. Visit the key pages one by one.
3. Extract the page DOM with table preservation when possible.
4. Save page Markdown, preserved table-like blocks, title, and URL to `spark_ui/browser/`.
5. If the output directory already exists, overwrite the previous browser snapshot.

The output is designed so `collect_case_context.py` can consume it later.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
import shutil
from dataclasses import dataclass, asdict
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


DEFAULT_PAGES = ["environment", "jobs", "stages", "executors", "sql"]


@dataclass
class PageArtifact:
    page: str
    url: str
    title: str
    file_name: str
    kind: str = "page"


@dataclass
class DetailArtifact(PageArtifact):
    parent_page: str = ""


def _run_osascript(script: str) -> str:
    result = subprocess.run(
        ["osascript", "-e", script],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _escape_applescript_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _run_chrome_javascript(script: str) -> str:
    escaped = _escape_applescript_string(script)
    applescript = f'''
tell application "Google Chrome"
  activate
  tell active tab of front window
    execute javascript "{escaped}"
  end tell
end tell
'''
    return _run_osascript(applescript)


def _build_page_url(base_url: str, page: str) -> str:
    parsed = urlsplit(base_url)
    base_path = parsed.path.rstrip("/")
    page_path = f"{base_path}/{page}/"
    return urlunsplit((parsed.scheme, parsed.netloc, page_path, parsed.query, parsed.fragment))


def _merge_query(base_query: str, extra_params: dict[str, str]) -> str:
    merged = dict(parse_qsl(base_query, keep_blank_values=True))
    merged.update(extra_params)
    return urlencode(merged, doseq=True)


def _is_url(value: str) -> bool:
    return value.startswith("http://") or value.startswith("https://")


def _resolve_page_spec(base_url: str, page_spec: str, index: int) -> tuple[str, str]:
    if _is_url(page_spec):
        return f"url-{index:02d}", page_spec
    return page_spec, _build_page_url(base_url, page_spec)


def _open_url(url: str) -> None:
    script = f'''
tell application "Google Chrome"
  activate
  set URL of active tab of front window to "{url}"
end tell
'''
    _run_osascript(script)


def _capture_page_snapshot() -> dict[str, object]:
    js = r"""
(function() {
  const clean = (value) => (value || "").replace(/\u00a0/g, " ").replace(/\r/g, "").trim();
  const tables = Array.from(document.querySelectorAll("table")).map((table, index) => {
    const rows = Array.from(table.querySelectorAll("tr")).map((tr) => {
      return Array.from(tr.querySelectorAll("th,td")).map((cell) => clean(cell.innerText));
    }).filter((row) => row.some((cell) => cell));
    const captionNode = table.querySelector("caption");
    return {
      index: index + 1,
      caption: clean(captionNode ? captionNode.innerText : ""),
      rows: rows
    };
  }).filter((table) => table.rows.length > 0);
  return JSON.stringify({
    title: clean(document.title),
    url: clean(window.location.href),
    text: clean(document.body ? document.body.innerText : ""),
    tables: tables
  });
})()
"""
    try:
        raw = _run_chrome_javascript(js)
        snapshot = json.loads(raw)
        if isinstance(snapshot, dict):
            snapshot.setdefault("title", "")
            snapshot.setdefault("url", "")
            snapshot.setdefault("text", "")
            snapshot.setdefault("tables", [])
            return snapshot
    except Exception:
        pass
    script = r'''
tell application "Google Chrome" to activate
tell application "System Events"
  keystroke "a" using command down
  keystroke "c" using command down
end tell
'''
    _run_osascript(script)
    result = subprocess.run(["pbpaste"], check=True, capture_output=True, text=True)
    title = _run_osascript('tell application "Google Chrome" to get title of active tab of front window')
    url = _run_osascript('tell application "Google Chrome" to get URL of active tab of front window')
    return {"title": title, "url": url, "text": result.stdout, "tables": []}


def _write_page_artifact(out_dir: Path, page: str, content: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    file_path = out_dir / f"{page}.md"
    file_path.write_text(content, encoding="utf-8")
    return file_path


def _split_blocks(lines: list[str]) -> list[list[str]]:
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in lines:
        if line.strip():
            current.append(line.rstrip())
            continue
        if current:
            blocks.append(current)
            current = []
    if current:
        blocks.append(current)
    return blocks


def _parse_table_block(block: list[str]) -> list[list[str]] | None:
    rows: list[list[str]] = []
    for line in block:
        raw = line.strip()
        if "\t" in raw:
            cells = [cell.strip() for cell in raw.split("\t") if cell.strip()]
        else:
            cells = [cell.strip() for cell in re.split(r"\s{2,}", raw) if cell.strip()]
        if len(cells) >= 2:
            rows.append(cells)
    if len(rows) < 2:
        return None
    width = max(len(row) for row in rows)
    if width < 2:
        return None
    return [row + [""] * (width - len(row)) for row in rows]


def _render_table(rows: list[list[str]]) -> str:
    if not rows:
        return ""
    width = max(len(row) for row in rows)
    normalized = [row + [""] * (width - len(row)) for row in rows]
    header = normalized[0]
    body = normalized[1:]
    lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join(["---"] * width) + " |",
    ]
    for row in body:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def _render_page_markdown(page: str, title: str, url: str, snapshot: dict[str, object]) -> str:
    content = str(snapshot.get("text") or "")
    tables = snapshot.get("tables", []) or []
    parts = [
        f"# `{page}`",
        "",
        f"- 标题：`{title}`",
        f"- 链接：`{url}`",
        "",
        "## 页面内容",
        "",
    ]
    rendered_any_table = False
    if tables:
        for table_meta in tables:
            rows = table_meta.get("rows", [])
            if not rows:
                continue
            caption = str(table_meta.get("caption") or "")
            rendered = _render_table(rows)
            if rendered:
                rendered_any_table = True
                if caption:
                    parts.append(f"### {caption}")
                parts.append(rendered)
                parts.append("")
                continue
            parts.extend([
                "```text",
                "\n".join("\t".join(str(cell) for cell in row) for row in rows),
                "```",
                "",
            ])
    if not rendered_any_table:
        blocks = _split_blocks(content.splitlines())
        for block in blocks:
            table = _parse_table_block(block)
            if table:
                rendered_any_table = True
                parts.append(_render_table(table))
                parts.append("")
                continue
            parts.extend([
                "```text",
                "\n".join(block),
                "```",
                "",
            ])
    if not rendered_any_table:
        parts.insert(parts.index("## 页面内容") + 2, "未识别到可直接转成表格的块。")
    parts.extend([
        "## 原始内容",
        "",
        "```text",
        content.rstrip(),
        "```",
        "",
    ])
    return "\n".join(parts)


def _snapshot_to_search_text(snapshot: dict[str, object]) -> str:
    pieces: list[str] = [str(snapshot.get("text") or "")]
    for table_meta in snapshot.get("tables", []) or []:
        for row in table_meta.get("rows", []) or []:
            pieces.append("\t".join(str(cell) for cell in row))
    return "\n".join(piece for piece in pieces if piece)


def _reset_output_dir(out_dir: Path) -> None:
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)


def _extract_candidate_ids(content: str, page: str) -> list[str]:
    if page == "stages":
        patterns = [
            r"(?m)^\s*(\d+)\s+.*?(?:\+details|insertInto|broadcast|Listing leaf files|spark job|window|join)",
            r"(?m)^\s*(\d+)\s*$",
        ]
    elif page == "jobs":
        patterns = [
            r"(?m)^\s*(\d+)\s+\([^)]+\)\s+.*?(?:insertInto|broadcast|Listing leaf files|spark job|join|write)",
            r"(?m)^\s*(\d+)\s*$",
        ]
    else:
        return []

    candidates: list[str] = []
    seen: set[str] = set()
    for line in content.splitlines():
        raw = line.strip()
        if raw.startswith("|") and raw.count("|") >= 2:
            cells = [cell.strip() for cell in raw.strip("|").split("|")]
            if cells and cells[0].isdigit():
                value = cells[0]
                if value not in seen:
                    seen.add(value)
                    candidates.append(value)
    for pattern in patterns:
        for match in re.finditer(pattern, content):
            value = match.group(1)
            if value not in seen:
                seen.add(value)
                candidates.append(value)
    return candidates


def _stage_detail_url(base_url: str, stage_id: str, attempt: str = "0") -> str:
    parsed = urlsplit(base_url)
    base_path = parsed.path.rstrip("/")
    page_path = f"{base_path}/stages/stage/"
    page_query = _merge_query(parsed.query, {"id": stage_id, "attempt": attempt})
    return urlunsplit((parsed.scheme, parsed.netloc, page_path, page_query, parsed.fragment))


def _job_detail_url(base_url: str, job_id: str) -> str:
    parsed = urlsplit(base_url)
    base_path = parsed.path.rstrip("/")
    page_path = f"{base_path}/job/"
    page_query = _merge_query(parsed.query, {"id": job_id})
    return urlunsplit((parsed.scheme, parsed.netloc, page_path, page_query, parsed.fragment))


def _collect_detail_pages(
    base_url: str,
    out_dir: Path,
    parent_page: str,
    page_kind: str,
    ids: list[str],
    wait_seconds: float,
    limit: int = 10,
) -> list[PageArtifact]:
    detail_dir = out_dir / "details"
    detail_dir.mkdir(parents=True, exist_ok=True)
    artifacts: list[PageArtifact] = []
    for item_id in ids[:limit]:
        if page_kind == "stage":
            detail_url = _stage_detail_url(base_url, item_id)
            detail_name = f"stage-{item_id}"
        else:
            detail_url = _job_detail_url(base_url, item_id)
            detail_name = f"job-{item_id}"
        _open_url(detail_url)
        time.sleep(wait_seconds)
        snapshot = _capture_page_snapshot()
        title = str(snapshot.get("title") or detail_name)
        current_url = str(snapshot.get("url") or detail_url)
        file_path = _write_page_artifact(
            detail_dir,
            detail_name,
            _render_page_markdown(detail_name, title, current_url, snapshot),
        )
        artifacts.append(
            PageArtifact(
                page=detail_name,
                url=current_url or detail_url,
                title=title,
                file_name=str(file_path.relative_to(out_dir.parent)),
                kind=f"{page_kind}-detail",
            )
        )
    return artifacts


def collect_spark_ui(
    base_url: str,
    out_dir: Path,
    pages: list[str],
    wait_seconds: float,
    detail_limit: int,
) -> list[PageArtifact]:
    _reset_output_dir(out_dir)
    artifacts: list[PageArtifact] = []
    page_texts: dict[str, str] = {}
    for index, page in enumerate(pages, start=1):
        page_name, page_url = _resolve_page_spec(base_url, page, index)
        _open_url(page_url)
        time.sleep(wait_seconds)
        snapshot = _capture_page_snapshot()
        title = str(snapshot.get("title") or page_name)
        current_url = str(snapshot.get("url") or page_url)
        rendered = _render_page_markdown(page_name, title, current_url, snapshot)
        file_path = _write_page_artifact(out_dir, page_name, rendered)
        page_texts[page_name] = _snapshot_to_search_text(snapshot)
        artifacts.append(
            PageArtifact(
                page=page_name,
                url=current_url or page_url,
                title=title,
                file_name=str(file_path.relative_to(out_dir.parent)),
            )
        )
    if "jobs" in page_texts:
        job_ids = _extract_candidate_ids(page_texts["jobs"], "jobs")
        artifacts.extend(
            _collect_detail_pages(base_url, out_dir, "jobs", "job", job_ids, wait_seconds, limit=detail_limit)
        )
    if "stages" in page_texts:
        stage_ids = _extract_candidate_ids(page_texts["stages"], "stages")
        artifacts.extend(
            _collect_detail_pages(base_url, out_dir, "stages", "stage", stage_ids, wait_seconds, limit=detail_limit)
        )
    manifest = {
        "base_url": base_url,
        "pages": [asdict(item) for item in artifacts],
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    manifest_md = ["# Spark 界面浏览器采集", ""]
    manifest_md.append(f"基础链接：`{base_url}`")
    manifest_md.append("")
    manifest_md.append("| 页面 | 标题 | 链接 | 文件 |")
    manifest_md.append("|---|---|---|---|")
    for item in artifacts:
        manifest_md.append(f"| `{item.page}` | `{item.title}` | `{item.url}` | `{item.file_name}` |")
    manifest_md.append("")
    manifest_md.append("## 说明")
    manifest_md.append("")
    manifest_md.append("- 每个文件都保存了浏览器导航后可见的页面内容，优先保留表格块为 Markdown table。")
    manifest_md.append("- `details/` 目录保存自动下钻的 job / stage 详情页，里面的 task 表也会按表格保留。")
    manifest_md.append("- 这个采集方式依赖当前 Chrome 登录态。")
    manifest_md.append("- 将生成的 `spark_ui/browser/` 目录交给 `collect_case_context.py` 继续处理。")
    (out_dir / "manifest.md").write_text("\n".join(manifest_md) + "\n", encoding="utf-8")
    return artifacts


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect Spark UI pages from an authenticated Chrome session.")
    parser.add_argument("--base-url", required=True, help="Spark UI proxy base URL, ending at the application root")
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="Directory where copied page content and manifest files should be written",
    )
    parser.add_argument(
        "--pages",
        default=",".join(DEFAULT_PAGES),
        help="Comma-separated page names to collect (default: environment,jobs,stages,executors,sql)",
    )
    parser.add_argument(
        "--wait-seconds",
        type=float,
        default=4.0,
        help="Seconds to wait after each navigation before copying the page content",
    )
    parser.add_argument(
        "--detail-limit",
        type=int,
        default=10,
        help="Maximum number of job/stage detail pages to collect from each overview page",
    )
    args = parser.parse_args()

    pages = [item.strip() for item in args.pages.split(",") if item.strip()]
    collect_spark_ui(args.base_url, args.output_dir, pages, args.wait_seconds, args.detail_limit)
    print(args.output_dir / "manifest.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
