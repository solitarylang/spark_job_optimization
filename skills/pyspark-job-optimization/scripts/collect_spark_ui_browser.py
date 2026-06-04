#!/usr/bin/env python3
"""Collect Spark UI pages from an authenticated Chrome session.

This helper is meant for Spark UI links that are reachable only through the
user's logged-in Chrome session. It opens the target Spark UI pages in Chrome,
copies the visible page text, and saves the result as reusable artifacts under
the case folder.

The collection strategy is intentionally simple and reproducible:

1. Open the Spark UI base URL in Chrome.
2. Visit the key pages one by one.
3. Copy the visible page text via the browser selection.
4. Save page text, title, and URL to `spark_ui/browser/`.
5. If the output directory already exists, overwrite the previous browser snapshot.

The output is designed so `collect_case_context.py` can consume it later.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
import shutil
from dataclasses import dataclass, asdict
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit


DEFAULT_PAGES = ["environment", "jobs", "stages", "executors", "sql"]


@dataclass
class PageArtifact:
    page: str
    url: str
    title: str
    file_name: str


def _run_osascript(script: str) -> str:
    result = subprocess.run(
        ["osascript", "-e", script],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _build_page_url(base_url: str, page: str) -> str:
    parsed = urlsplit(base_url)
    base_path = parsed.path.rstrip("/")
    page_path = f"{base_path}/{page}/"
    return urlunsplit((parsed.scheme, parsed.netloc, page_path, parsed.query, parsed.fragment))


def _open_url(url: str) -> None:
    script = f'''
tell application "Google Chrome"
  activate
  set URL of active tab of front window to "{url}"
end tell
'''
    _run_osascript(script)


def _copy_visible_text() -> str:
    script = r'''
tell application "Google Chrome" to activate
tell application "System Events"
  keystroke "a" using command down
  keystroke "c" using command down
end tell
'''
    _run_osascript(script)
    result = subprocess.run(["pbpaste"], check=True, capture_output=True, text=True)
    return result.stdout


def _get_active_title_and_url() -> tuple[str, str]:
    title = _run_osascript('tell application "Google Chrome" to get title of active tab of front window')
    url = _run_osascript('tell application "Google Chrome" to get URL of active tab of front window')
    return title, url


def _write_page_artifact(out_dir: Path, page: str, content: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    file_path = out_dir / f"{page}.txt"
    file_path.write_text(content, encoding="utf-8")
    return file_path


def _reset_output_dir(out_dir: Path) -> None:
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)


def collect_spark_ui(base_url: str, out_dir: Path, pages: list[str], wait_seconds: float) -> list[PageArtifact]:
    _reset_output_dir(out_dir)
    artifacts: list[PageArtifact] = []
    for page in pages:
        page_url = _build_page_url(base_url, page)
        _open_url(page_url)
        time.sleep(wait_seconds)
        text = _copy_visible_text()
        title, current_url = _get_active_title_and_url()
        file_path = _write_page_artifact(out_dir, page, text)
        artifacts.append(
            PageArtifact(
                page=page,
                url=current_url or page_url,
                title=title,
                file_name=str(file_path.relative_to(out_dir.parent)),
            )
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
    manifest_md.append("- 每个文件都保存了浏览器导航后可见的页面文本。")
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
        help="Directory where copied page text and manifest files should be written",
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
        help="Seconds to wait after each navigation before copying the page text",
    )
    args = parser.parse_args()

    pages = [item.strip() for item in args.pages.split(",") if item.strip()]
    collect_spark_ui(args.base_url, args.output_dir, pages, args.wait_seconds)
    print(args.output_dir / "manifest.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
