#!/usr/bin/env python3
"""Render the detailed step-4 infographic for the device PySpark case.

Layout goals:
- Show the full source code for the main execution files.
- Mark the current stage/job mapping next to the relevant code ranges.
- Highlight the top-5 bottlenecks in the full code with red emphasis.
- Attach a magnifier card per bottleneck with current state, reason, benefit,
  and a concrete proposed code sketch.

The script writes one SVG and, when possible, one PNG to the case output dir.
"""

from __future__ import annotations

import argparse
import html
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class HotspotSpec:
    rank: int
    title: str
    file_path: str
    line_start: int
    line_end: int
    red_lines: list[int]
    zoom_start: int
    zoom_end: int
    stage_label: str
    current_state: str
    why_slow: str
    expected_benefit: str
    proposed_code: list[str]
    optimization_direction: str | None = None
    proposal_mode: str = "code"
    proposal_note: str | None = None
    badge: str | None = None
    show_in_ranking: bool = True


@dataclass(frozen=True)
class SectionSpec:
    title: str
    subtitle: str
    file_path: str
    stage_summary: str
    hotspots: list[HotspotSpec] = field(default_factory=list)


def read_lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines()


def svg_text(
    x: int,
    y: int,
    content: str,
    *,
    size: float = 11,
    fill: str = "#111827",
    weight: int = 400,
    family: str = "Menlo, Consolas, Liberation Mono, monospace",
    anchor: str = "start",
    italic: bool = False,
) -> str:
    style = "font-style:italic;" if italic else ""
    return (
        f'<text x="{x}" y="{y}" font-family="{html.escape(family, quote=True)}" '
        f'font-size="{size}" font-weight="{weight}" fill="{fill}" '
        f'style="{style}" text-anchor="{anchor}" xml:space="preserve">{html.escape(content)}</text>'
    )


def rect(
    x: int,
    y: int,
    w: int,
    h: int,
    *,
    fill: str = "#ffffff",
    stroke: str = "#d1d5db",
    sw: float = 1,
    rx: int = 0,
) -> str:
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="{sw}"/>'
    )


def line(
    x1: int,
    y1: int,
    x2: int,
    y2: int,
    *,
    stroke: str = "#dc2626",
    sw: float = 1.5,
    dash: str = "6,4",
) -> str:
    return (
        f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
        f'stroke="{stroke}" stroke-width="{sw}" stroke-dasharray="{dash}"/>'
    )


def wrap_text(text: str, max_chars: int) -> list[str]:
    if not text:
        return [""]

    words = text.split()
    if not words:
        return chunk_text(text, max_chars)

    lines: list[str] = []
    cur = ""
    for word in words:
        parts = [word]
        if len(word) > max_chars:
            parts = chunk_text(word, max_chars)
        for part in parts:
            candidate = part if not cur else f"{cur} {part}"
            if len(candidate) > max_chars:
                if cur:
                    lines.append(cur)
                cur = part
            else:
                cur = candidate
    if cur:
        lines.append(cur)
    return lines


def chunk_text(text: str, max_chars: int) -> list[str]:
    if len(text) <= max_chars:
        return [text]
    return [text[i:i + max_chars] for i in range(0, len(text), max_chars)]


def format_line_ranges(lines: list[int]) -> str:
    if not lines:
        return ""
    ordered = sorted(set(lines))
    ranges: list[tuple[int, int]] = []
    start = prev = ordered[0]
    for line_no in ordered[1:]:
        if line_no == prev + 1:
            prev = line_no
            continue
        ranges.append((start, prev))
        start = prev = line_no
    ranges.append((start, prev))
    parts: list[str] = []
    for start, end in ranges:
        parts.append(str(start) if start == end else f"{start}-{end}")
    return ", ".join(parts)


def hotspot_focus_scope(hsp: HotspotSpec) -> str:
    if hsp.red_lines:
        return format_line_ranges(hsp.red_lines)
    return f"{hsp.line_start}-{hsp.line_end}"


def measure_wrapped_lines(text: str, max_chars: int) -> list[str]:
    return wrap_text(text, max_chars)


def render_wrapped_block(
    svg: list[str],
    *,
    x: int,
    y: int,
    lines: list[str],
    size: float,
    fill: str,
    line_height: float,
) -> None:
    for idx, row in enumerate(lines):
        svg.append(svg_text(x, y + idx * line_height, row, size=size, fill=fill))


def render_code_panel(
    svg: list[str],
    *,
    x: int,
    y: int,
    w: int,
    h: int,
    file_title: str,
    stage_summary: str,
    lines: list[str],
    hotspots: list[HotspotSpec],
) -> None:
    title_h = 48
    code_x = x + 16
    code_y = y + title_h + 18
    code_w = w - 32
    code_h = h - title_h - 34
    line_h = 10.85
    font_size = 8.55
    line_num_w = 42
    code_text_x = code_x + line_num_w + 12

    svg.append(rect(x, y, w, h, fill="#ffffff", stroke="#d1d5db", sw=1.4, rx=18))
    svg.append(rect(x, y, w, title_h, fill="#f8fafc", stroke="none", rx=18))
    svg.append(svg_text(x + 18, y + 29, file_title, size=16.5, weight=800))
    svg.append(svg_text(x + 18, y + 46, stage_summary, size=10.8, fill="#6b7280"))

    svg.append(rect(code_x, code_y, code_w, code_h, fill="#fcfcfd", stroke="#e5e7eb", sw=1, rx=14))

    ranked_hotspots = [hsp for hsp in hotspots if hsp.show_in_ranking]
    hotspot_ranges = [(hsp.line_start, hsp.line_end, hsp) for hsp in ranked_hotspots]
    hotspot_midpoints: list[tuple[int, HotspotSpec]] = []
    for hsp in ranked_hotspots:
        mid = int(code_y + 22 + (hsp.line_start + hsp.line_end - 2) / 2 * line_h)
        hotspot_midpoints.append((mid, hsp))

    # Render a small stage badge for each hotspot close to the right edge of the code block.
    for mid_y, hsp in hotspot_midpoints:
        badge_text = hsp.badge or f"TOP{hsp.rank}"
        badge_w = max(92, 12 + len(badge_text) * 8)
        badge_x = code_x + code_w - badge_w - 12
        badge_y = mid_y - 11
        svg.append(rect(badge_x, badge_y, badge_w, 22, fill="#eff6ff", stroke="#60a5fa", sw=1, rx=11))
        svg.append(svg_text(badge_x + badge_w / 2, mid_y + 4, badge_text, size=10.4, fill="#1d4ed8", weight=800, anchor="middle"))

    for idx, content in enumerate(lines, start=1):
        ly = int(code_y + 22 + (idx - 1) * line_h)
        if ly > code_y + code_h - 8:
            break
        is_hot = False
        is_red = False
        for start, end, _ in hotspot_ranges:
            if start <= idx <= end:
                is_hot = True
                if idx in _.red_lines:
                    is_red = True
                break
        if is_hot:
            svg.append(rect(code_x + 4, ly - 9, code_w - 8, 14, fill="#fff7ed", stroke="none", rx=4))
        svg.append(svg_text(code_x + 14, ly, f"{idx:>4}", size=8.1, fill="#9ca3af" if not is_hot else "#7c3aed", weight=500))
        fill = "#111827"
        weight = 400
        if is_red:
            fill = "#b91c1c"
            weight = 800
        svg.append(svg_text(code_text_x, ly, content, size=font_size, fill=fill, weight=weight))


def render_magnifier_card(
    svg: list[str],
    *,
    x: int,
    y: int,
    w: int,
    hotspot: HotspotSpec,
    file_lines: list[str],
) -> int:
    zoom_lines = list(range(max(1, hotspot.zoom_start), min(len(file_lines), hotspot.zoom_end) + 1))
    zoom_line_h = 14.2
    zoom_start_y = y + 64
    zoom_h = len(zoom_lines) * zoom_line_h + 18
    info_y = int(zoom_start_y + zoom_h + 14)

    current_lines = measure_wrapped_lines(hotspot.current_state, 48)[:4]
    why_lines = measure_wrapped_lines(hotspot.why_slow, 48)[:4]
    benefit_lines = measure_wrapped_lines(hotspot.expected_benefit, 48)[:3]
    direction_text = hotspot.optimization_direction or hotspot.proposal_note or "当前日志未直接给出可验证的优化方向。"
    if hotspot.proposed_code and hotspot.proposal_mode == "code":
        proposed_label = "预期修改代码"
        proposed_lines = hotspot.proposed_code
        proposed_h = max(50, len(proposed_lines) * 11 + 18)
    else:
        proposed_label = "优化方向"
        proposed_lines = measure_wrapped_lines(direction_text, 54)
        proposed_h = max(50, len(proposed_lines) * 11 + 16)

    title_h = 54
    info_h = 22 + max(len(current_lines), 1) * 14 + 14
    info_h += 22 + max(len(why_lines), 1) * 14 + 14
    info_h += 22 + max(len(benefit_lines), 1) * 14 + 10
    code_box_h = int(zoom_h + 14)
    h = title_h + code_box_h + info_h + proposed_h + 22

    svg.append(rect(x, y, w, h, fill="#ffffff", stroke="#e5e7eb", sw=1.2, rx=16))
    svg.append(rect(x, y, w, title_h, fill="#fff7ed", stroke="none", rx=16))
    title_prefix = hotspot.badge or (f"瓶颈{hotspot.rank}" if hotspot.rank else "上下文")
    focus_scope = hotspot_focus_scope(hotspot)
    svg.append(svg_text(x + 16, y + 24, f"{title_prefix} | {focus_scope} | {hotspot.title}", size=13.5, weight=800))
    svg.append(svg_text(x + 16, y + 42, f"{hotspot.file_path}:{focus_scope}  |  {hotspot.stage_label}", size=10.4, fill="#6b7280"))

    svg.append(rect(x + 14, y + 64, w - 28, int(zoom_h + 16), fill="#fffef9", stroke="#f59e0b", sw=1.4, rx=12))
    svg.append(svg_text(x + 28, y + 84, "放大镜", size=11.2, weight=800, fill="#b45309"))
    svg.append(svg_text(x + 108, y + 84, "瓶颈代码", size=10.7, fill="#6b7280"))

    num_x = x + 28
    txt_x = x + 72
    for offset, line_no in enumerate(zoom_lines):
        ly = int(zoom_start_y + offset * zoom_line_h)
        content = file_lines[line_no - 1]
        is_hot = hotspot.line_start <= line_no <= hotspot.line_end
        is_red = line_no in hotspot.red_lines
        if is_hot:
            svg.append(rect(x + 18, ly - 8, w - 36, 14, fill="#fff1f2", stroke="none", rx=4))
        svg.append(svg_text(num_x, ly, f"{line_no:>4}", size=8.8, fill="#9ca3af", weight=500))
        fill = "#111827"
        weight = 400
        if is_red:
            fill = "#b91c1c"
            weight = 800
        svg.append(svg_text(txt_x, ly, content, size=11.1, fill=fill, weight=weight))

    svg.append(rect(x + 14, info_y, w - 28, info_h, fill="#f8fafc", stroke="#dbe4f0", sw=1, rx=12))
    cur_y = info_y + 24
    svg.append(svg_text(x + 28, cur_y, "日志证据", size=11.6, weight=800, fill="#1d4ed8"))
    render_wrapped_block(svg, x=x + 170, y=cur_y, lines=current_lines, size=9.8, fill="#111827", line_height=14)

    why_y = cur_y + 22 + max(len(current_lines), 1) * 14 + 10
    svg.append(svg_text(x + 28, why_y, "待确认原因", size=11.6, weight=800, fill="#1d4ed8"))
    render_wrapped_block(svg, x=x + 170, y=why_y, lines=why_lines, size=9.8, fill="#111827", line_height=14)

    benefit_y = why_y + 22 + max(len(why_lines), 1) * 14 + 10
    svg.append(svg_text(x + 28, benefit_y, "待确认收益", size=11.6, weight=800, fill="#1d4ed8"))
    render_wrapped_block(svg, x=x + 170, y=benefit_y, lines=benefit_lines, size=9.8, fill="#059669", line_height=14)

    code_box_y = benefit_y + 22 + max(len(benefit_lines), 1) * 14 + 12
    code_box_h2 = h - (code_box_y - y) - 14
    fill = "#f8fafc"
    stroke = "#cbd5e1"
    label_fill = "#334155"
    svg.append(rect(x + 14, code_box_y, w - 28, code_box_h2, fill=fill, stroke=stroke, sw=1, rx=12))
    svg.append(svg_text(x + 28, code_box_y + 22, proposed_label, size=11.2, weight=800, fill=label_fill))
    code_y = code_box_y + 42
    render_wrapped_block(svg, x=x + 28, y=code_y, lines=proposed_lines, size=8.8, fill="#334155", line_height=12)

    return h


def build_device_spec(case_dir: Path) -> list[SectionSpec]:
    return [
        SectionSpec(
            title="source/schedule/pipeline/credit_risk_feature_pipeline.py",
            subtitle="对应源码文件",
            file_path="source/schedule/pipeline/credit_risk_feature_pipeline.py",
            stage_summary="对应源码文件。",
            hotspots=[
                HotspotSpec(
                    rank=0,
                    title="执行入口 + 写入收口",
                    file_path="source/schedule/pipeline/credit_risk_feature_pipeline.py",
                    line_start=9,
                    line_end=17,
                    red_lines=[],
                    zoom_start=9,
                    zoom_end=17,
                    stage_label="作业入口 / DESCRIBE",
                    current_state="待确认（当前日志未直接提供可核对的耗时指标）",
                    why_slow="当前日志没有单独暴露这一步的耗时或行数，因此不能把它单独判成慢因。",
                    expected_benefit="待确认（日志未提供收益数值）",
                    proposed_code=[],
                    optimization_direction="先确认这一步是否只是写入收口；如果是收口动作本身，优先把优化放在前序扫描和聚合链路，而不是在这里硬改。",
                    proposal_mode="idea",
                    proposal_note="待确认（当前日志未直接给出可验证的改动内容）",
                    badge="入口",
                ),
                HotspotSpec(
                    rank=0,
                    title="最终写入收口",
                    file_path="source/schedule/pipeline/credit_risk_feature_pipeline.py",
                    line_start=42,
                    line_end=47,
                    red_lines=[],
                    zoom_start=42,
                    zoom_end=48,
                    stage_label="写入收口",
                    current_state="待确认（当前日志未直接提供可核对的耗时指标）",
                    why_slow="当前日志没有单独暴露这一步的耗时或行数，因此只能把它当作收口位置。",
                    expected_benefit="待确认（日志未提供收益数值）",
                    proposed_code=[],
                    optimization_direction="先确认这一步是否只是最终写入；如果只是收口，优化方向应放到前面的 scan / join / 聚合链路，不要在收口处做无效改写。",
                    proposal_mode="idea",
                    proposal_note="待确认（当前日志未直接给出可验证的改动内容）",
                    badge="写入",
                ),
            ],
        ),
        SectionSpec(
            title="source/dao/device/device.py",
            subtitle="对应源码文件",
            file_path="source/dao/device/device.py",
            stage_summary="对应源码文件。",
            hotspots=[
                HotspotSpec(
                    rank=2,
                    title="Android explode / 聚合 / join 链",
                    file_path="source/dao/device/device.py",
                    line_start=187,
                    line_end=220,
                    red_lines=[188, 191, 193, 196, 206, 207, 210, 219, 220],
                    zoom_start=187,
                    zoom_end=220,
                    stage_label="作业 3-12 / 重 shuffle 链",
                    current_state="作业 16；shuffle 读取 1.5 TiB，写入 841.5 GiB。",
                    why_slow="作业 16 这条链路的 shuffle read/write 已达到 1.5 TiB / 841.5 GiB，说明 explode / groupBy / join 链路在大量搬运数据。",
                    expected_benefit="待确认（日志未提供收益数值）",
                    proposed_code=[],
                    optimization_direction="优先裁剪无用字段、尽早过滤，并考虑把这条 explode / groupBy / join 链路拆成可复用的中间表，减少重复 shuffle。",
                    badge="瓶颈2",
                ),
            ],
        ),
        SectionSpec(
            title="source/features/device/ft_device.py",
            subtitle="对应源码文件",
            file_path="source/features/device/ft_device.py",
            stage_summary="对应源码文件。",
            hotspots=[
                HotspotSpec(
                    rank=1,
                    title="最新设备窗口排序",
                    file_path="source/features/device/ft_device.py",
                    line_start=33,
                    line_end=40,
                    red_lines=[38, 39],
                    zoom_start=33,
                    zoom_end=40,
                    stage_label="作业 16 / 4.2 小时 / 6115 个任务 / 19 个失败",
                    current_state="作业 16，4.2 小时，6115 个任务，19 个失败。",
                    why_slow="待确认（当前日志未直接给出因果结论）",
                    expected_benefit="待确认（日志未提供收益数值）",
                    proposed_code=[],
                    optimization_direction="优先收窄历史扫描范围，裁掉最后不用的字段和分区；如果仍要保留全量窗口，就考虑把可复用结果沉淀成中间表。",
                    badge="瓶颈1",
                ),
                HotspotSpec(
                    rank=3,
                    title="周期指标聚合（待确认）",
                    file_path="source/features/device/ft_device.py",
                    line_start=83,
                    line_end=110,
                    red_lines=[],
                    zoom_start=83,
                    zoom_end=110,
                    stage_label="360 天历史上的宽聚合",
                    current_state="待确认（当前日志未直接提供可核对的 stage 指标）",
                    why_slow="当前 Spark 界面导出没有单独暴露这一段的 stage 耗时、行数或 shuffle，因此不能直接把它单独判成慢因。",
                    expected_benefit="待确认（日志未提供收益数值）",
                    proposed_code=[],
                    optimization_direction="先把这段标记为观察项，不直接动；如果后续证据补齐，再决定是裁字段、收窄分区还是拆成中间表。",
                    badge="观察项",
                    show_in_ranking=False,
                ),
                HotspotSpec(
                    rank=4,
                    title="最新价格快照 + model/brand 关联",
                    file_path="source/features/device/ft_device.py",
                    line_start=155,
                    line_end=165,
                    red_lines=[164],
                    zoom_start=155,
                    zoom_end=165,
                    stage_label="作业 15 / 广播交换路径",
                    current_state="作业 15，2.2 小时，走广播交换路径。",
                    why_slow="作业 15 的广播交换链路耗时 2.2 小时，说明这个 join 节点确实在拉长链路；维表大小未直接暴露，所以不能进一步确认优化方式。",
                    expected_benefit="待确认（日志未提供收益数值）",
                    proposed_code=[],
                    optimization_direction="优先确认这个维表是否能稳定广播；如果广播不稳，再考虑把 price 结果按天落中间表，减少重复 join 成本。",
                    proposal_mode="idea",
                    proposal_note="待确认（当前日志未直接给出可验证的改动内容）",
                    badge="瓶颈4",
                ),
                HotspotSpec(
                    rank=5,
                    title="最终宽 join",
                    file_path="source/features/device/ft_device.py",
                    line_start=48,
                    line_end=49,
                    red_lines=[48, 49],
                    zoom_start=44,
                    zoom_end=50,
                    stage_label="作业 15-16 / fan-in",
                    current_state="作业 15-16，重分支之后的宽汇聚 join。",
                    why_slow="该 join 是作业 15-16 的收口 fan-in，处在重分支之后；当前导出没有单独给出这一步的耗时，所以只能确认它是收口位置。",
                    expected_benefit="待确认（日志未提供收益数值）",
                    proposed_code=[],
                    optimization_direction="优先考虑把前序 last / period / price 的结果沉淀成中间表，再做最终 fan-in，减少末端宽 join 的读写压力。",
                    badge="瓶颈5",
                ),
            ],
        ),
        SectionSpec(
            title="source/dao/device/client_device.py",
            subtitle="对应源码文件",
            file_path="source/dao/device/client_device.py",
            stage_summary="对应源码文件。",
            hotspots=[
                HotspotSpec(
                    rank=0,
                    title="上游 device 关系抽取",
                    file_path="source/dao/device/client_device.py",
                    line_start=13,
                    line_end=38,
                    red_lines=[],
                    zoom_start=13,
                    zoom_end=38,
                    stage_label="上游依赖",
                    current_state="待确认（当前日志未直接提供可核对的耗时指标）",
                    why_slow="当前日志没有单独暴露上游抽取的耗时或行数，因此不能把它单独判成慢因。",
                    expected_benefit="待确认（日志未提供收益数值）",
                    proposed_code=[],
                    optimization_direction="先把它视为上游依赖，不直接改；如果后续证据显示它是稳定复用的输入，再考虑是否落中间表。",
                    proposal_mode="idea",
                    proposal_note="待确认（当前日志未直接给出可验证的改动内容）",
                    badge="上下文",
                ),
            ],
        ),
    ]


def build_underiwrinting_spec(case_dir: Path) -> list[SectionSpec]:
    return [
        SectionSpec(
            title="source/high_risk_app.py",
            subtitle="对应源码文件",
            file_path="source/high_risk_app.py",
            stage_summary="对应源码文件。",
            hotspots=[
                HotspotSpec(
                    rank=1,
                    title="360 天窗口扫描 + row_number()",
                    file_path="source/high_risk_app.py",
                    line_start=86,
                    line_end=120,
                    red_lines=[92, 100, 102, 103, 104, 105, 106, 107, 108, 120],
                    zoom_start=86,
                    zoom_end=120,
                    stage_label="Stage 1 / 225.5 h / 61,870 tasks / 300 failed",
                    current_state="Stage 1：225.5 小时，6.4 TiB 输入，129.4 GiB shuffle write，61,870 个任务，300 个失败任务。",
                    why_slow="Spark UI 直接证明这段扫描和窗口排序是当前任务的主慢点。",
                    expected_benefit="待确认（日志未提供收益数值）",
                    proposed_code=[],
                    optimization_direction="优先收窄 360 天扫描范围，裁掉最后没用的字段和分区；如果历史结果会被反复使用，优先把它沉淀成中间表。",
                    proposal_mode="idea",
                    proposal_note="待确认（当前日志未直接给出可验证的改动内容）",
                    badge="已确认1",
                ),
                HotspotSpec(
                    rank=0,
                    title="rank 中间列（观察项）",
                    file_path="source/high_risk_app.py",
                    line_start=100,
                    line_end=115,
                    red_lines=[100, 115],
                    zoom_start=100,
                    zoom_end=115,
                    stage_label="观察项",
                    current_state="rank 在 get_raw() 中生成，并在 get_apps_tag() 中一路保留，但 get_dwd() 没有消费它。",
                    why_slow="当前 Spark UI 导出没有单独暴露这一列对应的 stage 指标，因此不能把它当作已确认慢因。",
                    expected_benefit="待确认（日志未提供收益数值）",
                    proposed_code=[],
                    optimization_direction="如果后续确认这列确实不被下游使用，就在中间链路尽早删除，避免无效列一路传递到末端。",
                    proposal_mode="idea",
                    proposal_note="待确认（当前日志未直接给出可验证的改动内容）",
                    badge="观察0",
                    show_in_ranking=False,
                ),
                HotspotSpec(
                    rank=0,
                    title="JSON 解析 + explode + 去重链（观察项）",
                    file_path="source/high_risk_app.py",
                    line_start=124,
                    line_end=169,
                    red_lines=[],
                    zoom_start=124,
                    zoom_end=169,
                    stage_label="观察项",
                    current_state="源码上存在 from_json、explode、dropDuplicates 和 left join 链路，但当前导出未单独暴露可直接归因的 stage 指标。",
                    why_slow="当前 Spark UI 导出没有单独暴露这一段的耗时或 shuffle，因此只保留为观察项。",
                    expected_benefit="待确认（日志未提供收益数值）",
                    proposed_code=[],
                    optimization_direction="先检查这段是否能通过裁字段、前置过滤和中间表来减小数据膨胀；没有补充证据前不直接改。",
                    proposal_mode="idea",
                    proposal_note="待确认（当前日志未直接给出可验证的改动内容）",
                    badge="观察1",
                    show_in_ranking=False,
                ),
                HotspotSpec(
                    rank=0,
                    title="双路聚合 + join 链（观察项）",
                    file_path="source/high_risk_app.py",
                    line_start=176,
                    line_end=223,
                    red_lines=[],
                    zoom_start=176,
                    zoom_end=223,
                    stage_label="观察项",
                    current_state="源码上存在 latest_apps / recent_apps 双路聚合和最终 join，但当前导出未单独暴露可直接归因的 stage 指标。",
                    why_slow="当前 Spark UI 导出没有单独暴露这一段的耗时或 shuffle，因此只保留为观察项。",
                    expected_benefit="待确认（日志未提供收益数值）",
                    proposed_code=[],
                    optimization_direction="如果后续证据补齐，这类重复 join / groupBy 链优先考虑按天落中间表，减少重复 shuffle 和重复去重。",
                    proposal_mode="idea",
                    proposal_note="待确认（当前日志未直接给出可验证的改动内容）",
                    badge="观察2",
                    show_in_ranking=False,
                ),
                HotspotSpec(
                    rank=0,
                    title="repartition(10) 写出（观察项）",
                    file_path="source/high_risk_app.py",
                    line_start=232,
                    line_end=234,
                    red_lines=[],
                    zoom_start=232,
                    zoom_end=234,
                    stage_label="观察项",
                    current_state="写出前的收口动作，但当前导出里没有单独可证实的慢因。",
                    why_slow="当前 Spark UI 导出没有单独暴露这一段的耗时，因此只保留为观察项。",
                    expected_benefit="待确认（日志未提供收益数值）",
                    proposed_code=[],
                    optimization_direction="先把它视为写出收口，不直接在这里加复杂逻辑；优化重点放到前面的扫描和聚合链路。",
                    proposal_mode="idea",
                    proposal_note="待确认（当前日志未直接给出可验证的改动内容）",
                    badge="观察3",
                    show_in_ranking=False,
                ),
            ],
        ),
    ]


def build_spec(case_dir: Path) -> list[SectionSpec]:
    if case_dir.name == "underiwrinting_app":
        return build_underiwrinting_spec(case_dir)
    return build_device_spec(case_dir)


def draw_card_connector(svg: list[str], *, x1: int, y1: int, x2: int, y2: int) -> None:
    svg.append(line(x1, y1, x2, y2, stroke="#dc2626", sw=1.4, dash="5,5"))


def build_svg(repo_root: Path, case_dir: Path) -> Path:
    rel_case = case_dir.relative_to(repo_root / "input")
    out_dir = repo_root / "output" / rel_case
    out_dir.mkdir(parents=True, exist_ok=True)

    src_root = case_dir / "source"
    sections = build_spec(case_dir)

    file_cache: dict[str, list[str]] = {}
    for section in sections:
        file_cache[section.file_path] = read_lines(case_dir / section.file_path)

    # Section layout.
    page_w = 3020
    left_x = 48
    left_w = 1880
    right_x = 1974
    right_w = 998
    top_y = 96
    section_gap = 38

    # Pre-compute heights.
    section_heights: list[int] = []
    card_heights: dict[tuple[str, int], int] = {}
    for section in sections:
        code_lines = file_cache[section.file_path]
        code_h = int(54 + 18 + (len(code_lines) * 10.85) + 22)
        cards_h = 0
        for hsp in section.hotspots:
            if not hsp.show_in_ranking:
                continue
            card_h = render_card_height(hsp, file_cache[hsp.file_path])
            card_heights[(hsp.file_path, hsp.rank)] = card_h
            cards_h += card_h + 18
        section_heights.append(max(code_h, cards_h + 20) + 18)

    page_h = top_y + sum(section_heights) + section_gap * (len(sections) - 1) + 72

    svg: list[str] = []
    svg.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{page_w}" height="{page_h}" viewBox="0 0 {page_w} {page_h}">')
    svg.append(rect(0, 0, page_w, page_h, fill="#f3f4f6", stroke="#f3f4f6", sw=0, rx=0))

    svg.append(svg_text(56, 42, "PySpark 作业优化 | 第4步：已确认热点与观察项", size=24, weight=800))
    svg.append(svg_text(
        56,
        66,
        "左侧：完整源码和行号。右侧：Spark UI 中可直接观测到的已确认热点；不能直接确认的部分只保留为观察项。",
        size=12.0,
        fill="#6b7280",
    ))
    svg.append(svg_text(
        56,
        88,
        "优化顺序：先调 Spark 参数 -> 再改实现方式且不改业务逻辑 -> 最后才考虑改业务逻辑。",
        size=11.5,
        fill="#374151",
    ))

    y = top_y
    for section, section_h in zip(sections, section_heights):
        code_lines = file_cache[section.file_path]
        svg.append(rect(34, y - 6, page_w - 68, section_h, fill="#ffffff", stroke="#e5e7eb", sw=1.2, rx=20))
        render_code_panel(
            svg,
            x=left_x,
            y=y,
            w=left_w,
            h=section_h - 18,
            file_title=section.title,
            stage_summary=section.stage_summary,
            lines=code_lines,
            hotspots=section.hotspots,
        )

        card_y = y
        for idx, hsp in enumerate(section.hotspots):
            if not hsp.show_in_ranking:
                continue
            card_h = render_card_height(hsp, file_cache[hsp.file_path])
            card_x = right_x
            render_magnifier_card(
                svg,
                x=card_x,
                y=card_y,
                w=right_w,
                hotspot=hsp,
                file_lines=file_cache[hsp.file_path],
            )
            # Connect the hot line region on the left to the magnifier card.
            code_y = y + 48 + 18
            line_h = 10.85
            hot_mid = int(code_y + 22 + (hsp.line_start + hsp.line_end - 2) / 2 * line_h)
            draw_card_connector(svg, x1=left_x + left_w - 18, y1=hot_mid, x2=card_x, y2=card_y + 30)
            card_y += card_h + 18

        y += section_h + section_gap

    svg.append(rect(34, page_h - 54, page_w - 68, 34, fill="#f8fafc", stroke="#e5e7eb", sw=1, rx=14))
    svg.append(svg_text(56, page_h - 33, "第5步暂不执行。此图仅保留可直接观测到的数据。", size=10.7, fill="#6b7280"))
    svg.append("</svg>")

    svg_path = out_dir / "step4_top5_bottlenecks.svg"
    svg_path.write_text("\n".join(svg), encoding="utf-8")

    png_path = out_dir / "step4_top5_bottlenecks.png"
    export_png(svg_path, png_path)
    return svg_path


def render_card_height(hsp: HotspotSpec, file_lines: list[str]) -> int:
    zoom_count = max(1, min(len(file_lines), hsp.zoom_end) - max(1, hsp.zoom_start) + 1)
    zoom_h = zoom_count * 14.2 + 18
    current_lines = measure_wrapped_lines(hsp.current_state, 48)[:4]
    why_lines = measure_wrapped_lines(hsp.why_slow, 48)[:4]
    benefit_lines = measure_wrapped_lines(hsp.expected_benefit, 48)[:3]
    proposed_lines = measure_wrapped_lines(hsp.proposal_note or "当前日志未直接给出可验证的改动内容。", 54)
    info_h = 22 + max(len(current_lines), 1) * 14 + 14
    info_h += 22 + max(len(why_lines), 1) * 14 + 14
    info_h += 22 + max(len(benefit_lines), 1) * 14 + 10
    proposed_h = max(50, len(proposed_lines) * 11 + 16)
    code_box_h = int(zoom_h + 16)
    return 54 + code_box_h + info_h + proposed_h + 22


def export_png(svg_path: Path, png_path: Path) -> None:
    if subprocess.run(["bash", "-lc", "command -v rsvg-convert"], capture_output=True, text=True).returncode == 0:
        subprocess.run(["rsvg-convert", str(svg_path), "-o", str(png_path)], check=True)
        return
    try:
        import cairosvg  # type: ignore
    except Exception:
        return
    cairosvg.svg2png(url=str(svg_path), write_to=str(png_path))


def main() -> None:
    parser = argparse.ArgumentParser(description="Render the detailed step-4 visualization for the device case")
    parser.add_argument(
        "--repo-root",
        default=None,
        help="Repo root path. Defaults to the current file's ancestor repo root.",
    )
    parser.add_argument(
        "--case-dir",
        default=None,
        help="Case directory under input/. Defaults to the current device case.",
    )
    args = parser.parse_args()

    script_path = Path(__file__).resolve()
    repo_root = Path(args.repo_root).resolve() if args.repo_root else script_path.parents[3]
    case_dir = (
        Path(args.case_dir).resolve()
        if args.case_dir
        else repo_root / "input" / "device" / "bmart_udl_risk.ads_ft_device_wf_credit_risk_feature_pipeline"
    )
    if not case_dir.exists():
        raise SystemExit(f"Case directory not found: {case_dir}")

    svg_path = build_svg(repo_root=repo_root, case_dir=case_dir)
    print(svg_path)


if __name__ == "__main__":
    main()
