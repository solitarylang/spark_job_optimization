# PH underwriting app任务优化

案例：
优化前：`application_1772593899018_1090326`
优化后：`application_1772593899018_1093299`

## 总体结论

这次优化是有效的。主链路从 12.3 小时级收敛到 35 分钟级，运行时长下降到原来的约 5%。

优化前仍然保留了大规模历史扫描和大 shuffle 链路，`stage 1` / `stage 3` 都是 TiB 级输入。优化后，主链路已经收敛到秒级到分钟级 stage，未再出现 TiB 级扫描和长时大 shuffle stage。

## 关键变化

| 指标 | 优化前 | 优化后 | 说明 |
|---|---:|---:|---|
| 总体运行时长 | <span style="color:#b91c1c;font-weight:700">12.3 h</span> | <span style="color:#b91c1c;font-weight:700">35 min</span> | 主链路明显收敛 |
| Completed Jobs | 9 | 9 | job 数量一致，但单次耗时下降 |
| Completed Stages | 10 | 9 | 结构收敛，失败 stage 消失 |
| Failed Stages | 1 | 0 | 失败链路被消除 |
| 最大扫描输入 | 7.1 TiB | 未再出现 TiB 级扫描 | TiB 级大扫描被压缩 |
| 扫描路径数 | 360 paths | 356 paths | 逻辑窗口仍是 360 天，实际枚举到 356 个物理 leaf paths |
| 主扫描 stage 任务数 | <span style="color:#b91c1c;font-weight:700">70823</span> | <span style="color:#b91c1c;font-weight:700">565</span> | 调度和执行压力明显降低 |
| 最大 Shuffle Write | 146.2 GiB | 135.6 GiB | shuffle 仍存在，但规模下降 |
| 最大 Shuffle Read | 110.4 GiB | 126.0 GiB | 仍有中等规模读取 |
| 失败 task | <span style="color:#b91c1c;font-weight:700">928 failed / 68 failed / 3 failed 等</span> | <span style="color:#b91c1c;font-weight:700">少量 killed / another attempt succeeded</span> | task 失败和重试明显减少 |

## 主要瓶颈

- 360 天历史扫描
- `explode` 带来的数据膨胀
- `dropDuplicates` + `join` 放大中间数据
- 双路聚合重复 shuffle
- `rank` 等中间列冗余透传

## 代码优化点

| 优化点 | 优化前 | 优化后 / 建议 | 收益 |
|---|---|---|---|
| 历史扫描 | 360 天全量窗口扫描 | 收窄窗口，优先裁字段和分区 | 直接削减主慢点 |
| 数据膨胀 | `explode` 后继续透传 | `explode` 后尽早过滤和裁剪 | 减少中间行数放大 |
| 重复计算 | `latest_apps` / `recent_apps` 双路聚合 | 可复用链路考虑中间表 | 减少重复 shuffle |
| 冗余字段 | `rank` 一路透传到下游 | 若下游不使用，尽早删除 | 减少无效列传播 |
| 写出收口 | `repartition(10)` | 仅保留结果写出边界，不单列风险 | 避免把正常收口误判成瓶颈 |

## 代码前后对比

### 优化前代码

```python
def get_raw(self, str_anchor, offset=360):
    ...
    WHERE pt_date <= '{str_anchor}'
      AND pt_date >= '{start_date}'
    ...
    df_device_apps = df_device_apps['device_id', 'apps', 'app_info_list', 'pt_date', 'rank']
```

### 优化后建议

```python
raw_df = raw_df.select(*required_cols).filter(F.col("pt_date") >= start_pt_date)
apps = apps.select("deviceId", "apps_json", *kept_cols)
apps_base = apps_base.select("deviceId", "app", "pt_date").cache()
result = result.select(*final_cols)
```

### 说明

- 优化前重点问题是 360 天历史扫描、`explode` 放大和 `rank` 冗余透传。
- 优化后建议是先裁字段、收窄窗口、减少重复聚合，必要时把可复用链路落成中间表。
- 这里展示的是优化方向和草图，不是最终提交代码。

## 优化后的变化

- `stage 0` 仍然要做 `356 paths` 的 leaf files listing，但只用了 7 s
- `stage 1` 只有 24 s
- `stage 2` 只有 24 s，任务级别已经是 68/68
- `stage 3` 只有 3 s
- 主要 job 运行时间集中在 4.6 min、5.4 min、5.1 min、23 min 这些阶段

## 仍需关注的问题

- `stage 9` / `stage 7` / `stage 12` 依然有百 GiB 级输入和 shuffle 写入
- 说明还有进一步收敛空间，后续可以继续检查是否还能进一步裁字段、减少 `join` / `groupBy`，或者把可复用链路沉淀成中间表

## 360 与 356 的口径说明

报告里的 360 指的是源码里配置的 360 天逻辑窗口，356 paths 指的是 Spark 在这次运行里实际枚举到的物理 leaf 文件路径数。

这两者不是一回事，也不能直接推导为少了 4 个分区。更合理的解释是：

- Spark 本次文件枚举的路径数就是 356
- 这可能来自目录过滤、路径合并、某些路径未进入这次实际扫描计划
- 也可能是源表 / 中间表 / 物化路径的枚举口径与逻辑窗口并不完全一致

因此，这里应该只写成：逻辑窗口是 360 天，但 Spark 本次实际枚举到 356 个物理 leaf paths，不要写成缺少 4 个分区。

## Token Auto-Check

- Confluence token 已通过本地校验，本页已成功写入
