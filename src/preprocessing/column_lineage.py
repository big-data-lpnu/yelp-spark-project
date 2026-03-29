"""Column lineage: flatten adds vs prune drops vs final schema (JSON next to Parquet)."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from src.constants import PROCESSED_DIR
from src.preprocessing.config import PreprocessConfig


@dataclass
class TableLineage:
    column_count_after_clean: int
    column_count_after_flatten: int
    flatten_added_columns: list[str]
    pruned_columns: list[str]
    column_count_after_prune: int
    dropped_uninformative_columns: list[str]
    column_count_after_screen: int
    dropped_after_transform: list[str]
    column_count_final: int


def build_table_lineage(
    cols_clean: list[str],
    cols_flat: list[str],
    cols_after_prune: list[str],
    cols_final: list[str],
    cfg: PreprocessConfig,
    dropped_uninformative: list[str] | None = None,
) -> TableLineage:
    """Derive flatten adds and actual pruned column names from column lists at each stage."""
    flat_set = set(cols_flat)
    clean_set = set(cols_clean)
    after_prune_set = set(cols_after_prune)
    flatten_added = sorted(flat_set - clean_set)
    requested = cfg.columns_to_prune_after_flatten
    pruned = sorted(c for c in requested if c in flat_set and c not in after_prune_set)
    du = sorted(dropped_uninformative) if dropped_uninformative else []
    du_set = set(du)
    cols_after_screen = [c for c in cols_after_prune if c not in du_set]
    dropped_after_transform = sorted(set(cols_after_screen) - set(cols_final))
    return TableLineage(
        column_count_after_clean=len(cols_clean),
        column_count_after_flatten=len(cols_flat),
        flatten_added_columns=flatten_added,
        pruned_columns=pruned,
        column_count_after_prune=len(cols_after_prune),
        dropped_uninformative_columns=du,
        column_count_after_screen=len(cols_after_screen),
        dropped_after_transform=dropped_after_transform,
        column_count_final=len(cols_final),
    )


def build_lineage_all(
    cleaned: dict[str, list[str]],
    flattened: dict[str, list[str]],
    pruned: dict[str, list[str]],
    final: dict[str, list[str]],
    config_per_table: dict[str, PreprocessConfig],
    dropped_uninformative: dict[str, list[str]] | None = None,
) -> dict[str, TableLineage]:
    """Build lineage for every table present in all stage snapshots."""
    du_all = dropped_uninformative or {}
    out: dict[str, TableLineage] = {}
    for name in cleaned:
        if name not in flattened or name not in pruned or name not in final:
            continue
        cfg = config_per_table.get(name) or PreprocessConfig()
        out[name] = build_table_lineage(
            cleaned[name],
            flattened[name],
            pruned[name],
            final[name],
            cfg,
            dropped_uninformative=du_all.get(name, []),
        )
    return out


def lineage_to_json_serializable(lineage: dict[str, TableLineage]) -> dict[str, dict]:
    return {k: asdict(v) for k, v in lineage.items()}


def write_column_lineage(
    lineage: dict[str, TableLineage],
    path: Path | None = None,
) -> Path:
    """Write lineage JSON beside processed Parquet (default: PROCESSED_DIR/column_lineage.json)."""
    out = path or (PROCESSED_DIR / "column_lineage.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = lineage_to_json_serializable(lineage)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out


def read_column_lineage(path: Path | None = None) -> dict[str, dict] | None:
    """Load lineage JSON; return None if missing."""
    p = path or (PROCESSED_DIR / "column_lineage.json")
    if not p.is_file():
        return None
    return json.loads(p.read_text(encoding="utf-8"))
