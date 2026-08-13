import ast
import hashlib
import json
import re
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
CORE_PATH = ROOT / "cores" / "awareness_core.py"


def _event_query_namespace():
    tree = ast.parse(CORE_PATH.read_text(encoding="utf-8"))
    constants = {
        "_EVENTS_QUERY_MAX_CANDIDATE_EVENTS_FOR_LLM",
        "_EVENTS_QUERY_MAX_RELEVANT_EVENTS_FOR_ANSWER",
        "_EVENTS_QUERY_INPUT_TOKEN_BUDGET",
        "_EVENTS_QUERY_RETRY_TOKEN_BUDGET",
        "_EVENTS_QUERY_CHARS_PER_TOKEN_ESTIMATE",
        "_EVENTS_QUERY_MAX_TITLE_CHARS",
        "_EVENTS_QUERY_MAX_MESSAGE_CHARS",
        "_EVENTS_QUERY_MAX_DATA_TEXT_CHARS",
        "_EVENTS_QUERY_MAX_ROLLUP_SAMPLES",
        "_EVENTS_QUERY_IMMEDIATE_WINDOW_MINUTES",
        "_EVENTS_QUERY_IMMEDIATE_RE",
        "_EVENTS_QUERY_SAFE_DATA_FIELDS",
    }
    functions = {
        "_text",
        "_compact",
        "_parse_iso",
        "_events_query_source_to_area",
        "_events_query_event_dt",
        "_events_query_event_id",
        "_events_query_compact_data",
        "_events_query_compact_event_for_llm",
        "_events_query_estimate_tokens",
        "_events_query_budget_rows",
        "_events_query_rollup_events",
        "_events_query_is_immediate_query",
        "_events_query_parse_local_iso",
        "_events_query_normalize_interpretation",
        "_events_query_deterministic_summary",
        "get_hydra_kernel_tools",
    }
    body = []
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(target, ast.Name) and target.id in constants for target in targets):
                body.append(node)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in functions:
            body.append(node)
    module = ast.Module(body=body, type_ignores=[])
    ast.fix_missing_locations(module)
    namespace = {
        "hashlib": hashlib,
        "json": json,
        "re": re,
        "datetime": datetime,
        "timedelta": timedelta,
        "Any": Any,
        "Dict": Dict,
        "List": List,
        "Optional": Optional,
        "Tuple": Tuple,
    }
    exec(compile(module, str(CORE_PATH), "exec"), namespace)
    return namespace


class AwarenessEventQueryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.core = _event_query_namespace()

    def test_event_compaction_excludes_unbounded_data(self):
        compact = self.core["_events_query_compact_event_for_llm"](
            {
                "source": "back_yard",
                "ha_time": "2026-08-11T17:22:00",
                "title": "Camera Event",
                "message": "A delivery driver crosses the backyard. " * 30,
                "type": "camera_event",
                "entity_id": "camera.back_yard",
                "level": "info",
                "data": {
                    "area": "Back Yard",
                    "provider": "unifi_protect",
                    "object_types": ["person"],
                    "snapshot_id": "large-snapshot-reference",
                    "snapshot_bytes": 900_000,
                    "raw_payload": "x" * 100_000,
                },
            }
        )

        self.assertLessEqual(len(compact["message"]), 363)
        self.assertEqual(
            compact["data"],
            {"area": "Back Yard", "object_types": ["person"], "provider": "unifi_protect"},
        )

    def test_broad_history_rolls_up_and_stays_inside_budget(self):
        compact_event = self.core["_events_query_compact_event_for_llm"]
        rows = []
        for index in range(162):
            hour = index // 10
            rows.append(
                compact_event(
                    {
                        "source": "back_yard",
                        "ha_time": f"2026-08-11T{hour:02d}:{index % 10:02d}:00",
                        "title": "Camera Event",
                        "message": f"Observed backyard activity {index}",
                        "type": "camera_event",
                        "entity_id": "camera.back_yard",
                        "data": {"area": "Back Yard"},
                    }
                )
            )

        rollups = self.core["_events_query_rollup_events"](rows)
        bounded, _omitted, estimated_tokens = self.core["_events_query_budget_rows"](rollups)

        self.assertEqual(sum(row["event_count"] for row in rollups), 162)
        self.assertLessEqual(len(bounded), 40)
        self.assertLessEqual(estimated_tokens, 12_000)

    def test_event_compaction_keeps_known_people_searchable(self):
        compact = self.core["_events_query_compact_event_for_llm"](
            {
                "id": "fred-back-yard",
                "source": "back_yard",
                "ha_time": "2026-08-13T09:00:00",
                "title": "Back Yard Camera",
                "message": "A person walked through the back yard.",
                "type": "camera_event",
                "data": {
                    "area": "Back Yard",
                    "known_people": ["Fred"],
                    "recognized_people": ["Fred"],
                    "recognized_person_ids": ["person_fred"],
                    "face_count": 1,
                    "face_identity_ids": ["face_123"],
                },
            }
        )
        self.assertEqual(compact["event_id"], "fred-back-yard")
        self.assertEqual(compact["data"]["known_people"], ["Fred"])
        self.assertEqual(compact["data"]["recognized_people"], ["Fred"])
        self.assertEqual(compact["data"]["recognized_person_ids"], ["person_fred"])
        self.assertEqual(compact["data"]["face_count"], 1)

    def test_right_now_overrides_model_today_window(self):
        now = datetime(2026, 8, 11, 17, 22, 45)
        normalized, error = self.core["_events_query_normalize_interpretation"](
            interpretation={
                "query_type": "summary",
                "response_mode": "summary",
                "search_scope": "selected_sources",
                "source_ids": ["back_yard"],
                "semantic_focus": [],
                "time_window": {
                    "start_local": "2026-08-11T00:00:00",
                    "end_local": "2026-08-11T17:22:45",
                    "label": "today",
                },
            },
            sources_catalog=["back_yard"],
            now_local=now,
            user_query="what's going on in the back yard right now",
        )

        self.assertFalse(error)
        self.assertEqual(normalized["time_start"], now - timedelta(minutes=10))
        self.assertEqual(normalized["time_end"], now)
        self.assertEqual(normalized["time_label"], "the last 10 minutes")

    def test_tool_description_routes_live_views_to_camera_control(self):
        description = self.core["get_hydra_kernel_tools"]()[0]["description"].lower()
        self.assertIn("stored awareness event history", description)
        self.assertIn("right now", description)
        self.assertIn("camera_control", description)


if __name__ == "__main__":
    unittest.main()
