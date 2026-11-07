import asyncio
import json
import unittest

from tests.test_memory_core_history_cursor import FakeRedis, _load_memory_core


class MemoryRedis(FakeRedis):
    def __init__(self):
        super().__init__()
        self.hashes = {
            "memory_core_settings": {
                "use_people_identities": "false",
                "min_confidence": "0.65",
                "hydra_max_items": "12",
                "profile_card_items": "8",
                "recall_max_items": "12",
            }
        }

    def delete(self, *keys):
        deleted = 0
        for key in keys:
            if key in self.values:
                deleted += 1
                self.values.pop(key, None)
        return deleted

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, mapping=None, **kwargs):
        bucket = self.hashes.setdefault(key, {})
        updates = dict(mapping or {})
        updates.update(kwargs)
        bucket.update(updates)
        return len(updates)

    def scan_iter(self, match="*", count=100):
        del count
        import fnmatch

        for key in sorted(self.values):
            if fnmatch.fnmatch(key, match):
                yield key


class MemoryCoreContinuityTests(unittest.TestCase):
    def setUp(self):
        self.redis = MemoryRedis()
        self.core = _load_memory_core(self.redis)

    def test_explicit_equal_confidence_correction_supersedes_with_history(self):
        doc = self.core.load_memory_doc(self.redis, "mem:user:webui:fred", now=100.0)
        self.assertTrue(
            self.core.merge_observation(
                doc,
                {"candidate_key": "favorite_color", "value": "blue", "confidence": 0.9, "evidence": ["m1"]},
                min_confidence=0.65,
                default_ttl_sec=0,
                allow_new_keys=True,
                now=100.0,
            )
        )
        self.assertFalse(
            self.core.merge_observation(
                doc,
                {"candidate_key": "favorite_color", "value": "green", "confidence": 0.9, "evidence": ["m2"]},
                min_confidence=0.65,
                default_ttl_sec=0,
                allow_new_keys=True,
                now=200.0,
            )
        )
        self.assertTrue(
            self.core.merge_observation(
                doc,
                {
                    "candidate_key": "favorite_color",
                    "value": "green",
                    "confidence": 0.9,
                    "evidence": ["m2"],
                    "explicit_correction": True,
                },
                min_confidence=0.65,
                default_ttl_sec=0,
                allow_new_keys=True,
                now=200.0,
            )
        )
        self.assertEqual(doc["facts"]["favorite_color"]["value"], "green")
        self.assertEqual(doc["facts"]["favorite_color"]["valid_from"], 200.0)
        self.assertEqual(doc["retired_facts"][0]["value"], "blue")
        self.assertEqual(doc["retired_facts"][0]["valid_to"], 200.0)
        self.assertEqual(doc["retired_facts"][0]["reason"], "corrected")

    def test_extraction_payload_preserves_corrections_and_high_value_episodes(self):
        normalized = self.core._normalize_observation_payload(
            {
                "user_observations": [
                    {
                        "user_id": "fred",
                        "candidate_key": "favorite_color",
                        "value": "green",
                        "confidence": 0.9,
                        "evidence": ["m2"],
                        "explicit_correction": True,
                    }
                ],
                "room_observations": [],
                "user_removals": [],
                "room_removals": [],
                "user_episodes": [
                    {
                        "user_id": "fred",
                        "summary": "We decided to ship the memory update together.",
                        "kind": "decision",
                        "topics": ["memory", "release"],
                        "outcome": "One cohesive update.",
                        "commitments": ["Run regression tests"],
                        "status": "open",
                        "salience": 0.9,
                        "evidence": ["m2"],
                    }
                ],
                "room_episodes": [],
            },
            known_user_ids=["fred"],
            known_message_ids={"m2"},
            fallback_evidence_id="m2",
            current_user_profiles={"fred": {"favorite_color": {"value": "blue"}}},
            current_room_profile={},
            message_timestamps={"m2": 222.0},
        )
        self.assertTrue(normalized["user"][0]["explicit_correction"])
        self.assertEqual(normalized["user"][0]["operation"], "replace")
        self.assertEqual(normalized["user_episodes"][0]["occurred_at"], 222.0)
        self.assertEqual(normalized["user_episodes"][0]["commitments"], ["Run regression tests"])

    def test_episode_store_is_bounded_and_consolidates_duplicates(self):
        owner = "mem:user:webui:fred"
        first = {
            "summary": "We decided to keep prompt caching enabled for Hydra.",
            "kind": "decision",
            "topics": ["Hydra", "prompt caching"],
            "salience": 0.9,
            "status": "historical",
            "evidence": ["m1"],
            "occurred_at": 100.0,
        }
        duplicate = dict(first, evidence=["m2"], outcome="Keep the cache enabled.")
        self.assertEqual(
            self.core._memory_store_episodes(self.redis, owner, [first], min_salience=0.7, max_items=25, now_ts=100.0),
            1,
        )
        self.assertEqual(
            self.core._memory_store_episodes(self.redis, owner, [duplicate], min_salience=0.7, max_items=25, now_ts=110.0),
            1,
        )
        episodes = self.core._memory_load_episodes(self.redis, owner)
        self.assertEqual(len(episodes), 1)
        self.assertEqual(episodes[0]["evidence"], ["m1", "m2"])
        self.assertEqual(episodes[0]["outcome"], "Keep the cache enabled.")

    def test_profile_card_is_cached_and_excludes_sensitive_facts(self):
        owner = "mem:user:webui:fred"
        doc = {
            "schema_version": 2,
            "last_updated": 500.0,
            "facts": {
                "communication_style": {"value": "concise", "confidence": 0.9, "updated_at": 500.0},
                "health_condition": {"value": "private", "confidence": 0.9, "updated_at": 500.0},
            },
        }
        card = self.core._memory_profile_card(self.redis, owner, doc, max_items=8)
        self.assertEqual(card["items"], [{"key": "communication_style", "value": "concise"}])
        self.assertIn(self.core._memory_aux_key("profile", owner), self.redis.values)

    def test_consolidation_canonicalizes_aliases_without_losing_history(self):
        doc = {
            "facts": {
                "favourite_colour": {"value": "orange", "confidence": 0.8, "updated_at": 100.0},
                "favorite_color": {"value": "purple", "confidence": 0.9, "updated_at": 200.0},
            },
            "retired_facts": [],
            "last_updated": 200.0,
        }
        changed = self.core._memory_consolidate_doc(doc, scope="user", now_ts=300.0)
        self.assertEqual(changed, 1)
        self.assertNotIn("favourite_colour", doc["facts"])
        self.assertEqual(doc["facts"]["favorite_color"]["value"], "purple")
        self.assertEqual(doc["retired_facts"][0]["value"], "orange")

    def test_recall_searches_current_past_and_episodic_memory(self):
        owner = "mem:user:webui:fred"
        doc = {
            "schema_version": 2,
            "last_updated": 300.0,
            "facts": {
                "release_branch": {
                    "value": "main",
                    "confidence": 0.95,
                    "evidence": ["m3"],
                    "updated_at": 300.0,
                    "valid_from": 300.0,
                }
            },
            "retired_facts": [
                {
                    "key": "release_branch",
                    "value": "develop",
                    "confidence": 0.9,
                    "evidence": ["m1"],
                    "valid_from": 100.0,
                    "valid_to": 300.0,
                    "reason": "corrected",
                }
            ],
        }
        self.core.save_memory_doc(self.redis, owner, doc, now=300.0)
        self.core._memory_store_episodes(
            self.redis,
            owner,
            [
                {
                    "summary": "We moved releases from develop to the main branch.",
                    "kind": "decision",
                    "topics": ["release", "branch"],
                    "salience": 0.9,
                    "status": "historical",
                    "evidence": ["m2"],
                    "occurred_at": 250.0,
                }
            ],
            min_salience=0.7,
            max_items=25,
            now_ts=250.0,
        )
        results = self.core._hydra_memory_search_doc(
            redis_obj=self.redis,
            owner_doc_key=owner,
            query_text="what did we decide about the release branch",
            max_items=10,
            include_history=True,
        )
        self.assertEqual({row["type"] for row in results}, {"fact", "past_fact", "episode"})
        self.assertTrue(all("why_remembered" in row for row in results))

    def test_recall_tool_is_deterministic_and_requires_no_llm(self):
        owner = "mem:user:webui:fred"
        self.core.save_memory_doc(
            self.redis,
            owner,
            {
                "schema_version": 2,
                "last_updated": 100.0,
                "facts": {"favorite_console": {"value": "Dreamcast", "confidence": 0.9, "updated_at": 100.0}},
            },
            now=100.0,
        )
        result = asyncio.run(
            self.core.run_hydra_kernel_tool(
                tool_id="memory_recall",
                args={"query": "favorite console", "include_room": False},
                platform="webui",
                scope="session:test",
                origin={"user_id": "fred", "username": "Fred"},
                llm_client=None,
                redis_client=self.redis,
            )
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["results"][0]["value"], "Dreamcast")

    def test_unrelated_recall_does_not_invent_a_personal_memory(self):
        owner = "mem:user:webui:fred"
        self.core.save_memory_doc(
            self.redis,
            owner,
            {
                "schema_version": 2,
                "last_updated": 100.0,
                "facts": {"favorite_console": {"value": "Dreamcast", "confidence": 0.9, "updated_at": 100.0}},
            },
            now=100.0,
        )
        results = self.core._hydra_memory_search_doc(
            redis_obj=self.redis,
            owner_doc_key=owner,
            query_text="where did I park the bicycle",
            max_items=10,
            include_history=True,
        )
        self.assertEqual(results, [])

    def test_prompt_context_is_natural_and_does_not_expose_confidence(self):
        owner = "mem:user:webui:fred"
        self.core.save_memory_doc(
            self.redis,
            owner,
            {
                "schema_version": 2,
                "last_updated": 100.0,
                "facts": {"communication_style": {"value": "concise", "confidence": 0.91, "updated_at": 100.0}},
            },
            now=100.0,
        )
        payload = self.core.get_hydra_memory_context_payload(
            platform="webui",
            scope="session:test",
            origin={"user_id": "fred", "username": "Fred"},
            redis_client=self.redis,
            user_text="hello",
        )
        fragments = self.core.get_hydra_system_prompt_fragments(
            role="hermes",
            platform="webui",
            scope="session:test",
            origin={"user_id": "fred", "username": "Fred"},
            redis_client=self.redis,
            memory_context=payload,
        )
        prompt = fragments["hermes"][0]
        self.assertIn("Known about Fred", prompt)
        self.assertIn("Communication style: concise", prompt)
        self.assertNotIn("0.91", prompt)
        self.assertLessEqual(len(prompt), 2300)

    def test_memory_ui_is_a_friendly_two_tab_people_and_rooms_experience(self):
        owner = "mem:user:webui:fred"
        self.core.save_memory_doc(
            self.redis,
            owner,
            {
                "schema_version": 2,
                "last_updated": 300.0,
                "facts": {
                    "favorite_color": {
                        "value": "orange",
                        "confidence": 0.95,
                        "evidence": ["m2"],
                        "updated_at": 300.0,
                    }
                },
                "retired_facts": [
                    {
                        "key": "favorite_color",
                        "value": "blue",
                        "confidence": 0.9,
                        "evidence": ["m1"],
                        "valid_from": 100.0,
                        "valid_to": 300.0,
                        "reason": "corrected",
                    }
                ],
            },
            now=300.0,
        )
        self.core._memory_store_episodes(
            self.redis,
            owner,
            [
                {
                    "summary": "We agreed to finish the memory dashboard.",
                    "kind": "commitment",
                    "commitments": ["Finish the memory dashboard"],
                    "status": "open",
                    "salience": 0.95,
                    "evidence": ["m3"],
                    "occurred_at": 310.0,
                }
            ],
            min_salience=0.7,
            max_items=25,
            now_ts=310.0,
        )

        discovery = self.core._memory_core_doc_discovery()
        payload = self.core._memory_core_ui_manager_payload(discovery=discovery, stats={})

        self.assertEqual(payload["default_tab"], "knows")
        self.assertEqual([tab["label"] for tab in payload["manager_tabs"]], ["Knows", "Manage"])
        person_card = next(form for form in payload["item_forms"] if form.get("group") == "knows_people")
        self.assertTrue(person_card["hero_image_src"].startswith("data:image/svg+xml;base64,"))
        self.assertEqual(
            [section["label"] for section in person_card["sections"]],
            ["What Tater Knows", "Recent Chapters", "Still In Progress", "Changed Over Time"],
        )
        self.assertTrue(any(form.get("group") == "manage_memory" for form in payload["item_forms"]))
        self.assertTrue(any(form.get("group") == "manage_system" for form in payload["item_forms"]))

    def test_memory_ui_can_correct_facts_and_forget_selected_memories(self):
        owner = "mem:user:webui:fred"
        self.core.save_memory_doc(
            self.redis,
            owner,
            {
                "schema_version": 2,
                "last_updated": 100.0,
                "facts": {
                    "favorite_color": {"value": "blue", "confidence": 0.9, "evidence": ["m1"], "updated_at": 100.0},
                    "communication_style": {"value": "formal", "confidence": 0.9, "evidence": ["m1"], "updated_at": 100.0},
                },
            },
            now=100.0,
        )
        self.core._memory_store_episodes(
            self.redis,
            owner,
            [{"summary": "Temporary chapter", "kind": "milestone", "salience": 0.9, "evidence": ["m2"], "occurred_at": 120.0}],
            min_salience=0.7,
            max_items=25,
            now_ts=120.0,
        )
        episode_id = self.core._memory_load_episodes(self.redis, owner)[0]["id"]
        token = self.core._memory_core_ui_scope_token("user", "webui", "fred")

        result = self.core.handle_htmlui_tab_action(
            action="memory_manage_scope",
            payload={
                "id": token,
                "values": {
                    "edit_key": "favorite_color",
                    "edit_value": "orange",
                    "forget_keys": ["communication_style"],
                    "forget_episode_ids": [episode_id],
                },
            },
        )

        self.assertTrue(result["ok"])
        doc = self.core.load_memory_doc(self.redis, owner)
        self.assertEqual(doc["facts"]["favorite_color"]["value"], "orange")
        self.assertNotIn("communication_style", doc["facts"])
        self.assertEqual(doc["retired_facts"][0]["value"], "blue")
        self.assertEqual(self.core._memory_load_episodes(self.redis, owner), [])

    def test_memory_ui_recall_test_returns_readable_results(self):
        owner = "mem:user:webui:fred"
        self.core.save_memory_doc(
            self.redis,
            owner,
            {
                "schema_version": 2,
                "last_updated": 100.0,
                "facts": {"favorite_console": {"value": "Dreamcast", "confidence": 0.9, "evidence": ["m1"], "updated_at": 100.0}},
            },
            now=100.0,
        )

        message = self.core._memory_core_ui_run_tool_action(
            tool_action="recall_memory",
            import_backup_json="",
            import_backup_path="",
            replace_existing=False,
            restore_settings=True,
            confirm_text="",
            recall_target=owner,
            recall_query="favorite console",
            recall_include_history=True,
        )

        self.assertIn("Favorite console: Dreamcast", message)
        self.assertEqual(self.redis.hashes[self.core._MEMORY_CORE_UI_LAST_TOOL_HASH]["action"], "recall_memory")


if __name__ == "__main__":
    unittest.main()
