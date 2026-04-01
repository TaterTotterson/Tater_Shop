# verba/comfyui_audio_ace.py
import json
import asyncio
import re
import yaml
import random
import copy
import logging
import time
import requests
from urllib.parse import quote
from verba_base import ToolVerba
from helpers import redis_client, run_comfy_prompt
from verba_result import action_failure, action_success

def _build_media_metadata(binary: bytes, *, media_type: str, name: str, mimetype: str) -> dict:
    if not isinstance(binary, (bytes, bytearray)):
        raise TypeError("binary must be bytes")
    return {
        "type": media_type,
        "name": name,
        "mimetype": mimetype,
        "size": len(binary),
        "bytes": bytes(binary),
    }

logger = logging.getLogger("comfyui_audio_ace")
logger.setLevel(logging.INFO)


class ComfyUIAudioAcePlugin(ToolVerba):
    name = "comfyui_audio_ace"
    verba_name = "ComfyUI Audio Ace"
    version = "1.0.6"
    min_tater_version = "59"
    usage = '{"function":"comfyui_audio_ace","arguments":{"prompt":"<Concept for the song, e.g. happy summer song>"}}'
    description = "Creates original songs and music tracks using ComfyUI Audio Ace."
    verba_dec = "Compose a music track from a prompt with ComfyUI Audio Ace."
    pretty_name = "Your Song"
    settings_category = "ComfyUI Audio Ace"
    platforms = ["discord", "webui", "macos", "homeassistant", "matrix", "telegram"]

    required_settings = {
        "COMFYUI_AUDIO_ACE_URL": {
            "label": "ComfyUI Audio Ace URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "Base URL for the ComfyUI Ace Audio server."
        },
        "HA_DEFAULT_MEDIA_PLAYER": {
            "label": "Default media_player entity",
            "type": "string",
            "default": "",
            "description": (
                "Optional for Home Assistant. If unset, Tater will try to play on the Voice PE device "
                "that spoke (based on device_name/device_id context). Example: media_player.living_room_speaker"
            )
        }
    }

    waiting_prompt_template = (
        "Write a fun, upbeat message saying you’re writing lyrics and calling in a virtual band now! "
        "Only output that message."
    )
    when_to_use = ""
    common_needs = []
    missing_info_prompts = []


    # ---------------------------
    # Settings / URL helpers
    # ---------------------------
    @staticmethod
    def _settings():
        return redis_client.hgetall(f"verba_settings:{ComfyUIAudioAcePlugin.settings_category}") or {}

    @staticmethod
    def _as_text(value, default=""):
        if value is None:
            return str(default)
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", "ignore")
        return str(value)

    @staticmethod
    def get_base_http():
        settings = ComfyUIAudioAcePlugin._settings()
        url = ComfyUIAudioAcePlugin._as_text(settings.get("COMFYUI_AUDIO_ACE_URL"), "").strip() or "http://localhost:8188"
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "http://" + url
        return url.rstrip("/")

    @staticmethod
    def get_base_ws(base_http: str) -> str:
        scheme = "wss" if base_http.startswith("https://") else "ws"
        return base_http.replace("http", scheme, 1)

    # ---------------------------
    # Workflow template
    # ---------------------------
    @staticmethod
    def get_workflow_template():
        return {
          "14": {
            "inputs": {
              "tags": "",
              "lyrics": "",
              "lyrics_strength": 0.9900000000000002,
              "clip": ["40", 1]
            },
            "class_type": "TextEncodeAceStepAudio",
            "_meta": {"title": "TextEncodeAceStepAudio"}
          },
          "17": {
            "inputs": {"seconds": 120, "batch_size": 1},
            "class_type": "EmptyAceStepLatentAudio",
            "_meta": {"title": "EmptyAceStepLatentAudio"}
          },
          "18": {
            "inputs": {"samples": ["52", 0], "vae": ["40", 2]},
            "class_type": "VAEDecodeAudio",
            "_meta": {"title": "VAEDecodeAudio"}
          },
          "40": {
            "inputs": {"ckpt_name": "ace_step_v1_3.5b.safetensors"},
            "class_type": "CheckpointLoaderSimple",
            "_meta": {"title": "Load Checkpoint"}
          },
          "44": {
            "inputs": {"conditioning": ["14", 0]},
            "class_type": "ConditioningZeroOut",
            "_meta": {"title": "ConditioningZeroOut"}
          },
          "49": {
            "inputs": {"model": ["51", 0], "operation": ["50", 0]},
            "class_type": "LatentApplyOperationCFG",
            "_meta": {"title": "LatentApplyOperationCFG"}
          },
          "50": {
            "inputs": {"multiplier": 1.0000000000000002},
            "class_type": "LatentOperationTonemapReinhard",
            "_meta": {"title": "LatentOperationTonemapReinhard"}
          },
          "51": {
            "inputs": {"shift": 5.000000000000001, "model": ["40", 0]},
            "class_type": "ModelSamplingSD3",
            "_meta": {"title": "ModelSamplingSD3"}
          },
          "52": {
            "inputs": {
              "seed": 194793839343750,
              "steps": 50,
              "cfg": 5,
              "sampler_name": "euler",
              "scheduler": "simple",
              "denoise": 1,
              "model": ["49", 0],
              "positive": ["14", 0],
              "negative": ["44", 0],
              "latent_image": ["17", 0]
            },
            "class_type": "KSampler",
            "_meta": {"title": "KSampler"}
          },
          "59": {
            "inputs": {
              "filename_prefix": "audio/ComfyUI",
              "quality": "V0",
              "audioUI": "",
              "audio": ["18", 0]
            },
            "class_type": "SaveAudioMP3",
            "_meta": {"title": "Save Audio (MP3)"}
          }
        }

    # ---------------------------
    # LLM helpers
    # ---------------------------
    async def get_tags_and_lyrics(self, user_prompt, llm_client):
        return await self.generate_tags_and_lyrics(user_prompt, llm_client)

    @staticmethod
    async def generate_tags_and_lyrics(user_prompt, llm_client):
        if llm_client is None:
            safe = (user_prompt or "a warm uplifting song").strip()
            return "pop, uplifting, melodic", f"[inst]\n\n[verse]\n{safe}\n\n[chorus]\n{safe}\n"
        system_prompt = (
            f'The user wants a song: "{user_prompt}".\n\n'
            "Write a JSON object with these two fields:\n"
            "1. `tags`: a comma-separated list of music style keywords.\n"
            "2. `lyrics`: multiline string using the following format (in English):\n\n"
            "[inst]\\n\\n[verse]\\nline one\\nline two\\n...\n\n"
            "IMPORTANT:\n"
            "- Escape all newlines using double backslashes (\\n).\n"
            "- Use only these section headers: [inst], [verse], [chorus], [bridge], [outro].\n"
            "- Do NOT use [verse 1], [chorus 2], or any custom tag variants.\n"
            "- Output ONLY valid JSON, no explanation."
        )
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": "Write tags and lyrics for the song."}
            ]
        )
        content = response.get("message", {}).get("content", "").strip()
        try:
            cleaned = re.sub(r"^```(?:json)?\s*|```$", "", content, flags=re.MULTILINE).strip()
            try:
                result = json.loads(cleaned)
            except json.JSONDecodeError:
                result = yaml.safe_load(cleaned)
                cleaned = json.dumps(result)
                result = json.loads(cleaned)

            tags = result.get("tags", "").strip()
            lyrics = result.get("lyrics", "").strip()

            allowed_sections = ["[verse]", "[chorus]", "[bridge]", "[outro]"]
            if not tags or "[inst]" not in lyrics or not any(tag in lyrics for tag in allowed_sections):
                raise Exception("Missing or improperly formatted 'tags' or 'lyrics'.")

            return tags, lyrics
        except Exception as e:
            logger.exception("LLM response format error: %s", e)
            raise Exception(f"LLM response format error: {e}\nContent:\n{content}")

    # ---------------------------
    # ComfyUI helpers
    # ---------------------------
    @staticmethod
    def get_history(base_http: str, prompt_id: str):
        r = requests.get(f"{base_http}/history/{prompt_id}", timeout=60)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def get_audio_bytes(base_http: str, filename: str, subfolder: str, folder_type: str) -> bytes:
        params = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        r = requests.get(f"{base_http}/view", params=params, timeout=120)
        r.raise_for_status()
        return r.content

    @staticmethod
    def build_comfy_view_url(base_http: str, filename: str, subfolder: str | None, folder_type: str | None) -> str:
        return (
            f"{base_http}/view"
            f"?filename={quote(filename)}"
            f"&subfolder={quote(subfolder or '')}"
            f"&type={quote(folder_type or 'output')}"
        )

    # ---------------------------
    # Main generation pipeline
    # ---------------------------
    @staticmethod
    def build_workflow(tags: str, lyrics: str) -> dict:
        workflow = copy.deepcopy(ComfyUIAudioAcePlugin.get_workflow_template())
        workflow["14"]["inputs"]["tags"] = tags
        workflow["14"]["inputs"]["lyrics"] = lyrics

        # Estimate duration from WORD count (more stable than line counting)
        # Normalize common JSON-escaped newline style first
        if "\\n" in lyrics and "\n" not in lyrics:
            lyrics = lyrics.replace("\\n", "\n")
        lyrics = lyrics.replace("\r\n", "\n")

        # Count words (ignore section headers as best we can)
        # Remove bracketed headers like [verse], [chorus]
        lyrics_for_count = re.sub(r"\[[^\]]+\]", " ", lyrics)
        # Remove plain headers like "Verse", "Chorus", "Bridge", "Outro" on their own lines
        lyrics_for_count = re.sub(r"(?im)^\s*(verse|chorus|bridge|outro|inst)\s*$", " ", lyrics_for_count)

        words = re.findall(r"\b[\w']+\b", lyrics_for_count)
        word_count = len(words)

        # Rough pacing: ~2.2 words/sec + padding for intro/outro
        estimated_duration = int(word_count / 2.2) + 15
        duration = max(30, min(300, estimated_duration))
        workflow["17"]["inputs"]["seconds"] = duration

        logger.info("Audio duration calc: words=%s estimated=%ss final=%ss", word_count, estimated_duration, duration)

        # Randomize seeds for uniqueness
        for node in workflow.values():
            if isinstance(node, dict):
                inputs = node.get("inputs")
                if isinstance(inputs, dict) and "seed" in inputs:
                    inputs["seed"] = random.randint(0, 2**63 - 1)
        return workflow

    @staticmethod
    def process_prompt_sync(tags: str, lyrics: str):
        """
        Returns (media_url, audio_bytes_or_None).
        Prefer ComfyUI /view URL; also return bytes as a fallback for Discord/WebUI/Matrix.
        """
        base_http = ComfyUIAudioAcePlugin.get_base_http()
        base_ws   = ComfyUIAudioAcePlugin.get_base_ws(base_http)

        workflow = ComfyUIAudioAcePlugin.build_workflow(tags, lyrics)

        prompt_id, _ = run_comfy_prompt(base_http, base_ws, workflow)

        for _ in range(50):
            history = ComfyUIAudioAcePlugin.get_history(base_http, prompt_id).get(prompt_id, {})
            outputs = history.get("outputs", {}) if isinstance(history, dict) else {}

            for _, node_out in outputs.items():
                if "audio" in node_out:
                    for audio_meta in node_out["audio"]:
                        filename = audio_meta.get("filename")
                        subfolder = audio_meta.get("subfolder", "")
                        folder_type = audio_meta.get("type", "output")
                        if filename:
                            media_url = ComfyUIAudioAcePlugin.build_comfy_view_url(
                                base_http, filename, subfolder, folder_type
                            )
                            try:
                                audio_bytes = ComfyUIAudioAcePlugin.get_audio_bytes(base_http, filename, subfolder, folder_type)
                            except Exception:
                                audio_bytes = None
                            return media_url, audio_bytes
            time.sleep(0.5)

        raise Exception("No audio returned.")

    async def _generate(self, prompt: str, llm_client):
        tags, lyrics = await self.generate_tags_and_lyrics(prompt, llm_client)
        media_url, audio_bytes = await asyncio.to_thread(self.process_prompt_sync, tags, lyrics)

        audio_meta = None
        if audio_bytes:
            audio_meta = _build_media_metadata(
                audio_bytes,
                media_type="audio",
                name="ace_song.mp3",
                mimetype="audio/mpeg",
            )

        flair = ""
        if llm_client is not None:
            try:
                system_msg = f'The user received a ComfyUI-generated song based on: "{prompt}"'
                response = await llm_client.chat(
                    messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": "Send a short friendly comment about the new song. Only generate the message. Do not respond to this message."}
                    ]
                )
                flair = (response["message"].get("content", "") or "").strip()[:240]
            except Exception:
                flair = ""

        facts = {
            "prompt": prompt,
            "tags": tags,
            "lyrics_preview": (lyrics or "")[:280],
            "media_url": media_url,
            "artifact_count": 1 if audio_meta else 0,
        }
        return action_success(
            facts=facts,
            summary_for_user="Generated one song track.",
            flair=flair,
            say_hint="Confirm the song generation result and reference the attached audio or URL.",
            artifacts=[audio_meta] if audio_meta else [],
        )

    # ---------------------------------------
    # Discord
    # ---------------------------------------
    async def handle_discord(self, message, args, llm_client):
        args = args or {}
        user_prompt = str(args.get("prompt") or "").strip()
        if not user_prompt:
            return action_failure(
                code="missing_prompt",
                message="No prompt provided.",
                needs=["Provide a prompt describing the song you want."],
                say_hint="Ask the user for a music prompt.",
            )
        try:
            return await self._generate(user_prompt, llm_client)
        except Exception as e:
            logger.exception("ComfyUIAudioAcePlugin Discord error: %s", e)
            return action_failure(
                code="song_generation_failed",
                message=f"Failed to create song: {e}",
                say_hint="Explain the generation failure and suggest retrying.",
            )

    # ---------------------------------------
    # WebUI
    # ---------------------------------------
    async def handle_webui(self, args, llm_client):
        args = args or {}
        prompt = str(args.get("prompt") or "").strip()
        if not prompt:
            return action_failure(
                code="missing_prompt",
                message="No prompt provided.",
                needs=["Provide a prompt describing the song you want."],
                say_hint="Ask the user for a music prompt.",
            )
        try:
            return await self._generate(prompt, llm_client)
        except Exception as e:
            logger.exception("ComfyUIAudioAcePlugin WebUI error: %s", e)
            return action_failure(
                code="song_generation_failed",
                message=f"Failed to create song: {e}",
                say_hint="Explain the generation failure and suggest retrying.",
            )


    async def handle_macos(self, args, llm_client, context=None):
        try:
            return await self.handle_webui(args, llm_client, context=context)
        except TypeError:
            return await self.handle_webui(args, llm_client)
    async def handle_telegram(self, update, args, llm_client):
        return await self.handle_webui(args or {}, llm_client)

    # ---------------------------------------
    # Matrix
    # ---------------------------------------
    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        args = args or {}
        prompt = (args.get("prompt") or "").strip()
        if not prompt:
            return action_failure(
                code="missing_prompt",
                message="No prompt provided.",
                needs=["Provide a prompt describing the song you want."],
                say_hint="Ask the user for a music prompt.",
            )
        try:
            return await self._generate(prompt, llm_client)
        except Exception as e:
            logger.exception("ComfyUIAudioAcePlugin Matrix error: %s", e)
            return action_failure(
                code="song_generation_failed",
                message=f"Failed to create song: {e}",
                say_hint="Explain the generation failure and suggest retrying.",
            )

    # ---------------------------------------
    # Home Assistant
    # ---------------------------------------
    async def handle_homeassistant(self, args, llm_client, context: dict | None = None):
        """
        Quick ACK to HA; do heavy work off the pipeline:
          - generate audio via ComfyUI
          - build direct ComfyUI /view URL
          - play on configured media_player OR (if unset) try to play on the speaking Voice PE device
            using the HA room/device context passed from the conversation agent.
        """
        args = args or {}
        prompt = (args.get("prompt") or "").strip()
        if not prompt:
            return action_failure(
                code="missing_prompt",
                message="No prompt provided.",
                needs=["Provide a prompt describing the song you want."],
                say_hint="Ask the user for a music prompt.",
            )

        target_player = await self._pick_target_player(context=context)
        if not target_player:
            # Give a helpful hint including what we saw from context
            dev = ((context or {}).get("device_name") or (context or {}).get("device_id") or "").strip()
            area = ((context or {}).get("area_name") or (context or {}).get("area_id") or "").strip()
            sat = self._extract_satellite_entity_id(context or {})
            hint = ""
            if dev or area or sat:
                hint = (
                    f" (I heard you from device={dev or 'unknown'}, area={area or 'unknown'}, "
                    f"satellite={sat or 'unknown'}.)"
                )
            return action_failure(
                code="missing_media_player",
                message=(
                    "I can create your song, but I couldn't find a media player to play it on."
                    f"{hint} Set a default media_player entity in ComfyUI Audio Ace settings, "
                    "or ensure the Voice PE device exposes a media_player entity in Home Assistant."
                ),
                needs=["Set `HA_DEFAULT_MEDIA_PLAYER` in ComfyUI Audio Ace settings."],
                say_hint="Explain the missing media player configuration and ask the user to set it.",
            )

        # Fire-and-forget the heavy work
        try:
            asyncio.create_task(self._bg_generate_and_play(prompt, llm_client, target_player))
        except Exception as e:
            logger.exception("Failed to schedule background job: %s", e)
            return action_failure(
                code="background_job_failed",
                message=f"Failed to schedule song generation: {e}",
                say_hint="Explain that background scheduling failed and suggest retrying.",
            )

        return action_success(
            facts={
                "prompt": prompt,
                "target_player": target_player,
                "background_started": True,
            },
            summary_for_user=f"Started generating a song and queued playback on {target_player}.",
            say_hint="Confirm that generation started and playback will occur on the selected player.",
        )

    async def _bg_generate_and_play(self, prompt: str, llm_client, target_player: str):
        """
        Off the critical path:
         - generate song
         - build ComfyUI /view URL
         - play on media_player
        """
        try:
            tags, lyrics = await self.generate_tags_and_lyrics(prompt, llm_client)
            media_url, _audio_bytes = await asyncio.to_thread(self.process_prompt_sync, tags, lyrics)
        except Exception as e:
            logger.exception("ComfyUI generation failed: %s", e)
            return

        if not media_url:
            logger.error("No media URL returned from ComfyUI.")
            return

        try:
            ha = self._HA()
            ha.play_media(target_player, media_url, mimetype="music")
        except Exception as e:
            logger.exception("Failed to play media on %s: %s", target_player, e)
            return

    # ---------------------------------------
    # Helpers for HA
    # ---------------------------------------
    @staticmethod
    def _slugify(s: str) -> str:
        s = (s or "").strip().lower()
        # HA entity_id friendly: lowercase, underscores
        s = re.sub(r"[^a-z0-9]+", "_", s)
        s = re.sub(r"_+", "_", s).strip("_")
        return s

    @staticmethod
    def _ctx_text(ctx: dict | None, *keys: str) -> str:
        payload = ctx if isinstance(ctx, dict) else {}
        origin = payload.get("origin") if isinstance(payload.get("origin"), dict) else {}
        for key in keys:
            raw = payload.get(key)
            if raw in (None, ""):
                raw = origin.get(key)
            txt = str(raw or "").strip()
            if txt:
                return txt
        return ""

    @staticmethod
    def _dedupe_preserve(items: list[str]) -> list[str]:
        out = []
        seen = set()
        for item in items or []:
            key = str(item or "").strip()
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(key)
        return out

    def _strip_assist_suffixes(self, object_id_slug: str) -> str:
        base = (object_id_slug or "").strip("_")
        for suffix in (
            "_assist_satellite",
            "_satellite_assist",
            "_assist",
            "_satellite",
        ):
            if base.endswith(suffix):
                base = base[: -len(suffix)].strip("_")
        return base

    def _extract_satellite_entity_id(self, context: dict | None) -> str:
        ctx = context if isinstance(context, dict) else {}
        for key in (
            "assist_satellite_entity_id",
            "satellite_entity_id",
            "satellite_id",
            "source_entity_id",
            "entity_id",
            "source",
            "device_name",  # Some callers pass the entity_id string here.
        ):
            val = self._ctx_text(ctx, key)
            if val.startswith("assist_satellite."):
                return val
        return ""

    def _media_player_candidates_from_satellite_entity(self, satellite_entity_id: str) -> list[str]:
        sat = (satellite_entity_id or "").strip()
        if not sat.startswith("assist_satellite.") or "." not in sat:
            return []

        object_id = sat.split(".", 1)[1]
        obj_slug = self._slugify(object_id)
        base = self._strip_assist_suffixes(obj_slug)

        candidates = [
            f"media_player.{obj_slug}",
            f"media_player.{obj_slug}_media_player",
            f"media_player.{obj_slug}_speaker",
        ]
        if "assist_satellite" in obj_slug:
            candidates.extend(
                [
                    f"media_player.{obj_slug.replace('assist_satellite', 'media_player')}",
                    f"media_player.{obj_slug.replace('_assist_satellite', '')}",
                ]
            )
        if base:
            candidates.extend(
                [
                    f"media_player.{base}",
                    f"media_player.{base}_media_player",
                    f"media_player.{base}_speaker",
                ]
            )

        return self._dedupe_preserve(candidates)

    def _media_player_candidates_from_device_name(self, device_name: str) -> list[str]:
        raw = (device_name or "").strip()
        if not raw:
            return []
        if raw.startswith("assist_satellite."):
            return self._media_player_candidates_from_satellite_entity(raw)

        slug = self._slugify(raw)
        base = self._strip_assist_suffixes(slug)

        candidates = [
            f"media_player.{slug}",
            f"media_player.{slug}_media_player",
            f"media_player.{slug}_speaker",
        ]
        if "assist_satellite" in slug:
            candidates.extend(
                [
                    f"media_player.{slug.replace('assist_satellite', 'media_player')}",
                    f"media_player.{slug.replace('_assist_satellite', '')}",
                ]
            )
        if base:
            candidates.extend(
                [
                    f"media_player.{base}",
                    f"media_player.{base}_media_player",
                    f"media_player.{base}_speaker",
                ]
            )
        return self._dedupe_preserve(candidates)

    def _entity_exists(self, ha, entity_id: str) -> bool:
        eid = (entity_id or "").strip()
        if not eid.startswith("media_player."):
            return False
        try:
            st = ha.get_state(eid)
            return bool(isinstance(st, dict) and str(st.get("entity_id") or "").startswith("media_player."))
        except Exception:
            return False

    def _rank_media_player_entity_id(
        self,
        entity_id: str,
        *,
        device_name: str = "",
        satellite_entity_id: str = "",
        preferred_ids: set[str] | None = None,
    ) -> int:
        low = str(entity_id or "").strip().lower()
        if not low.startswith("media_player."):
            return -1000

        score = 0
        preferred = preferred_ids or set()
        if low in preferred:
            score += 100

        want_slug = self._slugify(device_name)
        sat_obj = ""
        if str(satellite_entity_id or "").startswith("assist_satellite.") and "." in satellite_entity_id:
            sat_obj = self._slugify(satellite_entity_id.split(".", 1)[1])
        sat_base = self._strip_assist_suffixes(sat_obj)

        if want_slug and want_slug in low:
            score += 18
        if sat_obj and sat_obj in low:
            score += 24
        if sat_base and sat_base in low:
            score += 20
        if low.endswith("_media_player"):
            score += 8
        if low.endswith("_speaker"):
            score += 5
        if ".group" in low:
            score -= 4
        return score

    async def _media_player_for_device_id(
        self,
        ha,
        *,
        device_id: str,
        device_name: str = "",
        satellite_entity_id: str = "",
    ) -> str | None:
        did = (device_id or "").strip()
        if not did:
            return None

        entity_reg = await ha.entity_registry_list()
        raw_candidates: list[str] = []
        for row in entity_reg or []:
            if not isinstance(row, dict):
                continue
            if str(row.get("device_id") or "").strip() != did:
                continue
            if row.get("disabled_by") not in (None, ""):
                continue
            ent = str(row.get("entity_id") or "").strip()
            if ent.startswith("media_player."):
                raw_candidates.append(ent)

        if not raw_candidates:
            return None

        preferred = set(
            self._dedupe_preserve(
                self._media_player_candidates_from_satellite_entity(satellite_entity_id)
                + self._media_player_candidates_from_device_name(device_name)
            )
        )

        ranked = sorted(
            set(raw_candidates),
            key=lambda eid: (
                -self._rank_media_player_entity_id(
                    eid,
                    device_name=device_name,
                    satellite_entity_id=satellite_entity_id,
                    preferred_ids=preferred,
                ),
                eid,
            ),
        )

        for eid in ranked:
            if self._entity_exists(ha, eid):
                return eid
        return ranked[0] if ranked else None

    async def _pick_target_player(self, context: dict | None = None) -> str | None:
        """
        Pick a media_player in this order:
          1) Explicit plugin setting HA_DEFAULT_MEDIA_PLAYER
          2) Deterministic registry lookup by speaking device_id (same HA device)
          3) Deterministic entity-id patterns from assist_satellite/device_name
          4) Fuzzy fallback scan by friendly_name/entity_id
        """
        sett = self._settings()
        default_mp = (sett.get("HA_DEFAULT_MEDIA_PLAYER") or "").strip()
        if default_mp:
            return default_mp

        ctx = context or {}
        device_name = self._ctx_text(ctx, "device_name")
        device_id = self._ctx_text(ctx, "device_id")
        satellite_entity_id = self._extract_satellite_entity_id(ctx)

        try:
            ha = self._HA()
        except Exception as e:
            logger.warning("Cannot initialize Home Assistant client for media_player resolution: %s", e)
            return None

        # Deterministic path: resolve media_player entities registered to this exact HA device.
        if device_id:
            try:
                by_device = await self._media_player_for_device_id(
                    ha,
                    device_id=device_id,
                    device_name=device_name,
                    satellite_entity_id=satellite_entity_id,
                )
                if by_device:
                    return by_device
            except Exception as e:
                logger.warning("Device-id media_player lookup failed for %s: %s", device_id, e)

        # Deterministic fallback: known Voice PE naming patterns.
        direct_candidates = self._dedupe_preserve(
            self._media_player_candidates_from_satellite_entity(satellite_entity_id)
            + self._media_player_candidates_from_device_name(device_name)
        )
        for eid in direct_candidates:
            if self._entity_exists(ha, eid):
                return eid

        # Last resort: fuzzy scan by friendly_name / entity_id.
        try:
            all_states = ha.list_states()
            best = self._find_best_media_player(
                all_states,
                device_name=device_name,
                satellite_entity_id=satellite_entity_id,
                preferred_ids=direct_candidates,
            )
            if best:
                return best
        except Exception:
            logger.exception("Failed scanning HA states for a matching media_player.")

        return None

    def _find_best_media_player(
        self,
        all_states: list[dict],
        *,
        device_name: str = "",
        satellite_entity_id: str = "",
        preferred_ids: list[str] | None = None,
    ) -> str | None:
        """
        Try to find the most likely media_player by scoring friendly_name/entity_id hints.
        """
        want = (device_name or "").strip().lower()
        preferred = {str(e or "").strip().lower() for e in (preferred_ids or []) if e}

        best_id = None
        best_score = -1000
        for st in all_states or []:
            eid = st.get("entity_id") or ""
            if not eid.startswith("media_player."):
                continue
            low_eid = eid.lower()
            attrs = st.get("attributes") or {}
            fname = (attrs.get("friendly_name") or "").strip()
            fname_low = fname.lower()

            score = self._rank_media_player_entity_id(
                low_eid,
                device_name=device_name,
                satellite_entity_id=satellite_entity_id,
                preferred_ids=preferred,
            )
            if want and fname_low == want:
                score += 28
            elif want and want in fname_low:
                score += 14

            if score > best_score:
                best_score = score
                best_id = eid

        return best_id if best_score > 0 else None

    class _HA:
        def __init__(self):
            s = redis_client.hgetall("homeassistant_settings") or {}
            self.base = (s.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
            token = (s.get("HA_TOKEN") or "").strip()
            if not token:
                raise ValueError(
                    "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                    "and add a Long-Lived Access Token."
                )
            self.token = token
            self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        def _req(self, method: str, path: str, json=None, timeout=120):
            r = requests.request(method, f"{self.base}{path}", headers=self.headers, json=json, timeout=timeout)
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
            try:
                return r.json()
            except Exception:
                return r.text

        def list_states(self) -> list[dict]:
            return self._req("GET", "/api/states", timeout=30)

        def get_state(self, entity_id: str):
            return self._req("GET", f"/api/states/{entity_id}", timeout=10)

        def play_media(self, entity_id: str, url: str, mimetype="music"):
            data = {"entity_id": entity_id, "media_content_id": url, "media_content_type": mimetype}
            return self._req("POST", "/api/services/media_player/play_media", json=data, timeout=30)

        def ws_url(self) -> str:
            if self.base.startswith("https://"):
                return self.base.replace("https://", "wss://", 1) + "/api/websocket"
            return self.base.replace("http://", "ws://", 1) + "/api/websocket"

        async def ws_call(self, msg_type: str, timeout_s: float = 20.0):
            try:
                import aiohttp
            except Exception as e:
                raise RuntimeError(f"aiohttp is required for HA websocket calls: {e}")

            ws_url = self.ws_url()
            timeout = aiohttp.ClientTimeout(total=timeout_s)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.ws_connect(ws_url, heartbeat=30) as ws:
                    first = await ws.receive_json()
                    if (first or {}).get("type") == "auth_required":
                        await ws.send_json({"type": "auth", "access_token": self.token})
                        auth = await ws.receive_json()
                        if (auth or {}).get("type") != "auth_ok":
                            raise RuntimeError(f"HA websocket auth failed: {auth}")
                    elif (first or {}).get("type") != "auth_ok":
                        await ws.send_json({"type": "auth", "access_token": self.token})
                        auth = await ws.receive_json()
                        if (auth or {}).get("type") != "auth_ok":
                            raise RuntimeError(f"Unexpected HA websocket hello/auth flow: first={first}, auth={auth}")

                    await ws.send_json({"id": 1, "type": msg_type})
                    while True:
                        msg = await ws.receive_json()
                        if msg.get("type") != "result" or msg.get("id") != 1:
                            continue
                        if not msg.get("success", False):
                            raise RuntimeError(f"HA websocket call failed: {msg}")
                        return msg.get("result")

        async def entity_registry_list(self) -> list[dict]:
            res = await self.ws_call("config/entity_registry/list", timeout_s=30.0)
            return res if isinstance(res, list) else []


verba = ComfyUIAudioAcePlugin()
