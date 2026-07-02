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
from typing import Any
from urllib.parse import quote, unquote
from verba_base import ToolVerba
from helpers import get_llm_client_from_env, redis_client, run_comfy_prompt
try:
    from helpers import get_primary_llm_client_from_env as _get_primary_llm_client_from_env
except Exception:  # pragma: no cover - compatibility with older Tater runtimes.
    _get_primary_llm_client_from_env = get_llm_client_from_env
from tateros import integration_store as integration_store_module
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


def load_homeassistant_config(*, required: bool = False, client: Any = None) -> dict:
    module = integration_store_module.integration_module("homeassistant")
    if module is not None:
        return module.load_homeassistant_config(required=required, client=client)
    if required:
        raise ValueError("Home Assistant integration is not enabled.")
    return {"base": "", "token": ""}


class ComfyUIAudioAcePlugin(ToolVerba):
    name = "comfyui_audio_ace"
    verba_name = "ComfyUI Audio Ace"
    version = "1.0.27"
    min_tater_version = "59"
    usage = '{"function":"comfyui_audio_ace","arguments":{"prompt":"<Concept for the song, e.g. happy summer song>"}}'
    description = (
        "Creates original songs and music tracks using ComfyUI Audio Ace. "
        "On voice satellites, playback automatically uses the current room's preferred media player when configured; otherwise it falls back to the speaking satellite."
    )
    verba_dec = "Compose a music track from a prompt with ComfyUI Audio Ace."
    pretty_name = "Your Song"
    settings_category = "ComfyUI Audio Ace"
    platforms = ['discord', 'webui', 'little_spud', 'macos', 'voice_core', 'homeassistant', 'matrix', 'telegram']

    required_settings = {
        "COMFYUI_AUDIO_ACE_URL": {
            "label": "ComfyUI Audio Ace URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "Base URL for the ComfyUI Ace Audio server."
        },
        "COMFYUI_AUDIO_ACE_WORKFLOW": {
            "label": "Workflow Template (JSON)",
            "type": "file",
            "accept": ".json,application/json",
            "default": "",
            "description": (
                "Upload your ComfyUI Audio Ace workflow .json file. "
                "If empty, Tater uses the bundled Ace Step workflow."
            ),
        }
    }

    waiting_prompt_template = (
        "Write a fun, upbeat message saying you’re writing lyrics and calling in a virtual band now! "
        "Only output that message."
    )
    when_to_use = (
        "Use when the user asks Tater to create, compose, generate, or make an original song, music track, jingle, or instrumental."
    )
    common_needs = ["prompt"]
    missing_info_prompts = []

    @staticmethod
    def _background_llm_client(llm_client=None):
        host = str(getattr(llm_client, "host", "") or "").strip()
        model = str(getattr(llm_client, "model", "") or "").strip()
        kwargs = {"redis_conn": redis_client}
        if host:
            kwargs["host"] = host
        if model:
            kwargs["model"] = model
        return _get_primary_llm_client_from_env(**kwargs)


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
        settings = ComfyUIAudioAcePlugin._settings()
        workflow_raw = settings.get("COMFYUI_AUDIO_ACE_WORKFLOW", b"")
        if isinstance(workflow_raw, dict):
            return workflow_raw

        workflow_str = ComfyUIAudioAcePlugin._as_text(workflow_raw, "").strip()
        if workflow_str:
            try:
                workflow = json.loads(workflow_str)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON in COMFYUI_AUDIO_ACE_WORKFLOW: {exc}") from exc
            if not isinstance(workflow, dict):
                raise ValueError("COMFYUI_AUDIO_ACE_WORKFLOW must contain a JSON object.")
            return workflow

        return ComfyUIAudioAcePlugin._default_workflow_template()

    @staticmethod
    def _default_workflow_template():
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
    def _repair_llm_json_string_escapes(raw: str) -> str:
        text = str(raw or "")
        if not text:
            return ""
        return re.sub(r'\\(?!["\\/bfnrtu])', r"\\\\", text)

    @staticmethod
    def _normalize_generated_lyrics(value: str) -> str:
        lyrics = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
        # Any literal backslashes that survive parsing are almost always broken
        # newline escapes from the model, so recover them as line breaks.
        lyrics = lyrics.replace("\\n", "\n")
        lyrics = re.sub(r"\\+", "\n", lyrics)
        lyrics = re.sub(r"\n{3,}", "\n\n", lyrics)
        return lyrics.strip()

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
            repaired = ComfyUIAudioAcePlugin._repair_llm_json_string_escapes(cleaned)
            try:
                result = json.loads(cleaned)
            except json.JSONDecodeError:
                try:
                    result = json.loads(repaired)
                except json.JSONDecodeError:
                    result = yaml.safe_load(repaired)
                    repaired = json.dumps(result)
                    result = json.loads(repaired)

            tags = result.get("tags", "").strip()
            lyrics = ComfyUIAudioAcePlugin._normalize_generated_lyrics(result.get("lyrics", ""))

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
    def _node_title(node: dict) -> str:
        meta = node.get("_meta") if isinstance(node, dict) else {}
        return ((meta or {}).get("title", "") or "").strip().lower()

    @staticmethod
    def _is_audio_text_encode_node(node: dict) -> bool:
        if not isinstance(node, dict):
            return False
        inputs = node.get("inputs")
        if not isinstance(inputs, dict):
            return False
        class_type = str(node.get("class_type") or "")
        if class_type.startswith("TextEncodeAceStepAudio"):
            return True
        return "tags" in inputs and "lyrics" in inputs

    @staticmethod
    def _set_tags_and_lyrics(workflow: dict, tags: str, lyrics: str) -> int:
        patched = 0
        for node in workflow.values():
            if not ComfyUIAudioAcePlugin._is_audio_text_encode_node(node):
                continue
            inputs = node.setdefault("inputs", {})
            inputs["tags"] = tags
            inputs["lyrics"] = lyrics
            patched += 1
        if patched <= 0:
            raise ValueError(
                "Workflow has no Audio Ace text encode node with `tags` and `lyrics` inputs."
            )
        return patched

    @staticmethod
    def _set_duration(workflow: dict, duration: int) -> dict:
        stats = {"seconds": 0, "duration": 0}
        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            inputs = node.get("inputs")
            if not isinstance(inputs, dict):
                continue

            class_type = str(node.get("class_type") or "").lower()
            title = ComfyUIAudioAcePlugin._node_title(node)
            is_ace_audio_latent = (
                ("acestep" in class_type or "ace step" in title)
                and "latent" in class_type + title
                and "audio" in class_type + title
            )

            if "seconds" in inputs and is_ace_audio_latent:
                inputs["seconds"] = int(duration)
                stats["seconds"] += 1

            if "duration" in inputs and ComfyUIAudioAcePlugin._is_audio_text_encode_node(node):
                inputs["duration"] = int(duration)
                stats["duration"] += 1

        return stats

    @staticmethod
    def _estimate_duration_seconds(lyrics: str) -> int:
        text = str(lyrics or "")
        if "\\n" in text and "\n" not in text:
            text = text.replace("\\n", "\n")
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text_for_count = re.sub(r"\[[^\]]+\]", " ", text)
        text_for_count = re.sub(
            r"(?im)^\s*(verse|chorus|bridge|outro|inst|pre-chorus|final chorus)\s*$",
            " ",
            text_for_count,
        )
        words = re.findall(r"\b[\w']+\b", text_for_count)
        word_count = len(words)
        estimated_duration = int(word_count / 2.2) + 15
        duration = max(30, min(300, estimated_duration))
        logger.info(
            "Audio duration calc: words=%s estimated=%ss final=%ss",
            word_count,
            estimated_duration,
            duration,
        )
        return duration

    @staticmethod
    def build_workflow(tags: str, lyrics: str) -> dict:
        workflow = copy.deepcopy(ComfyUIAudioAcePlugin.get_workflow_template())
        patched_text_nodes = ComfyUIAudioAcePlugin._set_tags_and_lyrics(workflow, tags, lyrics)

        duration = ComfyUIAudioAcePlugin._estimate_duration_seconds(lyrics)
        duration_stats = ComfyUIAudioAcePlugin._set_duration(workflow, duration)
        logger.info(
            "Audio workflow patched: text_nodes=%s seconds_nodes=%s duration_nodes=%s",
            patched_text_nodes,
            duration_stats["seconds"],
            duration_stats["duration"],
        )

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

    async def handle_little_spud(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self.handle_webui(args or {}, llm_client)


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
        prompt, requested_room = self._song_prompt_and_requested_room(args)
        if not prompt:
            return action_failure(
                code="missing_prompt",
                message="No prompt provided.",
                needs=["Provide a prompt describing the song you want."],
                say_hint="Ask the user for a music prompt.",
            )

        playback_context = self._playback_context(context, requested_room)
        preferred = self._preferred_room_media_player_target(playback_context)
        preferred_target = str(preferred.get("target") or "").strip()
        if preferred_target:
            try:
                asyncio.create_task(
                    self._bg_generate_and_play_target(
                        prompt,
                        llm_client,
                        preferred_target,
                        respect_reply_playback=False,
                    )
                )
            except Exception as e:
                logger.exception("Failed to schedule preferred room playback background job: %s", e)
                return action_failure(
                    code="background_job_failed",
                    message=f"Failed to schedule song generation: {e}",
                    say_hint="Explain that background scheduling failed and suggest retrying.",
                )

            return action_success(
                facts={
                    "prompt": prompt,
                    "preferred_media_player": preferred_target,
                    "preferred_room_id": str(preferred.get("room_id") or ""),
                    "room_resolution": str(preferred.get("source") or ""),
                    "resolved_room_names": preferred.get("source_room_names") or [],
                    "playback_target": preferred_target,
                    "playback_mode": "room_preferred",
                    "background_started": True,
                },
                summary_for_user=f"Started generating a song and queued playback on {preferred_target}.",
                say_hint="Confirm that generation started and playback will happen in the preferred room speaker.",
            )

        target_player = await self._pick_target_player(context=playback_context)
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
                    f"{hint} Use Voice Core playback or set a preferred player for the room in Integrations > Rooms."
                ),
                needs=["Set a preferred player for the room or retry from a Voice Core satellite."],
                say_hint="Explain the missing media player configuration and ask the user to set it.",
            )

        # Fire-and-forget the heavy work
        try:
            asyncio.create_task(
                self._bg_generate_and_play_target(
                    prompt,
                    llm_client,
                    f"ha:{target_player}",
                    respect_reply_playback=False,
                )
            )
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
    async def handle_voice_core(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        args = args or {}
        prompt, requested_room = self._song_prompt_and_requested_room(args)
        if not prompt:
            return action_failure(
                code="missing_prompt",
                message="No prompt provided.",
                needs=["Provide a prompt describing the song you want."],
                say_hint="Ask the user for a music prompt.",
            )

        selector = self._voice_core_selector(context)
        playback_context = self._playback_context(context, requested_room)
        preferred = self._preferred_room_media_player_target(playback_context)
        preferred_target = str(preferred.get("target") or "").strip()
        playback_target = preferred_target or (f"voice_core:{selector}" if selector else "")

        if not playback_target:
            return action_failure(
                code="missing_voice_core_satellite",
                message="I can create your song, but I couldn't determine which Voice Core satellite to play it on.",
                say_hint="Explain that Voice Core playback needs the speaking satellite selector and ask the user to retry from a satellite.",
            )

        playback_mode = ""
        try:
            playback_mode = "room_preferred" if preferred_target else "satellite"
            asyncio.create_task(
                self._bg_generate_and_play_target(
                    prompt,
                    llm_client,
                    playback_target,
                    respect_reply_playback=False,
                )
            )
        except Exception as e:
            logger.exception("Failed to schedule Voice Core background job: %s", e)
            return action_failure(
                code="background_job_failed",
                message=f"Failed to schedule song generation: {e}",
                say_hint="Explain that background scheduling failed and suggest retrying.",
            )

        return action_success(
            facts={
                "prompt": prompt,
                "selector": selector,
                "preferred_media_player": preferred_target,
                "playback_target": playback_target,
                "preferred_room_id": str(preferred.get("room_id") or ""),
                "room_resolution": str(preferred.get("source") or ""),
                "resolved_room_names": preferred.get("source_room_names") or [],
                "playback_mode": playback_mode,
                "background_started": True,
            },
            summary_for_user=(
                f"Started generating a song and queued playback on {playback_target}."
                if preferred_target
                else f"Started generating a song and will play it on {playback_target}."
            ),
            say_hint="Confirm that generation started and playback will happen in the current room or on the current satellite.",
        )


    async def _bg_generate_and_play_target(
        self,
        prompt: str,
        llm_client,
        target: str,
        *,
        respect_reply_playback: bool = False,
    ):
        try:
            async with self._background_llm_client(llm_client) as bg_llm_client:
                tags, lyrics = await self.generate_tags_and_lyrics(prompt, bg_llm_client)
            media_url, audio_bytes = await asyncio.to_thread(self.process_prompt_sync, tags, lyrics)
        except Exception as e:
            logger.exception("ComfyUI target playback generation failed: %s", e)
            return

        if not media_url:
            logger.error("No media URL returned from ComfyUI for target playback.")
            return

        try:
            from media_playback import play_media_url_targets

            result = await asyncio.to_thread(
                play_media_url_targets,
                target,
                media_url,
                audio_bytes=audio_bytes,
                media_type="audio/mpeg",
                media_content_type="music",
                filename="ace_song.mp3",
                text="Playing your new song.",
                timeout_s=360.0,
                respect_reply_playback=respect_reply_playback,
            )
            if isinstance(result, dict) and result.get("ok") is False:
                logger.warning("Media playback failed for %s: %s", target, result.get("error"))
        except Exception as e:
            logger.exception("Failed to play generated media on %s: %s", target, e)
            return

    def _voice_core_selector(self, context: dict | None) -> str:
        ctx = context if isinstance(context, dict) else {}
        selector = self._ctx_text(ctx, "satellite_selector")
        if selector:
            return selector
        device_id = self._ctx_text(ctx, "device_id")
        if self._looks_like_transport_selector(device_id):
            return device_id
        return ""

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

    @staticmethod
    def _normalize_room_token(value: Any) -> str:
        txt = str(value or "").strip().lower()
        if not txt:
            return ""
        return re.sub(r"[^a-z0-9]+", " ", txt).strip()

    def _context_room_names(self, context: dict | None) -> list[str]:
        ctx = context if isinstance(context, dict) else {}
        room_names = self._dedupe_preserve(
            [
                self._ctx_text(ctx, "area_name"),
                self._ctx_text(ctx, "room_name"),
                self._ctx_text(ctx, "room"),
                self._ctx_text(ctx, "area"),
                self._ctx_text(ctx, "area_id"),
                self._ctx_text(ctx, "room_id"),
            ]
        )
        if room_names:
            return room_names
        selector = self._voice_core_selector(ctx)
        return self._room_names_for_satellite_selector(selector)

    def _room_names_for_satellite_selector(self, selector: Any) -> list[str]:
        raw_selector = str(selector or "").strip()
        if not raw_selector:
            return []
        wanted = {self._lookup_token(raw_selector)}
        if raw_selector.casefold().startswith("voice_core:"):
            wanted.add(self._lookup_token(raw_selector[len("voice_core:") :]))
        else:
            wanted.add(self._lookup_token(f"voice_core:{raw_selector}"))
        wanted.discard("")
        if not wanted:
            return []

        try:
            raw_registry = redis_client.get("tater:voice:satellites:registry:v1")
            registry = json.loads(raw_registry) if raw_registry else []
        except Exception as e:
            logger.warning("Audio Ace could not read Voice Core satellite registry for room lookup: %s", e)
            return []

        rows = registry if isinstance(registry, list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            row_tokens = {
                self._lookup_token(row.get("selector")),
                self._lookup_token(row.get("device_id")),
                self._lookup_token(row.get("host")),
                self._lookup_token(f"host:{row.get('host')}") if row.get("host") else "",
                self._lookup_token(f"voice_core:{row.get('selector')}") if row.get("selector") else "",
            }
            row_tokens.discard("")
            if not wanted.intersection(row_tokens):
                continue
            return self._dedupe_preserve(
                [
                    metadata.get("area_name"),
                    metadata.get("room_name"),
                    metadata.get("room"),
                    metadata.get("area"),
                    metadata.get("area_id"),
                    metadata.get("room_id"),
                    row.get("area_name"),
                    row.get("room_name"),
                    row.get("room"),
                    row.get("area"),
                    row.get("area_id"),
                    row.get("room_id"),
                ]
            )
        return []

    @staticmethod
    def _explicit_room_from_args(args: dict | None) -> str:
        payload = args if isinstance(args, dict) else {}
        for key in ("room", "playback_room", "area", "area_name", "room_name", "play_in_room"):
            value = str(payload.get(key) or "").strip()
            if value:
                return value
        return ""

    def _song_prompt_and_requested_room(self, args: dict | None) -> tuple[str, str]:
        payload = args if isinstance(args, dict) else {}
        raw_prompt = str(payload.get("prompt") or "").strip()
        requested_room = self._explicit_room_from_args(payload)
        return raw_prompt, requested_room

    def _playback_context(self, context: dict | None, requested_room: str = "") -> dict | None:
        if not requested_room:
            return context
        ctx = dict(context or {})
        ctx["requested_playback_room"] = requested_room
        ctx["room_name"] = requested_room
        ctx["area_name"] = requested_room
        ctx["room"] = requested_room
        ctx["area"] = requested_room
        return ctx

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

    @staticmethod
    def _normalize_mac(value: Any) -> str:
        txt = str(value or "").strip().lower()
        if not txt:
            return ""
        return re.sub(r"[^0-9a-f]", "", txt)

    @staticmethod
    def _looks_like_transport_selector(value: str) -> bool:
        txt = str(value or "").strip().lower()
        if not txt:
            return False
        if txt.startswith(("host:", "selector:", "mac:")):
            return True
        return bool(re.fullmatch(r"(?:\d{1,3}\.){3}\d{1,3}", txt))

    @staticmethod
    def _looks_like_satellite_media_player(entity_id: str, friendly_name: str = "") -> bool:
        low = f"{entity_id or ''} {friendly_name or ''}".lower()
        return any(token in low for token in ("assist_satellite", "voice_pe", "voice pe", "satellite"))

    @staticmethod
    def _lookup_token(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            text = unquote(text)
        except Exception:
            pass
        return re.sub(r"\s+", " ", text).strip().casefold()

    def _target_lookup_tokens(self, value: Any) -> set[str]:
        raw = str(value or "").strip()
        if not raw or raw.casefold() in {"device", "silent"}:
            return set()

        rows: set[str] = set()

        def add(item: Any) -> None:
            token = self._lookup_token(item)
            if token:
                rows.add(token)

        add(raw)
        try:
            from announcement_targets import normalize_announcement_targets, parse_integration_target

            targets = normalize_announcement_targets([raw])
        except Exception:
            parse_integration_target = None
            targets = [raw]

        for target in targets:
            token = str(target or "").strip()
            lower = token.casefold()
            add(token)

            if lower.startswith("ha:"):
                entity_id = token[3:].strip()
                add(entity_id)
                add(f"ha:{entity_id}")
                continue

            if lower.startswith("media_player."):
                add(f"ha:{token}")
                continue

            if lower.startswith("sonos:"):
                sonos_id = token[6:].strip()
                if sonos_id.casefold().startswith("uuid:"):
                    sonos_id = sonos_id[5:].strip()
                add(sonos_id)
                add(f"speaker:{sonos_id}")
                add(f"sonos:{sonos_id}")
                add(f"uuid:{sonos_id}")
                continue

            if lower.startswith("integration:"):
                parsed = {}
                try:
                    parsed = parse_integration_target(token) if parse_integration_target else {}
                except Exception:
                    parsed = {}
                if not parsed:
                    body = token[len("integration:") :]
                    integration_id, sep, encoded_device = body.partition(":")
                    if sep:
                        parsed = {
                            "integration_id": integration_id.strip().casefold(),
                            "device_id": unquote(encoded_device).strip(),
                        }
                integration_id = str(parsed.get("integration_id") or "").strip().casefold()
                device_id = str(parsed.get("device_id") or "").strip()
                if integration_id and device_id:
                    add(device_id)
                    add(f"{integration_id}:{device_id}")
                    add(f"integration:{integration_id}:{device_id}")
                    add(f"integration:{integration_id}:{quote(device_id, safe='')}")
                    if integration_id == "sonos":
                        sonos_id = device_id
                        if sonos_id.casefold().startswith("speaker:"):
                            sonos_id = sonos_id.split(":", 1)[1].strip()
                        if sonos_id.casefold().startswith("uuid:"):
                            sonos_id = sonos_id[5:].strip()
                        add(sonos_id)
                        add(f"speaker:{sonos_id}")
                        add(f"sonos:{sonos_id}")
                        add(f"uuid:{sonos_id}")
                continue

            if lower.startswith("voice_core:"):
                add(token[len("voice_core:") :])

        return rows

    def _device_lookup_tokens(self, device: dict) -> set[str]:
        row = device if isinstance(device, dict) else {}
        details = row.get("details") if isinstance(row.get("details"), dict) else {}
        integration_id = str(row.get("integration_id") or "").strip().casefold()
        rows: set[str] = set()

        def add(item: Any) -> None:
            token = self._lookup_token(item)
            if token:
                rows.add(token)

        for key in (
            "id",
            "ref",
            "device_id",
            "entity_id",
            "resource_ref",
            "resource_id",
            "device_ref",
            "target",
            "target_id",
            "uid",
            "uuid",
            "mac",
            "mac_address",
        ):
            add(row.get(key))
            add(details.get(key))

        for token in list(rows):
            if integration_id:
                add(f"{integration_id}:{token}")
                add(f"integration:{integration_id}:{token}")
                add(f"integration:{integration_id}:{quote(token, safe='')}")
            if token.startswith("media_player."):
                add(f"ha:{token}")
            if integration_id == "sonos":
                sonos_id = token
                if sonos_id.startswith("speaker:"):
                    sonos_id = sonos_id.split(":", 1)[1].strip()
                if sonos_id.startswith("uuid:"):
                    sonos_id = sonos_id[5:].strip()
                add(sonos_id)
                add(f"speaker:{sonos_id}")
                add(f"sonos:{sonos_id}")
                add(f"uuid:{sonos_id}")

        return rows

    def _room_names_from_device(self, device: dict) -> list[str]:
        row = device if isinstance(device, dict) else {}
        details = row.get("details") if isinstance(row.get("details"), dict) else {}
        return self._dedupe_preserve(
            [
                row.get("room_id"),
                row.get("room"),
                row.get("area"),
                row.get("room_override_id"),
                row.get("reported_room_id"),
                row.get("reported_room"),
                details.get("room_id"),
                details.get("room"),
                details.get("area"),
                details.get("area_id"),
                details.get("room_name"),
                details.get("area_name"),
            ]
        )

    def _room_names_for_playback_target(self, target: Any) -> list[str]:
        target_tokens = self._target_lookup_tokens(target)
        if not target_tokens:
            return []
        try:
            from integration_registry import get_integration_device_registry

            registry = get_integration_device_registry(redis_client, refresh=False, use_cache=True)
        except Exception as e:
            logger.warning("Audio Ace could not read integration registry for playback target room lookup: %s", e)
            return []

        devices = registry.get("devices") if isinstance(registry, dict) else []
        for device in devices or []:
            if not isinstance(device, dict):
                continue
            if target_tokens.intersection(self._device_lookup_tokens(device)):
                room_names = self._room_names_from_device(device)
                if room_names:
                    return room_names
        return []

    def _preferred_room_media_player_for_room_names(self, room_names: list[str]) -> dict:
        if not room_names:
            return {}
        try:
            from integration_registry import get_integration_room_preferred_media_player

            preference = get_integration_room_preferred_media_player(room_names, redis_client)
            return dict(preference) if isinstance(preference, dict) else {}
        except Exception as e:
            logger.warning("Room preferred media player lookup failed for Audio Ace: %s", e)
            return {}

    def _preferred_room_media_player_target(self, context: dict | None) -> dict:
        ctx = context if isinstance(context, dict) else {}
        room_names = self._context_room_names(ctx)
        preference = self._preferred_room_media_player_for_room_names(room_names)
        if preference:
            preference["source"] = "context_room"
            preference["source_room_names"] = room_names
            return preference

        reply_target = self._ctx_text(ctx, "reply_playback_target")
        reply_room_names = self._room_names_for_playback_target(reply_target)
        preference = self._preferred_room_media_player_for_room_names(reply_room_names)
        if preference:
            preference["source"] = "reply_playback_target_room"
            preference["source_target"] = reply_target
            preference["source_room_names"] = reply_room_names
            return preference
        return {}

    def _candidate_device_names(self, context: dict | None) -> list[str]:
        ctx = context if isinstance(context, dict) else {}
        return self._dedupe_preserve(
            [
                self._ctx_text(ctx, "device_friendly_name"),
                self._ctx_text(ctx, "device_info_name"),
                self._ctx_text(ctx, "device_name"),
                self._ctx_text(ctx, "satellite_name"),
            ]
        )

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

    async def _assist_satellite_for_device_id(self, ha, device_id: str) -> str | None:
        did = str(device_id or "").strip()
        if not did:
            return None

        entity_reg = await ha.entity_registry_list()
        for row in entity_reg or []:
            if not isinstance(row, dict):
                continue
            if str(row.get("device_id") or "").strip() != did:
                continue
            if row.get("disabled_by") not in (None, ""):
                continue
            ent = str(row.get("entity_id") or "").strip()
            if ent.startswith("assist_satellite."):
                return ent
        return None

    async def _device_id_for_mac(
        self,
        ha,
        *,
        mac_address: str = "",
        bluetooth_mac_address: str = "",
    ) -> str | None:
        want = {
            self._normalize_mac(mac_address),
            self._normalize_mac(bluetooth_mac_address),
        }
        want.discard("")
        if not want:
            return None

        devices = await ha.device_registry_list()
        for row in devices or []:
            if not isinstance(row, dict):
                continue
            device_id = str(row.get("id") or "").strip()
            if not device_id:
                continue
            connections = row.get("connections") if isinstance(row.get("connections"), list) else []
            for conn in connections:
                if not isinstance(conn, (list, tuple)) or len(conn) < 2:
                    continue
                conn_value = self._normalize_mac(conn[1])
                if conn_value and conn_value in want:
                    return device_id
        return None

    async def _media_player_for_context_room(
        self,
        ha,
        *,
        context: dict | None,
        device_name: str = "",
        satellite_entity_id: str = "",
    ) -> str | None:
        room_names = self._context_room_names(context)
        wanted = {self._normalize_room_token(item) for item in room_names}
        wanted.discard("")
        if not wanted:
            return None

        def matches_room(value: Any) -> bool:
            token = self._normalize_room_token(value)
            return bool(token and token in wanted)

        area_names_by_id: dict[str, list[str]] = {}
        target_area_ids: set[str] = set()
        try:
            for area in await ha.area_registry_list():
                if not isinstance(area, dict):
                    continue
                area_id = str(area.get("area_id") or area.get("id") or "").strip()
                names = self._dedupe_preserve([area_id, area.get("name")])
                if area_id:
                    area_names_by_id[area_id] = names
                if area_id and any(matches_room(name) for name in names):
                    target_area_ids.add(area_id)
        except Exception as e:
            logger.warning("HA area registry lookup failed for Audio Ace room playback: %s", e)

        device_area_tokens: dict[str, list[str]] = {}
        try:
            for device in await ha.device_registry_list():
                if not isinstance(device, dict):
                    continue
                device_id = str(device.get("id") or "").strip()
                if not device_id:
                    continue
                area_id = str(device.get("area_id") or "").strip()
                tokens = self._dedupe_preserve(
                    [
                        area_id,
                        *(area_names_by_id.get(area_id) or []),
                        device.get("suggested_area"),
                    ]
                )
                device_area_tokens[device_id] = tokens
        except Exception as e:
            logger.warning("HA device registry lookup failed for Audio Ace room playback: %s", e)

        candidates: dict[str, int] = {}

        def add_candidate(entity_id: Any, score: int, *, friendly_name: Any = "", platform: Any = "") -> None:
            eid = str(entity_id or "").strip()
            if not eid.startswith("media_player."):
                return
            if self._looks_like_satellite_media_player(eid, str(friendly_name or "")):
                return
            adjusted = int(score)
            if str(platform or "").strip().lower() == "sonos":
                adjusted += 8
            adjusted += self._rank_media_player_entity_id(
                eid,
                device_name=device_name,
                satellite_entity_id=satellite_entity_id,
            )
            candidates[eid] = max(candidates.get(eid, -1000), adjusted)

        try:
            for row in await ha.entity_registry_list():
                if not isinstance(row, dict):
                    continue
                if row.get("disabled_by") not in (None, ""):
                    continue
                entity_id = str(row.get("entity_id") or "").strip()
                if not entity_id.startswith("media_player."):
                    continue
                entity_area_id = str(row.get("area_id") or "").strip()
                device_id = str(row.get("device_id") or "").strip()
                area_tokens = self._dedupe_preserve(
                    [
                        entity_area_id,
                        *(area_names_by_id.get(entity_area_id) or []),
                        *(device_area_tokens.get(device_id) or []),
                    ]
                )
                if entity_area_id and entity_area_id in target_area_ids:
                    matched = True
                else:
                    matched = any(matches_room(token) for token in area_tokens)
                if not matched:
                    continue
                add_candidate(
                    entity_id,
                    80,
                    friendly_name=row.get("name") or row.get("original_name"),
                    platform=row.get("platform"),
                )
        except Exception as e:
            logger.warning("HA entity registry lookup failed for Audio Ace room playback: %s", e)

        try:
            for state in ha.list_states():
                if not isinstance(state, dict):
                    continue
                entity_id = str(state.get("entity_id") or "").strip()
                if not entity_id.startswith("media_player."):
                    continue
                attrs = state.get("attributes") if isinstance(state.get("attributes"), dict) else {}
                friendly_name = str(attrs.get("friendly_name") or "").strip()
                haystack = self._normalize_room_token(f"{entity_id} {friendly_name}")
                if not haystack:
                    continue
                if any(room and room in haystack for room in wanted):
                    add_candidate(entity_id, 35, friendly_name=friendly_name)
        except Exception as e:
            logger.warning("HA state scan failed for Audio Ace room playback: %s", e)

        if not candidates:
            return None

        ranked = sorted(candidates, key=lambda eid: (-candidates[eid], eid))
        for entity_id in ranked:
            if self._entity_exists(ha, entity_id):
                return entity_id
        return ranked[0]

    async def _pick_target_player(
        self,
        context: dict | None = None,
        *,
        room_only: bool = False,
    ) -> str | None:
        """
        Pick a media_player in this order:
          1) Room/area media_player lookup from context
          2) Deterministic registry lookup by HA device_id from context
          3) Deterministic registry lookup by MAC address from Voice Core context
          4) Deterministic entity-id patterns from assist_satellite/device_name
          5) Fuzzy fallback scan by friendly_name/entity_id
        """
        ctx = context or {}
        device_names = self._candidate_device_names(ctx)
        device_name = device_names[0] if device_names else ""
        raw_device_id = self._ctx_text(ctx, "ha_device_id", "device_id")
        satellite_entity_id = self._extract_satellite_entity_id(ctx)
        mac_address = self._ctx_text(ctx, "device_mac_address", "mac_address")
        bluetooth_mac_address = self._ctx_text(ctx, "device_bluetooth_mac_address", "bluetooth_mac_address")

        try:
            ha = self._HA()
        except Exception as e:
            logger.warning("Cannot initialize Home Assistant client for media_player resolution: %s", e)
            return None

        room_player = await self._media_player_for_context_room(
            ha,
            context=ctx,
            device_name=device_name,
            satellite_entity_id=satellite_entity_id,
        )
        if room_player:
            return room_player
        if room_only:
            return None

        resolved_device_id = ""
        if raw_device_id and not self._looks_like_transport_selector(raw_device_id):
            resolved_device_id = raw_device_id

        if not resolved_device_id and (mac_address or bluetooth_mac_address):
            try:
                resolved_device_id = await self._device_id_for_mac(
                    ha,
                    mac_address=mac_address,
                    bluetooth_mac_address=bluetooth_mac_address,
                ) or ""
            except Exception as e:
                logger.warning(
                    "MAC-based device lookup failed for mac=%s bluetooth_mac=%s: %s",
                    mac_address,
                    bluetooth_mac_address,
                    e,
                )

        if resolved_device_id and not satellite_entity_id:
            try:
                satellite_entity_id = await self._assist_satellite_for_device_id(ha, resolved_device_id) or satellite_entity_id
            except Exception as e:
                logger.warning(
                    "Assist satellite lookup failed for device_id=%s: %s",
                    resolved_device_id,
                    e,
                )

        if resolved_device_id:
            try:
                by_device = await self._media_player_for_device_id(
                    ha,
                    device_id=resolved_device_id,
                    device_name=device_name,
                    satellite_entity_id=satellite_entity_id,
                )
                if by_device:
                    return by_device
            except Exception as e:
                logger.warning("Device-id media_player lookup failed for %s: %s", resolved_device_id, e)

        direct_candidates: list[str] = []
        direct_candidates.extend(self._media_player_candidates_from_satellite_entity(satellite_entity_id))
        for candidate_name in device_names:
            direct_candidates.extend(self._media_player_candidates_from_device_name(candidate_name))
        direct_candidates = self._dedupe_preserve(direct_candidates)
        for eid in direct_candidates:
            if self._entity_exists(ha, eid):
                return eid

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
            ha = load_homeassistant_config(required=False)
            self.base = ha.get("base", "")
            token = ha.get("token", "")
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

        async def device_registry_list(self) -> list[dict]:
            res = await self.ws_call("config/device_registry/list", timeout_s=30.0)
            return res if isinstance(res, list) else []

        async def area_registry_list(self) -> list[dict]:
            res = await self.ws_call("config/area_registry/list", timeout_s=30.0)
            return res if isinstance(res, list) else []


verba = ComfyUIAudioAcePlugin()
