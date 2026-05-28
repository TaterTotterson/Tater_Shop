# verba/comfyui_music_video.py
import os
import asyncio
import base64
import subprocess
import json
import time
import copy
import secrets
import random
import re
import yaml
import logging
import requests
import mimetypes
import uuid
from PIL import Image
from moviepy.video.io.ImageSequenceClip import ImageSequenceClip
from verba_base import ToolVerba
from verba_settings import get_verba_settings
from helpers import redis_client, run_comfy_prompt
from verba_result import action_failure, action_success
from vision_settings import get_vision_settings as get_shared_vision_settings

SETTINGS_CATEGORY = "ComfyUI Music Video"

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

logger = logging.getLogger("comfyui_music_video")
logger.setLevel(logging.INFO)


class _ComfyUIImageHelper:
    settings_category = SETTINGS_CATEGORY

    @staticmethod
    def get_base_http():
        settings = redis_client.hgetall(f"verba_settings:{_ComfyUIImageHelper.settings_category}") or {}
        raw = settings.get("COMFYUI_URL", b"")
        url = raw.decode("utf-8").strip() if isinstance(raw, (bytes, bytearray)) else (raw or "").strip()
        if not url:
            url = "http://localhost:8188"
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "http://" + url
        return url.rstrip("/")

    @staticmethod
    def get_base_ws(base_http: str) -> str:
        scheme = "wss" if base_http.startswith("https://") else "ws"
        return base_http.replace("http", scheme, 1)

    @staticmethod
    def get_workflow_template():
        settings = redis_client.hgetall(f"verba_settings:{_ComfyUIImageHelper.settings_category}") or {}
        workflow_raw = settings.get("COMFYUI_WORKFLOW", b"")
        workflow_str = workflow_raw.decode("utf-8").strip() if isinstance(workflow_raw, (bytes, bytearray)) else (workflow_raw or "").strip()
        if not workflow_str:
            raise Exception("No workflow template set in COMFYUI_WORKFLOW. Please provide a valid JSON template.")
        return json.loads(workflow_str)

    @staticmethod
    def get_history(base_http: str, prompt_id: str):
        r = requests.get(f"{base_http}/history/{prompt_id}", timeout=30)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def get_image_bytes(base_http: str, filename: str, subfolder: str, folder_type: str) -> bytes:
        params = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        r = requests.get(f"{base_http}/view", params=params, timeout=60)
        r.raise_for_status()
        return r.content

    @staticmethod
    def _insert_prompts(workflow: dict, user_prompt: str, negative_prompt: str = ""):
        positive_found = False
        negative_found = False
        encode_nodes = []

        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            if node.get("class_type") == "CLIPTextEncode":
                encode_nodes.append(node)
                title = (node.get("_meta", {}).get("title", "") or "").lower()
                if "positive" in title:
                    node.setdefault("inputs", {})
                    node["inputs"]["text"] = user_prompt
                    node["widgets_values"] = [user_prompt]
                    positive_found = True
                elif "negative" in title:
                    node.setdefault("inputs", {})
                    node["inputs"]["text"] = negative_prompt
                    node["widgets_values"] = [negative_prompt]
                    negative_found = True

        if not positive_found and encode_nodes:
            encode_nodes[0].setdefault("inputs", {})
            encode_nodes[0]["inputs"]["text"] = user_prompt
            encode_nodes[0]["widgets_values"] = [user_prompt]
        if not negative_found and len(encode_nodes) > 1:
            encode_nodes[1].setdefault("inputs", {})
            encode_nodes[1]["inputs"]["text"] = negative_prompt
            encode_nodes[1]["widgets_values"] = [negative_prompt]

    @staticmethod
    def _fetch_first_image(base_http: str, prompt_id: str, tries: int = 6, delay: float = 0.5) -> bytes:
        for _ in range(max(1, tries)):
            hist_all = _ComfyUIImageHelper.get_history(base_http, prompt_id)
            history = hist_all.get(prompt_id, {}) if isinstance(hist_all, dict) else {}
            outputs = history.get("outputs", {}) if isinstance(history, dict) else {}

            for node_output in outputs.values():
                images = node_output.get("images") or []
                for img_meta in images:
                    filename = img_meta.get("filename")
                    if filename:
                        subfolder = img_meta.get("subfolder", "")
                        folder_type = img_meta.get("type", "output")
                        return _ComfyUIImageHelper.get_image_bytes(base_http, filename, subfolder, folder_type)

            time.sleep(delay)

        raise Exception("No images returned from ComfyUI (timeout).")

    @staticmethod
    def process_prompt(user_prompt: str, width: int = None, height: int = None) -> bytes:
        base_http = _ComfyUIImageHelper.get_base_http()
        base_ws = _ComfyUIImageHelper.get_base_ws(base_http)

        workflow = copy.deepcopy(_ComfyUIImageHelper.get_workflow_template())
        _ComfyUIImageHelper._insert_prompts(workflow, user_prompt, "")

        random_seed = secrets.randbits(63)
        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            inputs = node.get("inputs", {})
            if not isinstance(inputs, dict):
                continue
            if "seed" in inputs:
                inputs["seed"] = int(random_seed)
            if "noise_seed" in inputs:
                inputs["noise_seed"] = int(random_seed)

        res_map = {
            "144p": (256, 144),
            "240p": (426, 240),
            "360p": (480, 360),
            "480p": (640, 480),
            "720p": (1280, 720),
            "1080p": (1920, 1080),
        }
        settings = redis_client.hgetall(f"verba_settings:{_ComfyUIImageHelper.settings_category}") or {}
        raw_res = settings.get("IMAGE_RESOLUTION", b"720p")
        resolution = raw_res.decode("utf-8") if isinstance(raw_res, (bytes, bytearray)) else (raw_res or "720p")
        default_w, default_h = res_map.get(resolution, (1280, 720))

        w = int(width) if width else default_w
        h = int(height) if height else default_h

        for node in workflow.values():
            if isinstance(node, dict) and node.get("class_type") in ("EmptyLatentImage", "EmptySD3LatentImage", "ModelSamplingFlux"):
                node.setdefault("inputs", {})
                node["inputs"]["width"] = w
                node["inputs"]["height"] = h

        prompt_id, _ = run_comfy_prompt(base_http, base_ws, workflow)
        return _ComfyUIImageHelper._fetch_first_image(base_http, prompt_id)


class _ComfyUIImageVideoHelper:
    settings_category = SETTINGS_CATEGORY

    @staticmethod
    def get_base_http() -> str:
        settings = redis_client.hgetall(f"verba_settings:{_ComfyUIImageVideoHelper.settings_category}") or {}
        url_raw = settings.get("COMFYUI_VIDEO_URL", b"")
        url = url_raw.decode("utf-8").strip() if isinstance(url_raw, (bytes, bytearray)) else (url_raw or "").strip()
        if not url:
            url = "http://localhost:8188"
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "http://" + url
        return url.rstrip("/")

    @staticmethod
    def get_base_ws(base_http: str) -> str:
        scheme = "wss" if base_http.startswith("https://") else "ws"
        return base_http.replace("http", scheme, 1)

    @staticmethod
    def get_workflow_template() -> dict:
        settings = redis_client.hgetall(f"verba_settings:{_ComfyUIImageVideoHelper.settings_category}") or {}
        raw = settings.get("COMFYUI_VIDEO_WORKFLOW", b"")
        workflow_str = raw.decode("utf-8").strip() if isinstance(raw, (bytes, bytearray)) else (raw or "").strip()
        if not workflow_str:
            raise RuntimeError("No workflow found in Redis. Please upload a valid JSON workflow.")
        return json.loads(workflow_str)

    @staticmethod
    def upload_image(base_http: str, image_bytes: bytes, filename: str) -> str:
        resp = requests.post(
            f"{base_http}/upload/image",
            files={"image": (filename, image_bytes)},
            data={"overwrite": "true"},
            timeout=60
        )
        resp.raise_for_status()
        result = resp.json()
        name = result.get("name") or result.get("filename")
        sub = result.get("subfolder", "")
        return f"{sub}/{name}" if sub else name

    @staticmethod
    def get_history(base_http: str, prompt_id: str) -> dict:
        r = requests.get(f"{base_http}/history/{prompt_id}", timeout=30)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def fetch_asset(base_http: str, filename: str, subfolder: str, folder_type: str) -> bytes:
        params = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        r = requests.get(f"{base_http}/view", params=params, timeout=60)
        r.raise_for_status()
        return r.content

    @staticmethod
    def _node_title(node: dict) -> str:
        return ((node.get("_meta", {}) or {}).get("title", "") or "").strip().lower()

    @staticmethod
    def _coerce_int(value, default=None):
        try:
            if isinstance(value, bool):
                return int(value)
            return int(float(str(value).strip()))
        except Exception:
            return default

    @staticmethod
    def _eval_numeric_expression(expression: str, variables: dict):
        import ast
        import operator

        if not expression or len(expression) > 120:
            return None

        bin_ops = {
            ast.Add: operator.add,
            ast.Sub: operator.sub,
            ast.Mult: operator.mul,
            ast.Div: operator.truediv,
            ast.FloorDiv: operator.floordiv,
            ast.Mod: operator.mod,
        }
        unary_ops = {ast.UAdd: operator.pos, ast.USub: operator.neg}

        def visit(node):
            if isinstance(node, ast.Expression):
                return visit(node.body)
            if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
                return node.value
            if isinstance(node, ast.Name) and node.id in variables:
                return variables[node.id]
            if isinstance(node, ast.UnaryOp) and type(node.op) in unary_ops:
                return unary_ops[type(node.op)](visit(node.operand))
            if isinstance(node, ast.BinOp) and type(node.op) in bin_ops:
                return bin_ops[type(node.op)](visit(node.left), visit(node.right))
            raise ValueError("unsupported expression")

        try:
            return visit(ast.parse(expression, mode="eval"))
        except Exception:
            return None

    @staticmethod
    def _resolve_numeric_input(workflow: dict, value, default=None, seen=None):
        parsed = _ComfyUIImageVideoHelper._coerce_int(value, None)
        if parsed is not None:
            return parsed

        if not isinstance(value, list) or not value:
            return default

        node_id = str(value[0])
        seen = set(seen or ())
        if node_id in seen:
            return default
        seen.add(node_id)

        node = workflow.get(node_id)
        if not isinstance(node, dict):
            return default

        inputs = node.get("inputs") or {}
        class_type = str(node.get("class_type") or "")

        if class_type in {"PrimitiveInt", "PrimitiveFloat"}:
            return _ComfyUIImageVideoHelper._coerce_int(inputs.get("value"), default)

        if class_type == "ComfyMathExpression":
            variables = {}
            for key, linked_value in inputs.items():
                if not str(key).startswith("values."):
                    continue
                var_name = str(key).split(".", 1)[1]
                resolved = _ComfyUIImageVideoHelper._resolve_numeric_input(
                    workflow, linked_value, None, seen=set(seen)
                )
                if resolved is not None:
                    variables[var_name] = resolved
            result = _ComfyUIImageVideoHelper._eval_numeric_expression(
                str(inputs.get("expression") or "").strip(), variables
            )
            return _ComfyUIImageVideoHelper._coerce_int(result, default)

        for key in ("value", "fps", "frame_rate", "length"):
            if key in inputs:
                resolved = _ComfyUIImageVideoHelper._resolve_numeric_input(
                    workflow, inputs.get(key), None, seen=set(seen)
                )
                if resolved is not None:
                    return resolved

        return default

    @staticmethod
    def _set_primitive_prompt_nodes(workflow: dict, prompt_text: str) -> bool:
        patched = False
        fallback_candidates = []

        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            if node.get("class_type") not in {"PrimitiveStringMultiline", "PrimitiveString"}:
                continue

            title = _ComfyUIImageVideoHelper._node_title(node)
            if "negative" in title:
                continue

            node.setdefault("inputs", {})
            if "prompt" in title or "positive" in title:
                node["inputs"]["value"] = prompt_text
                patched = True
            else:
                fallback_candidates.append(node)

        if not patched and len(fallback_candidates) == 1:
            fallback_candidates[0].setdefault("inputs", {})
            fallback_candidates[0]["inputs"]["value"] = prompt_text
            patched = True

        return patched

    @staticmethod
    def _set_direct_clip_prompt(workflow: dict, prompt_text: str, allow_fallback: bool) -> bool:
        encode_nodes = [
            node for node in workflow.values()
            if isinstance(node, dict) and node.get("class_type") == "CLIPTextEncode"
        ]

        for node in encode_nodes:
            title = _ComfyUIImageVideoHelper._node_title(node)
            inputs = node.setdefault("inputs", {})
            if "positive" in title and not isinstance(inputs.get("text"), list):
                inputs["text"] = prompt_text
                node["widgets_values"] = [prompt_text]
                return True

        if not allow_fallback:
            return False

        for node in encode_nodes:
            title = _ComfyUIImageVideoHelper._node_title(node)
            inputs = node.setdefault("inputs", {})
            if "negative" in title or isinstance(inputs.get("text"), list):
                continue
            inputs["text"] = prompt_text
            node["widgets_values"] = [prompt_text]
            return True

        return False

    @staticmethod
    def get_fps_from_workflow(wf: dict, default: int = 16) -> int:
        for node in wf.values():
            if not isinstance(node, dict) or node.get("class_type") != "CreateVideo":
                continue
            fps = _ComfyUIImageVideoHelper._resolve_numeric_input(wf, (node.get("inputs") or {}).get("fps"), None)
            if fps:
                return fps

        for node in wf.values():
            if not isinstance(node, dict) or node.get("class_type") != "PrimitiveInt":
                continue
            title = _ComfyUIImageVideoHelper._node_title(node)
            if "frame rate" in title or "fps" in title:
                fps = _ComfyUIImageVideoHelper._coerce_int((node.get("inputs") or {}).get("value"), None)
                if fps:
                    return fps

        return default

    @staticmethod
    def _apply_overrides(workflow: dict, prompt_text: str, uploaded_image_path: str, width: int, height: int, frames: int):
        fps = _ComfyUIImageVideoHelper.get_fps_from_workflow(workflow, default=16)
        prompt_is_primitive = _ComfyUIImageVideoHelper._set_primitive_prompt_nodes(workflow, prompt_text)
        _ComfyUIImageVideoHelper._set_direct_clip_prompt(
            workflow, prompt_text, allow_fallback=not prompt_is_primitive
        )

        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            inputs = node.setdefault("inputs", {})

            if node.get("class_type") == "LoadImage":
                inputs["image"] = uploaded_image_path

            if node.get("class_type") == "PrimitiveInt":
                title = _ComfyUIImageVideoHelper._node_title(node)
                if "width" in title:
                    inputs["value"] = int(width)
                elif "height" in title:
                    inputs["value"] = int(height)
                elif "frame rate" in title or "fps" in title:
                    inputs["value"] = int(fps)
                elif "duration" in title and fps:
                    inputs["value"] = max(1, int(round(int(frames) / int(fps))))

            if node.get("class_type") in {
                "WanImageToVideo",
                "WanVaceToVideo",
                "CosmosPredict2ImageToVideoLatent",
                "Wan22ImageToVideoLatent",
            }:
                inputs["width"] = width
                inputs["height"] = height
                inputs["length"] = frames

            if node.get("class_type") == "EmptyLTXVLatentVideo":
                inputs["length"] = int(frames)
            elif node.get("class_type") == "LTXVEmptyLatentAudio":
                inputs["frames_number"] = int(frames)
                if not isinstance(inputs.get("frame_rate"), list):
                    inputs["frame_rate"] = int(fps)
            elif node.get("class_type") == "LTXVConditioning":
                if not isinstance(inputs.get("frame_rate"), list):
                    inputs["frame_rate"] = int(fps)

    @staticmethod
    def process_prompt(prompt: str, image_bytes: bytes, filename: str, width: int = None, height: int = None, length: int = None):
        base_http = _ComfyUIImageVideoHelper.get_base_http()
        base_ws = _ComfyUIImageVideoHelper.get_base_ws(base_http)

        uploaded = _ComfyUIImageVideoHelper.upload_image(base_http, image_bytes, filename)
        wf = copy.deepcopy(_ComfyUIImageVideoHelper.get_workflow_template())

        settings = redis_client.hgetall(f"verba_settings:{_ComfyUIImageVideoHelper.settings_category}") or {}
        res_map = {
            "144p": (256, 144), "240p": (426, 240), "360p": (480, 360),
            "480p": (640, 480), "720p": (1280, 720), "1080p": (1920, 1080)
        }
        raw_res = settings.get("IMAGE_RESOLUTION", b"480p")
        resolution = raw_res.decode("utf-8") if isinstance(raw_res, (bytes, bytearray)) else (raw_res or "480p")
        default_w, default_h = res_map.get(resolution, (640, 480))

        fps = _ComfyUIImageVideoHelper.get_fps_from_workflow(wf, default=16)

        raw_length = settings.get("LENGTH", b"1")
        try:
            default_seconds = int(raw_length.decode() if isinstance(raw_length, (bytes, bytearray)) else raw_length)
        except ValueError:
            default_seconds = 1
        default_frames = max(1, default_seconds * fps)

        w = width or default_w
        h = height or default_h
        frames = length or default_frames

        random_seed = secrets.randbits(63)
        for node in wf.values():
            if not isinstance(node, dict):
                continue
            inputs = node.get("inputs", {})
            if not isinstance(inputs, dict):
                continue
            if "noise_seed" in inputs and not isinstance(inputs.get("noise_seed"), list):
                inputs["noise_seed"] = int(random_seed)
            if "seed" in inputs and not isinstance(inputs.get("seed"), list):
                inputs["seed"] = int(random_seed)

        _ComfyUIImageVideoHelper._apply_overrides(wf, prompt, uploaded, w, h, frames)

        prompt_id, _ = run_comfy_prompt(base_http, base_ws, wf)
        for _ in range(40):
            hist = _ComfyUIImageVideoHelper.get_history(base_http, prompt_id).get(prompt_id, {})
            outputs = hist.get("outputs", {}) if isinstance(hist, dict) else {}

            for node in outputs.values():
                if "images" in node and node["images"]:
                    img = node["images"][0]
                    content = _ComfyUIImageVideoHelper.fetch_asset(base_http, img["filename"], img.get("subfolder", ""), img.get("type", "output"))
                    ext = os.path.splitext(img["filename"])[-1].lstrip(".") or "webp"
                    return content, ext

            for node in outputs.values():
                if "videos" in node and node["videos"]:
                    vid = node["videos"][0]
                    content = _ComfyUIImageVideoHelper.fetch_asset(base_http, vid["filename"], vid.get("subfolder", ""), vid.get("type", "output"))
                    ext = os.path.splitext(vid["filename"])[-1].lstrip(".") or "mp4"
                    return content, ext
            time.sleep(0.5)

        for node in wf.values():
            if node.get("class_type") == "SaveVideo":
                prefix = node["inputs"].get("filename_prefix", "ComfyUI")
                if "/" in prefix:
                    subfolder, base = prefix.split("/", 1)
                else:
                    subfolder, base = "", prefix
                guessed_filename = f"{base}.mp4"
                content = _ComfyUIImageVideoHelper.fetch_asset(base_http, guessed_filename, subfolder, "output")
                return content, "mp4"

        raise RuntimeError("No output found in ComfyUI history or disk.")


class _ComfyUIAudioAceHelper:
    settings_category = SETTINGS_CATEGORY

    @staticmethod
    def _settings():
        return redis_client.hgetall(f"verba_settings:{_ComfyUIAudioAceHelper.settings_category}") or {}

    @staticmethod
    def _as_text(value, default=""):
        if value is None:
            return str(default)
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", "ignore")
        return str(value)

    @staticmethod
    def get_base_http():
        settings = _ComfyUIAudioAceHelper._settings()
        raw = settings.get("COMFYUI_AUDIO_ACE_URL", b"")
        url = raw.decode("utf-8").strip() if isinstance(raw, (bytes, bytearray)) else (raw or "").strip()
        url = url or "http://localhost:8188"
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "http://" + url
        return url.rstrip("/")

    @staticmethod
    def get_base_ws(base_http: str) -> str:
        scheme = "wss" if base_http.startswith("https://") else "ws"
        return base_http.replace("http", scheme, 1)

    @staticmethod
    def get_workflow_template():
        settings = _ComfyUIAudioAceHelper._settings()
        workflow_raw = settings.get("COMFYUI_AUDIO_ACE_WORKFLOW", b"")
        if isinstance(workflow_raw, dict):
            return workflow_raw

        workflow_str = _ComfyUIAudioAceHelper._as_text(workflow_raw, "").strip()
        if workflow_str:
            try:
                workflow = json.loads(workflow_str)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON in COMFYUI_AUDIO_ACE_WORKFLOW: {exc}") from exc
            if not isinstance(workflow, dict):
                raise ValueError("COMFYUI_AUDIO_ACE_WORKFLOW must contain a JSON object.")
            return workflow

        return _ComfyUIAudioAceHelper._default_workflow_template()

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

    @staticmethod
    async def generate_tags_and_lyrics(user_prompt, llm_client):
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
            if not _ComfyUIAudioAceHelper._is_audio_text_encode_node(node):
                continue
            inputs = node.setdefault("inputs", {})
            inputs["tags"] = tags
            inputs["lyrics"] = lyrics
            patched += 1
        if patched <= 0:
            raise ValueError("Workflow has no Audio Ace text encode node with `tags` and `lyrics` inputs.")
        return patched

    @staticmethod
    def _set_duration(workflow: dict, duration: int) -> None:
        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            inputs = node.get("inputs")
            if not isinstance(inputs, dict):
                continue

            class_type = str(node.get("class_type") or "").lower()
            title = _ComfyUIAudioAceHelper._node_title(node)
            is_ace_audio_latent = (
                ("acestep" in class_type or "ace step" in title)
                and "latent" in class_type + title
                and "audio" in class_type + title
            )

            if "seconds" in inputs and is_ace_audio_latent:
                inputs["seconds"] = int(duration)
            if "duration" in inputs and _ComfyUIAudioAceHelper._is_audio_text_encode_node(node):
                inputs["duration"] = int(duration)

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
        return max(30, min(300, int(len(words) / 2.2) + 15))

    @staticmethod
    def build_workflow(tags: str, lyrics: str) -> dict:
        workflow = copy.deepcopy(_ComfyUIAudioAceHelper.get_workflow_template())
        _ComfyUIAudioAceHelper._set_tags_and_lyrics(workflow, tags, lyrics)
        _ComfyUIAudioAceHelper._set_duration(
            workflow,
            _ComfyUIAudioAceHelper._estimate_duration_seconds(lyrics),
        )

        for node in workflow.values():
            if isinstance(node, dict):
                inputs = node.get("inputs")
                if isinstance(inputs, dict) and "seed" in inputs:
                    inputs["seed"] = random.randint(0, 2**63 - 1)
        return workflow

    @staticmethod
    def process_prompt_sync(tags: str, lyrics: str):
        base_http = _ComfyUIAudioAceHelper.get_base_http()
        base_ws = _ComfyUIAudioAceHelper.get_base_ws(base_http)

        workflow = _ComfyUIAudioAceHelper.build_workflow(tags, lyrics)
        prompt_id, _ = run_comfy_prompt(base_http, base_ws, workflow)

        for _ in range(50):
            history = _ComfyUIAudioAceHelper.get_history(base_http, prompt_id).get(prompt_id, {})
            outputs = history.get("outputs", {}) if isinstance(history, dict) else {}

            for _, node_out in outputs.items():
                if "audio" in node_out:
                    for audio_meta in node_out["audio"]:
                        filename = audio_meta.get("filename")
                        subfolder = audio_meta.get("subfolder", "")
                        folder_type = audio_meta.get("type", "output")
                        if filename:
                            try:
                                audio_bytes = _ComfyUIAudioAceHelper.get_audio_bytes(base_http, filename, subfolder, folder_type)
                            except Exception:
                                audio_bytes = None
                            if audio_bytes:
                                return audio_bytes
            time.sleep(0.5)

        raise Exception("No audio returned.")


class _VisionHelper:
    settings_category = SETTINGS_CATEGORY

    @staticmethod
    def get_vision_settings():
        settings = get_shared_vision_settings(
            default_api_base="http://127.0.0.1:1234",
            default_model="gemma3-27b-abliterated-dpo",
        )
        return (
            str(settings.get("api_base") or "http://127.0.0.1:1234").rstrip("/"),
            str(settings.get("model") or "gemma3-27b-abliterated-dpo"),
            str(settings.get("api_key") or ""),
        )

    @staticmethod
    def _to_data_url(image_bytes: bytes, filename: str = "image.png") -> str:
        mime = mimetypes.guess_type(filename)[0] or "image/png"
        b64 = base64.b64encode(image_bytes).decode("utf-8")
        return f"data:{mime};base64,{b64}"

    @staticmethod
    def _call_openai_vision(api_base: str, model: str, image_bytes: bytes, prompt: str, filename: str = "image.png") -> str:
        url = f"{api_base}/v1/chat/completions"
        data_url = _VisionHelper._to_data_url(image_bytes, filename)
        payload = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt or "Describe this image."},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                }
            ],
            "temperature": 0.2,
        }
        headers = {"Content-Type": "application/json"}
        _, _, api_key = _VisionHelper.get_vision_settings()
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        resp = requests.post(url, json=payload, headers=headers, timeout=90)
        if resp.status_code != 200:
            return f"Error: Vision service returned status {resp.status_code}.\nResponse: {resp.text}"
        j = resp.json()
        try:
            return j["choices"][0]["message"]["content"].strip()
        except Exception:
            return f"Error: Unexpected response shape: {j}"

    @staticmethod
    async def describe_image(image_bytes: bytes, filename: str):
        prompt = (
            "You are an expert visual assistant. Describe the contents of this image in detail, "
            "mentioning key objects, scenes, or actions if recognizable."
        )
        api_base, model, _ = _VisionHelper.get_vision_settings()
        return await asyncio.to_thread(
            _VisionHelper._call_openai_vision,
            api_base,
            model,
            image_bytes,
            prompt,
            filename
        )

class ComfyUIMusicVideoPlugin(ToolVerba):
    name = "comfyui_music_video"
    verba_name = "ComfyUI Music Video"
    version = "1.0.5"
    min_tater_version = "59"
    usage = '{"function":"comfyui_music_video","arguments":{"prompt":"<Concept for the song>"}}'
    description = "Generates a complete AI music video including lyrics, music, and animated visuals by orchestrating ComfyUI verba."
    verba_dec = "Build a full AI music video\u2014lyrics, music, and animated visuals\u2014using ComfyUI."
    pretty_name = "Your Music Video"
    platforms = ["webui", "macos"]
    waiting_prompt_template = "Generate a fun, upbeat message saying you're composing the full music video now! Only output that message."
    settings_category = SETTINGS_CATEGORY
    required_settings = {
        "COMFYUI_URL": {
            "label": "ComfyUI URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "The base URL for the ComfyUI API (do not include endpoint paths)."
        },
        "COMFYUI_WORKFLOW": {
            "label": "Image Workflow Template (JSON)",
            "type": "file",
            "accept": ".json,application/json",
            "default": "",
            "description": "Upload your ComfyUI image workflow .json file."
        },
        "IMAGE_RESOLUTION": {
            "label": "Image Resolution",
            "type": "select",
            "default": "720p",
            "options": ["144p", "240p", "360p", "480p", "720p", "1080p"],
            "description": "Resolution for the base image used in animations."
        },
        "COMFYUI_VIDEO_URL": {
            "label": "ComfyUI Video URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "ComfyUI endpoint for image-to-video workflows."
        },
        "COMFYUI_VIDEO_WORKFLOW": {
            "label": "Video Workflow Template (JSON)",
            "type": "file",
            "accept": ".json,application/json",
            "default": "",
            "description": "Upload your ComfyUI animation workflow .json file."
        },
        "LENGTH": {
            "label": "Animation Length (seconds)",
            "type": "number",
            "default": 10,
            "description": "Length in seconds for each animation clip."
        },
        "COMFYUI_AUDIO_ACE_URL": {
            "label": "ComfyUI Audio Ace URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "ComfyUI endpoint for AceStep audio generation."
        },
        "COMFYUI_AUDIO_ACE_WORKFLOW": {
            "label": "Audio Workflow Template (JSON)",
            "type": "file",
            "accept": ".json,application/json",
            "default": "",
            "description": (
                "Upload your ComfyUI Audio Ace workflow .json file. "
                "If empty, Tater uses the bundled Ace Step workflow."
            ),
        },
        "api_base": {
            "label": "Vision API Base URL",
            "description": "OpenAI-compatible base URL (e.g., http://127.0.0.1:1234).",
            "type": "string",
            "default": "http://127.0.0.1:1234"
        },
        "model": {
            "label": "Vision Model",
            "description": "OpenAI-compatible model name (e.g., qwen2.5-vl-7b-instruct, gemma-3-12b-it, etc.).",
            "type": "string",
            "default": "gemma3-27b-abliterated-dpo"
        },
        "MUSIC_VIDEO_RESOLUTION": {
            "label": "ComfyUI Animation Resolution",
            "type": "select",
            "default": "720p",
            "options": ["144p", "240p", "360p", "480p", "720p", "1080p"],
            "description": "Target resolution for animation clips."
        }
    }
    when_to_use = ""
    common_needs = []
    missing_info_prompts = []


    @staticmethod
    def split_sections(lyrics):
        sections, current_section, current_tag = [], "", None
        allowed_tags = ["[verse]", "[chorus]", "[bridge]", "[outro]"]
        for line in lyrics.splitlines():
            line = line.strip().lower()
            if line in allowed_tags:
                if current_section:
                    sections.append(current_section.strip())
                    current_section = ""
                current_tag = line
                continue
            if current_tag:
                current_section += line + " "
        if current_section:
            sections.append(current_section.strip())
        return sections

    @staticmethod
    def get_mp3_duration(filename):
        cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "json", filename]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        info = json.loads(result.stdout)
        return float(info["format"]["duration"])

    def ffmpeg_concat(self, video_paths, audio_path, out):
        listpath = f"{out}_concat_list.txt"
        with open(listpath, "w") as f:
            for p in video_paths:
                f.write(f"file '{p}'\n")
        cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", listpath, "-i", audio_path, "-c:v", "libx264", "-c:a", "aac", "-shortest", out]
        subprocess.run(cmd, check=True)
        os.remove(listpath)

    def cleanup_temp_files(self, job_id, count, exts):
        try:
            for ext in ["mp3", "mp4"]:
                for suffix in ["audio", "final", "final_small"]:
                    path = f"/tmp/{job_id}_{suffix}.{ext}"
                    if os.path.exists(path):
                        os.remove(path)
            for i in range(count):
                for ext in exts:
                    path = f"/tmp/{job_id}_clip_{i}.{ext}"
                    if os.path.exists(path):
                        os.remove(path)
        except Exception as e:
            logger.warning("[Cleanup warning] %s", e)

    def webp_to_mp4(self, input_file, output_file, fps=16, duration=5):
        frames, tmp_dir, frame_files = [], f"{os.path.dirname(input_file)}/frames_{uuid.uuid4().hex[:6]}", []
        os.makedirs(tmp_dir, exist_ok=True)
        try:
            im = Image.open(input_file)
            while True:
                frames.append(im.copy().convert("RGBA"))
                im.seek(im.tell() + 1)
        except EOFError:
            pass
        for idx, frame in enumerate(frames):
            path = f"{tmp_dir}/frame_{idx}.png"
            frame.save(path, "PNG")
            frame_files.append(path)
        if len(frame_files) == 1 or duration < 1:
            frame_files *= max(1, int(fps * duration))
        clip = ImageSequenceClip(frame_files, fps=fps)
        clip.write_videofile(output_file, codec='libx264', fps=fps, audio=False, logger=None)
        for p in frame_files:
            os.remove(p)
        os.rmdir(tmp_dir)

    async def _generate_music_video(self, prompt, llm_client):
        if llm_client is None:
            return action_failure(
                code="llm_required",
                message="This plugin requires an available LLM to generate lyrics and scene prompts.",
                say_hint="Explain that the model is required for lyrics and scene generation.",
            )
        job_id = str(uuid.uuid4())[:8]

        # --- 1) Lyrics + tags via Audio Ace ---
        tags, lyrics = await _ComfyUIAudioAceHelper.generate_tags_and_lyrics(prompt, llm_client)
        if not lyrics:
            return action_failure(
                code="lyrics_missing",
                message="No lyrics were returned for visuals.",
                say_hint="Explain that lyric generation failed and suggest retrying.",
            )

        # --- 2) Render audio using the refactored per-job Comfy helper ---
        audio_path = f"/tmp/{job_id}_audio.mp3"
        final_video_path = f"/tmp/{job_id}_final.mp4"

        # CHANGED: call the new sync method and run it in a worker thread
        audio_bytes = await asyncio.to_thread(_ComfyUIAudioAceHelper.process_prompt_sync, tags, lyrics)
        if not audio_bytes:
            return action_failure(
                code="audio_missing",
                message="No audio was generated for the music video.",
                say_hint="Explain that audio generation failed and suggest retrying.",
            )
        with open(audio_path, "wb") as f:
            f.write(audio_bytes)

        duration = self.get_mp3_duration(audio_path)
        duration = max(30, min(300, duration))

        sections = self.split_sections(lyrics)
        if not sections:
            return action_failure(
                code="lyrics_sections_missing",
                message="No lyric sections were found for animation.",
                say_hint="Explain that section parsing failed and suggest retrying.",
            )

        num_clips = len(sections) * 2
        per = duration / num_clips

        # Pull FPS once from the image→video workflow stored in Redis
        wf = _ComfyUIImageVideoHelper.get_workflow_template()
        fps = 16  # default
        fps = _ComfyUIImageVideoHelper.get_fps_from_workflow(wf, default=16)

        vids = []
        exts_used = set()

        res_map = {
            "144p": (256, 144),
            "240p": (426, 240),
            "360p": (480, 360),
            "480p": (640, 480),
            "720p": (1280, 720),
            "1080p": (1920, 1080)
        }

        settings = redis_client.hgetall(f"verba_settings:{self.settings_category}")
        raw = settings.get("MUSIC_VIDEO_RESOLUTION", b"720p")
        resolution = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        w, h = res_map.get(resolution, (1280, 720))

        clip_idx = 0

        try:
            for section in sections:
                for part_num in range(2):
                    part_hint = f" (Part {part_num + 1})" if part_num > 0 else ""

                    img_desc_prompt = (
                        f'The following are song lyrics:\n\n"{section}"\n\n'
                        "Write a single clear sentence describing a visual scene or illustration that conveys the meaning, emotion, or subject of these lyrics. "
                        "If the lyrics are subjective or abstract (e.g., 'Do you think I'm beautiful?'), imagine a representative visual. "
                        "Avoid text overlays and focus on a vivid scene." + part_hint
                    )

                    try:
                        img_resp = await llm_client.chat([
                            {"role": "system", "content": "You help generate creative prompts for AI-generated illustrations."},
                            {"role": "user", "content": img_desc_prompt}
                        ])
                        image_prompt = ((img_resp.get("message", {}) or {}).get("content", "") or "").strip() or section
                    except Exception:
                        image_prompt = section

                    image_bytes = await asyncio.to_thread(
                        _ComfyUIImageHelper.process_prompt,
                        image_prompt,
                        w,
                        h
                    )

                    tmp_img = f"/tmp/{job_id}_frame_{clip_idx}.png"
                    with open(tmp_img, "wb") as f:
                        f.write(image_bytes)

                    with open(tmp_img, "rb") as f:
                        image_content = f.read()
                    desc = await _VisionHelper.describe_image(image_content, tmp_img)
                    desc = desc.strip() or "An interesting scene"

                    animation_prompt = (
                        f'The following is a visual description of an image:\n\n"{desc}"\n\n'
                        f'And here is a section of song lyrics:\n\n"{section}"\n\n'
                        "Write a single clear sentence that describes what this image depicts and how it might animate to reflect the lyrics."
                    )

                    try:
                        resp = await llm_client.chat([
                            {"role": "system", "content": "You generate vivid single-sentence descriptions that combine image content and lyric context for animation."},
                            {"role": "user", "content": animation_prompt}
                        ])
                        animation_desc = ((resp.get("message", {}) or {}).get("content", "") or "").strip() or "A scene that reflects the lyrics."
                    except Exception:
                        animation_desc = "A scene that reflects the lyrics."

                    upload_filename = f"frame_{clip_idx}.png"

                    # frame count uses the fps we pulled from the template
                    frame_count = int(per * fps)

                    anim_bytes, ext = await asyncio.to_thread(
                        _ComfyUIImageVideoHelper.process_prompt,
                        animation_desc,
                        image_bytes,
                        upload_filename,
                        w,
                        h,
                        frame_count
                    )

                    exts_used.add(ext)
                    tmp_path = f"/tmp/{job_id}_clip_{clip_idx}.{ext}"
                    with open(tmp_path, "wb") as f:
                        f.write(anim_bytes)

                    if ext == "webp":
                        tmp_mp4 = f"/tmp/{job_id}_clip_{clip_idx}.mp4"
                        # keep playback speed consistent with generation
                        self.webp_to_mp4(tmp_path, tmp_mp4, fps=fps, duration=per)
                        vids.append(tmp_mp4)
                    else:
                        vids.append(tmp_path)

                    clip_idx += 1

            if not vids:
                return action_failure(
                    code="no_clips_generated",
                    message="Failed to generate any video clips.",
                    say_hint="Explain that clip generation failed and suggest retrying.",
                )

            self.ffmpeg_concat(vids, audio_path, final_video_path)

            with open(final_video_path, "rb") as f:
                final_bytes = f.read()

            try:
                msg = await llm_client.chat([
                    {"role": "system", "content": f"User got a music video for '{prompt}'"},
                    {"role": "user", "content": "Send short celebration text."}
                ])
                msg_text = (((msg.get("message", {}) or {}).get("content", "") or "").strip() or "")[:240]
            except Exception:
                msg_text = ""

            artifact = _build_media_metadata(
                final_bytes,
                media_type="video",
                name="music_video.mp4",
                mimetype="video/mp4",
            )
            return action_success(
                facts={
                    "prompt": prompt,
                    "tags": tags,
                    "sections_count": len(sections),
                    "clip_count": len(vids),
                    "duration_seconds": duration,
                    "artifact_type": "video",
                    "artifact_count": 1,
                    "file_name": "music_video.mp4",
                },
                summary_for_user="Generated one music video.",
                flair=msg_text,
                say_hint="Confirm the music video result and reference the attached video.",
                artifacts=[artifact],
            )
        finally:
            self.cleanup_temp_files(job_id, clip_idx, list(exts_used))

    async def handle_webui(self, args, llm_client):
        args = args or {}
        if "prompt" not in args:
            return action_failure(
                code="missing_prompt",
                message="No prompt given.",
                needs=["Provide a prompt describing the music video concept."],
                say_hint="Ask the user for a music video prompt.",
            )
        try:
            return await self._generate_music_video(args["prompt"], llm_client)
        except Exception as e:
            logger.exception("ComfyUI music-video generation failed: %s", e)
            return action_failure(
                code="music_video_failed",
                message=f"Error generating music video: {e}",
                say_hint="Explain the generation failure and suggest retrying.",
            )


    async def handle_macos(self, args, llm_client, context=None):
        try:
            return await self.handle_webui(args, llm_client, context=context)
        except TypeError:
            return await self.handle_webui(args, llm_client)
verba = ComfyUIMusicVideoPlugin()
