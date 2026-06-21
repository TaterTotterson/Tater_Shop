# verba/comfyui_image_video.py
import os
import json
import requests
import asyncio
import secrets
import copy
import logging
import time
import uuid
import base64
import mimetypes
from pathlib import Path
from urllib.parse import unquote, urlparse
from PIL import Image
from moviepy.video.io.ImageSequenceClip import ImageSequenceClip
from verba_base import ToolVerba
from helpers import redis_client, run_comfy_prompt
try:
    from helpers import redis_blob_client
except Exception:
    redis_blob_client = redis_client
from verba_result import action_failure, action_success

logger = logging.getLogger("comfyui_image_video")
logger.setLevel(logging.INFO)

WEBUI_FILE_BLOB_PREFIX = "webui:file:"
SOURCE_IMAGE_MIMETYPES = {"image/png", "image/jpeg", "image/jpg", "image/webp"}
SOURCE_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp"}


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

class ComfyUIImageVideoPlugin(ToolVerba):
    name = "comfyui_image_video"
    verba_name = "ComfyUI Animate Image"
    version = "1.0.6"
    min_tater_version = "59"
    usage = '{"function":"comfyui_image_video","arguments":{"prompt":"<Describe how you want the animation to move or behave>"}}'
    description = "Animates the most recent image in chat into a looping WebP or MP4 using ComfyUI."
    verba_dec = "Animate the most recent image in chat into a looping WebP or MP4 via ComfyUI."
    pretty_name = "Animating Your Image"
    settings_category = "ComfyUI Animate Image"
    required_settings = {
        "COMFYUI_VIDEO_URL": {
            "label": "ComfyUI Server URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "Base URL (host:port) for your ComfyUI instance."
        },
        "IMAGE_RESOLUTION": {
            "label": "Animation Resolution",
            "type": "select",
            "default": "720p",
            "options": ["144p", "240p", "360p", "480p", "720p", "1080p"],
            "description": "Default resolution for generated animations."
        },
        "LENGTH": {
            "label": "Default Animation Length (seconds)",
            "type": "number",
            "default": 10,
            "description": "Approximate animation length in seconds."
        },
        "COMFYUI_VIDEO_WORKFLOW": {
            "label": "Workflow Template (JSON)",
            "type": "file",
            "accept": ".json,application/json",
            "default": "",
            "description": "Upload your ComfyUI animation workflow .json file."
        }
    }
    waiting_prompt_template = "Generate a playful, friendly message saying you’re bringing their image to life now! Only output that message."
    platforms = ["webui", "little_spud", "macos"]
    when_to_use = ""
    common_needs = []
    missing_info_prompts = []


    @staticmethod
    def _text(value) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="ignore").strip()
        return str(value or "").strip()

    @staticmethod
    def _guess_image_mimetype(binary: bytes) -> str:
        if binary.startswith(b"\x89PNG\r\n\x1a\n"):
            return "image/png"
        if binary.startswith(b"\xff\xd8\xff"):
            return "image/jpeg"
        if binary.startswith(b"RIFF") and binary[8:12] == b"WEBP":
            return "image/webp"
        return ""

    @staticmethod
    def _image_mimetype_for_payload(payload: dict, filename: str = "") -> str:
        mimetype = ComfyUIImageVideoPlugin._text(
            payload.get("mimetype") or payload.get("mime_type") or payload.get("content_type")
        ).lower()
        if mimetype:
            return mimetype
        guessed = mimetypes.guess_type(filename or ComfyUIImageVideoPlugin._text(payload.get("name")))[0]
        return ComfyUIImageVideoPlugin._text(guessed).lower()

    @staticmethod
    def _filename_from_payload(payload: dict, mimetype: str = "") -> str:
        name = ComfyUIImageVideoPlugin._text(payload.get("name") or payload.get("filename"))
        if not name:
            for key in ("path", "file_path", "artifact_path", "url", "uri"):
                value = ComfyUIImageVideoPlugin._text(payload.get(key))
                if value:
                    parsed = urlparse(value)
                    tail = unquote((parsed.path or value).rstrip("/").rsplit("/", 1)[-1])
                    if tail:
                        name = tail
                        break
        if not name:
            name = "input.png"

        root, ext = os.path.splitext(name)
        if not ext:
            guessed_ext = mimetypes.guess_extension(mimetype or "") or ".png"
            name = f"{root or 'input'}{guessed_ext}"
        return name

    @staticmethod
    def _looks_like_animation_output(payload: dict, filename: str) -> bool:
        name = ComfyUIImageVideoPlugin._text(filename or payload.get("name") or payload.get("filename")).lower()
        mimetype = ComfyUIImageVideoPlugin._text(payload.get("mimetype") or payload.get("mime_type")).lower()
        artifact_type = ComfyUIImageVideoPlugin._text(payload.get("type")).lower()
        if artifact_type == "video" or mimetype.startswith("video/"):
            return True
        return name.startswith("animated.") or name.startswith("animation.")

    @staticmethod
    def _is_source_image_payload(payload: dict) -> bool:
        if not isinstance(payload, dict):
            return False
        filename = ComfyUIImageVideoPlugin._filename_from_payload(payload)
        if ComfyUIImageVideoPlugin._looks_like_animation_output(payload, filename):
            return False
        mimetype = ComfyUIImageVideoPlugin._image_mimetype_for_payload(payload, filename)
        artifact_type = ComfyUIImageVideoPlugin._text(payload.get("type")).lower()
        ext = os.path.splitext(filename.lower())[1]
        if mimetype in SOURCE_IMAGE_MIMETYPES:
            return True
        if artifact_type == "image" and mimetype.startswith("image/") and mimetype != "image/svg+xml":
            return True
        return ext in SOURCE_IMAGE_EXTS

    @staticmethod
    def _decode_data_url(value: str):
        text = ComfyUIImageVideoPlugin._text(value)
        if not text.lower().startswith("data:") or "," not in text:
            return None, ""
        header, encoded = text.split(",", 1)
        if ";base64" not in header.lower():
            return None, ""
        mimetype = header[5:].split(";", 1)[0].strip().lower()
        if mimetype and mimetype not in SOURCE_IMAGE_MIMETYPES:
            return None, mimetype
        try:
            decoded = base64.b64decode("".join(encoded.split()), validate=False)
        except Exception:
            return None, mimetype
        return (bytes(decoded) if decoded else None), mimetype

    @staticmethod
    def _blob_get(key: str):
        token = ComfyUIImageVideoPlugin._text(key)
        if not token:
            return None
        for candidate in (token, token.encode("utf-8", errors="ignore")):
            try:
                raw = redis_blob_client.get(candidate)
            except Exception:
                raw = None
            if isinstance(raw, (bytes, bytearray)):
                return bytes(raw)
        return None

    @staticmethod
    def _file_id_from_url(value: str) -> str:
        text = ComfyUIImageVideoPlugin._text(value)
        if not text:
            return ""
        parsed = urlparse(text)
        parts = [part for part in (parsed.path or "").split("/") if part]
        for idx, part in enumerate(parts[:-1]):
            if part == "files" and idx > 0 and parts[idx - 1] in {"chat", "v1"}:
                return unquote(parts[idx + 1])
        return ""

    @staticmethod
    def _read_file_id(file_id: str):
        token = ComfyUIImageVideoPlugin._text(file_id)
        if not token:
            return None
        if token.startswith(WEBUI_FILE_BLOB_PREFIX):
            return ComfyUIImageVideoPlugin._blob_get(token)
        return ComfyUIImageVideoPlugin._blob_get(f"{WEBUI_FILE_BLOB_PREFIX}{token}")

    @staticmethod
    def _materialize_image_payload(payload: dict):
        if not isinstance(payload, dict) or not ComfyUIImageVideoPlugin._is_source_image_payload(payload):
            return None

        filename = ComfyUIImageVideoPlugin._filename_from_payload(payload)
        mimetype = ComfyUIImageVideoPlugin._image_mimetype_for_payload(payload, filename)
        binary = None

        for key in ("data_url", "url", "uri", "data"):
            raw_value = ComfyUIImageVideoPlugin._text(payload.get(key))
            if raw_value.lower().startswith("data:"):
                binary, data_mimetype = ComfyUIImageVideoPlugin._decode_data_url(raw_value)
                if data_mimetype:
                    mimetype = data_mimetype
                if binary:
                    break

        if binary is None:
            blob_key = ComfyUIImageVideoPlugin._text(payload.get("blob_key"))
            if blob_key:
                binary = ComfyUIImageVideoPlugin._blob_get(blob_key)

        if binary is None:
            file_id = ComfyUIImageVideoPlugin._text(payload.get("file_id") or payload.get("id"))
            if not file_id:
                file_id = ComfyUIImageVideoPlugin._file_id_from_url(
                    payload.get("url") or payload.get("uri") or ""
                )
            if file_id:
                binary = ComfyUIImageVideoPlugin._read_file_id(file_id)

        if binary is None:
            path_value = ComfyUIImageVideoPlugin._text(
                payload.get("path") or payload.get("file_path") or payload.get("artifact_path")
            )
            if path_value:
                try:
                    path = Path(path_value).expanduser()
                    if path.is_file():
                        binary = path.read_bytes()
                        filename = path.name or filename
                except Exception:
                    binary = None

        if not binary:
            return None

        detected_mimetype = ComfyUIImageVideoPlugin._guess_image_mimetype(binary)
        if detected_mimetype:
            mimetype = detected_mimetype
        if mimetype not in SOURCE_IMAGE_MIMETYPES:
            return None
        return binary, ComfyUIImageVideoPlugin._filename_from_payload(
            {"name": filename, "mimetype": mimetype}, mimetype
        )

    @staticmethod
    def _payloads_from_context(context, hinted_value: str = ""):
        if not isinstance(context, dict):
            return []

        rows = []
        containers = [context]
        origin = context.get("origin") if isinstance(context.get("origin"), dict) else None
        if origin:
            containers.append(origin)

        for container in containers:
            for key in ("input_artifacts", "available_artifacts", "attachments"):
                values = container.get(key)
                if not isinstance(values, list):
                    continue
                for item in values:
                    if isinstance(item, dict) and ComfyUIImageVideoPlugin._is_source_image_payload(item):
                        rows.append(item)

        hint = ComfyUIImageVideoPlugin._text(hinted_value).lower()
        if hint:
            matched = []
            for item in rows:
                tokens = [
                    item.get("artifact_id"),
                    item.get("id"),
                    item.get("file_id"),
                    item.get("blob_key"),
                    item.get("name"),
                    item.get("filename"),
                    item.get("path"),
                    item.get("url"),
                ]
                if any(ComfyUIImageVideoPlugin._text(token).lower() == hint for token in tokens):
                    matched.append(item)
            if matched:
                return matched
        return rows

    @staticmethod
    def _payloads_from_args(args):
        if not isinstance(args, dict):
            return []
        rows = []
        direct = {}
        for key in (
            "artifact_id",
            "file_id",
            "id",
            "blob_key",
            "url",
            "uri",
            "path",
            "file_path",
            "artifact_path",
            "data_url",
            "mimetype",
            "mime_type",
            "name",
            "filename",
            "type",
        ):
            if args.get(key) not in (None, ""):
                direct[key] = args.get(key)
        if direct:
            rows.append(direct)

        for key in ("image", "source_image", "input_image", "artifact"):
            value = args.get(key)
            if isinstance(value, dict):
                rows.append(value)
            elif isinstance(value, str) and value.strip():
                rows.append({"artifact_id": value.strip(), "name": value.strip()})
        return rows

    @staticmethod
    def _latest_image_from_chat_history(key: str = "webui:chat_history"):
        try:
            history = redis_client.lrange(key, -300, -1)
        except Exception:
            history = []

        for entry in reversed(history or []):
            try:
                msg = json.loads(entry)
            except Exception:
                continue
            content = msg.get("content") if isinstance(msg, dict) else None
            if isinstance(content, dict) and content.get("marker") == "plugin_response":
                content = content.get("content", {})

            candidates = []
            if isinstance(content, dict):
                candidates.append(content)
                artifacts = content.get("artifacts")
                if isinstance(artifacts, list):
                    candidates.extend([item for item in artifacts if isinstance(item, dict)])
            elif isinstance(content, list):
                candidates.extend([item for item in content if isinstance(item, dict)])

            for payload in candidates:
                resolved = ComfyUIImageVideoPlugin._materialize_image_payload(payload)
                if resolved:
                    return resolved
        return None, None

    @staticmethod
    def _resolve_source_image(args, context=None):
        args = args or {}
        hinted = ComfyUIImageVideoPlugin._text(
            args.get("artifact_id")
            or args.get("image")
            or args.get("source_image")
            or args.get("input_image")
        )
        for payload in (
            ComfyUIImageVideoPlugin._payloads_from_args(args)
            + ComfyUIImageVideoPlugin._payloads_from_context(context, hinted_value=hinted)
        ):
            resolved = ComfyUIImageVideoPlugin._materialize_image_payload(payload)
            if resolved:
                return resolved
        return ComfyUIImageVideoPlugin._latest_image_from_chat_history("webui:chat_history")

    # ---------------------------
    # URL helpers
    # ---------------------------
    @staticmethod
    def get_base_http() -> str:
        settings = redis_client.hgetall(f"verba_settings:{ComfyUIImageVideoPlugin.settings_category}")
        url_raw = settings.get("COMFYUI_VIDEO_URL", b"")
        url = url_raw.decode("utf-8").strip() if isinstance(url_raw, (bytes, bytearray)) else (url_raw or "").strip()
        if not url:
            url = "http://localhost:8188"
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "http://" + url
        return url.rstrip("/")

    @staticmethod
    def get_base_ws(base_http: str) -> str:
        # http->ws, https->wss
        scheme = "wss" if base_http.startswith("https://") else "ws"
        return base_http.replace("http", scheme, 1)

    # ---------------------------
    # Workflow/template + I/O
    # ---------------------------
    @staticmethod
    def get_workflow_template() -> dict:
        settings = redis_client.hgetall(f"verba_settings:{ComfyUIImageVideoPlugin.settings_category}")
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
        parsed = ComfyUIImageVideoPlugin._coerce_int(value, None)
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
            return ComfyUIImageVideoPlugin._coerce_int(inputs.get("value"), default)

        if class_type == "ComfyMathExpression":
            variables = {}
            for key, linked_value in inputs.items():
                if not str(key).startswith("values."):
                    continue
                var_name = str(key).split(".", 1)[1]
                resolved = ComfyUIImageVideoPlugin._resolve_numeric_input(
                    workflow, linked_value, None, seen=set(seen)
                )
                if resolved is not None:
                    variables[var_name] = resolved
            result = ComfyUIImageVideoPlugin._eval_numeric_expression(
                str(inputs.get("expression") or "").strip(), variables
            )
            return ComfyUIImageVideoPlugin._coerce_int(result, default)

        for key in ("value", "fps", "frame_rate", "length"):
            if key in inputs:
                resolved = ComfyUIImageVideoPlugin._resolve_numeric_input(
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

            title = ComfyUIImageVideoPlugin._node_title(node)
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
            title = ComfyUIImageVideoPlugin._node_title(node)
            inputs = node.setdefault("inputs", {})
            if "positive" in title and not isinstance(inputs.get("text"), list):
                inputs["text"] = prompt_text
                node["widgets_values"] = [prompt_text]
                return True

        if not allow_fallback:
            return False

        for node in encode_nodes:
            title = ComfyUIImageVideoPlugin._node_title(node)
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
            fps = ComfyUIImageVideoPlugin._resolve_numeric_input(wf, (node.get("inputs") or {}).get("fps"), None)
            if fps:
                return fps

        for node in wf.values():
            if not isinstance(node, dict) or node.get("class_type") != "PrimitiveInt":
                continue
            title = ComfyUIImageVideoPlugin._node_title(node)
            if "frame rate" in title or "fps" in title:
                fps = ComfyUIImageVideoPlugin._coerce_int((node.get("inputs") or {}).get("value"), None)
                if fps:
                    return fps

        return default

    # Convert animated webp (from Comfy) into mp4 for better compatibility
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

    # ---------------------------
    # Core generation (sync)
    # ---------------------------
    @staticmethod
    def _apply_overrides(workflow: dict, prompt_text: str, uploaded_image_path: str, width: int, height: int, frames: int):
        """Patch the uploaded image path, prompt text, and geometry/length into the workflow."""
        fps = ComfyUIImageVideoPlugin.get_fps_from_workflow(workflow, default=16)
        prompt_is_primitive = ComfyUIImageVideoPlugin._set_primitive_prompt_nodes(workflow, prompt_text)
        ComfyUIImageVideoPlugin._set_direct_clip_prompt(
            workflow, prompt_text, allow_fallback=not prompt_is_primitive
        )

        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            inputs = node.setdefault("inputs", {})

            # point loaders to uploaded image
            if node.get("class_type") == "LoadImage":
                inputs["image"] = uploaded_image_path

            if node.get("class_type") == "PrimitiveInt":
                title = ComfyUIImageVideoPlugin._node_title(node)
                if "width" in title:
                    inputs["value"] = int(width)
                elif "height" in title:
                    inputs["value"] = int(height)
                elif "frame rate" in title or "fps" in title:
                    inputs["value"] = int(fps)
                elif "duration" in title and fps:
                    inputs["value"] = max(1, int(round(int(frames) / int(fps))))

            # set width/height/length on common I2V nodes (WAN 2.2 etc.)
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
        base_http = ComfyUIImageVideoPlugin.get_base_http()
        base_ws   = ComfyUIImageVideoPlugin.get_base_ws(base_http)

        # upload the source image
        uploaded = ComfyUIImageVideoPlugin.upload_image(base_http, image_bytes, filename)

        # pull and clone template per job
        wf = copy.deepcopy(ComfyUIImageVideoPlugin.get_workflow_template())

        # resolution defaults
        settings = redis_client.hgetall(f"verba_settings:{ComfyUIImageVideoPlugin.settings_category}") or {}
        res_map = {
            "144p": (256, 144), "240p": (426, 240), "360p": (480, 360),
            "480p": (640, 480), "720p": (1280, 720), "1080p": (1920, 1080)
        }
        raw_res = settings.get("IMAGE_RESOLUTION", b"480p")
        resolution = raw_res.decode("utf-8") if isinstance(raw_res, (bytes, bytearray)) else (raw_res or "480p")
        default_w, default_h = res_map.get(resolution, (640, 480))

        # fps from workflow
        fps = ComfyUIImageVideoPlugin.get_fps_from_workflow(wf, default=16)

        # length defaults
        raw_length = settings.get("LENGTH", b"1")
        try:
            default_seconds = int(raw_length.decode() if isinstance(raw_length, (bytes, bytearray)) else raw_length)
        except Exception:
            default_seconds = 1
        default_frames = max(1, default_seconds * fps)

        try:
            w = int(width) if width is not None else int(default_w)
        except Exception:
            w = int(default_w)
        try:
            h = int(height) if height is not None else int(default_h)
        except Exception:
            h = int(default_h)
        try:
            frames = int(length) if length is not None else int(default_frames)
        except Exception:
            frames = int(default_frames)

        w = max(64, min(w, 4096))
        h = max(64, min(h, 4096))
        frames = max(1, min(frames, 6000))

        # randomize motion seeds
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

        # patch prompt/image/geometry into the per-job workflow
        ComfyUIImageVideoPlugin._apply_overrides(wf, prompt, uploaded, w, h, frames)

        # run Comfy with a per-job client_id + WS and wait by prompt_id
        prompt_id, _ = run_comfy_prompt(base_http, base_ws, wf)

        # fetch outputs from history (poll because Comfy history can lag after prompt completion)
        for _ in range(40):
            hist = ComfyUIImageVideoPlugin.get_history(base_http, prompt_id).get(prompt_id, {})
            outputs = hist.get("outputs", {}) if isinstance(hist, dict) else {}

            for node in outputs.values():
                if "images" in node and node["images"]:
                    img = node["images"][0]
                    content = ComfyUIImageVideoPlugin.fetch_asset(
                        base_http,
                        img["filename"],
                        img.get("subfolder", ""),
                        img.get("type", "output"),
                    )
                    ext = os.path.splitext(img["filename"])[-1].lstrip(".") or "webp"
                    return content, ext

            for node in outputs.values():
                if "videos" in node and node["videos"]:
                    vid = node["videos"][0]
                    content = ComfyUIImageVideoPlugin.fetch_asset(
                        base_http,
                        vid["filename"],
                        vid.get("subfolder", ""),
                        vid.get("type", "output"),
                    )
                    ext = os.path.splitext(vid["filename"])[-1].lstrip(".") or "mp4"
                    return content, ext

            time.sleep(0.5)

        # fallback: guess SaveVideo prefix on disk
        for node in wf.values():
            if node.get("class_type") == "SaveVideo":
                prefix = node["inputs"].get("filename_prefix", "ComfyUI")
                if "/" in prefix:
                    subfolder, base = prefix.split("/", 1)
                else:
                    subfolder, base = "", prefix
                guessed_filename = f"{base}.mp4"
                try:
                    content = ComfyUIImageVideoPlugin.fetch_asset(base_http, guessed_filename, subfolder, "output")
                    return content, "mp4"
                except Exception:
                    pass

        raise RuntimeError("No output found in ComfyUI history or disk.")

    # ---------------------------
    # Orchestration
    # ---------------------------
    async def _generate(self, args, llm_client, context=None):
        args = args or {}
        prompt = str(args.get("prompt") or "").strip()
        if not prompt:
            prompt = "subtle natural motion, smooth cinematic movement"
        image_bytes, filename = self._resolve_source_image(args, context=context)

        if not image_bytes:
            return action_failure(
                code="missing_source_image",
                message="No recent image file was found. Upload or generate an image first, then ask to animate it.",
                needs=["Provide or generate an image before requesting animation."],
                say_hint="Explain that no source image is available yet, or the prior image file may have expired.",
            )

        try:
            animated_bytes, ext = await asyncio.to_thread(
                self.process_prompt, prompt, image_bytes, filename
            )

            if ext == "webp":
                tmp_webp = f"/tmp/{uuid.uuid4().hex}.webp"
                tmp_mp4 = f"/tmp/{uuid.uuid4().hex}.mp4"
                try:
                    with open(tmp_webp, "wb") as f:
                        f.write(animated_bytes)

                    settings = redis_client.hgetall(f"verba_settings:{self.settings_category}") or {}
                    raw_len = settings.get("LENGTH", b"10")
                    try:
                        duration = int(raw_len.decode() if isinstance(raw_len, (bytes, bytearray)) else raw_len)
                    except Exception:
                        duration = 10

                    wf = ComfyUIImageVideoPlugin.get_workflow_template()
                    fps = ComfyUIImageVideoPlugin.get_fps_from_workflow(wf, default=16)

                    self.webp_to_mp4(tmp_webp, tmp_mp4, fps=fps, duration=duration)

                    with open(tmp_mp4, "rb") as f:
                        animated_bytes = f.read()
                    ext = "mp4"
                    mime = "video/mp4"
                    file_name = "animated.mp4"
                finally:
                    for p in (tmp_webp, tmp_mp4):
                        try:
                            if os.path.exists(p):
                                os.remove(p)
                        except Exception:
                            pass

                if llm_client is not None:
                    try:
                        msg = await llm_client.chat([
                            {"role": "system", "content": f'The user has just been shown an animated video based on the prompt: "{prompt}".'},
                            {"role": "user", "content": "Reply with a short, fun message celebrating the animation. No lead-in phrases or instructions."}
                        ])
                        followup_text = ((msg.get("message", {}) or {}).get("content", "") or "").strip()[:240]
                    except Exception:
                        followup_text = ""
                else:
                    followup_text = ""
            else:
                mime = "video/mp4" if ext == "mp4" else "image/webp"
                file_name = f"animated.{ext}"
                followup_text = ""

            media_type = "video" if ext == "mp4" else "image"
            artifact = _build_media_metadata(
                animated_bytes,
                media_type=media_type,
                name=file_name,
                mimetype=mime,
            )
            return action_success(
                facts={
                    "prompt": prompt,
                    "source_image": filename,
                    "artifact_type": media_type,
                    "artifact_count": 1,
                    "file_name": file_name,
                },
                summary_for_user=f"Generated an animated {media_type}.",
                flair=followup_text,
                say_hint="Confirm the animation result and reference the attached artifact.",
                artifacts=[artifact],
            )
        except Exception as e:
            logger.exception("ComfyUI image-video generation failed: %s", e)
            return action_failure(
                code="animation_failed",
                message=f"Failed to generate animation: {e}",
                say_hint="Explain the animation failure and suggest retrying.",
            )

    # --- Discord Handler ---
    async def handle_discord(self, message, args, llm_client):
        return action_failure(
            code="unsupported_platform",
            message="`comfyui_image_video` is only available in WebUI due to file size limitations.",
            say_hint="Explain this plugin is webui-only.",
            available_on=["webui"],
        )

    # --- WebUI Handler ---
    async def handle_webui(self, args, llm_client, context=None):
        return await self._generate(args or {}, llm_client, context=context)

    async def handle_little_spud(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self.handle_webui(args or {}, llm_client, context=context)


    async def handle_macos(self, args, llm_client, context=None):
        return await self.handle_webui(args or {}, llm_client, context=context)
    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return action_failure(
            code="unsupported_platform",
            message="`comfyui_image_video` is only available in WebUI.",
            say_hint="Explain this plugin is webui-only.",
            available_on=["webui"],
        )

verba = ComfyUIImageVideoPlugin()
