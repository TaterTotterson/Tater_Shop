# verba/comfyui_image_edit.py
import asyncio
import base64
import copy
import imghdr
import json
import logging
import mimetypes
import os
import secrets
import time
import uuid
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests

from verba_base import ToolVerba
from helpers import redis_client, run_comfy_prompt
try:
    from helpers import redis_blob_client
except Exception:
    redis_blob_client = redis_client
from verba_result import action_failure, action_success

logger = logging.getLogger("comfyui_image_edit")
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


class ComfyUIImageEditPlugin(ToolVerba):
    name = "comfyui_image_edit"
    verba_name = "ComfyUI Image Edit"
    version = "1.0.1"
    min_tater_version = "59"
    usage = '{"function":"comfyui_image_edit","arguments":{"prompt":"<Specific edit instruction for the existing/latest image, e.g. make the woman have red hair>","artifact_id":"<Optional exact artifact_id/image reference if the user points to a specific image>"}}'
    description = "Use this when the user wants to edit or transform an existing image from the chat, not create a new image from scratch. It changes the latest or referenced image with instructions like change hair color, replace an object, add or remove details, restyle the image, or keep the scene but alter part of it."
    verba_dec = "Edit or transform the latest/referenced image in chat with ComfyUI image-to-image."
    pretty_name = "Editing Your Image"
    settings_category = "ComfyUI Image Edit"
    required_settings = {
        "COMFYUI_IMAGE_EDIT_URL": {
            "label": "ComfyUI Server URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "Base URL (host:port) for your ComfyUI instance.",
        },
        "COMFYUI_IMAGE_EDIT_WORKFLOW": {
            "label": "Workflow Template (JSON)",
            "type": "file",
            "accept": ".json,application/json",
            "default": "",
            "description": "Upload a ComfyUI image-edit workflow .json file.",
        },
    }
    waiting_prompt_template = "Write a short, friendly message saying you’re editing their image now. Only output that message."
    platforms = ["webui", "little_spud", "macos"]
    when_to_use = "Use for requests that refer to an existing image and ask to change it, such as edit this image, make her hair red, replace the cat with a dog, remove the background, add sunglasses, or restyle that picture."
    common_needs = ["edit image", "change image", "image to image", "modify photo", "replace object", "change hair color", "restyle image"]
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
        mimetype = ComfyUIImageEditPlugin._text(
            payload.get("mimetype") or payload.get("mime_type") or payload.get("content_type")
        ).lower()
        if mimetype:
            return mimetype
        guessed = mimetypes.guess_type(filename or ComfyUIImageEditPlugin._text(payload.get("name")))[0]
        return ComfyUIImageEditPlugin._text(guessed).lower()

    @staticmethod
    def _filename_from_payload(payload: dict, mimetype: str = "") -> str:
        name = ComfyUIImageEditPlugin._text(payload.get("name") or payload.get("filename"))
        if not name:
            for key in ("path", "file_path", "artifact_path", "url", "uri"):
                value = ComfyUIImageEditPlugin._text(payload.get(key))
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
        name = ComfyUIImageEditPlugin._text(filename or payload.get("name") or payload.get("filename")).lower()
        mimetype = ComfyUIImageEditPlugin._text(payload.get("mimetype") or payload.get("mime_type")).lower()
        artifact_type = ComfyUIImageEditPlugin._text(payload.get("type")).lower()
        if artifact_type == "video" or mimetype.startswith("video/"):
            return True
        return name.startswith("animated.") or name.startswith("animation.")

    @staticmethod
    def _is_source_image_payload(payload: dict) -> bool:
        if not isinstance(payload, dict):
            return False
        filename = ComfyUIImageEditPlugin._filename_from_payload(payload)
        if ComfyUIImageEditPlugin._looks_like_animation_output(payload, filename):
            return False
        mimetype = ComfyUIImageEditPlugin._image_mimetype_for_payload(payload, filename)
        artifact_type = ComfyUIImageEditPlugin._text(payload.get("type")).lower()
        ext = os.path.splitext(filename.lower())[1]
        if mimetype in SOURCE_IMAGE_MIMETYPES:
            return True
        if artifact_type == "image" and mimetype.startswith("image/") and mimetype != "image/svg+xml":
            return True
        return ext in SOURCE_IMAGE_EXTS

    @staticmethod
    def _decode_data_url(value: str):
        text = ComfyUIImageEditPlugin._text(value)
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
        token = ComfyUIImageEditPlugin._text(key)
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
        text = ComfyUIImageEditPlugin._text(value)
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
        token = ComfyUIImageEditPlugin._text(file_id)
        if not token:
            return None
        if token.startswith(WEBUI_FILE_BLOB_PREFIX):
            return ComfyUIImageEditPlugin._blob_get(token)
        return ComfyUIImageEditPlugin._blob_get(f"{WEBUI_FILE_BLOB_PREFIX}{token}")

    @staticmethod
    def _materialize_image_payload(payload: dict):
        if not isinstance(payload, dict) or not ComfyUIImageEditPlugin._is_source_image_payload(payload):
            return None

        filename = ComfyUIImageEditPlugin._filename_from_payload(payload)
        mimetype = ComfyUIImageEditPlugin._image_mimetype_for_payload(payload, filename)
        binary = None

        for key in ("data_url", "url", "uri", "data"):
            raw_value = ComfyUIImageEditPlugin._text(payload.get(key))
            if raw_value.lower().startswith("data:"):
                binary, data_mimetype = ComfyUIImageEditPlugin._decode_data_url(raw_value)
                if data_mimetype:
                    mimetype = data_mimetype
                if binary:
                    break

        if binary is None:
            blob_key = ComfyUIImageEditPlugin._text(payload.get("blob_key"))
            if blob_key:
                binary = ComfyUIImageEditPlugin._blob_get(blob_key)

        if binary is None:
            file_id = ComfyUIImageEditPlugin._text(payload.get("file_id") or payload.get("id"))
            if not file_id:
                file_id = ComfyUIImageEditPlugin._file_id_from_url(payload.get("url") or payload.get("uri") or "")
            if file_id:
                binary = ComfyUIImageEditPlugin._read_file_id(file_id)

        if binary is None:
            path_value = ComfyUIImageEditPlugin._text(
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

        detected_mimetype = ComfyUIImageEditPlugin._guess_image_mimetype(binary)
        if detected_mimetype:
            mimetype = detected_mimetype
        if mimetype not in SOURCE_IMAGE_MIMETYPES:
            return None
        return binary, ComfyUIImageEditPlugin._filename_from_payload(
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
                    if isinstance(item, dict) and ComfyUIImageEditPlugin._is_source_image_payload(item):
                        rows.append(item)

        hint = ComfyUIImageEditPlugin._text(hinted_value).lower()
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
                if any(ComfyUIImageEditPlugin._text(token).lower() == hint for token in tokens):
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
                resolved = ComfyUIImageEditPlugin._materialize_image_payload(payload)
                if resolved:
                    return resolved
        return None, None

    @staticmethod
    def _resolve_source_image(args, context=None):
        args = args or {}
        hinted = ComfyUIImageEditPlugin._text(
            args.get("artifact_id")
            or args.get("image")
            or args.get("source_image")
            or args.get("input_image")
        )
        for payload in (
            ComfyUIImageEditPlugin._payloads_from_args(args)
            + ComfyUIImageEditPlugin._payloads_from_context(context, hinted_value=hinted)
        ):
            resolved = ComfyUIImageEditPlugin._materialize_image_payload(payload)
            if resolved:
                return resolved
        return ComfyUIImageEditPlugin._latest_image_from_chat_history("webui:chat_history")

    @staticmethod
    def _resolve_prompt(args: dict | None) -> str:
        args = args or {}
        for key in ("prompt", "edit", "instruction", "request", "message", "query", "text"):
            value = args.get(key)
            if isinstance(value, str) and value.strip():
                return " ".join(value.split())
        return ""

    @staticmethod
    def get_base_http() -> str:
        settings = redis_client.hgetall(f"verba_settings:{ComfyUIImageEditPlugin.settings_category}") or {}
        raw = settings.get("COMFYUI_IMAGE_EDIT_URL", b"")
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
    def get_workflow_template() -> dict:
        settings = redis_client.hgetall(f"verba_settings:{ComfyUIImageEditPlugin.settings_category}") or {}
        raw = settings.get("COMFYUI_IMAGE_EDIT_WORKFLOW", b"")
        workflow_str = raw.decode("utf-8").strip() if isinstance(raw, (bytes, bytearray)) else (raw or "").strip()
        if not workflow_str:
            raise RuntimeError("No workflow found. Upload a valid image-edit workflow JSON in settings.")
        return json.loads(workflow_str)

    @staticmethod
    def upload_image(base_http: str, image_bytes: bytes, filename: str) -> str:
        safe_name = os.path.basename(filename or "input.png").replace("/", "_").replace("\\", "_") or "input.png"
        upload_name = f"tater_edit_source_{uuid.uuid4().hex[:8]}_{safe_name}"
        resp = requests.post(
            f"{base_http}/upload/image",
            files={"image": (upload_name, image_bytes)},
            data={"overwrite": "true"},
            timeout=60,
        )
        resp.raise_for_status()
        result = resp.json()
        name = result.get("name") or result.get("filename") or upload_name
        subfolder = result.get("subfolder", "")
        return f"{subfolder}/{name}" if subfolder else name

    @staticmethod
    def get_history(base_http: str, prompt_id: str) -> dict:
        resp = requests.get(f"{base_http}/history/{prompt_id}", timeout=30)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def fetch_asset(base_http: str, filename: str, subfolder: str, folder_type: str) -> bytes:
        params = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        resp = requests.get(f"{base_http}/view", params=params, timeout=60)
        resp.raise_for_status()
        return resp.content

    @staticmethod
    def _node_title(node: dict) -> str:
        return ((node.get("_meta", {}) or {}).get("title", "") or "").strip().lower()

    @staticmethod
    def _set_prompt_nodes(workflow: dict, prompt_text: str) -> bool:
        patched = False
        fallback = []

        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            if node.get("class_type") not in {"PrimitiveStringMultiline", "PrimitiveString"}:
                continue
            title = ComfyUIImageEditPlugin._node_title(node)
            if "negative" in title:
                continue
            node.setdefault("inputs", {})
            if "prompt" in title or "instruction" in title or "edit" in title:
                node["inputs"]["value"] = prompt_text
                patched = True
            else:
                fallback.append(node)

        if not patched and len(fallback) == 1:
            fallback[0].setdefault("inputs", {})
            fallback[0]["inputs"]["value"] = prompt_text
            patched = True

        return patched

    @staticmethod
    def _set_direct_prompt_nodes(workflow: dict, prompt_text: str, allow_ambiguous_fallback: bool) -> bool:
        patched = False
        direct_candidates = []

        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            inputs = node.setdefault("inputs", {})
            if "prompt" not in inputs or isinstance(inputs.get("prompt"), list):
                continue
            title = ComfyUIImageEditPlugin._node_title(node)
            if "negative" in title:
                continue
            if "positive" in title or "prompt" in title or "edit" in title:
                inputs["prompt"] = prompt_text
                patched = True
            else:
                direct_candidates.append(node)

        if not patched and allow_ambiguous_fallback and len(direct_candidates) == 1:
            direct_candidates[0].setdefault("inputs", {})
            direct_candidates[0]["inputs"]["prompt"] = prompt_text
            patched = True

        return patched

    @staticmethod
    def _apply_overrides(workflow: dict, prompt_text: str, uploaded_image_path: str) -> None:
        prompt_patched = ComfyUIImageEditPlugin._set_prompt_nodes(workflow, prompt_text)
        ComfyUIImageEditPlugin._set_direct_prompt_nodes(
            workflow,
            prompt_text,
            allow_ambiguous_fallback=not prompt_patched,
        )

        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            inputs = node.setdefault("inputs", {})
            class_type = str(node.get("class_type") or "")
            if class_type == "LoadImage":
                inputs["image"] = uploaded_image_path
            if "seed" in inputs and not isinstance(inputs.get("seed"), list):
                inputs["seed"] = int(secrets.randbits(63))
            if "noise_seed" in inputs and not isinstance(inputs.get("noise_seed"), list):
                inputs["noise_seed"] = int(secrets.randbits(63))

    @staticmethod
    def _infer_mime_and_ext(data: bytes):
        kind = imghdr.what(None, h=data)
        mime = {
            "png": "image/png",
            "jpeg": "image/jpeg",
            "gif": "image/gif",
            "webp": "image/webp",
            "bmp": "image/bmp",
            "tiff": "image/tiff",
        }.get(kind, ComfyUIImageEditPlugin._guess_image_mimetype(data) or "image/png")
        ext = {
            "image/png": "png",
            "image/jpeg": "jpg",
            "image/gif": "gif",
            "image/webp": "webp",
            "image/bmp": "bmp",
            "image/tiff": "tiff",
        }.get(mime, "png")
        return mime, ext

    @staticmethod
    def _fetch_first_image(base_http: str, prompt_id: str, tries: int = 40, delay: float = 0.5) -> bytes:
        for _ in range(max(1, tries)):
            history_all = ComfyUIImageEditPlugin.get_history(base_http, prompt_id)
            history = history_all.get(prompt_id, {}) if isinstance(history_all, dict) else {}
            outputs = history.get("outputs", {}) if isinstance(history, dict) else {}

            for node_output in outputs.values():
                images = node_output.get("images") if isinstance(node_output, dict) else []
                for image in images or []:
                    filename = image.get("filename")
                    if filename:
                        return ComfyUIImageEditPlugin.fetch_asset(
                            base_http,
                            filename,
                            image.get("subfolder", ""),
                            image.get("type", "output"),
                        )
            time.sleep(delay)

        raise RuntimeError("No edited image returned from ComfyUI history.")

    @staticmethod
    def process_prompt(prompt: str, image_bytes: bytes, filename: str) -> bytes:
        base_http = ComfyUIImageEditPlugin.get_base_http()
        base_ws = ComfyUIImageEditPlugin.get_base_ws(base_http)
        uploaded = ComfyUIImageEditPlugin.upload_image(base_http, image_bytes, filename)
        workflow = copy.deepcopy(ComfyUIImageEditPlugin.get_workflow_template())
        ComfyUIImageEditPlugin._apply_overrides(workflow, prompt, uploaded)
        prompt_id, _ = run_comfy_prompt(base_http, base_ws, workflow)
        return ComfyUIImageEditPlugin._fetch_first_image(base_http, prompt_id)

    @staticmethod
    async def _celebrate(llm_client, prompt: str) -> str:
        if llm_client is None:
            return ""
        safe_prompt = "".join(ch for ch in prompt[:300] if ch.isprintable()).strip()
        try:
            final_response = await llm_client.chat(
                messages=[
                    {"role": "system", "content": f'The user has just been shown an edited image. Edit request: "{safe_prompt}".'},
                    {"role": "user", "content": "Respond with a short, fun message celebrating the edited image. No lead-in phrases or instructions."},
                ]
            )
            return ((final_response.get("message", {}) or {}).get("content", "") or "").strip()[:240]
        except Exception:
            return ""

    async def _generate(self, args, llm_client, context=None):
        args = args or {}
        prompt = self._resolve_prompt(args)
        if not prompt:
            return action_failure(
                code="missing_edit_prompt",
                message="Tell me what to change in the image.",
                needs=["Describe the edit to apply to the image."],
                say_hint="Ask for the image edit instruction.",
            )

        image_bytes, filename = self._resolve_source_image(args, context=context)
        if not image_bytes:
            return action_failure(
                code="missing_source_image",
                message="No recent image file was found. Upload or generate an image first, then ask to edit it.",
                needs=["Provide or generate an image before requesting an edit."],
                say_hint="Explain that no source image is available yet, or the prior image file may have expired.",
            )

        try:
            edited_bytes = await asyncio.to_thread(self.process_prompt, prompt, image_bytes, filename)
            mime, ext = self._infer_mime_and_ext(edited_bytes)
            file_name = f"edited_image.{ext}"
            image_artifact = _build_media_metadata(
                edited_bytes,
                media_type="image",
                name=file_name,
                mimetype=mime,
            )
            flair = await self._celebrate(llm_client, prompt)
            return action_success(
                facts={
                    "prompt": prompt,
                    "source_image": filename,
                    "artifact_type": "image",
                    "artifact_count": 1,
                    "file_name": file_name,
                },
                summary_for_user="Generated one edited image.",
                flair=flair,
                say_hint="Confirm the image edit result and reference the attached image.",
                artifacts=[image_artifact],
            )
        except Exception as exc:
            logger.exception("ComfyUI image edit failed: %s", exc)
            return action_failure(
                code="image_edit_failed",
                message=f"Failed to edit image: {type(exc).__name__}: {exc}",
                say_hint="Explain the image edit failure and suggest checking the ComfyUI workflow/settings.",
            )

    async def handle_webui(self, args, llm_client, context=None):
        return await self._generate(args or {}, llm_client, context=context)

    async def handle_little_spud(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self._generate(args or {}, llm_client, context=context)

    async def handle_macos(self, args, llm_client, context=None):
        return await self._generate(args or {}, llm_client, context=context)

    async def handle_discord(self, message, args, llm_client):
        return action_failure(
            code="unsupported_platform",
            message="`comfyui_image_edit` is only available in WebUI, Little Spud, and macOS.",
            say_hint="Explain this plugin is unavailable on this platform and list supported platforms.",
            available_on=self.platforms,
        )


verba = ComfyUIImageEditPlugin()
