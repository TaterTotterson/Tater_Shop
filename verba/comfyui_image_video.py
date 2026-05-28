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
from PIL import Image
from moviepy.video.io.ImageSequenceClip import ImageSequenceClip
from verba_base import ToolVerba
from helpers import redis_client, get_latest_image_from_history, run_comfy_prompt
from verba_result import action_failure, action_success

logger = logging.getLogger("comfyui_image_video")
logger.setLevel(logging.INFO)


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
    version = "1.0.4"
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
    platforms = ["webui", "macos"]
    when_to_use = ""
    common_needs = []
    missing_info_prompts = []


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
    async def _generate(self, args, llm_client):
        args = args or {}
        prompt = str(args.get("prompt") or "").strip()
        if not prompt:
            prompt = "subtle natural motion, smooth cinematic movement"
        image_bytes, filename = get_latest_image_from_history("webui:chat_history")

        if not image_bytes:
            return action_failure(
                code="missing_source_image",
                message="No image found. Upload one or generate one first.",
                needs=["Provide or generate an image before requesting animation."],
                say_hint="Explain that no source image is available yet.",
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
    async def handle_webui(self, args, llm_client):
        return await self._generate(args or {}, llm_client)


    async def handle_macos(self, args, llm_client, context=None):
        try:
            return await self.handle_webui(args, llm_client, context=context)
        except TypeError:
            return await self.handle_webui(args, llm_client)
    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return action_failure(
            code="unsupported_platform",
            message="`comfyui_image_video` is only available in WebUI.",
            say_hint="Explain this plugin is webui-only.",
            available_on=["webui"],
        )

verba = ComfyUIImageVideoPlugin()
