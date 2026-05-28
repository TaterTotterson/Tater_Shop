# verba/comfyui_video_plugin.py
import os
import asyncio
import subprocess
import json
import time
import copy
import secrets
import logging
import uuid
import requests
from PIL import Image
from moviepy.video.io.ImageSequenceClip import ImageSequenceClip
from verba_base import ToolVerba
from helpers import redis_client, run_comfy_prompt
from verba_result import action_failure, action_success

SETTINGS_CATEGORY = "ComfyUI Video"
logger = logging.getLogger("comfyui_video_plugin")
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
    def process_prompt(user_prompt: str, negative_prompt: str = "", width: int = None, height: int = None) -> bytes:
        base_http = _ComfyUIImageHelper.get_base_http()
        base_ws = _ComfyUIImageHelper.get_base_ws(base_http)

        workflow = copy.deepcopy(_ComfyUIImageHelper.get_workflow_template())
        _ComfyUIImageHelper._insert_prompts(workflow, user_prompt, negative_prompt)

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
                    content = _ComfyUIImageVideoHelper.fetch_asset(
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
                    content = _ComfyUIImageVideoHelper.fetch_asset(
                        base_http,
                        vid["filename"],
                        vid.get("subfolder", ""),
                        vid.get("type", "output"),
                    )
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

class ComfyUIVideoPlugin(ToolVerba):
    name = "comfyui_video_plugin"
    verba_name = "ComfyUI Video"
    version = "1.0.4"
    min_tater_version = "59"
    usage = '{"function":"comfyui_video_plugin","arguments":{"prompt":"<Describe the video you want>"}}'
    description = "Generates a video from a text prompt by creating multiple animated clips using ComfyUI, then merging them into one MP4."
    verba_dec = "Create a short video from a text prompt by stitching ComfyUI-generated clips."
    pretty_name = "Your Video"
    platforms = ["webui", "macos"]
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
            "description": "Resolution for the base image used in the animation."
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
            "description": "Length in seconds for each animated clip."
        },
        "VIDEO_RESOLUTION": {
            "label": "Video Resolution",
            "type": "select",
            "default": "720p",
            "options": ["144p", "240p", "360p", "480p", "720p", "1080p"],
            "description": "Resolution of the generated video."
        },
        "VIDEO_LENGTH": {
            "label": "Clip Length (seconds)",
            "type": "number",
            "default": 5,
            "description": "Length of each individual clip."
        },
        "VIDEO_CLIPS": {
            "label": "Number of Clips",
            "type": "number",
            "default": 1,
            "description": "How many clips to generate and merge into one video."
        }
    }
    waiting_prompt_template = "Generate a fun, upbeat message saying you’re directing a short video now! Only output that message."

    res_map = {
        "144p": (256, 144),
        "240p": (426, 240),
        "360p": (480, 360),
        "480p": (640, 480),
        "720p": (1280, 720),
        "1080p": (1920, 1080)
    }
    when_to_use = ""
    common_needs = []
    missing_info_prompts = []


    def cleanup_temp_files(self, paths):
        for path in paths:
            try:
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

    def ffmpeg_concat(self, video_paths, out_path):
        listpath = f"{out_path}_concat.txt"
        with open(listpath, "w") as f:
            for p in video_paths:
                f.write(f"file '{p}'\n")
        cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", listpath, "-c:v", "libx264", "-pix_fmt", "yuv420p", out_path]
        subprocess.run(cmd, check=True)
        os.remove(listpath)

    async def _derive_motion_directive(self, raw: str, llm_client) -> str:
        if llm_client is None:
            return "hand to face; soft laugh; brush hair behind ear"
        sys = (
            "Extract the intended animation as ONE concise directive (<= 24 words). "
            "Prioritize clear SUBJECT ACTIONS over camera motion (e.g., hand to face, laugh, brush hair behind ear). "
            "You may include a SINGLE gentle camera cue. "
            "Do not contradict explicit pose/clothing/framing if specified."
        )
        usr = f'User request: "{raw}"\nReturn only the motion directive (example: "hand to face; soft laugh; brush hair behind ear; slight dolly-in").'
        try:
            resp = await llm_client.chat([
                {"role": "system", "content": sys},
                {"role": "user", "content": usr}
            ])
            motion = (resp.get("message", {}) or {}).get("content", "").strip()
            return motion[:200] if motion else ""
        except Exception:
            return ""

    async def _per_clip_motion_prompts(self, scene_prompts, base_motion, llm_client):
        if not scene_prompts:
            return []
        if not base_motion:
            base_motion = "hand to face; soft laugh; brush hair behind ear"
        if llm_client is None:
            return [base_motion] * len(scene_prompts)

        n = len(scene_prompts)
        scenes_block = "\n".join([f"{i+1}. {sp}" for i, sp in enumerate(scene_prompts)])
        sys = (
            "For each scene, write a short 2–3 beat SUBJECT ACTION choreography for a ~15s clip. "
            "Each line <= 28 words. "
            "Beats should read in order using semicolons (e.g., 'touch cheek; laugh; push hair behind ear'). "
            "Include optional gentle camera cue last (e.g., 'subtle dolly-in'). "
            "Keep pose/clothing/framing exactly as given; do not invent props."
        )
        usr = (
            f'Base actions to preserve if present: "{base_motion}"\n\n'
            f"Scenes ({n} lines):\n{scenes_block}\n\n"
            f"Return exactly {n} lines. Numbered or plain is fine."
        )
        try:
            resp = await llm_client.chat([
                {"role": "system", "content": sys},
                {"role": "user", "content": usr}
            ])
            raw = (resp.get("message", {}) or {}).get("content", "") or ""
            lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
            out = []
            for ln in lines:
                if ln and ln[0].isdigit():
                    parts = ln.split(".", 1)
                    if len(parts) == 1:
                        parts = ln.split(")", 1)
                    ln = parts[1].strip() if len(parts) == 2 else ln
                out.append(ln)
            if len(out) < n:
                out += [base_motion] * (n - len(out))
            return out[:n]
        except Exception:
            return [base_motion] * len(scene_prompts)

    async def _generate_video(self, prompt, llm_client):
        # --- read settings safely
        settings = redis_client.hgetall(f"verba_settings:{self.settings_category}") or {}
        raw_res = settings.get("VIDEO_RESOLUTION", b"720p")
        resolution = raw_res.decode() if isinstance(raw_res, bytes) else raw_res
        w, h = self.res_map.get(resolution, (1280, 720))

        def _as_int(raw, default):
            try:
                return int(raw.decode() if isinstance(raw, bytes) else raw)
            except Exception:
                return default

        seconds_per_clip = max(1, _as_int(settings.get("VIDEO_LENGTH", b"5"), 5))
        num_clips = max(1, min(20, _as_int(settings.get("VIDEO_CLIPS", b"1"), 1)))  # cap to avoid runaway jobs

        # FPS from the image→video workflow (fallback 16)
        try:
            wf = _ComfyUIImageVideoHelper.get_workflow_template()
            fps = _ComfyUIImageVideoHelper.get_fps_from_workflow(wf, default=16)
        except Exception:
            fps = 16

        frame_count = max(1, int(seconds_per_clip * fps))

        job_id = str(uuid.uuid4())[:8]
        temp_paths, final_clips = [], []
        # --- Ask LLM for N short scene variations of the same concept ---
        scene_list_prompt = (
            f'Original brief:\n"{prompt}"\n\n'
            f"Task: Write {num_clips} distinct, concise scene prompts.\n"
            "HARD CONSTRAINTS (apply to EVERY line):\n"
            "- Keep the SAME subject, mood, number of people, and gender.\n"
            '- If the brief explicitly states pose/position (e.g., "laying on her side"), clothing, accessories, or framing (e.g., "full body"), REPEAT those words VERBATIM in every line.\n'
            "- Do NOT contradict any explicit motion; you may omit motion unless essential.\n"
            "- Do NOT change pose, clothing, or framing; do NOT introduce new props or wardrobe.\n"
            "ALLOWED VARIATION:\n"
            "- Only small changes to camera angle, lens, lighting, time of day, or background details.\n"
            "OUTPUT FORMAT:\n"
            f"- Exactly {num_clips} lines, each ≤ 25 words.\n"
            "- Numbered list using '1. ', '2. ', etc. No 'Scene:' prefixes.\n"
            "- Return ONLY the list.\n"
        )

        if llm_client is None:
            image_prompts = [prompt] * num_clips
        else:
            try:
                resp = await llm_client.chat([
                    {"role": "system", "content": "You write concise, varied scene prompts that keep subject & mood consistent."},
                    {"role": "user", "content": scene_list_prompt}
                ])
                raw_list = (resp.get("message", {}) or {}).get("content", "") or ""
                image_prompts = []
                for line in raw_list.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    if line[0].isdigit():
                        parts = line.split(".", 1)
                        if len(parts) == 1:
                            parts = line.split(")", 1)
                        if len(parts) == 1:
                            parts = line.split("-", 1)
                        if len(parts) == 2 and parts[1].strip():
                            image_prompts.append(parts[1].strip())
                            continue
                    image_prompts.append(line)
                if not image_prompts:
                    image_prompts = [prompt] * num_clips
                elif len(image_prompts) < num_clips:
                    image_prompts += [image_prompts[-1]] * (num_clips - len(image_prompts))
                else:
                    image_prompts = image_prompts[:num_clips]
            except Exception:
                image_prompts = [prompt] * num_clips

        # NEW: derive motion + tailor per-clip motion prompts
        base_motion = await self._derive_motion_directive(prompt, llm_client)
        motion_prompts = await self._per_clip_motion_prompts(image_prompts, base_motion, llm_client)

        out_path = f"/tmp/{job_id}_final.mp4"

        try:
            for i in range(num_clips):
                scene_prompt = image_prompts[i]
                motion_prompt = motion_prompts[i] if i < len(motion_prompts) else base_motion

                # 1) Generate a starting image (clean scene prompt)
                image_bytes = await asyncio.to_thread(
                    _ComfyUIImageHelper.process_prompt,
                    scene_prompt, "", w, h
                )

                tmp_img = f"/tmp/{job_id}_frame_{i}.png"
                with open(tmp_img, "wb") as f:
                    f.write(image_bytes)
                temp_paths.append(tmp_img)

                # 2) Animate using scene + motion in the SAME 'prompt'
                anim_prompt = f"{scene_prompt}\nMotion: {motion_prompt}"

                anim_bytes, ext = await asyncio.to_thread(
                    _ComfyUIImageVideoHelper.process_prompt,
                    anim_prompt,
                    image_bytes,
                    f"clip_{i}.png",
                    w,
                    h,
                    frame_count
                )

                anim_path = f"/tmp/{job_id}_clip_{i}.{ext}"
                with open(anim_path, "wb") as f:
                    f.write(anim_bytes)
                temp_paths.append(anim_path)

                if ext == "webp":
                    mp4_path = f"/tmp/{job_id}_clip_{i}.mp4"
                    self.webp_to_mp4(anim_path, mp4_path, fps=fps, duration=seconds_per_clip)
                    temp_paths.append(mp4_path)
                    final_clips.append(mp4_path)
                else:
                    final_clips.append(anim_path)

            if not final_clips:
                return action_failure(
                    code="no_clips_generated",
                    message="No clips were generated.",
                    say_hint="Explain that no video clips were produced and suggest retrying.",
                )

            self.ffmpeg_concat(final_clips, out_path)
            with open(out_path, "rb") as f:
                final_bytes = f.read()
            temp_paths.append(out_path)

            if llm_client is not None:
                msg = await llm_client.chat([
                    {"role": "system", "content": f"The user has just been shown a video based on '{prompt}'."},
                    {"role": "user", "content": "Reply with a short, fun message celebrating the video. No lead-in phrases or instructions."}
                ])
                msg_text = (msg["message"]["content"].strip() if msg and msg.get("message") else "")[:240]
            else:
                msg_text = ""

            artifact = _build_media_metadata(
                final_bytes,
                media_type="video",
                name="generated_video.mp4",
                mimetype="video/mp4",
            )
            return action_success(
                facts={
                    "prompt": prompt,
                    "clip_count": len(final_clips),
                    "artifact_type": "video",
                    "artifact_count": 1,
                    "file_name": "generated_video.mp4",
                },
                summary_for_user="Generated one video.",
                flair=msg_text,
                say_hint="Confirm video generation and reference the attached file.",
                artifacts=[artifact],
            )
        finally:
            self.cleanup_temp_files(temp_paths)

    async def handle_discord(self, message, args, llm_client):
        return action_failure(
            code="unsupported_platform",
            message="`comfyui_video_plugin` is only available in WebUI due to file size limitations.",
            say_hint="Explain this plugin is webui-only.",
            available_on=["webui"],
        )

    async def handle_webui(self, args, llm_client):
        args = args or {}
        if "prompt" not in args:
            return action_failure(
                code="missing_prompt",
                message="No prompt provided.",
                needs=["Provide a prompt describing the video you want."],
                say_hint="Ask the user for a video prompt.",
            )
        try:
            return await self._generate_video(args["prompt"], llm_client)
        except Exception as e:
            logger.exception("ComfyUI video generation failed: %s", e)
            return action_failure(
                code="video_generation_failed",
                message=f"Error generating video: {e}",
                say_hint="Explain the generation failure and suggest retrying.",
            )


    async def handle_macos(self, args, llm_client, context=None):
        try:
            return await self.handle_webui(args, llm_client, context=context)
        except TypeError:
            return await self.handle_webui(args, llm_client)
    async def handle_irc(self, bot, channel, user, raw, args, llm_client):
        return action_failure(
            code="unsupported_platform",
            message="`comfyui_video_plugin` is only available in WebUI.",
            say_hint="Explain this plugin is webui-only.",
            available_on=["webui"],
        )

verba = ComfyUIVideoPlugin()
