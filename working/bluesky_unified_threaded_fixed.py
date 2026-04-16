#!/usr/bin/env python3
"""
bluesky_unified_threaded_fixed.py

Pure-Python, non-blocking Bluesky helper.
No TouchDesigner globals are used here.

New in this version:
- post_video(video_path, text="")
- get_author_posts(handle=None, limit=20)
- get_timeline(limit=20)
- get_friend_posts(handle, limit=20)

TouchDesigner should consume status updates and returned feed rows
through a separate wrapper script.
"""

import io
import json
import mimetypes
import os
import queue
import threading
import urllib.request
from pathlib import Path
from typing import Callable, Optional, Dict, Any, List

from dotenv import load_dotenv
from atproto import Client
from PIL import Image


class BlueskyUnifiedThreaded:
    def __init__(
        self,
        babbler_path: str,
        env_path: Optional[str] = None,
        post_history_file: Optional[str] = None,
    ) -> None:
        self.babbler_path = Path(babbler_path)
        self.env_path = Path(env_path) if env_path else self.babbler_path / ".env"
        self.post_history_file = (
            Path(post_history_file) if post_history_file else self.babbler_path / "last_post.json"
        )

        load_dotenv(self.env_path)

        self._active_thread: Optional[threading.Thread] = None
        self._status_queue: "queue.Queue[dict]" = queue.Queue()

        self.image_cache_dir = self.babbler_path / "image_cache"
        self.image_cache_dir.mkdir(exist_ok=True)

    # -------------------------------------------------------------------------
    # Queue / status helpers
    # -------------------------------------------------------------------------

    def push_status(self, status: str, message: str, **extra: Any) -> None:
        payload = {"status": status, "message": str(message)[:500]}
        payload.update(extra)
        self._status_queue.put(payload)

    def get_next_status(self) -> Optional[dict]:
        try:
            return self._status_queue.get_nowait()
        except queue.Empty:
            return None

    def drain_status(self) -> list[dict]:
        items = []
        while True:
            item = self.get_next_status()
            if item is None:
                break
            items.append(item)
        return items

    # -------------------------------------------------------------------------
    # File helpers
    # -------------------------------------------------------------------------

    def save_last_post(self, post_uri: str, post_cid: str) -> None:
        try:
            with open(self.post_history_file, "w", encoding="utf-8") as f:
                json.dump({"uri": post_uri, "cid": post_cid}, f)
        except Exception as e:
            self.push_status("ERROR", f"Error saving post info: {e}")

    def load_last_post(self) -> Optional[dict]:
        try:
            if self.post_history_file.exists():
                with open(self.post_history_file, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            self.push_status("ERROR", f"Error loading post info: {e}")
        return None

    # -------------------------------------------------------------------------
    # Core helpers
    # -------------------------------------------------------------------------

    def _get_credentials(self) -> tuple[str, str]:
        username = os.getenv("BLUESKY_USERNAME")
        password = os.getenv("BLUESKY_PASSWORD")
        if not username or not password:
            raise RuntimeError(
                f"Missing BLUESKY_USERNAME / BLUESKY_PASSWORD in {self.env_path}"
            )
        return username, password

    def _login_client(self) -> tuple[Client, str]:
        username, password = self._get_credentials()
        client = Client()
        client.login(username, password)
        return client, username

    def get_profile_record(self, client: Client, did: str) -> Optional[dict]:
        try:
            record = client.com.atproto.repo.get_record(
                {
                    "repo": did,
                    "collection": "app.bsky.actor.profile",
                    "rkey": "self",
                }
            )

            if hasattr(record.value, "dict"):
                return record.value.dict()

            profile_dict: Dict[str, Any] = {}
            for attr in dir(record.value):
                if not attr.startswith("_") and hasattr(record.value, attr):
                    value = getattr(record.value, attr)
                    if not callable(value):
                        profile_dict[attr] = value
            return profile_dict

        except Exception as e:
            self.push_status("ERROR", f"Error getting profile record: {e}")
            return None

    def resize_image_if_needed(self, image_path: str) -> bytes:
        with open(image_path, "rb") as f:
            image_data = f.read()

        if len(image_data) <= 1_000_000:
            return image_data

        self.push_status("INFO", f"Image is {len(image_data)} bytes, resizing...")

        with Image.open(io.BytesIO(image_data)) as img:
            width, height = img.size
            new_size = (max(1, int(width * 0.7)), max(1, int(height * 0.7)))

            img_resized = img.resize(new_size, Image.Resampling.LANCZOS)

            buffer = io.BytesIO()
            img_resized.save(buffer, format="JPEG", quality=85, optimize=True)
            image_data = buffer.getvalue()

            while len(image_data) > 1_000_000:
                new_size = (max(1, int(new_size[0] * 0.8)), max(1, int(new_size[1] * 0.8)))
                img_resized = img.resize(new_size, Image.Resampling.LANCZOS)
                buffer = io.BytesIO()
                img_resized.save(buffer, format="JPEG", quality=80, optimize=True)
                image_data = buffer.getvalue()

        self.push_status("INFO", f"Resized image to {len(image_data)} bytes")
        return image_data

    def _read_binary_file(self, path: str) -> bytes:
        with open(path, "rb") as f:
            return f.read()

    def _guess_mime_type(self, path: str, fallback: str) -> str:
        mime_type, _ = mimetypes.guess_type(path)
        return mime_type or fallback

    def _extract_image_urls(self, post: Any) -> list:
        urls = []
        try:
            embed = getattr(post, "embed", None)
            if embed:
                if hasattr(embed, "images"):
                    for img in embed.images:
                        if hasattr(img, "fullsize"):
                            urls.append(img.fullsize)
                        elif hasattr(img, "thumb"):
                            urls.append(img.thumb)
                elif hasattr(embed, "external"):
                    thumb = getattr(embed.external, "thumb", None)
                    if thumb:
                        urls.append(thumb)
        except Exception:
            pass
        return urls

    def _download_image(self, url: str, post_id: str, index: int = 0) -> str:
        try:
            safe_id = post_id.replace(":", "_").replace("/", "_")[:50]
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as response:
                content_type = response.headers.get("Content-Type", "image/jpeg")
                ext = {"image/jpeg": ".jpg", "image/jpg": ".jpg", "image/png": ".png",
                       "image/gif": ".gif", "image/webp": ".webp"}.get(content_type, ".jpg")
                filepath = self.image_cache_dir / f"{safe_id}_{index}{ext}"
                with open(filepath, "wb") as f:
                    f.write(response.read())
                return str(filepath)
        except Exception as e:
            self.push_status("WARN", f"Image download failed: {e}")
            return ""

    def _extract_text(self, post_view: Any) -> str:
        try:
            record = getattr(post_view, "record", None)
            if record is None:
                return ""
            if hasattr(record, "text"):
                return record.text or ""
            if hasattr(record, "dict"):
                return str(record.dict().get("text", "") or "")
            if isinstance(record, dict):
                return str(record.get("text", "") or "")
        except Exception:
            pass
        return ""

    def _extract_created_at(self, post_view: Any) -> str:
        try:
            record = getattr(post_view, "record", None)
            created_at = getattr(record, "created_at", None)
            if created_at:
                return str(created_at).replace("T", " ").replace("Z", "")[:16]
        except Exception:
            pass
        return "Unknown"

    def _feed_item_to_row(self, item: Any, fallback_type: str = "post", username: str = "") -> Optional[dict]:
        try:
            post = getattr(item, "post", None)
            if not post:
                return None

            author_handle = getattr(getattr(post, "author", None), "handle", "") or ""
            text = self._extract_text(post)
            if not text and not getattr(post, "embed", None):
                return None

            post_type = fallback_type
            if username and author_handle == username:
                post_type = "self_post"
            elif username:
                bare = username.split(".")[0] if "." in username else username
                if bare and bare in text:
                    post_type = "mention"

            post_id = getattr(post, "uri", "").split("/")[-1] or "unknown"
            image_urls = self._extract_image_urls(post)
            image_path = self._download_image(image_urls[0], post_id) if image_urls else ""

            return {
                "type": post_type,
                "author": author_handle,
                "message": text[:100] + "..." if len(text) > 100 else text,
                "full_text": text,
                "time_posted": self._extract_created_at(post),
                "likes": str(getattr(post, "like_count", 0) or 0),
                "reposts": str(getattr(post, "repost_count", 0) or 0),
                "has_image": "1" if image_path else "0",
                "image_path": image_path,
            }
        except Exception:
            return None

    def _start_thread(self, target_func: Callable, *args: Any) -> tuple[bool, str]:
        if self._active_thread and self._active_thread.is_alive():
            self.push_status("BUSY", "Operation already running")
            return False, "Operation already running"

        def wrapper() -> None:
            try:
                target_func(*args)
            except Exception as e:
                self.push_status("ERROR", str(e))

        self._active_thread = threading.Thread(target=wrapper, daemon=True)
        self._active_thread.start()
        return True, "Started"

    # -------------------------------------------------------------------------
    # Background operations
    # -------------------------------------------------------------------------

    def _do_update_display_name(self, new_display_name: str) -> None:
        client, username = self._login_client()
        self.push_status("UPDATING", f"Setting display name: {new_display_name}")

        profile_record = self.get_profile_record(client, client.me.did)
        if not profile_record:
            self.push_status("ERROR", "Could not get profile record")
            return

        update_data = profile_record.copy()
        update_data["displayName"] = new_display_name

        client.com.atproto.repo.put_record(
            {
                "repo": client.me.did,
                "collection": "app.bsky.actor.profile",
                "rkey": "self",
                "record": update_data,
            }
        )

        updated_profile = client.get_profile(username)
        self.push_status("SUCCESS", f"Display name: {updated_profile.display_name}")

    def _do_update_profile_picture(self, image_path: str) -> None:
        if not os.path.exists(image_path):
            self.push_status("ERROR", f"Image not found: {image_path}")
            return

        client, username = self._login_client()
        self.push_status("UPDATING", f"Updating avatar: {os.path.basename(image_path)}")

        image_data = self.resize_image_if_needed(image_path)
        uploaded_blob = client.upload_blob(image_data)

        profile_record = self.get_profile_record(client, client.me.did)
        if not profile_record:
            self.push_status("ERROR", "Could not get profile record")
            return

        update_data = profile_record.copy()
        update_data["avatar"] = uploaded_blob.blob

        client.com.atproto.repo.put_record(
            {
                "repo": client.me.did,
                "collection": "app.bsky.actor.profile",
                "rkey": "self",
                "record": update_data,
            }
        )

        updated_profile = client.get_profile(username)
        self.push_status("SUCCESS", f"Profile picture updated: {updated_profile.avatar}")

    def _do_update_banner(self, image_path: str) -> None:
        if not os.path.exists(image_path):
            self.push_status("ERROR", f"Image not found: {image_path}")
            return

        client, username = self._login_client()
        self.push_status("UPDATING", f"Updating banner: {os.path.basename(image_path)}")

        image_data = self.resize_image_if_needed(image_path)
        uploaded_blob = client.upload_blob(image_data)

        profile_record = self.get_profile_record(client, client.me.did)
        if not profile_record:
            self.push_status("ERROR", "Could not get profile record")
            return

        update_data = profile_record.copy()
        update_data["banner"] = uploaded_blob.blob

        client.com.atproto.repo.put_record(
            {
                "repo": client.me.did,
                "collection": "app.bsky.actor.profile",
                "rkey": "self",
                "record": update_data,
            }
        )

        updated_profile = client.get_profile(username)
        self.push_status("SUCCESS", f"Banner updated: {updated_profile.banner}")

    def _do_post_text(self, text: str) -> None:
        client, _username = self._login_client()
        self.push_status("POSTING", f"Sending: {text[:80]}")

        response = client.send_post(text)
        self.save_last_post(response.uri, response.cid)

        self.push_status("SUCCESS", f"Posted: {text[:80]}")

    def _do_post_with_image(self, text: str, image_path: str) -> None:
        if not os.path.exists(image_path):
            self.push_status("ERROR", f"Image not found: {image_path}")
            return

        client, _username = self._login_client()
        self.push_status("POSTING", f"Sending with image: {text[:80]}")

        image_data = self.resize_image_if_needed(image_path)
        uploaded_blob = client.upload_blob(image_data)

        embed_data = {
            "$type": "app.bsky.embed.images",
            "images": [{"alt": text, "image": uploaded_blob.blob}],
        }

        response = client.send_post(text, embed=embed_data)
        self.save_last_post(response.uri, response.cid)

        self.push_status("SUCCESS", f"Posted with image: {text[:80]}")

    def _do_post_video(self, video_path: str, text: str) -> None:
        if not os.path.exists(video_path):
            self.push_status("ERROR", f"Video not found: {video_path}")
            return

        client, _username = self._login_client()
        self.push_status("POSTING", f"Uploading video: {os.path.basename(video_path)}")

        video_data = self._read_binary_file(video_path)
        mime_type = self._guess_mime_type(video_path, "video/mp4")
        uploaded_blob = client.upload_blob(video_data)

        embed_data = {
            "$type": "app.bsky.embed.video",
            "video": uploaded_blob.blob,
            "alt": text or os.path.basename(video_path),
        }

        response = client.send_post(text or "", embed=embed_data)
        self.save_last_post(response.uri, response.cid)

        self.push_status("SUCCESS", f"Posted video: {os.path.basename(video_path)}", mime_type=mime_type)

    def _do_post_reply(self, text: str) -> None:
        last_post = self.load_last_post()
        if not last_post:
            self.push_status("ERROR", "No previous post to reply to")
            return

        client, _username = self._login_client()
        self.push_status("POSTING", f"Replying: {text[:80]}")

        reply = {
            "root": {"uri": last_post["uri"], "cid": last_post["cid"]},
            "parent": {"uri": last_post["uri"], "cid": last_post["cid"]},
        }

        response = client.send_post(text, reply_to=reply)
        self.save_last_post(response.uri, response.cid)

        self.push_status("SUCCESS", f"Replied: {text[:80]}")

    def _do_update_all(self, display_name: str, avatar_path: str, banner_path: str) -> None:
        client, _username = self._login_client()
        self.push_status("UPDATING", "Updating all profile fields...")

        profile_record = self.get_profile_record(client, client.me.did)
        if not profile_record:
            self.push_status("ERROR", "Could not get profile record")
            return

        update_data = profile_record.copy()

        if display_name:
            update_data["displayName"] = display_name

        if avatar_path and os.path.exists(avatar_path):
            avatar_data = self.resize_image_if_needed(avatar_path)
            avatar_blob = client.upload_blob(avatar_data)
            update_data["avatar"] = avatar_blob.blob

        if banner_path and os.path.exists(banner_path):
            banner_data = self.resize_image_if_needed(banner_path)
            banner_blob = client.upload_blob(banner_data)
            update_data["banner"] = banner_blob.blob

        client.com.atproto.repo.put_record(
            {
                "repo": client.me.did,
                "collection": "app.bsky.actor.profile",
                "rkey": "self",
                "record": update_data,
            }
        )

        self.push_status("SUCCESS", "All profile fields updated")

    def _do_get_author_posts(self, handle: Optional[str], limit: int) -> None:
        client, username = self._login_client()
        target_handle = (handle or username).strip()
        self.push_status("LOADING", f"Fetching posts for @{target_handle}...")

        author_feed = client.get_author_feed(target_handle, limit=limit)
        rows: List[dict] = []
        for item in getattr(author_feed, "feed", []):
            row = self._feed_item_to_row(item, fallback_type="self_post" if target_handle == username else "friend_post", username=username)
            if row:
                if target_handle != username:
                    row["type"] = "friend_post"
                else:
                    row["type"] = "self_post"
                rows.append(row)

        self.push_status("DATA", f"Loaded {len(rows)} author posts", data_type="author_posts", rows=rows, handle=target_handle)

    def _do_get_timeline(self, limit: int) -> None:
        client, username = self._login_client()
        self.push_status("LOADING", "Fetching timeline...")

        timeline = client.get_timeline(limit=limit)
        rows: List[dict] = []
        for item in getattr(timeline, "feed", []):
            row = self._feed_item_to_row(item, fallback_type="timeline", username=username)
            if row:
                rows.append(row)

        self.push_status("DATA", f"Loaded {len(rows)} timeline posts", data_type="timeline", rows=rows)

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def update_display_name(self, new_display_name: str) -> tuple[bool, str]:
        new_display_name = (new_display_name or "").strip()
        if not new_display_name:
            self.push_status("ERROR", "No display name provided")
            return False, "No display name provided"
        return self._start_thread(self._do_update_display_name, new_display_name)

    def update_profile_picture(self, image_path: str) -> tuple[bool, str]:
        image_path = (image_path or "").strip()
        if not image_path:
            self.push_status("ERROR", "No image path provided")
            return False, "No image path provided"
        return self._start_thread(self._do_update_profile_picture, image_path)

    def update_banner(self, image_path: str) -> tuple[bool, str]:
        image_path = (image_path or "").strip()
        if not image_path:
            self.push_status("ERROR", "No image path provided")
            return False, "No image path provided"
        return self._start_thread(self._do_update_banner, image_path)

    def post_message(self, text: str) -> tuple[bool, str]:
        text = (text or "").strip()
        if not text:
            self.push_status("ERROR", "No text to post")
            return False, "No text to post"
        return self._start_thread(self._do_post_text, text)

    def post_with_image(self, text: str, image_path: str) -> tuple[bool, str]:
        text = (text or "").strip()
        image_path = (image_path or "").strip()
        if not text:
            self.push_status("ERROR", "No text to post")
            return False, "No text to post"
        if not image_path:
            self.push_status("ERROR", "No image path provided")
            return False, "No image path provided"
        return self._start_thread(self._do_post_with_image, text, image_path)

    def post_video(self, video_path: str, text: str = "") -> tuple[bool, str]:
        video_path = (video_path or "").strip()
        text = (text or "").strip()
        if not video_path:
            self.push_status("ERROR", "No video path provided")
            return False, "No video path provided"
        return self._start_thread(self._do_post_video, video_path, text)

    def post_reply(self, text: str) -> tuple[bool, str]:
        text = (text or "").strip()
        if not text:
            self.push_status("ERROR", "No text to post")
            return False, "No text to post"
        return self._start_thread(self._do_post_reply, text)

    def update_all_profile(
        self,
        display_name: str = "",
        avatar_path: str = "",
        banner_path: str = "",
    ) -> tuple[bool, str]:
        if not display_name and not avatar_path and not banner_path:
            self.push_status("ERROR", "Nothing to update")
            return False, "Nothing to update"
        return self._start_thread(self._do_update_all, display_name, avatar_path, banner_path)

    def get_author_posts(self, handle: Optional[str] = None, limit: int = 20) -> tuple[bool, str]:
        return self._start_thread(self._do_get_author_posts, handle, int(limit))

    def get_friend_posts(self, handle: str, limit: int = 20) -> tuple[bool, str]:
        handle = (handle or "").strip()
        if not handle:
            self.push_status("ERROR", "No friend handle provided")
            return False, "No friend handle provided"
        return self._start_thread(self._do_get_author_posts, handle, int(limit))

    def get_timeline(self, limit: int = 20) -> tuple[bool, str]:
        return self._start_thread(self._do_get_timeline, int(limit))

    def _do_get_profile_info(self) -> None:
        client, username = self._login_client()
        self.push_status("LOADING", "Fetching profile info...")
        profile = client.get_profile(username)
        display_name = profile.display_name or ""
        description = profile.description or ""
        self.push_status("DATA", f"Profile: {display_name}", data_type="profile_info",
                         display_name=display_name, description=description)

    def get_profile_info(self) -> tuple[bool, str]:
        """Fetch display name and description in a background thread.
        poll_status() will write to 'displayname' and 'description' tables when done.
        """
        return self._start_thread(self._do_get_profile_info)


__all__ = ["BlueskyUnifiedThreaded"]
