"""
touchdesigner_bluesky_bridge.py

TouchDesigner-side wrapper for bluesky_unified_threaded_fixed.py

Suggested TD setup:
- Table DAT: update_status   (2 rows, 1 col)
- Table DAT: display_name    (1 row, 1 col)
- Table DAT: image_path      (1 row, 1 col)
- Table DAT: video_path      (1 row, 1 col)   optional, for video posts
- Table DAT: friend          (1 row, 1 col)   optional, friend handle
- Text DAT:  post_text
- Table DAT: jonny           output for your own posts
- Table DAT: friend_output   output for friend posts
- Table DAT: timeline        output for timeline

Usage:
    mod.touchdesigner_bluesky_bridge.update_display_name()
    mod.touchdesigner_bluesky_bridge.post_message()
    mod.touchdesigner_bluesky_bridge.post_video()
    mod.touchdesigner_bluesky_bridge.populate_jonny_posts()
    mod.touchdesigner_bluesky_bridge.populate_friend_posts()
    mod.touchdesigner_bluesky_bridge.populate_timeline()

Polling:
- Call poll_status() from a Timer CHOP, Execute DAT, or manually after starting an operation.
"""

import os
import sys
import importlib

BABBLER_PATH = os.path.dirname(os.path.abspath(__file__))

if BABBLER_PATH not in sys.path:
    sys.path.append(BABBLER_PATH)

import bluesky_unified_threaded_fixed
importlib.reload(bluesky_unified_threaded_fixed)
from bluesky_unified_threaded_fixed import BlueskyUnifiedThreaded

_client = BlueskyUnifiedThreaded(babbler_path=BABBLER_PATH)


def reload_modules():
    """Force reload of the underlying Python modules from disk. Call this after updating .py files."""
    global _client
    importlib.reload(bluesky_unified_threaded_fixed)
    from bluesky_unified_threaded_fixed import BlueskyUnifiedThreaded as _BU
    _client = _BU(babbler_path=BABBLER_PATH)
    print("Modules reloaded.")


def _safe_cell_text(cell):
    text = str(cell).strip()
    if "type:Cell" in text:
        text = text.split("...")[-1].replace(")", "")
    return text.strip()


def _set_status(status, message):
    try:
        table = op("update_status")
        table.clear()
        table.setSize(2, 1)
        table[0, 0] = status
        table[1, 0] = str(message)[:200]
    except Exception as e:
        print(f"Could not update update_status table: {e}")


def _write_rows_to_table(table_name, rows):
    table = op(table_name)
    table.clear()

    headers = ["Type", "Author", "Message", "Time Posted", "Likes", "Reposts"]
    table.setSize(len(rows) + 1, len(headers))

    for col, header in enumerate(headers):
        table[0, col] = header

    for row_idx, row in enumerate(rows, start=1):
        table[row_idx, 0] = row.get("type", "")
        table[row_idx, 1] = row.get("author", "")
        table[row_idx, 2] = row.get("message", "")
        table[row_idx, 3] = row.get("time_posted", "")
        table[row_idx, 4] = row.get("likes", "")
        table[row_idx, 5] = row.get("reposts", "")


def poll_status():
    """
    Pull queued status messages from the pure-Python module.
    If a DATA payload arrives, write it into the correct TouchDesigner table.
    """
    last_item = None
    for item in _client.drain_status():
        last_item = item

        data_type = item.get("data_type")
        rows = item.get("rows")

        if data_type == "profile_info":
            try:
                op('displayname').setSize(1, 1)
                op('displayname')[0, 0] = item.get("display_name", "")
                op('description').setSize(1, 1)
                op('description')[0, 0] = item.get("description", "")
            except Exception as e:
                print(f"Could not write profile tables: {e}")

        elif data_type == "author_posts":
            handle = item.get("handle", "")
            try:
                friend_handle = ""
                if op("friend").numRows > 0:
                    friend_handle = _safe_cell_text(op("friend")[0, 0])
            except Exception:
                friend_handle = ""

            if handle and friend_handle and handle == friend_handle:
                _write_rows_to_table("friend_output", rows or [])
            else:
                _write_rows_to_table("jonny", rows or [])

        elif data_type == "timeline":
            _write_rows_to_table("timeline", rows or [])

        _set_status(item.get("status", "INFO"), item.get("message", ""))

    return last_item


def update_display_name():
    try:
        table = op("display_name")
        if table.numRows == 0:
            _set_status("ERROR", "display_name table empty")
            return False, "No display name found"

        new_name = _safe_cell_text(table[0, 0])
        ok, msg = _client.update_display_name(new_name)
        poll_status()
        return ok, msg

    except Exception as e:
        _set_status("ERROR", f"Cannot access display_name: {e}")
        return False, str(e)


def update_profile_picture():
    try:
        table = op("image_path")
        if table.numRows == 0:
            _set_status("ERROR", "image_path table empty")
            return False, "No image path found"

        image_path = _safe_cell_text(table[0, 0])
        ok, msg = _client.update_profile_picture(image_path)
        poll_status()
        return ok, msg

    except Exception as e:
        _set_status("ERROR", f"Cannot access image_path: {e}")
        return False, str(e)


def update_banner():
    try:
        table = op("image_path")
        if table.numRows == 0:
            _set_status("ERROR", "image_path table empty")
            return False, "No image path found"

        image_path = _safe_cell_text(table[0, 0])
        ok, msg = _client.update_banner(image_path)
        poll_status()
        return ok, msg

    except Exception as e:
        _set_status("ERROR", f"Cannot access image_path: {e}")
        return False, str(e)


def post_message():
    try:
        text = op("post_text").text
        ok, msg = _client.post_message(text)
        poll_status()
        return ok, msg

    except Exception as e:
        _set_status("ERROR", f"Cannot access post_text: {e}")
        return False, str(e)


def post_with_image():
    try:
        text = op("post_text").text
        table = op("image_path")
        if table.numRows == 0:
            _set_status("ERROR", "image_path table empty")
            return False, "No image path found"

        image_path = _safe_cell_text(table[0, 0])
        ok, msg = _client.post_with_image(text, image_path)
        poll_status()
        return ok, msg

    except Exception as e:
        _set_status("ERROR", f"Cannot access post_text/image_path: {e}")
        return False, str(e)


def post_video():
    try:
        text = ""
        try:
            text = op("post_text").text
        except Exception:
            text = ""

        table = op("video_path")
        if table.numRows == 0:
            _set_status("ERROR", "video_path table empty")
            return False, "No video path found"

        video_path = _safe_cell_text(table[0, 0])
        ok, msg = _client.post_video(video_path, text)
        poll_status()
        return ok, msg

    except Exception as e:
        _set_status("ERROR", f"Cannot access video_path/post_text: {e}")
        return False, str(e)


def post_reply():
    try:
        text = op("post_text").text
        ok, msg = _client.post_reply(text)
        poll_status()
        return ok, msg

    except Exception as e:
        _set_status("ERROR", f"Cannot access post_text: {e}")
        return False, str(e)


def update_all_profile():
    try:
        display_name = ""
        avatar_path = ""
        banner_path = ""

        name_table = op("display_name")
        if name_table.numRows > 0:
            display_name = _safe_cell_text(name_table[0, 0])

        candidate_avatar = os.path.join(BABBLER_PATH, "profile.jpg")
        candidate_banner = os.path.join(BABBLER_PATH, "banner.jpg")

        if os.path.exists(candidate_avatar):
            avatar_path = candidate_avatar
        if os.path.exists(candidate_banner):
            banner_path = candidate_banner

        ok, msg = _client.update_all_profile(display_name, avatar_path, banner_path)
        poll_status()
        return ok, msg

    except Exception as e:
        _set_status("ERROR", f"Error accessing tables/files: {e}")
        return False, str(e)


def populate_jonny_posts(limit=20):
    try:
        ok, msg = _client.get_author_posts(limit=int(limit))
        poll_status()
        return ok, msg
    except Exception as e:
        _set_status("ERROR", f"Could not load your posts: {e}")
        return False, str(e)


def populate_friend_posts(limit=20):
    try:
        friend_table = op("friend")
        if friend_table.numRows == 0:
            _set_status("ERROR", "friend table empty")
            return False, "No friend handle found"

        friend_handle = _safe_cell_text(friend_table[0, 0])
        if not friend_handle:
            _set_status("ERROR", "No friend handle found in friend[0,0]")
            return False, "No friend handle found"

        ok, msg = _client.get_friend_posts(friend_handle, limit=int(limit))
        poll_status()
        return ok, msg

    except Exception as e:
        _set_status("ERROR", f"Could not load friend posts: {e}")
        return False, str(e)


def populate_timeline(limit=20):
    try:
        ok, msg = _client.get_timeline(limit=int(limit))
        poll_status()
        return ok, msg
    except Exception as e:
        _set_status("ERROR", f"Could not load timeline: {e}")
        return False, str(e)


def get_profile_info():
    """Start background fetch of display name + description.
    Tables 'displayname' and 'description' are written when poll_status() is called.
    """
    try:
        ok, msg = _client.get_profile_info()
        poll_status()
        return ok, msg
    except Exception as e:
        _set_status("ERROR", str(e)[:200])
        return False, str(e)


__all__ = [
    "poll_status",
    "update_display_name",
    "update_profile_picture",
    "update_banner",
    "update_all_profile",
    "post_message",
    "post_with_image",
    "post_video",
    "post_reply",
    "populate_jonny_posts",
    "populate_friend_posts",
    "populate_timeline",
    "get_profile_info",
]
