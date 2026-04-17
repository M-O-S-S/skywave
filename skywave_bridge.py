"""
skywave_bridge.py

TouchDesigner-side wrapper for skywave_threaded.py

Suggested TD setup:
- Table DAT: update_status   (2 rows, 1 col)
- Table DAT: display_name    (1 row, 1 col)
- Table DAT: description     (1 row, 1 col)   for reading/writing bio
- Table DAT: profile_image_path  (1 row, 1 col)  path for profile picture
- Table DAT: banner_image_path   (1 row, 1 col)  path for banner image
- Table DAT: post_image_path     (1 row, 1 col)  path for image posts
- Table DAT: video_path      (1 row, 1 col)   optional, for video posts
- Table DAT: friend          (1 row, 1 col)   optional, friend handle
- Text DAT:  post_text
- Table DAT: jonny           output for your own posts
- Table DAT: friend_output   output for friend posts
- Table DAT: timeline        output for timeline
- Table DAT: user_stats      output for follower/following/posts counts

Usage:
    mod.skywave_bridge.update_display_name()
    mod.skywave_bridge.update_description()
    mod.skywave_bridge.update_profile_picture()
    mod.skywave_bridge.update_banner()
    mod.skywave_bridge.update_all_profile()
    mod.skywave_bridge.post_message()
    mod.skywave_bridge.post_with_image()
    mod.skywave_bridge.post_video()
    mod.skywave_bridge.post_reply()
    mod.skywave_bridge.populate_jonny_posts()
    mod.skywave_bridge.populate_friend_posts()
    mod.skywave_bridge.populate_timeline()
    mod.skywave_bridge.get_profile_info()
    mod.skywave_bridge.get_user_stats()

Polling:
- Call poll_status() from a Timer CHOP, Execute DAT, or manually after starting an operation.
"""

import os
import sys
import importlib

_client = None

def init_client(babbler_path):
    global _client

    if babbler_path not in sys.path:
        sys.path.insert(0, babbler_path)

    import skywave_threaded
    importlib.reload(skywave_threaded)

    SkyWaveThreaded = skywave_threaded.SkyWaveThreaded
    _client = SkyWaveThreaded(babbler_path=babbler_path)
    return _client
    
def clear_busy():
    """Force-clear a stuck BUSY state. Drains the queue and resets the active thread."""
    _client._active_thread = None
    while not _client._status_queue.empty():
        try:
            _client._status_queue.get_nowait()
        except Exception:
            break
    _set_status("IDLE", "")
    print("Cleared BUSY state.")


def reload_modules():
    """Force reload of the underlying Python modules from disk. Call this after updating .py files."""
    global _client, skywave_threaded
    # Remove cached module so Python re-imports from filesystem
    if "skywave_threaded" in sys.modules:
        del sys.modules["skywave_threaded"]
    # Ensure our path is first so it wins over bluesky-babbler
    if BABBLER_PATH in sys.path:
        sys.path.remove(BABBLER_PATH)
    sys.path.insert(0, BABBLER_PATH)
    import skywave_threaded as _st
    skywave_threaded = _st
    from skywave_threaded import SkyWaveThreaded as _SW
    _client = _SW(babbler_path=BABBLER_PATH)
    print(f"Modules reloaded from: {getattr(skywave_threaded, '__file__', 'unknown')}")


def _safe_cell_text(cell):
    text = str(cell).strip()
    if "type:Cell" in text:
        text = text.split("...")[-1].replace(")", "")
    return text.strip()


def _set_status(status, message):
    try:
        table = op('update_status')
        table.clear()
        table.setSize(2, 1)
        table[0, 0] = status
        table[1, 0] = str(message)[:200]
    except Exception as e:
        print(f"Could not update update_status table: {e}")


def _write_rows_to_table(table_name, rows):
    table = op(table_name)
    table.clear()

    headers = ["Type", "Author", "Message", "Time Posted", "Likes", "Reposts", "Author Avatar", "Image URL"]
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
        table[row_idx, 6] = row.get("author_avatar", "")
        table[row_idx, 7] = row.get("image_url", "")

    avatars = sum(1 for r in rows if r.get("author_avatar"))
    images = sum(1 for r in rows if r.get("image_url"))
    print(f"[skywave] Wrote {len(rows)} rows to {table_name} ({avatars} avatars, {images} images)")
    if rows:
        print(f"[skywave] Sample row keys: {list(rows[0].keys())}")


def _write_stats_to_table(item):
    try:
        table = op("user_stats")
        table.clear()
        table.setSize(2, 7)

        table[0, 0] = "Handle"
        table[0, 1] = "Display Name"
        table[0, 2] = "Followers"
        table[0, 3] = "Following"
        table[0, 4] = "Posts"
        table[0, 5] = "Description"
        table[0, 6] = "Avatar"

        table[1, 0] = item.get("handle", "")
        table[1, 1] = item.get("display_name", "")
        table[1, 2] = item.get("followers_count", "0")
        table[1, 3] = item.get("following_count", "0")
        table[1, 4] = item.get("posts_count", "0")
        table[1, 5] = item.get("description", "")
        table[1, 6] = item.get("avatar", "")
    except Exception as e:
        print(f"Could not write user_stats table: {e}")


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

        elif data_type == "user_stats":
            _write_stats_to_table(item)

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

    if last_item:
        print(f"[skywave] poll_status: {last_item.get('status', '')} - {last_item.get('message', '')}")
    return last_item


# -------------------------------------------------------------------------
# TD-facing wrappers
# -------------------------------------------------------------------------

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


def update_description():
    try:
        table = op("description")
        if table.numRows == 0:
            _set_status("ERROR", "description table empty")
            return False, "No description found"

        new_desc = _safe_cell_text(table[0, 0])
        ok, msg = _client.update_description(new_desc)
        poll_status()
        return ok, msg

    except Exception as e:
        _set_status("ERROR", f"Cannot access description: {e}")
        return False, str(e)


def update_profile_picture():
    try:
        table = op("profile_image_path")
        if table.numRows == 0:
            _set_status("ERROR", "profile_image_path table empty")
            return False, "No image path found"

        image_path = _safe_cell_text(table[0, 0])
        ok, msg = _client.update_profile_picture(image_path)
        poll_status()
        return ok, msg

    except Exception as e:
        _set_status("ERROR", f"Cannot access profile_image_path: {e}")
        return False, str(e)


def update_banner():
    try:
        table = op("banner_image_path")
        if table.numRows == 0:
            _set_status("ERROR", "banner_image_path table empty")
            return False, "No image path found"

        image_path = _safe_cell_text(table[0, 0])
        ok, msg = _client.update_banner(image_path)
        poll_status()
        return ok, msg

    except Exception as e:
        _set_status("ERROR", f"Cannot access banner_image_path: {e}")
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
        table = op("post_image_path")
        if table.numRows == 0:
            _set_status("ERROR", "post_image_path table empty")
            return False, "No image path found"

        image_path = _safe_cell_text(table[0, 0])
        ok, msg = _client.post_with_image(text, image_path)
        poll_status()
        return ok, msg

    except Exception as e:
        _set_status("ERROR", f"Cannot access post_text/post_image_path: {e}")
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

        avatar_table = op("profile_image_path")
        if avatar_table.numRows > 0:
            avatar_path = _safe_cell_text(avatar_table[0, 0])

        banner_table = op("banner_image_path")
        if banner_table.numRows > 0:
            banner_path = _safe_cell_text(banner_table[0, 0])

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


def populate_timeline(limit=100):
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


def get_user_stats(handle=None):
    """Fetch follower/following/post counts.
    If handle is None, fetches for the logged-in user.
    Table 'user_stats' is written when poll_status() is called.
    """
    try:
        ok, msg = _client.get_user_stats(handle)
        poll_status()
        return ok, msg
    except Exception as e:
        _set_status("ERROR", str(e)[:200])
        return False, str(e)


def logout():
    """Clear all in-memory credentials including any loaded from .env."""
    if _client is not None:
        _client.clear_credentials()
    os.environ.pop("BLUESKY_USERNAME", None)
    os.environ.pop("BLUESKY_PASSWORD", None)
    _set_status("IDLE", "Logged out")
    return True, "Logged out"


def login_from_td(babbler_path=None):
    """Read credentials from op('creds')[0,1] and op('creds')[1,1],
    store them on the client for all future operations, and verify
    the login in a background thread. Status appears via poll_status().
    Clear the creds table afterwards to avoid storing credentials.
    Pass babbler_path=project.folder from your TD script for reliable path resolution.
    """
    global _client
    try:
        if _client is None:
            path = babbler_path or project.folder
            init_client(path)
        creds_table = op.creds.op('table')
        username = str(creds_table[0, 1]).strip()
        password = str(creds_table[1, 1]).strip()
        if not username or not password:
            _set_status("ERROR", "creds table missing username or password")
            return False, "Missing credentials"
        _client.set_credentials(username, password)
        ok, msg = _client.verify_login()
        poll_status()
        return ok, msg
    except Exception as e:
        _set_status("ERROR", f"Login failed: {e}")
        return False, str(e)


__all__ = [
    "poll_status",
    "clear_busy",
    "reload_modules",
    "update_display_name",
    "update_description",
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
    "get_user_stats",
    "login_from_td",
    "logout",
]
