"""
skywave_clear.py

Wipe all SkyWave data tables back to empty (with headers).
Place this as a Text DAT and run it, or paste into a Python shell.

Usage in TD:
    exec(op('skywave_clear').text)
"""

parent_op = op("/skywave")

# Input tables – clear to 1 row, 1 col, empty
table_dats_1x1 = [
    "display_name",
    "description",
    "profile_image_path",
    "banner_image_path",
    "post_image_path",
    "video_path",
    "friend",
]

for name in table_dats_1x1:
    t = parent_op.op(name)
    if t is not None:
        t.clear()
        t.setSize(1, 1)
        t[0, 0] = ""
        print(f"Cleared: {name}")
    else:
        print(f"Not found: {name}")

# Output tables – clear and re-add headers
output_table_headers = {
    "update_status": None,
    "jonny":         ["Type", "Author", "Message", "Time Posted", "Likes", "Reposts", "Author Avatar", "Image URL"],
    "friend_output": ["Type", "Author", "Message", "Time Posted", "Likes", "Reposts", "Author Avatar", "Image URL"],
    "timeline":      ["Type", "Author", "Message", "Time Posted", "Likes", "Reposts", "Author Avatar", "Image URL"],
    "user_stats":    ["Handle", "Display Name", "Followers", "Following", "Posts", "Description", "Avatar"],
    "displayname":   None,
}

for name, headers in output_table_headers.items():
    t = parent_op.op(name)
    if t is None:
        print(f"Not found: {name}")
        continue

    t.clear()

    if name == "update_status":
        t.setSize(2, 1)
        t[0, 0] = "IDLE"
        t[1, 0] = ""
    elif headers:
        t.setSize(1, len(headers))
        for col, header in enumerate(headers):
            t[0, col] = header
    else:
        t.setSize(1, 1)
        t[0, 0] = ""

    print(f"Cleared: {name}")

# Text DATs – clear content
text_dats = [
    "post_text",
]

for name in text_dats:
    t = parent_op.op(name)
    if t is not None:
        t.text = ""
        print(f"Cleared: {name}")
    else:
        print(f"Not found: {name}")

print("\nSkyWave data wiped.")
