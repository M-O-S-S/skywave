"""
skywave_setup.py

Run once in TouchDesigner to create all the ops required by skywave_bridge.
Place this as a Text DAT and run it, or paste into a Python shell.

Usage in TD:
    exec(op('skywave_setup').text)
"""

# Table DATs (1 row, 1 col) – input tables
table_dats_1x1 = [
    "display_name",
    "description",
    "profile_image_path",
    "banner_image_path",
    "post_image_path",
    "video_path",
    "friend",
]

# Table DATs – output tables
output_tables = [
    "update_status",
    "jonny",
    "friend_output",
    "timeline",
    "user_stats",
    "displayname",
]

# Text DATs
text_dats = [
    "post_text",
]

parent_op = op("/skywave")

for name in table_dats_1x1:
    if parent_op.op(name) is None:
        t = parent_op.create(tableDAT, name)
        t.setSize(1, 1)
        t[0, 0] = ""
        print(f"Created Table DAT: {name}")
    else:
        print(f"Already exists: {name}")

for name in output_tables:
    if parent_op.op(name) is None:
        t = parent_op.create(tableDAT, name)
        if name == "update_status":
            t.setSize(2, 1)
            t[0, 0] = "IDLE"
            t[1, 0] = ""
        elif name == "user_stats":
            t.setSize(2, 7)
            for col, header in enumerate(["Handle", "Display Name", "Followers", "Following", "Posts", "Description", "Avatar"]):
                t[0, col] = header
        else:
            t.setSize(1, 8)
            for col, header in enumerate(["Type", "Author", "Message", "Time Posted", "Likes", "Reposts", "Author Avatar", "Image URL"]):
                t[0, col] = header
        print(f"Created Table DAT: {name}")
    else:
        print(f"Already exists: {name}")

for name in text_dats:
    if parent_op.op(name) is None:
        t = parent_op.create(textDAT, name)
        t.text = ""
        print(f"Created Text DAT: {name}")
    else:
        print(f"Already exists: {name}")

print("\nSkyWave setup complete.")
