key = "MAVERICK"
count = 0

with open("top-gun-maverick_final.txt", "r", encoding="utf-8") as file:
    # Read the entire file content as a single string
    content = file.read()

    # Use standard Python string counting (which matches text editor behaviors)
    count = content.count(key)

print(f"The exact case-sensitive word '{key}' appears {count} times.")
