import re

with open('desktop_app.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Fix the specific lines with backslash issues
for i, line in enumerate(lines):
    if 'result.get(\'error\', \'Unknown error\')' in line:
        lines[i] = line.replace("result.get(\'error\', \'Unknown error\')", 'result.get("error", "Unknown error")')

with open('desktop_app.py', 'w', encoding='utf-8') as f:
    f.writelines(lines)

print('Fixed backslash issues in f-strings')