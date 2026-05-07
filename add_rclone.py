import re

with open('desktop_app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the bad f-string
content = re.sub(r"result\.get\(\\\'error\\\', \\\'Unknown error\\'\)", r'result.get("error", "Unknown error")', content)