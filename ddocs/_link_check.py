import re
from pathlib import Path

root = Path('ddocs')
md_files = list(root.rglob('*.md'))
link_re = re.compile(r'\[[^\]]*\]\(([^)]+)\)')
issues = []

for md in md_files:
    text = md.read_text(encoding='utf-8', errors='ignore')
    for i, line in enumerate(text.splitlines(), start=1):
        for m in link_re.finditer(line):
            raw = m.group(1).strip()
            if not raw:
                continue
            target = raw.split()[0]
            if target.startswith('<') and target.endswith('>'):
                target = target[1:-1]
            if target.startswith(('http://', 'https://', 'mailto:', '#')):
                continue
            if '://' in target:
                continue
            target = target.split('#', 1)[0].strip()
            if not target:
                continue

            if target.startswith('/'):
                candidate = root / target.lstrip('/')
            else:
                candidate = md.parent / target

            candidate = Path(str(candidate).replace('%20', ' '))
            if not candidate.exists():
                issues.append((md.as_posix(), i, raw, candidate.as_posix()))

print(f'MARKDOWN_FILES={len(md_files)}')
print(f'BROKEN_LINKS={len(issues)}')
for md, line, raw, cand in issues:
    print(f'{md}:{line} -> {raw} (resolved: {cand})')
