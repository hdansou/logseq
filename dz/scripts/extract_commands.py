#!/usr/bin/env python3
"""
Extract all Logseq command palette commands and generate a comprehensive table.
This script parses the shortcuts config and translation files to create a complete command reference.
"""

import re
import json
from pathlib import Path
from collections import defaultdict

# File paths
SHORTCUTS_FILE = Path("src/main/frontend/modules/shortcut/config.cljs")
TRANSLATIONS_FILE = Path("src/resources/dicts/en.edn")

def parse_shortcuts():
    """Parse all shortcuts from config.cljs"""
    content = SHORTCUTS_FILE.read_text()
    
    # Find the all-built-in-keyboard-shortcuts map
    # Use a more greedy pattern to capture the entire map
    pattern = r'all-built-in-keyboard-shortcuts\s*\n\s*\{(.*?)(?:\n\(def\s|\n\(defonce\s)'
    shortcuts_match = re.search(pattern, content, re.DOTALL)
    if not shortcuts_match:
        print("ERROR: Could not find shortcuts map")
        return {}
    
    shortcuts_section = shortcuts_match.group(1)
    
    # Parse each command entry using a multi-line pattern
    # Pattern matches: :command/id {:binding ... :fn ...}
    # We need to capture the entire command block
    
    commands = {}
    
    # More robust pattern: match command ID and capture everything until next command or end
    entries = re.finditer(
        r':([a-z0-9/-]+)\s+\{:binding\s+([^\n]+)',
        shortcuts_section
    )
    
    for match in entries:
        cmd_id = match.group(1)
        binding_line = match.group(2)
        
        # Extract just the binding value (everything before :fn or end of line)
        binding_str = re.sub(r'\s*:fn.*$', '', binding_line).strip()
        
        commands[cmd_id] = binding_str
    
    return commands

def parse_categories():
    """Parse category mappings from config.cljs"""
    content = SHORTCUTS_FILE.read_text()
    
    # Find the *category atom
    cat_match = re.search(r'\*category\s*\(atom\s*\{(.*?)\}\)\)', content, re.DOTALL)
    if not cat_match:
        print("ERROR: Could not find category atom")
        return {}
    
    cat_section = cat_match.group(1)
    
    categories = {}
    cmd_to_category = {}
    
    # Parse each category
    cat_entries = re.findall(r':shortcut\.category/([a-z-]+)\s*\[(.*?)\]', cat_section, re.DOTALL)
    
    for cat_name, commands_str in cat_entries:
        # Extract command IDs
        cmd_ids = re.findall(r':([a-z0-9/-]+)', commands_str)
        categories[cat_name] = cmd_ids
        
        # Build reverse mapping
        for cmd in cmd_ids:
            cmd_to_category[cmd] = cat_name
    
    return cmd_to_category

def parse_translations():
    """Parse English translations from en.edn"""
    content = TRANSLATIONS_FILE.read_text()
    
    translations = {}
    
    # Find all command.* translations - use more flexible pattern
    # Handle both :command.* and other formats
    matches = re.findall(r':(?:command\.)?([a-z0-9/-]+)\s+"([^"]+)"', content)
    
    for cmd_id, description in matches:
        # Only keep entries that look like commands (contain /)
        if '/' in cmd_id or cmd_id.startswith(('editor', 'go', 'ui', 'command', 'graph', 'pdf', 'cards', 'auto-complete', 'sidebar', 'misc', 'dev', 'search', 'publish', 'window')):
            translations[cmd_id] = description
    
    return translations

def clean_binding(binding_str):
    """Clean and format keyboard binding strings"""
    if not binding_str:
        return "—"
    
    # Remove outer parentheses and quotes
    binding_str = binding_str.strip('"[]{}()')
    
    # Handle false (no binding)
    if binding_str == 'false' or binding_str == '' or not binding_str.strip():
        return "—"
    
    # Handle conditional bindings (if mac? ...)
    if 'if mac?' in binding_str:
        # Pattern: (if mac? MAC_VALUE OTHER_VALUE)
        # Extract values carefully
        match = re.search(r'if mac\?\s+(.*?)\s+([^\)]+)\)?$', binding_str, re.DOTALL)
        if match:
            mac_part = match.group(1).strip()
            other_part = match.group(2).strip()
            
            # Clean up each part
            mac_binding = mac_part.strip('"[]{}()').strip()
            other_binding = other_part.strip('"[]{}()').strip()
            
            # Handle false values
            if mac_binding == 'false' or not mac_binding:
                if other_binding and other_binding != 'false':
                    return f"`{other_binding}`"
                return "—"
            elif other_binding == 'false' or not other_binding:
                if mac_binding and mac_binding != 'false':
                    return f"`{mac_binding}`"
                return "—"
            else:
                # Both have values - show platform-specific
                # Handle array notation [...]
                if '[' in mac_binding:
                    mac_binding = re.sub(r'[\[\]]', '', mac_binding).replace('"', '').strip()
                if '[' in other_binding:
                    other_binding = re.sub(r'[\[\]]', '', other_binding).replace('"', '').strip()
                    
                return f"Mac: `{mac_binding}` / Win/Linux: `{other_binding}`"
    
    # Handle array notation for multiple bindings
    if '[' in binding_str or '"' in binding_str:
        # Extract all quoted strings
        bindings = re.findall(r'"([^"]+)"', binding_str)
        if bindings:
            if len(bindings) == 1:
                return f"`{bindings[0]}`"
            else:
                return " or ".join(f"`{b}`" for b in bindings)
    
    # Clean final formatting
    binding_str = binding_str.strip('"[]{}()').strip()
    
    if binding_str and binding_str != 'false':
        return f"`{binding_str}`"
    
    return "—"

def format_category(cat_name):
    """Format category name for display"""
    if not cat_name:
        return "Others"
    return cat_name.replace('-', ' ').title()

def generate_table():
    """Generate the complete command reference table"""
    print("Parsing shortcuts...")
    shortcuts = parse_shortcuts()
    
    print("Parsing categories...")
    cmd_to_category = parse_categories()
    
    print("Parsing translations...")
    translations = parse_translations()
    
    print(f"\nFound {len(shortcuts)} shortcuts")
    print(f"Found {len(cmd_to_category)} categorized commands")
    print(f"Found {len(translations)} translations")
    
    # Combine all data
    all_commands = []
    
    for cmd_id, binding in shortcuts.items():
        category = cmd_to_category.get(cmd_id, "others")
        description = translations.get(cmd_id, cmd_id.replace('/', ' ').replace('-', ' ').title())
        
        all_commands.append({
            'id': cmd_id,
            'binding': binding,
            'description': description,
            'category': category
        })
    
    # Sort by category, then by command ID
    all_commands.sort(key=lambda x: (x['category'], x['id']))
    
    # Generate markdown table
    output_lines = []
    output_lines.append("# Logseq Command Palette - Complete Command Reference")
    output_lines.append("")
    output_lines.append("**Generated automatically from source code**")
    output_lines.append("")
    output_lines.append("This document lists all built-in commands available in Logseq's command palette (`Cmd/Ctrl+Shift+P`).")
    output_lines.append("")
    output_lines.append("**Legend:**")
    output_lines.append("- `mod` = `Cmd` on Mac / `Ctrl` on Windows/Linux")
    output_lines.append("- `—` = No keyboard shortcut assigned")
    output_lines.append("- Platform-specific shortcuts are indicated where different")
    output_lines.append("")
    output_lines.append(f"**Total Commands:** {len(all_commands)}")
    output_lines.append("")
    output_lines.append("---")
    output_lines.append("")
    output_lines.append("## All Commands")
    output_lines.append("")
    output_lines.append("| Command ID | Keyboard Shortcut | Description | Category |")
    output_lines.append("|------------|-------------------|-------------|----------|")
    
    for cmd in all_commands:
        cmd_id = f":{cmd['id']}"
        binding = clean_binding(cmd['binding'])
        description = cmd['description']
        category = format_category(cmd['category'])
        
        output_lines.append(f"| `{cmd_id}` | {binding} | {description} | {category} |")
    
    output_lines.append("")
    output_lines.append("---")
    output_lines.append("")
    output_lines.append("## Summary by Category")
    output_lines.append("")
    
    by_category = defaultdict(list)
    for cmd in all_commands:
        by_category[cmd['category']].append(cmd)
    
    for cat in sorted(by_category.keys()):
        cmds = by_category[cat]
        output_lines.append(f"### {format_category(cat)} ({len(cmds)} commands)")
        output_lines.append("")
        for cmd in cmds:
            binding = clean_binding(cmd['binding'])
            # Only show binding if it exists and is not just backticks
            binding_display = f" - {binding}" if binding != '—' else ''
            output_lines.append(f"- **`:{cmd['id']}`** {binding_display} - {cmd['description']}")
        output_lines.append("")
    
    # Print to console
    for line in output_lines:
        print(line)
    
    # Save to file
    output_file = Path("dz/docs/command-palette-reference.md")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text('\n'.join(output_lines))
    print(f"\n✓ Output saved to: {output_file}")

if __name__ == "__main__":
    generate_table()
