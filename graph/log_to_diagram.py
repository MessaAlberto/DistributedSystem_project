#!/usr/bin/env python3
"""
Log to Draw.io Diagram Converter - Enhanced Version

Converts run.log output into aesthetic Mermaid sequence diagrams for draw.io.
Usage: python log_to_diagram.py [input_log] [output_file]

To import in draw.io:
1. Open draw.io
2. Go to Arrange -> Insert -> Advanced -> Mermaid
3. Paste the content of the generated .mmd file
"""

import re
import sys
from dataclasses import dataclass
from typing import Optional, List, Dict
from collections import defaultdict

# Color palette for beautiful diagrams
COLORS = {
    'client': '#2E86AB',      # Steel blue
    'node': '#A23B72',        # Magenta
    'manager': '#F18F01',     # Orange
    'success': '#C73E1D',     # Red-orange
    'error': '#E94F37',       # Coral red
    'info': '#3C1642',        # Dark purple
}

# Node colors for topology (gradient-like palette)
NODE_COLORS = [
    ('#E3F2FD', '#1565C0'),  # Light blue -> Blue
    ('#E8F5E9', '#2E7D32'),  # Light green -> Green
    ('#FFF3E0', '#E65100'),  # Light orange -> Orange
    ('#F3E5F5', '#7B1FA2'),  # Light purple -> Purple
    ('#E0F7FA', '#00838F'),  # Light cyan -> Cyan
    ('#FCE4EC', '#C2185B'),  # Light pink -> Pink
    ('#FFFDE7', '#F9A825'),  # Light yellow -> Yellow
    ('#EFEBE9', '#4E342E'),  # Light brown -> Brown
    ('#ECEFF1', '#455A64'),  # Light gray -> Gray
    ('#E8EAF6', '#283593'),  # Light indigo -> Indigo
]

@dataclass
class LogEvent:
    line_num: int
    source: str
    action: str
    target: Optional[str]
    details: str
    event_type: str = "info"  # write, read, notify, error, system, phase

def parse_log_line(line: str, line_num: int) -> Optional[LogEvent]:
    """Parse a single log line and extract relevant information."""

    match = re.match(r'\[([^\]]+)\]\s+(.+)', line.strip())
    if not match:
        return None

    source = match.group(1).strip()
    message = match.group(2).strip()

    # Detect test phase headers (e.g., "Starting single-client basic test")
    phase_match = re.search(r'Starting\s+(.+?)\s+test', message, re.IGNORECASE)
    if phase_match:
        phase_name = phase_match.group(1).strip()
        return LogEvent(line_num, source, "PHASE", None, phase_name, "phase")

    # Skip non-interesting messages
    skip_patterns = [
        r'^===', r'^-+$', r'^\[AUTO\]', r'^Configuration:', r'^Active nodes:',
        r'^Crashed nodes:', r'^Client list:', r'^Clients:', r'^Summary:',
        r'^All nodes initialized', r'^Creating multiple clients', r'^Iniitalizing',
        r'^Current system status', r'^Testing', r'^Priming', r'^Crashing nodes',
        r'^Attempting', r'^Performing', r'^Recovering nodes', r'^Adding new node',
        r'^Storing initial', r'^Crashing a node', r'^Continuing operations',
        r'^Recovering the crashed', r'^Testing data consistency', r'^Adding more data',
        r'^All automated', r'^Key:', r'^\(empty\)', r'^PRINTING STORE',
        r'^COMPLETED PRINTING', r'^Press ENTER', r'^While', r'^Node\d+ crashing',
        r'^Node\d+ reovering', r'^Simulating', r'^Join node', r'^Removing node',
        r'^Adding new node', r'^Crashing node', r'^Recovering node',
    ]

    for pattern in skip_patterns:
        if re.search(pattern, message):
            return None

    target = None
    action = ""
    details = message
    event_type = "info"

    # Client sending request
    if "Sending WriteRequest" in message:
        m = re.search(r'Sending WriteRequest key=(\d+),\s*value="([^"]+)"\s+to\s+(\w+)', message)
        if m:
            target = m.group(3)
            action = "WRITE"
            details = f"key={m.group(1)}"
            event_type = "write"

    elif "Sending ReadRequest" in message:
        m = re.search(r'Sending ReadRequest key=(\d+)\s+to\s+(\w+)', message)
        if m:
            target = m.group(2)
            action = "READ"
            details = f"key={m.group(1)}"
            event_type = "read"

    elif "Received WriteRequest" in message:
        action = "RecvWrite"
        event_type = "write"
        m = re.search(r'key=(\d+)', message)
        if m:
            details = f"key={m.group(1)}"

    elif "Received ReadRequest" in message:
        action = "RecvRead"
        event_type = "read"
        m = re.search(r'key=(\d+)', message)
        if m:
            details = f"key={m.group(1)}"

    elif "Notifying node" in message:
        m = re.search(r'Notifying node (\d+) for (\w+):\s*key=(\d+)', message)
        if m:
            target = f"Node{m.group(1)}"
            action = "GetVersion"
            details = f"key={m.group(3)}"
            event_type = "notify"

    elif "Replied GetVersion" in message:
        m = re.search(r'Replied (GetVersion\w+) for key=(\d+) v=(\d+)', message)
        if m:
            action = "Version"
            details = f"v={m.group(3)}"
            event_type = "notify"

    elif "Stored key=" in message:
        action = "STORED"
        event_type = "write"
        m = re.search(r'Stored key=(\d+) version=(\d+)', message)
        if m:
            details = f"key={m.group(1)} v={m.group(2)}"

    elif "Received WriteResponse" in message:
        action = "WriteOK"
        event_type = "write"
        m = re.search(r'key=(\d+).*version=(\d+)', message)
        if m:
            details = f"key={m.group(1)} v={m.group(2)}"

    elif "Received ReadResponse" in message:
        action = "ReadOK"
        event_type = "read"
        m = re.search(r'key=(\d+)\s+value="([^"]+)"', message)
        if m:
            details = f"key={m.group(1)} = '{m.group(2)[:15]}'"

    elif "WriteTimeout" in message or "ReadTimeout" in message:
        action = "TIMEOUT"
        event_type = "error"
        m = re.search(r'key\s*(\d+)', message)
        details = f"key={m.group(1)}" if m else ""

    elif "Crashing node" in message:
        action = "CRASH"
        event_type = "system"
        details = ""

    elif "RECOVERY completed" in message:
        action = "RECOVERED"
        event_type = "system"
        details = ""

    elif "JOIN completed" in message and "maintenance" in message:
        action = "JOINED"
        event_type = "system"
        details = ""

    elif "Sending response to manager" in message:
        target = "TestManager"
        m = re.search(r'(WRITE|READ)\s+(\d+).*success=(true|false)', message)
        if m:
            action = f"{m.group(1)}"
            status = "OK" if m.group(3) == "true" else "FAIL"
            details = f"key={m.group(2)} [{status}]"
            event_type = "write" if m.group(1) == "WRITE" else "read"

    elif "Rejected" in message:
        action = "REJECTED"
        event_type = "error"

    elif "Ignored" in message:
        return None  # Skip ignored messages for cleaner diagrams

    elif "Computed new version" in message:
        action = "NewVersion"
        event_type = "write"
        m = re.search(r'version\s*(\d+)', message)
        details = f"v={m.group(1)}" if m else ""

    elif "Returning value" in message:
        action = "ReturnVal"
        event_type = "read"

    else:
        return None  # Skip unrecognized messages

    return LogEvent(line_num, source, action, target, details, event_type)


def generate_mermaid_diagram(events: List[LogEvent], title: str = "", max_events: int = 100) -> str:
    """Generate an aesthetic Mermaid sequence diagram."""

    # Collect participants (exclude phases from participant collection)
    participants = set()
    for event in events:
        if event.event_type == "phase":
            continue
        source = event.source.replace(" ", "")
        participants.add(source)
        if event.target:
            participants.add(event.target.replace(" ", ""))

    # Sort: clients, nodes by ID, manager
    def sort_key(p):
        if p.startswith("client"):
            return (0, int(p.replace("client", "")))
        elif p.startswith("Node"):
            try:
                return (1, int(p.replace("Node", "")))
            except:
                return (1, 999)
        elif p == "TestManager":
            return (2, 0)
        return (3, 0)

    sorted_participants = sorted(participants, key=sort_key)

    # Build diagram with theme
    lines = [
        "%%{init: {'theme': 'base', 'themeVariables': {",
        "  'primaryColor': '#E3F2FD',",
        "  'primaryTextColor': '#1565C0',",
        "  'primaryBorderColor': '#1565C0',",
        "  'lineColor': '#5C6BC0',",
        "  'secondaryColor': '#F3E5F5',",
        "  'tertiaryColor': '#E8F5E9',",
        "  'noteBkgColor': '#FFF9C4',",
        "  'noteTextColor': '#333',",
        "  'noteBorderColor': '#FBC02D',",
        "  'actorBkg': '#E3F2FD',",
        "  'actorBorder': '#1565C0',",
        "  'actorTextColor': '#1565C0',",
        "  'signalColor': '#5C6BC0',",
        "  'signalTextColor': '#333'",
        "}}}%%",
        "sequenceDiagram",
        "    autonumber"
    ]

    # Add participants with custom styling
    for p in sorted_participants:
        if p.startswith("client"):
            lines.append(f"    participant {p} as {p}")
        elif p.startswith("Node"):
            lines.append(f"    participant {p} as {p}")
        elif p == "TestManager":
            lines.append(f"    participant {p} as Manager")

    lines.append("")

    # Phase colors for visual distinction
    phase_colors = [
        "rgba(173, 216, 230, 0.3)",  # Light blue
        "rgba(144, 238, 144, 0.3)",  # Light green
        "rgba(255, 218, 185, 0.3)",  # Peach
        "rgba(221, 160, 221, 0.3)",  # Plum
        "rgba(176, 224, 230, 0.3)",  # Powder blue
        "rgba(255, 182, 193, 0.3)",  # Light pink
        "rgba(255, 255, 224, 0.3)",  # Light yellow
        "rgba(211, 211, 211, 0.3)",  # Light gray
    ]

    # Track phases for grouping
    event_count = 0
    phase_count = 0
    in_rect = False
    first_participant = sorted_participants[0] if sorted_participants else "client0"
    last_participant = sorted_participants[-1] if sorted_participants else "TestManager"

    for event in events:
        if event_count >= max_events:
            if in_rect:
                lines.append("    end")
            lines.append(f"    Note over {first_participant},{last_participant}: ... {len(events) - max_events} more events ...")
            break

        # Handle phase separators
        if event.event_type == "phase":
            if in_rect:
                lines.append("    end")
            phase_color = phase_colors[phase_count % len(phase_colors)]
            phase_name = event.details.upper()
            lines.append(f"    rect {phase_color}")
            lines.append(f"    Note over {first_participant},{last_participant}: {phase_name} TEST")
            in_rect = True
            phase_count += 1
            continue

        source = event.source.replace(" ", "")

        if event.target:
            target = event.target.replace(" ", "")

            # Choose arrow style based on event type
            if event.event_type == "write":
                arrow = "->>"
            elif event.event_type == "read":
                arrow = "->>"
            elif event.event_type == "error":
                arrow = "--x"
            elif event.event_type == "notify":
                arrow = "-->>"
            else:
                arrow = "->>"

            # Format message
            msg = f"{event.action}"
            if event.details:
                details = event.details[:30] + "..." if len(event.details) > 30 else event.details
                msg += f" ({details})"

            lines.append(f"    {source}{arrow}{target}: {msg}")
            event_count += 1

        elif event.action in ["TIMEOUT", "CRASH", "RECOVERED", "JOINED"]:
            # System events as colored notes
            if event.action == "TIMEOUT":
                note_style = "TIMEOUT"
            elif event.action == "CRASH":
                note_style = "CRASH"
            elif event.action == "RECOVERED":
                note_style = "RECOVERED"
            elif event.action == "JOINED":
                note_style = "JOINED"
            else:
                note_style = event.action

            lines.append(f"    Note over {source}: {note_style}")
            event_count += 1

    # Close any open rect
    if in_rect:
        lines.append("    end")

    return "\n".join(lines)


def generate_filtered_diagram(events: List[LogEvent],
                              include_notifications: bool = False,
                              max_events: int = 100) -> str:
    """Generate a filtered diagram with improved aesthetics."""

    filtered_events = []
    for event in events:
        # Always include phase separators
        if event.event_type == "phase":
            filtered_events.append(event)
        # Always include client requests
        elif event.source.startswith("client") and event.target:
            filtered_events.append(event)
        # Include responses to TestManager
        elif event.target == "TestManager":
            filtered_events.append(event)
        # Optionally include notifications between nodes
        elif include_notifications and event.event_type == "notify" and event.target:
            filtered_events.append(event)
        # Include system events
        elif event.action in ["CRASH", "RECOVERED", "TIMEOUT", "JOINED"]:
            filtered_events.append(event)

    return generate_mermaid_diagram(filtered_events, max_events=max_events)


def create_topology_csv(output_file: str, node_ids: List[int] = None):
    """Create an aesthetic CSV for draw.io topology diagram."""

    if node_ids is None:
        node_ids = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

    # Header with improved styling
    csv_lines = [
        "# label: %name%",
        "# style: shape=%shape%;fillColor=%fill%;strokeColor=%stroke%;fontColor=%fontcolor%;fontSize=14;fontStyle=1;shadow=1;rounded=1;arcSize=20;",
        "# namespace: csvimport-",
        '# connect: {"from": "connects", "to": "name", "style": "curved=1;endArrow=classic;endFill=1;strokeWidth=2;strokeColor=#5C6BC0;"}',
        "# width: 80",
        "# height: 80",
        "# padding: 20",
        "# ignore: shape,fill,stroke,fontcolor,connects",
        "# nodespacing: 60",
        "# levelspacing: 80",
        "# edgespacing: 40",
        "# layout: circle",
        "name,shape,fill,stroke,fontcolor,connects"
    ]

    # Add nodes with gradient colors
    for i, node_id in enumerate(sorted(node_ids)):
        color_idx = i % len(NODE_COLORS)
        fill, stroke = NODE_COLORS[color_idx]

        # Connect to next and previous in ring
        sorted_ids = sorted(node_ids)
        idx = sorted_ids.index(node_id)
        next_id = sorted_ids[(idx + 1) % len(sorted_ids)]
        prev_id = sorted_ids[(idx - 1) % len(sorted_ids)]

        connects = f"Node{next_id}"
        csv_lines.append(f"Node{node_id},ellipse,{fill},{stroke},{stroke},\"{connects}\"")

    # Add clients with different style
    client_colors = [
        ('#E8F5E9', '#2E7D32'),  # Green
        ('#FFF3E0', '#E65100'),  # Orange
        ('#F3E5F5', '#7B1FA2'),  # Purple
    ]

    for i in range(3):
        fill, stroke = client_colors[i % len(client_colors)]
        csv_lines.append(f"Client{i},rectangle,{fill},{stroke},{stroke},\"\"")

    # Add TestManager
    csv_lines.append(f"TestManager,hexagon,#FFECB3,#FF8F00,#FF8F00,\"\"")

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("\n".join(csv_lines))
    print(f"Topology CSV written to: {output_file}")


def create_legend_csv(output_file: str):
    """Create a legend explaining the diagram symbols."""

    csv_content = """# label: %name%
# style: shape=%shape%;fillColor=%fill%;strokeColor=%stroke%;fontColor=%fontcolor%;fontSize=12;fontStyle=1;
# namespace: legend-
# width: 120
# height: 40
# padding: 10
# ignore: shape,fill,stroke,fontcolor
# layout: verticalflow
name,shape,fill,stroke,fontcolor
🖥️ Node (Storage),rectangle,#E3F2FD,#1565C0,#1565C0
👤 Client,rectangle,#E8F5E9,#2E7D32,#2E7D32
📊 TestManager,rectangle,#FFECB3,#FF8F00,#FF8F00
🔵 Write Operation,rectangle,#BBDEFB,#1976D2,#1976D2
🟢 Read Operation,rectangle,#C8E6C9,#388E3C,#388E3C
🔴 Error/Timeout,rectangle,#FFCDD2,#D32F2F,#D32F2F
💥 Node Crash,rectangle,#FFCCBC,#E64A19,#E64A19
✅ Node Recovery,rectangle,#C8E6C9,#388E3C,#388E3C
"""
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(csv_content)
    print(f"Legend CSV written to: {output_file}")


def extract_node_ids_from_log(events: List[LogEvent]) -> List[int]:
    """Extract unique node IDs from log events."""
    node_ids = set()
    for event in events:
        # Extract from source
        if event.source.startswith("Node"):
            m = re.search(r'Node\s*(\d+)', event.source)
            if m:
                node_ids.add(int(m.group(1)))
        # Extract from target
        if event.target and event.target.startswith("Node"):
            m = re.search(r'Node(\d+)', event.target)
            if m:
                node_ids.add(int(m.group(1)))
    return sorted(node_ids) if node_ids else [0, 10, 20, 30, 40]


def main():
    input_file = sys.argv[1] if len(sys.argv) > 1 else "../run.log"
    output_file = sys.argv[2] if len(sys.argv) > 2 else "diagram.mmd"

    print(f"[*] Reading log from: {input_file}")

    events = []
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                stripped = line.strip()
                if not stripped:
                    continue

                cleaned = re.sub(r'^\s*\d+[→\t]\s*', '', stripped)
                cleaned = re.sub(r'^\d{2}:\d{2}:\d{2}\.\d{3}\s+\[[^\]]+\]\s+\w+\s+[^\-]+\s+-\s+', '', cleaned)

                event = parse_log_line(cleaned, line_num)
                if event:
                    events.append(event)
    except FileNotFoundError:
        print(f"[ERROR] File '{input_file}' not found")
        sys.exit(1)

    print(f"[OK] Parsed {len(events)} events")

    # Extract node IDs for topology
    node_ids = extract_node_ids_from_log(events)
    print(f"[*] Found nodes: {node_ids}")

    # Generate diagrams
    simple_diagram = generate_filtered_diagram(events, include_notifications=False, max_events=150)
    detailed_diagram = generate_filtered_diagram(events, include_notifications=True, max_events=200)

    # Write output files
    simple_output = output_file.replace('.mmd', '_simple.mmd')
    detailed_output = output_file.replace('.mmd', '_detailed.mmd')
    topology_output = output_file.replace('.mmd', '_topology.csv')
    legend_output = output_file.replace('.mmd', '_legend.csv')

    with open(simple_output, 'w', encoding='utf-8') as f:
        f.write(simple_diagram)
    print(f"[+] Simple diagram: {simple_output}")

    with open(detailed_output, 'w', encoding='utf-8') as f:
        f.write(detailed_diagram)
    print(f"[+] Detailed diagram: {detailed_output}")

    create_topology_csv(topology_output, node_ids)
    create_legend_csv(legend_output)

    print("\n" + "="*60)
    print("HOW TO IMPORT IN DRAW.IO:")
    print("="*60)
    print("\nFor Mermaid diagrams (.mmd):")
    print("   1. Open draw.io (https://app.diagrams.net)")
    print("   2. Arrange -> Insert -> Advanced -> Mermaid")
    print("   3. Paste content of .mmd file")
    print("\nFor Topology/Legend (.csv):")
    print("   1. Arrange -> Insert -> Advanced -> CSV")
    print("   2. Paste content of .csv file")
    print("="*60)


if __name__ == "__main__":
    main()
