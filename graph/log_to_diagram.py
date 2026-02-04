#!/usr/bin/env python3
"""
Log to Draw.io Diagram Converter

Converts run.log output into a Mermaid sequence diagram that can be imported into draw.io.
Usage: python log_to_diagram.py [input_log] [output_file]

To import in draw.io:
1. Open draw.io
2. Go to Arrange -> Insert -> Advanced -> Mermaid
3. Paste the content of the generated .mmd file
"""

import re
import sys
from dataclasses import dataclass
from typing import Optional
from collections import defaultdict

@dataclass
class LogEvent:
    line_num: int
    source: str
    action: str
    target: Optional[str]
    details: str

def parse_log_line(line: str, line_num: int) -> Optional[LogEvent]:
    """Parse a single log line and extract relevant information."""

    # Pattern for [Component] Message
    match = re.match(r'\[([^\]]+)\]\s+(.+)', line.strip())
    if not match:
        return None

    source = match.group(1).strip()
    message = match.group(2).strip()

    # Skip non-interesting messages
    skip_patterns = [
        r'^===',
        r'^-+$',
        r'^\[AUTO\]',
        r'^Configuration:',
        r'^Active nodes:',
        r'^Crashed nodes:',
        r'^Client list:',
        r'^Clients:',
        r'^Summary:',
        r'^All nodes initialized',
        r'^Creating multiple clients',
        r'^Iniitalizing',
        r'^Current system status',
        r'^Testing',
        r'^Priming',
        r'^Crashing nodes',
        r'^Attempting',
        r'^Performing',
        r'^Recovering nodes',
        r'^Adding new node',
        r'^Storing initial',
        r'^Crashing a node',
        r'^Continuing operations',
        r'^Recovering the crashed',
        r'^Testing data consistency',
        r'^Adding more data',
        r'^All automated',
        r'^Key:',
        r'^\(empty\)',
        r'^PRINTING STORE',
        r'^COMPLETED PRINTING',
    ]

    for pattern in skip_patterns:
        if re.search(pattern, message):
            return None

    # Parse specific message types
    target = None
    action = ""
    details = message

    # Client sending request
    if "Sending WriteRequest" in message:
        m = re.search(r'Sending WriteRequest key=(\d+),\s*value="([^"]+)"\s+to\s+(\w+)', message)
        if m:
            target = m.group(3)
            action = "WriteRequest"
            details = f"key={m.group(1)} value=\"{m.group(2)}\""

    elif "Sending ReadRequest" in message:
        m = re.search(r'Sending ReadRequest key=(\d+)\s+to\s+(\w+)', message)
        if m:
            target = m.group(2)
            action = "ReadRequest"
            details = f"key={m.group(1)}"

    # Node received request
    elif "Received WriteRequest" in message:
        action = "ReceivedWrite"
        m = re.search(r'key=(\d+)\s+value="([^"]+)"', message)
        if m:
            details = f"key={m.group(1)} value=\"{m.group(2)}\""

    elif "Received ReadRequest" in message:
        action = "ReceivedRead"
        m = re.search(r'key=(\d+)', message)
        if m:
            details = f"key={m.group(1)}"

    # Node notifying another node
    elif "Notifying node" in message:
        m = re.search(r'Notifying node (\d+) for (\w+):\s*(.+)', message)
        if m:
            target = f"Node{m.group(1)}"
            action = f"Notify{m.group(2)}"
            details = m.group(3)

    # Version responses
    elif "GetVersionWrite responses" in message or "GetVersionRead responses" in message:
        action = "VersionResponses"
        details = message

    elif "Replied GetVersion" in message:
        m = re.search(r'Replied (GetVersion\w+) for key=(\d+) v=(\d+)', message)
        if m:
            action = "VersionReply"
            details = f"{m.group(1)} key={m.group(2)} v={m.group(3)}"

    # Stored data
    elif "Stored key=" in message:
        action = "Store"
        m = re.search(r'Stored key=(\d+) version=(\d+) value="([^"]+)"', message)
        if m:
            details = f"key={m.group(1)} v={m.group(2)} value=\"{m.group(3)}\""

    # Write/Read response
    elif "Received WriteResponse" in message:
        action = "WriteResponse"
        m = re.search(r'key=(\d+)\s+value="([^"]+)"\s+version=(\d+)', message)
        if m:
            details = f"key={m.group(1)} v={m.group(3)}"

    elif "Received ReadResponse" in message:
        action = "ReadResponse"
        m = re.search(r'key=(\d+)\s+value="([^"]+)"\s+version=(\d+)', message)
        if m:
            details = f"key={m.group(1)} value=\"{m.group(2)}\" v={m.group(3)}"

    # Timeout
    elif "WriteTimeout" in message or "ReadTimeout" in message:
        action = "Timeout"
        details = message

    # Crash/Recovery
    elif "Crashing node" in message:
        action = "Crash"
        details = "Node crashed"

    elif "RECOVERY completed" in message:
        action = "Recovered"
        details = "Node recovered"

    # Sending response to manager
    elif "Sending response to manager" in message:
        target = "TestManager"
        action = "Response"
        m = re.search(r'manager:\s+(.+)', message)
        if m:
            details = m.group(1)

    # Rejected
    elif "Rejected" in message:
        action = "Rejected"

    # Ignored
    elif "Ignored" in message:
        action = "Ignored"

    # Computed version
    elif "Computed new version" in message:
        action = "ComputeVersion"

    # Returning value
    elif "Returning value" in message:
        action = "ReturnValue"

    else:
        action = "Info"

    return LogEvent(line_num, source, action, target, details)

def generate_mermaid_diagram(events: list[LogEvent], max_events: int = 100) -> str:
    """Generate a Mermaid sequence diagram from parsed events."""

    # Collect all participants
    participants = set()
    for event in events:
        # Normalize participant names
        source = event.source.replace(" ", "")
        participants.add(source)
        if event.target:
            participants.add(event.target.replace(" ", ""))

    # Sort participants: clients first, then nodes by ID, then others
    def sort_key(p):
        if p.startswith("client"):
            return (0, p)
        elif p.startswith("Node"):
            try:
                return (1, int(p.replace("Node", "")))
            except:
                return (1, 999)
        elif p == "TestManager":
            return (2, p)
        else:
            return (3, p)

    sorted_participants = sorted(participants, key=sort_key)

    # Build diagram
    lines = ["sequenceDiagram"]
    lines.append("    autonumber")

    # Add participants
    for p in sorted_participants:
        if p.startswith("client"):
            lines.append(f"    participant {p} as {p}")
        elif p.startswith("Node"):
            lines.append(f"    participant {p} as {p}")
        else:
            lines.append(f"    participant {p} as {p}")

    lines.append("")

    # Add events (limited to max_events for readability)
    event_count = 0
    for event in events:
        if event_count >= max_events:
            lines.append(f"    Note over {sorted_participants[0]}: ... ({len(events) - max_events} more events)")
            break

        source = event.source.replace(" ", "")

        # Only include events with arrows (source -> target)
        if event.target:
            target = event.target.replace(" ", "")

            # Determine arrow type based on action
            if "Request" in event.action:
                arrow = "->>"
            elif "Response" in event.action or "Reply" in event.action:
                arrow = "-->>"
            elif "Notify" in event.action:
                arrow = "->>"
            else:
                arrow = "->>"

            # Truncate long details
            details = event.details[:50] + "..." if len(event.details) > 50 else event.details
            details = details.replace('"', "'")

            lines.append(f"    {source}{arrow}{target}: {event.action}: {details}")
            event_count += 1

        elif event.action in ["Timeout", "Crash", "Recovered"]:
            lines.append(f"    Note over {source}: {event.action}")
            event_count += 1

    return "\n".join(lines)

def generate_filtered_diagram(events: list[LogEvent],
                               include_notifications: bool = False,
                               include_version_replies: bool = False,
                               max_events: int = 100) -> str:
    """Generate a simplified diagram with filtering options."""

    filtered_events = []
    for event in events:
        # Always include client requests and responses
        if event.source.startswith("client") and event.target:
            filtered_events.append(event)
        # Include node -> client responses
        elif event.target and event.target.startswith("client"):
            filtered_events.append(event)
        # Include responses to TestManager
        elif event.target == "TestManager":
            filtered_events.append(event)
        # Optionally include notifications
        elif include_notifications and "Notify" in event.action:
            filtered_events.append(event)
        # Include crashes and recoveries
        elif event.action in ["Crash", "Recovered", "Timeout"]:
            filtered_events.append(event)

    return generate_mermaid_diagram(filtered_events, max_events)

def main():
    # Parse arguments
    input_file = sys.argv[1] if len(sys.argv) > 1 else "../run.log"
    output_file = sys.argv[2] if len(sys.argv) > 2 else "diagram.mmd"

    print(f"Reading log from: {input_file}")

    # Read and parse log
    events = []
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                # Skip empty lines and timestamp lines
                stripped = line.strip()
                if not stripped:
                    continue

                # Remove line number prefix if present (from cat -n format)
                cleaned = re.sub(r'^\s*\d+[→\t]\s*', '', stripped)

                # Also handle timestamp prefix
                cleaned = re.sub(r'^\d{2}:\d{2}:\d{2}\.\d{3}\s+\[[^\]]+\]\s+\w+\s+[^\-]+\s+-\s+', '', cleaned)

                event = parse_log_line(cleaned, line_num)
                if event:
                    events.append(event)
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found")
        sys.exit(1)

    print(f"Parsed {len(events)} events")

    # Generate simplified diagram (client interactions only)
    simple_diagram = generate_filtered_diagram(events,
                                                include_notifications=False,
                                                max_events=150)

    # Generate detailed diagram (with notifications)
    detailed_diagram = generate_filtered_diagram(events,
                                                  include_notifications=True,
                                                  max_events=200)

    # Write output files
    simple_output = output_file.replace('.mmd', '_simple.mmd')
    detailed_output = output_file.replace('.mmd', '_detailed.mmd')

    with open(simple_output, 'w', encoding='utf-8') as f:
        f.write(simple_diagram)
    print(f"Simple diagram written to: {simple_output}")

    with open(detailed_output, 'w', encoding='utf-8') as f:
        f.write(detailed_diagram)
    print(f"Detailed diagram written to: {detailed_output}")

    # Also create a draw.io compatible CSV for topology
    create_topology_csv(output_file.replace('.mmd', '_topology.csv'))

    print("\nTo import in draw.io:")
    print("1. Open draw.io (https://app.diagrams.net)")
    print("2. Go to: Arrange -> Insert -> Advanced -> Mermaid")
    print("3. Paste the content of the .mmd file")
    print("\nOr for CSV topology:")
    print("1. Go to: Arrange -> Insert -> Advanced -> CSV")
    print("2. Paste the content of the _topology.csv file")

def create_topology_csv(output_file: str):
    """Create a CSV file for draw.io topology diagram."""
    csv_content = """# label: %name%
# style: shape=%shape%;fillColor=%fill%;strokeColor=%stroke%;
# namespace: csvimport-
# connect: {"from": "connects", "to": "name", "style": "curved=1;endArrow=none;"}
# width: auto
# height: auto
# padding: 15
# ignore: shape,fill,stroke,connects
# nodespacing: 40
# levelspacing: 100
# edgespacing: 40
# layout: circle
name,shape,fill,stroke,connects
Node0,ellipse,#dae8fc,#6c8ebf,"Node10,Node90"
Node10,ellipse,#dae8fc,#6c8ebf,"Node20,Node0"
Node20,ellipse,#dae8fc,#6c8ebf,"Node30,Node10"
Node30,ellipse,#dae8fc,#6c8ebf,"Node40,Node20"
Node40,ellipse,#dae8fc,#6c8ebf,"Node50,Node30"
Node50,ellipse,#dae8fc,#6c8ebf,"Node60,Node40"
Node60,ellipse,#dae8fc,#6c8ebf,"Node70,Node50"
Node70,ellipse,#dae8fc,#6c8ebf,"Node80,Node60"
Node80,ellipse,#dae8fc,#6c8ebf,"Node90,Node70"
Node90,ellipse,#dae8fc,#6c8ebf,"Node0,Node80"
Client0,rectangle,#d5e8d4,#82b366,""
Client1,rectangle,#d5e8d4,#82b366,""
Client2,rectangle,#d5e8d4,#82b366,""
"""
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(csv_content)
    print(f"Topology CSV written to: {output_file}")

if __name__ == "__main__":
    main()