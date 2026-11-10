#!/usr/bin/env python3
"""Summarize Oxia shard placement from the coordinator's status ConfigMap.

Reads the YAML stored under data.status of the <release>-status ConfigMap on stdin and
prints one line:

    <balanced 0|1> <term-sum> shards=<n> leaders=<per-server> replicas=<per-server>

Balanced means every shard has a leader, and both leaders and replicas differ by at most
one between data servers. term-sum changes whenever any shard holds an election, so two
equal readings some seconds apart mean nothing moved in between.

Usage: oxia_placement.py <number-of-data-servers>

Only the standard library is used: the status is the plain block YAML that the
coordinator writes (maps, and lists of maps), which the small parser below covers.
"""

import sys


def parse(text):
    """Parse block YAML made of nested maps, lists of maps and scalars."""
    lines = []
    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        lines.append((len(raw) - len(raw.lstrip(" ")), raw.strip()))

    def scalar(v):
        v = v.strip()
        if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
            return v[1:-1]
        return v

    def block(i, indent):
        # A list block starts with "- ", a map block with "key:".
        if i < len(lines) and lines[i][1].startswith("- "):
            items = []
            while i < len(lines) and lines[i][0] == indent and lines[i][1].startswith("- "):
                rest = lines[i][1][2:]
                if ":" in rest and not rest.endswith(":"):
                    # "- key: value" opens a map whose other keys sit 2 deeper.
                    k, v = rest.split(":", 1)
                    item = {k.strip(): scalar(v)}
                    i += 1
                    if i < len(lines) and lines[i][0] > indent:
                        more, i = block(i, lines[i][0])
                        item.update(more)
                    items.append(item)
                else:
                    items.append(scalar(rest))
                    i += 1
            return items, i
        result = {}
        while i < len(lines) and lines[i][0] == indent:
            key, _, value = lines[i][1].partition(":")
            i += 1
            if value.strip():
                result[key.strip()] = scalar(value)
            elif i < len(lines) and lines[i][0] > indent:
                result[key.strip()], i = block(i, lines[i][0])
            elif i < len(lines) and lines[i][0] == indent and lines[i][1].startswith("- "):
                # A list may sit at the same indentation as its key.
                result[key.strip()], i = block(i, indent)
            else:
                result[key.strip()] = None
        return result, i

    if not lines:
        return {}
    parsed, _ = block(0, lines[0][0])
    return parsed


def main():
    servers = int(sys.argv[1])
    try:
        status = parse(sys.stdin.read())
    except Exception as e:  # noqa: BLE001 - report, don't crash the harness
        print(f"0 -1 unparseable-status ({e})")
        return
    leaders, replicas = {}, {}
    shards = terms = leaderless = 0
    for ns in (status.get("namespaces") or {}).values():
        for shard in ((ns or {}).get("shards") or {}).values():
            shards += 1
            terms += int(shard.get("term") or 0)
            leader = (shard.get("leader") or {}).get("name") if isinstance(shard.get("leader"), dict) else None
            if leader:
                leaders[leader] = leaders.get(leader, 0) + 1
            else:
                leaderless += 1
            for member in shard.get("ensemble") or []:
                name = member.get("name") if isinstance(member, dict) else member
                replicas[name] = replicas.get(name, 0) + 1
    names = set(leaders) | set(replicas)
    missing = max(servers - len(names), 0)
    lv = sorted([leaders.get(n, 0) for n in names] + [0] * missing)
    rv = sorted([replicas.get(n, 0) for n in names] + [0] * missing)
    balanced = (
        shards > 0
        and leaderless == 0
        and lv[-1] - lv[0] <= 1
        and rv[-1] - rv[0] <= 1
    )
    print(f"{int(balanced)} {terms} shards={shards} leaders={lv} replicas={rv}")


if __name__ == "__main__":
    main()
