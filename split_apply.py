#!/usr/bin/env python3
"""Split `apply_request_with_events` into three tier-specific methods.

Reads the current apply function, finds each outer match arm, partitions by
tier (based on the arm's outer pattern's `LedgerRequest::<Tier>` prefix), and
emits three new methods. Then replaces the body of `apply_request_with_events`
with a minimal dispatcher.

Expected invariant: every outer arm starts with `LedgerRequest::System(`,
`LedgerRequest::Region(`, or `LedgerRequest::Organization(`.
"""
import re
from pathlib import Path

OPS = 'crates/raft/src/log_storage/operations/mod.rs'
p = Path(OPS)
text = p.read_text()
lines = text.split('\n')


def find_line(pred, start=0):
    for i in range(start, len(lines)):
        if pred(lines[i]):
            return i
    return None


# 1. Find apply_request_with_events signature line.
sig_line = find_line(lambda l: 'fn apply_request_with_events' in l)
assert sig_line is not None, 'apply_request_with_events not found'

# 2. Find `match request {` inside the function.
match_line = find_line(lambda l: l.strip() == 'match request {', start=sig_line)
assert match_line is not None, 'match request { not found'

# 3. Find closing `}` of the match block.
depth = 0
started = False
match_end = None
for i in range(match_line, len(lines)):
    for c in lines[i]:
        if c == '{':
            depth += 1
            started = True
        elif c == '}':
            depth -= 1
            if started and depth == 0:
                match_end = i
                break
    if match_end is not None:
        break
assert match_end is not None

print(f'Match block: lines {match_line+1} - {match_end+1}  ({match_end-match_line+1} lines)')

# Body of the function before the match.
prefix_lines = lines[sig_line:match_line]
# Content inside the match braces (excluding the `match request {` line and closing `}`)
match_body_lines = lines[match_line+1:match_end]
# Content after the match closing `}` and before the function closing (if any).
# Actually the function might end right after the match. Let's check.
# Find function closing `}`.
depth = 0
started = False
fn_end = None
for i in range(sig_line, len(lines)):
    for c in lines[i]:
        if c == '{':
            depth += 1
            started = True
        elif c == '}':
            depth -= 1
            if started and depth == 0:
                fn_end = i
                break
    if fn_end is not None:
        break
assert fn_end is not None

# Lines between match closing and fn closing.
between = lines[match_end+1:fn_end]
print(f'Function spans: {sig_line+1} - {fn_end+1}')
print(f'Between match close and fn close: {len(between)} lines')


# 4. Split match arms by their outer LedgerRequest::<Tier>(...) prefix.
# An arm starts with `            LedgerRequest::` at indent level 12.
# Use brace counting to find the end of each arm.
def parse_arms(content_lines):
    """Yield (tier, arm_lines) for each top-level arm in the match body."""
    i = 0
    current_arm_start = None
    current_tier = None
    while i < len(content_lines):
        line = content_lines[i]
        stripped = line.lstrip()
        # Detect start of an arm.
        m = re.match(r'LedgerRequest::(System|Region|Organization)\(', stripped)
        if m and current_arm_start is None:
            current_arm_start = i
            current_tier = m.group(1)
            # This arm spans until the closing `},` at the same indent.
            # Count braces starting from this line.
            depth = 0
            j = i
            started = False
            while j < len(content_lines):
                for c in content_lines[j]:
                    if c == '{':
                        depth += 1
                        started = True
                    elif c == '}':
                        depth -= 1
                        if started and depth == 0:
                            # End of arm body. Body might have trailing `,`.
                            break
                if started and depth == 0:
                    break
                j += 1
            assert j < len(content_lines), f'Unclosed arm at line {i}'
            yield current_tier, content_lines[current_arm_start : j + 1]
            i = j + 1
            current_arm_start = None
            current_tier = None
            continue
        i += 1


# 5. Partition arms by tier.
arms_by_tier = {'System': [], 'Region': [], 'Organization': []}
arm_count = 0
for tier, arm_lines in parse_arms(match_body_lines):
    arms_by_tier[tier].append(arm_lines)
    arm_count += 1

print(f'Arms found: {arm_count}')
for tier, arms in arms_by_tier.items():
    print(f'  {tier}: {len(arms)}')


# 6. Rewrite arms for the typed functions.
# The outer pattern `LedgerRequest::<Tier>(inner)` becomes just `inner`.
# e.g. `LedgerRequest::System(SystemRequest::X { ... }) => { body },`
# becomes `SystemRequest::X { ... } => { body },`
def strip_outer(arm_lines, tier):
    """Strip the `LedgerRequest::<Tier>(` prefix and matching `)` suffix."""
    # The first line looks like:
    #     `            LedgerRequest::Tier(SomeInnerPattern... possibly { or spanning multiple lines)`
    # The outer `(` after `::Tier` matches the closing `)` before ` =>`.
    joined = '\n'.join(arm_lines)
    prefix = f'LedgerRequest::{tier}('
    idx = joined.find(prefix)
    assert idx != -1, f'prefix not found in arm'
    # Find matching close paren for the `(` after `Tier`.
    open_idx = idx + len(prefix) - 1  # index of `(`
    depth = 0
    k = open_idx
    in_str = False
    while k < len(joined):
        c = joined[k]
        if in_str:
            if c == '\\':
                k += 2
                continue
            if c == '"':
                in_str = False
            k += 1
            continue
        if c == '"':
            in_str = True
            k += 1
            continue
        if c == '(':
            depth += 1
        elif c == ')':
            depth -= 1
            if depth == 0:
                close_idx = k
                break
        k += 1
    else:
        raise RuntimeError('Unmatched paren in arm')
    # Replace joined[idx : close_idx+1] with joined[idx+len(prefix) : close_idx]
    # i.e. strip the outer wrapper.
    new_joined = joined[:idx] + joined[open_idx + 1 : close_idx] + joined[close_idx + 1 :]
    return new_joined.split('\n')


# Fragment shared body (block_height computation).
# Look at the prefix_lines to extract it.
shared_setup_lines = []
for line in prefix_lines:
    if 'match request {' in line:
        break
    shared_setup_lines.append(line)
# Find the actual function signature and body prelude — everything before `match request {`.
# prefix_lines[0] is the signature line.
# We need: everything from `fn apply_request_with_events` up to but not including `match request {`.
# (Already the prefix_lines.)

# Extract the signature + prelude as template for the three new functions.
# Original signature:
SIG_ORIG = lines[sig_line]  # e.g. `    pub(super) fn apply_request_with_events(`
# Find the `) -> (LedgerResponse, Option<VaultEntry>) {` line.
sig_close_line = None
for i in range(sig_line, match_line):
    if ') -> (LedgerResponse, Option<VaultEntry>) {' in lines[i]:
        sig_close_line = i
        break
assert sig_close_line is not None

# The request parameter is in the signature. Need to change `request: &LedgerRequest,`
# to `request: &SystemRequest,` etc.
# Collect signature lines.
sig_lines = lines[sig_line : sig_close_line + 1]
# Find the request parameter line.
req_line_idx = None
for i, l in enumerate(sig_lines):
    if 'request: &LedgerRequest' in l:
        req_line_idx = i
        break
assert req_line_idx is not None

# Prelude between `) -> ... {` and `match request {` (e.g. block_height computation).
prelude_lines = lines[sig_close_line + 1 : match_line]


def build_function(fn_name, tier_name, arms):
    """Construct a new function with the tier-specific arms."""
    out = list(sig_lines)
    # Replace function name.
    out[0] = out[0].replace('apply_request_with_events', fn_name)
    # Replace request type.
    out[req_line_idx] = out[req_line_idx].replace('&LedgerRequest', f'&{tier_name}')
    # Prelude.
    out += prelude_lines
    # Match.
    out.append('        match request {')
    # Emit arms with outer wrapper stripped.
    for arm_lines in arms:
        stripped = strip_outer(arm_lines, fn_name_to_tier[fn_name])
        out.extend(stripped)
    out.append('        }')
    # Between match close and fn close (post-processing).
    out += between
    out.append('    }')
    return out


fn_name_to_tier = {
    'apply_system_request_with_events': 'System',
    'apply_region_request_with_events': 'Region',
    'apply_organization_request_with_events': 'Organization',
}


tier_to_typename = {
    'System': 'SystemRequest',
    'Region': 'RegionRequest',
    'Organization': 'OrganizationRequest',
}


def build_typed_fn(fn_name, tier_key, arms):
    typed_name = tier_to_typename[tier_key]
    out = list(sig_lines)
    out[0] = out[0].replace('apply_request_with_events', fn_name)
    out[req_line_idx] = out[req_line_idx].replace('&LedgerRequest', f'&{typed_name}')
    out += prelude_lines
    out.append('        match request {')
    for arm_lines in arms:
        stripped = strip_outer(arm_lines, tier_key)
        out.extend(stripped)
    out.append('        }')
    out += between
    out.append('    }')
    return out


system_fn = build_typed_fn('apply_system_request_with_events', 'System', arms_by_tier['System'])
region_fn = build_typed_fn('apply_region_request_with_events', 'Region', arms_by_tier['Region'])
org_fn = build_typed_fn('apply_organization_request_with_events', 'Organization', arms_by_tier['Organization'])

# Dispatcher replacement for apply_request_with_events.
dispatcher_body = [
    '        match request {',
    '            LedgerRequest::System(r) => self.apply_system_request_with_events(',
    '                r, state, block_timestamp, op_index, events, ttl_days, pending,',
    '                log_id_bytes, skip_state_writes, caller, defer_state_root,',
    '            ),',
    '            LedgerRequest::Region(r) => self.apply_region_request_with_events(',
    '                r, state, block_timestamp, op_index, events, ttl_days, pending,',
    '                log_id_bytes, skip_state_writes, caller, defer_state_root,',
    '            ),',
    '            LedgerRequest::Organization(r) => self.apply_organization_request_with_events(',
    '                r, state, block_timestamp, op_index, events, ttl_days, pending,',
    '                log_id_bytes, skip_state_writes, caller, defer_state_root,',
    '            ),',
    '        }',
]

# Rebuild the file:
#   [0..sig_line) unchanged
#   [sig_line..fn_end] replaced with: dispatcher(apply_request_with_events) + three typed fns
#   [fn_end+1..] unchanged

dispatcher_fn = list(sig_lines) + prelude_lines + dispatcher_body + ['    }']

# After fn_end, there's content (other methods, tests, etc.) — leave untouched.
new_lines = (
    lines[:sig_line]
    + dispatcher_fn
    + ['']
    + system_fn
    + ['']
    + region_fn
    + ['']
    + org_fn
    + lines[fn_end + 1 :]
)

p.write_text('\n'.join(new_lines))
print(f'Wrote {len(new_lines)} lines. Original was {len(lines)}.')
