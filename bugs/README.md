# Bug Tracking

This directory contains bug reports discovered during cntryl-go SDK development and testing.

## Active Bugs

| ID | Title | Priority | Component | Status |
|----|-------|----------|-----------|--------|
| [001](001-websocket-handshake-failure.md) | WebSocket Connection Handshake Failure | High | Transport/WebSocket | Open (Fix Available) |

## Bug Template

When creating a new bug report, use this template:

```markdown
# Bug #XXX: [Short Title]

**Status:** Open/In Progress/Resolved/Closed  
**Priority:** Critical/High/Medium/Low  
**Component:** [Component Name]  
**Reported:** YYYY-MM-DD  
**Affects:** [Test/Feature affected]

## Summary
[Brief description of the issue]

## Environment
- **Broker:** [version/config]
- **Client:** [version]
- **Date:** [date]

## Expected Behavior
[What should happen]

## Actual Behavior
[What actually happens]

## Steps to Reproduce
1. [Step 1]
2. [Step 2]
3. [Step 3]

## Logs/Error Messages
```
[Relevant logs]
```

## Workaround
[If any workaround exists]

## Impact
[How this affects functionality]

## Related Files
- `file1.go` - [description]
- `file2.go` - [description]

## Next Steps
1. [Action item 1]
2. [Action item 2]
```

## Naming Convention
- Files: `XXX-short-descriptive-name.md` (where XXX is zero-padded number)
- Format: Markdown with clear sections
- Keep concise but include all reproduction details

## Status Workflow
- **Open** - Bug discovered, not yet investigated
- **In Progress** - Actively working on fix
- **Resolved** - Fix implemented, awaiting verification
- **Closed** - Verified fixed and closed
