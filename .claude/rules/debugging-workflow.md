---
description: "Debug process to follow before touching any code"
---

# Debug Workflow

1. Read the original log output before making any change.
2. Identify the root cause — do not patch the symptom.
3. Fix one problem at a time; verify it is resolved before moving to the next.
4. After a fix: restart only the affected service and confirm the issue is gone.
5. Never restart the entire stack just to fix one service.
