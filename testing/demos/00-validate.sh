#!/usr/bin/env bash
# Demo 00: Validate all example files
source /opt/cgr/demos/lib.sh

banner "Demo 0: Validate all examples"

narrate "Checking that the engine parses and resolves every example file."
narrate "Both .cg (structured) and .cgr (readable) formats tested."
echo ""

ALL_OK=true

for f in "$EXAMPLES"/*.cg "$EXAMPLES"/*.cgr; do
    [ -f "$f" ] || continue
    name=$(basename "$f")
    if cgr validate "$f" --repo "$REPO" >/dev/null 2>&1; then
        output=$(cgr validate "$f" --repo "$REPO" 2>&1 | head -2)
        success "$name  →  $(echo "$output" | grep -o '[0-9]* resource.*, [0-9]* wave.*')"
    else
        fail "$name  →  FAILED"
        cgr validate "$f" --repo "$REPO" 2>&1 | head -5
        ALL_OK=false
    fi
done

echo ""
if $ALL_OK; then
    success "All example files validated successfully."
else
    fail "Some files failed validation."
fi
