#!/usr/bin/env bash
# Check that all Java source files have the PostgreSQL License header.

DIR=$(git rev-parse --show-toplevel)
error=0

while IFS= read -r -d '' f; do
    if ! grep -q "PostgreSQL License" "$f"; then
        echo "Missing license header: $f"
        error=1
    fi
done < <(find "$DIR/src/main" "$DIR/src/test" -name "*.java" -print0 2>/dev/null)

if [ $error -eq 0 ]; then
    echo "License check passed."
fi
exit $error
