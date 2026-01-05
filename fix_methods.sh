#!/bin/bash

# Fix elements() to getElements()
find src/main/java/com/mini/redis/command -type f -name "*.java" -exec sed -i 's/\.elements()/.getElements()/g' {} \;

# Fix value() to getStringValue() for BulkString
find src/main/java/com/mini/redis/command -type f -name "*.java" -exec sed -i 's/BulkString\) args\.get([^)]*)\)\.value()/BulkString) args.get(\1)).getStringValue()/g' {} \;

echo "Fixed method calls"