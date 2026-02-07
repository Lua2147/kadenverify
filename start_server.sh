#!/bin/bash
# Start KadenVerify API Server

cd "$(dirname "$0")"

# Load config
if [ -f "config.json" ]; then
    export KADENVERIFY_API_KEY=$(python3 -c "import json; print(json.load(open('config.json'))['api_key'])")
    export KADENVERIFY_HELO_DOMAIN=$(python3 -c "import json; print(json.load(open('config.json'))['helo_domain'])")
    export KADENVERIFY_FROM_ADDRESS=$(python3 -c "import json; print(json.load(open('config.json'))['from_address'])")
    export KADENVERIFY_CONCURRENCY=$(python3 -c "import json; print(json.load(open('config.json'))['concurrency'])")
else
    echo "Warning: config.json not found. Using defaults."
    export KADENVERIFY_API_KEY="dev-key-replace-in-production"
    export KADENVERIFY_HELO_DOMAIN="verify.kadenwood.com"
    export KADENVERIFY_FROM_ADDRESS="verify@kadenwood.com"
    export KADENVERIFY_CONCURRENCY="5"
fi

# Enable tiered verification
export KADENVERIFY_TIERED="true"

echo "Starting KadenVerify API Server..."
echo "URL: http://localhost:8000"
echo "Docs: http://localhost:8000/docs"
echo ""

# Start server with hot reload
uvicorn server:app --host 0.0.0.0 --port 8000 --reload
