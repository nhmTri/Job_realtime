#!/bin/bash

# Start Cassandra in foreground + force root mode
echo "🚀 Starting Cassandra foreground with -R (root allowed)..."
cassandra -f -R &
CASS_PID=$!

# Wait until Cassandra is ready
echo "⏳ Waiting for Cassandra via cqlsh..."
until cqlsh -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; do
  echo "🔁 Cassandra not ready yet..."
  sleep 5
done

# Only run init.cql if not already created
if ! cqlsh -e "DESCRIBE TABLE job_raw_data.job_posts" >/dev/null 2>&1; then
  echo "✅ Running init.cql..."
  cqlsh -f /init.cql
else
  echo "🟡 Table already exists. Skipping init."
fi

echo "📦 Cassandra is ready. Keeping container alive..."
wait $CASS_PID
