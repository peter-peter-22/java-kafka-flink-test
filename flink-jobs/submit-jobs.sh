#!/bin/bash

# Start the Flink JobManager (default entry point)
/opt/flink/bin/jobmanager.sh start-foreground &

# Wait for JobManager to be ready (check REST API availability)
echo "Waiting for JobManager to start..."
until curl -s http://localhost:8081 >/dev/null; do
  sleep 2
  echo "test"
done
echo "JobManager is up!"

# Submit Flink jobs (adjust JAR path and class names as needed)
echo "Submitting Flink jobs..."
flink run -c com.me.UppercaseJob /opt/flink/usrlib/myjob.jar
flink run -c com.me.MessageCounterJob /opt/flink/usrlib/myjob.jar
echo "All jobs submitted!"

# Keep container running
wait