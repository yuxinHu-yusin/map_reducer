#!/bin/bash
set -e

# === CONFIG ===
BUCKET="<your-bucket-name>"
SPLITTER_IP="<splitter-ip>"
MAPPER_IP="<mapper-ip>"
REDUCER_IP="<reducer-ip>"


INPUT="s3://$BUCKET/doc.txt"

# Helper: current time in ms
now_ms() {
  python3 -c 'import time; print(int(time.time()*1000))'
}

# Number of mappers (default = 3)
PARTS=${1:-3}

OUT_PREFIX="splits/${RUNID}-${PARTS}/"
MAP_PREFIX="maps/${RUNID}-${PARTS}/"
REDUCE_OUT="reduce/${RUNID}-${PARTS}/final.json"

echo "=== Split phase ($PARTS parts) ==="
start=$(now_ms)
curl -s "http://$SPLITTER_IP:8080/split?input_s3=$INPUT&parts=$PARTS&out_prefix=$OUT_PREFIX" > /dev/null
end=$(now_ms)
split_time=$((end-start))
echo "Split time: ${split_time} ms"

echo "=== Map phase ==="
start=$(now_ms)
for ((i=0; i<$PARTS; i++)); do
    CHUNK="s3://$BUCKET/${OUT_PREFIX}chunk-$i.txt"
    OUTMAP="s3://$BUCKET/${MAP_PREFIX}map-$i.json"
    (curl -s "http://$MAPPER_IP:8080/map?chunk_s3=$CHUNK&out_s3=$OUTMAP" > /dev/null &)
done
wait
end=$(now_ms)
map_time=$((end-start))
echo "Map time: ${map_time} ms"

echo "=== Reduce phase ==="
# Build all input params for reducer
IN_PARAMS=""
for ((i=0; i<$PARTS; i++)); do
    IN_PARAMS+="&in=s3://$BUCKET/${MAP_PREFIX}map-$i.json"
done
start=$(now_ms)
curl -s "http://$REDUCER_IP:8080/reduce?${IN_PARAMS:1}&out_s3=s3://$BUCKET/$REDUCE_OUT" > /dev/null
end=$(now_ms)
reduce_time=$((end-start))
echo "Reduce time: ${reduce_time} ms"

total=$((split_time + map_time + reduce_time))
echo "=== Summary ==="
echo "Split:  ${split_time} ms"
echo "Map:    ${map_time} ms"
echo "Reduce: ${reduce_time} ms"
echo "TOTAL:  ${total} ms"
