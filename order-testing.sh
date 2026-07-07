#!/usr/bin/env bash
# manual-test.sh - Fire 10 concurrent orders
BASE="http://localhost:8080/api/v1/orders"
for i in $(seq 1 10); do
  pid=$(( (i % 3) + 1 ))          # productId ∈ {1,2,3}
  qty=$(( (RANDOM % 98) + 1 ))    # quantity ∈ [1,99]
  curl -s -X POST "$BASE?productId=$pid&quantity=$qty" &
done
wait
echo "Done: 10 concurrent orders fired"
