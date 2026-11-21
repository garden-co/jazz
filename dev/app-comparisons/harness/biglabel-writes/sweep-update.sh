#!/bin/bash
# sweep-update.sh <variant> <tag> <samples> <fix>...: bl_update_status per app knob set ("none" = upstream).
v=$1; tag=$2; n=$3; shift 3
for fix in "$@"; do
  f=$fix; [ "$f" = none ] && f=""
  echo "## $v $tag fix=$fix"
  node probe.mjs --variant $v --samples $n --only update,cross --tag $tag --fix "$f" 2>&1 | tail -n 1
done
