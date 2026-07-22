#!/bin/sh
# Run the benchmark against an EBS-like volume: a loop-backed ext4 filesystem
# throttled with cgroup v2 io.max (IOPS + bandwidth caps), so real device I/O is
# rate-limited the way an EBS gp3 volume is, while the OS page cache still serves
# warm reads for free. Requires a --privileged container.
#
#   EBS_IOPS  read+write IOPS cap      (default 3000  = gp3 baseline)
#   EBS_BPS   read+write bytes/sec cap (default 125 MiB/s = gp3 baseline)
#   EBS_DROP_CACHES=1  drop the page cache after ingest is impossible mid-process;
#                      instead reads stay cache-warm unless --cold-per-query is used.
# Any further args are passed through to the benchmark binary.
#
#   docker run --rm --privileged \
#     -v "$PWD/dev/benchmarks/jazz-ingest/docker/ebs-run.sh:/ebs-run.sh" \
#     -e EBS_IOPS=3000 -e EBS_BPS=131072000 \
#     jazz-ingest-bench 'sh /ebs-run.sh --raw rocksdb,slatedb'
set -e

IOPS="${EBS_IOPS:-3000}"
BPS="${EBS_BPS:-131072000}" # 125 * 1024 * 1024

# mkfs.ext4 lives in e2fsprogs; losetup/mount are in util-linux (present).
if ! command -v mkfs.ext4 >/dev/null 2>&1; then
    apt-get update -qq && apt-get install -y -qq e2fsprogs >/dev/null
fi

dd if=/dev/zero of=/back.img bs=1M count=2048 status=none
LOOP=$(losetup -f)
losetup "$LOOP" /back.img
mkfs.ext4 -qF "$LOOP"
mkdir -p /data
mount "$LOOP" /data

MM=$(cat "/sys/block/$(basename "$LOOP")/dev")
echo "$MM riops=$IOPS wiops=$IOPS rbps=$BPS wbps=$BPS" > /sys/fs/cgroup/io.max

echo "EBS sim: dev $MM  iops=$IOPS  bps=$BPS ($((BPS / 1048576)) MiB/s)  data->/data"
echo "io.max: $(cat /sys/fs/cgroup/io.max | tr '\n' ' ')"

export TMPDIR=/data
exec /src/target/release/jazz-ingest-bench "$@"
