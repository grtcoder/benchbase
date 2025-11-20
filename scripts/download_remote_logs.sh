#!/usr/bin/env bash
#
# fetch_emulab_logs_serial.sh
# Serial scp logs from N servers and M brokers into ./emulogs

set -Eeuo pipefail
source ./env-vars.sh
REMOTE_HOME="/users/${cloudLabUserName}"

# -------- Configuration (edit these) --------
USERNAME=${cloudLabUserName}
DOMAIN="${experimentName}.${projectName}.${clusterType}.${suffix}"
REMOTE_DIR="${REMOTE_HOME}/logs"
DEST_DIR="./emulogs"

NUM_SERVERS=${numServer}     # number of servers (server1..serverN)
NUM_BROKERS=${numBroker}     # number of brokers (broker1..brokerM)

SCP_OPTS=(-C -o ConnectTimeout=10 -o ServerAliveInterval=30 -o ServerAliveCountMax=2)
# --------------------------------------------

mkdir -p "${DEST_DIR}"

copy_host() {
  local host="$1"
  local fqdn="${host}.${DOMAIN}"
  local src="${USERNAME}@${fqdn}:${REMOTE_DIR}"

  echo "==> Copying from ${fqdn}:${REMOTE_DIR} ..."
  if scp "${SCP_OPTS[@]}" -r "${src}" "${DEST_DIR}"; then
    echo "✅ Done: ${host}"
  else
    echo "❌ Failed: ${host}" >&2
  fi
}

# ---- Serial execution ----
for i in $(seq 1 "${NUM_SERVERS}"); do
  copy_host "server${i}"
done

for i in $(seq 1 "${NUM_BROKERS}"); do
  copy_host "broker${i}"
done

echo "All done. Logs are in: ${DEST_DIR}"
