#!/usr/bin/env bash

# Restarts the rss worker on the machine this script is executed on.

set -x

# one worker in one machine
LOCKFILE="/tmp/restart-worker-lock"
if [ -f ${LOCKFILE} ]
  then
    echo "restart-worker is in progress"
    exit
  else
    touch ${LOCKFILE}
fi

if [ -z "${RSS_HOME}" ]; then
  export RSS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${RSS_HOME}/sbin/rss-config.sh"

"${RSS_HOME}/sbin/stop-worker.sh" "$@"
"${RSS_HOME}/sbin/start-worker.sh" "$@"

echo -e "restart-worker process is done."
if [ -f ${LOCKFILE} ]
  then
    rm -rf ${LOCKFILE}
fi