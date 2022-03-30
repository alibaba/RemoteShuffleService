#!/usr/bin/env bash

# Restarts the rss worker on the machine this script is executed on.

trap '' SIGINT SIGTERM SIGQUIT SIGHUP SIGTSTP

if [ -z "${RSS_HOME}" ]; then
  export RSS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${RSS_HOME}/sbin/rss-config.sh"

UUID=$(cat /proc/sys/kernel/random/uuid)
setsid "${RSS_HOME}/sbin/restart-worker-impl.sh" "$@" > "/tmp/restart-worker-${UUID}.log" 2>&1  &
