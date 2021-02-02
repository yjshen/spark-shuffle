#!/usr/bin/env bash

if [ -z "${SS_HOME}" ]; then
  export SS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

exec "${SS_HOME}/sbin"/ss-daemon.sh stop com.kwai.SparkShuffleService
