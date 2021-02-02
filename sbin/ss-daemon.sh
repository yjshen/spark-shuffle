#!/usr/bin/env bash

usage="Usage: ss-daemon.sh (start|stop|status) <args...>"

if [ -z "${SS_HOME}" ]; then
  export SS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${SS_HOME}/conf/env.sh"

option=$1
shift
command="Spark shuffle service"


spark_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}


if [ "$SS_IDENT_STRING" = "" ]; then
  export SS_IDENT_STRING="$USER"
fi

export SS_PRINT_LAUNCH_COMMAND="1"

# get log directory
if [ "$SS_LOG_DIR" = "" ]; then
  export SS_LOG_DIR="${SS_HOME}/logs"
fi
mkdir -p "$SS_LOG_DIR"
touch "$SS_LOG_DIR"/.spark_test > /dev/null 2>&1
TEST_LOG_DIR=$?
if [ "${TEST_LOG_DIR}" = "0" ]; then
  rm -f "$SS_LOG_DIR"/.spark_test
else
  chown "$SS_IDENT_STRING" "$SS_LOG_DIR"
fi

if [ "$SS_PID_DIR" = "" ]; then
  SS_PID_DIR=/tmp
fi

# some variables
log="$SS_LOG_DIR/spark-shuffle-service-$SS_IDENT_STRING-$HOSTNAME.out"
pid="$SS_PID_DIR/spark-shuffle-service-$SS_IDENT_STRING.pid"

SS_JARS_DIR="${SS_HOME}/jars"
CLASSPATH=$(JARS=("$SS_JARS_DIR"/*.jar); IFS=:; echo "${JARS[*]}")

execute_command() {
  if [ -z ${SS_NO_DAEMONIZE+set} ]; then
      nohup -- "$@" >> $log 2>&1 < /dev/null &
      newpid="$!"

      echo "$newpid" > "$pid"

      # Poll for up to 5 seconds for the java process to start
      for i in {1..10}
      do
        if [[ $(ps -p "$newpid" -o comm=) =~ "java" ]]; then
           break
        fi
        sleep 0.5
      done

      sleep 2
      # Check if the process has died; in that case we'll tail the log so the user can see
      if [[ ! $(ps -p "$newpid" -o comm=) =~ "java" ]]; then
        echo "failed to launch: $@"
        tail -10 "$log" | sed 's/^/  /'
        echo "full log in $log"
      fi
  else
      "$@"
  fi
}

run_command() {
  mode="$1"
  shift

  mkdir -p "$SS_PID_DIR"

  if [ -f "$pid" ]; then
    TARGET_ID="$(cat "$pid")"
    if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
      echo "$command running as process $TARGET_ID.  Stop it first."
      exit 1
    fi
  fi

  spark_rotate_log "$log"
  echo "Starting $command, logging to $log"

  case "$mode" in
    (class)
      execute_command $JAVA -cp $CLASSPATH "$@"
      ;;

    (*)
      echo "unknown mode: $mode"
      exit 1
      ;;
  esac

}

case $option in

  (start)
    echo "$@"
    run_command class "$@"
    ;;

  (stop)

    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo "Stopping $command"
        kill "$TARGET_ID" && rm -f "$pid"
      else
        echo "no $command to stop"
      fi
    else
      echo "no $command to stop"
    fi
    ;;

  (status)

    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo $command is running.
        exit 0
      else
        echo $pid file is present but $command not running
        exit 1
      fi
    else
      echo $command not running.
      exit 2
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
