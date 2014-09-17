#!/usr/bin/env bash

SELF=${0##*/}

help() {
cat <<END
NAME
  $SELF - start multiple instances of slave nodes

SYNOPSIS
  $SELF [ options ]

DESCRIPTION
  If -n N options is supplied, then start N instances of slave nodes. Otherwise,
  start 1 instance of a slave node.

OPTIONS
  -H/-h       Display this document.
  -n          Number of slave node instances to start.
END
}

N=1

while getopts 'n:hH' opt
do
  case $opt in
  n)
    N="$OPTARG"
    ;;
  [hH])
    help
    exit 0
    ;;
  *)
    help >&2
    exit 4
    ;;
  esac
done
shift $((OPTIND - 1))

for PORT in $(seq 33311 $((33311 + $N - 1))); do
  python commands/start_slave.py -p ${PORT} &>> log/slave_"${PORT}".log &
  PID=$!
  sleep 1
  if ps -p "$PID" &> /dev/null; then
    echo ${PID} >> /tmp/antnest_slave_pids
  else
    echo "Failed to start slave with port ${PORT}."
  fi
done
