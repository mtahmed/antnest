#!/usr/bin/env bash

SELF=${0##*/}

help() {
cat <<END
NAME
  $SELF - stop all running slaves

SYNOPSIS
  $SELF

DESCRIPTION
  If there are instances of slave Nodes running on the machine, it stops them.

OPTIONS
  -H/-h       Display this document.
END
}

N=1

while getopts 'n:hH' opt
do
  case $opt in
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

if [[ ! -f /tmp/antnest_slave_pids ]]; then
  echo "No slave nodes running."
  exit 0
fi

NUM_SLAVES=$(wc -l < /tmp/antnest_slave_pids)
echo "Killing ${NUM_SLAVES} slaves."

for PID in $(cat /tmp/antnest_slave_pids); do
  kill $PID
done

rm /tmp/antnest_slave_pids
