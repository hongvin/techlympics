#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
LOGFILE="$DIR/log.txt"
nohup python3 "$DIR/daemon.py"  2>&1 $LOGFILE &
nohup python3 "$DIR/app.py" 2>&1 $LOGFILE &

