#!/bin/sh
### BEGIN INIT INFO
# Provides:          guojj.com
# Required-Start:    $network $local_fs $remote_fs
# Required-Stop::    $network $local_fs $remote_fs
# Should-Start:      $all
# Should-Stop:       $all
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: access - service
# Description:       access - service
### END INIT INFO

NAME=access

# 以前/data/service/access/start.sh 里面有这个操作

if [ "$USER" = "root" ] || [ -z "$USER" ]; then
    # switch to user00 for init rc
    USER=user00
fi
if [ "$USER" = "user00" ]; then
    HOME=/data
fi

WORK_DIR=$HOME/service/$NAME/src
MAIN=$WORK_DIR/$NAME.py
CONF=../config/config.json
ARGS="${CONF}"
PIDFILE=$HOME/logs/service/$NAME/$NAME.pid

test -f $MAIN || { echo "file not found: $MAIN"; exit 1; }

. /lib/lsb/init-functions

case "$1" in
start)  log_daemon_msg "Starting" "$NAME"
        /sbin/start-stop-daemon -S -b -m -p $PIDFILE -c $USER -d $WORK_DIR --exec /usr/bin/python $MAIN -- $ARGS
        log_end_msg $?
        ;;
stop)   log_daemon_msg "Stopping" "$NAME"
        ps aux | grep access.py | grep -v grep | awk '{print $2}' | xargs -I{} kill -9 {}
        # /sbin/start-stop-daemon -K -R TERM/9/QUIT/3 -p $PIDFILE
        log_end_msg $?
        ;;
restart|reload|force-reload)
        log_daemon_msg "Restarting" "$NAME"
        # TODO make restart more quick
        # /sbin/start-stop-daemon -K -R TERM/9/QUIT/3 -p $PIDFILE
        ps aux | grep access.py | grep -v grep | awk '{print $2}' | xargs -I{} kill -9 {}
        /sbin/start-stop-daemon -S -b -m -p $PIDFILE -c $USER -d $WORK_DIR --exec /usr/bin/python  $MAIN -- $ARGS
        log_end_msg $?
        ;;
status)
        status_of_proc -p $PIDFILE $MAIN $NAME && exit 0 || exit $?
        ;;
*)      log_action_msg "Usage: $0 {start|stop|restart|reload|force-reload|status}"
        exit 2
        ;;
esac
exit 0
