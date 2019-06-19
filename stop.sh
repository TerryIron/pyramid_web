#!/usr/bin/env python
# coding:utf-8
import commands
import os
import sys


def kill_process(port):
    port_info = commands.getoutput('netstat -tanp 2>&1 | grep tcp | grep LISTEN | grep ":{}"'.format(port))
    if port_info:
        port_pid = commands.getoutput('echo {} '.format(port_info) + " | awk '{print $7}'").split('\n')
        port_pid = [i.split('/')[0] for i in port_pid]
        os.system('kill -9 {} 2>/dev/null'.format(' '.join(port_pid)))
        return port_pid
    return []


if __name__ == '__main__':
    port = sys.argv[1]
    while True:
        ret = kill_process(port)
        if not ret:
            break
