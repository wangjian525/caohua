#!/bin/bash
ppid=`lsof -i:9191 | awk 'END{print $2}'`
kill -9 ${ppid}
