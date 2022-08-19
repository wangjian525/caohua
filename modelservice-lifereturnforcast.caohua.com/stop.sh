#!/bin/bash
ppid=`lsof -i:8183 | awk 'END{print $2}'`
kill -9 ${ppid}
