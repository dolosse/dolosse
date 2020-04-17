#!/bin/sh

. setup.sh

./kill_daq.sh

odbedit -c clean
#odbedit -c "rm /Analyzer/Trigger/Statistics"
#odbedit -c "rm /Analyzer/Scaler/Statistics"

mhttpd -p 8081 -D
sleep 2
xterm -e ./frontend &
xterm -e ./analyzer &
mlogger -D

echo Please point your web browser to http://localhost:8081
echo Or run: mozilla http://localhost:8081 &
echo To look at live histograms, run: roody -Hlocalhost

#end file
