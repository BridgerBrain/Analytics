#!/bin/bash
echo "Start cron"
printenv >> /etc/environment
cron -f -l 2
echo "cron started"

# Run forever
tail -f /dev/null
