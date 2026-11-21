#!/bin/bash
# ssh to a camera-ready box: gb.sh P1|P2|CLIENT 'command'
source "$(dirname "$0")/hostsb.env"
H=$(eval echo \$${1}_PUB); shift
ssh -i "$KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 -o ServerAliveInterval=30 -o LogLevel=ERROR ec2-user@"$H" "$@" 2>&1 | grep -v -i 'quantum\|pq.html\|upgraded\|store now'
