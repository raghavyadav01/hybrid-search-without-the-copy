#!/bin/bash
# ssh to a 1 TB-run box: g.sh GEN|PINOT 'command'
source "$(dirname "$0")/hosts1tb.env"
H=$(eval echo \$${1}_PUB); shift
exec ssh -i "$KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 -o ServerAliveInterval=30 ec2-user@"$H" "$@"
