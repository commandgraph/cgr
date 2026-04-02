#!/bin/bash
set -e

# Inject the authorized key from environment variable
if [ -n "$AUTHORIZED_KEY" ]; then
    mkdir -p /home/deploy/.ssh
    echo "$AUTHORIZED_KEY" > /home/deploy/.ssh/authorized_keys
    chmod 600 /home/deploy/.ssh/authorized_keys
    chmod 700 /home/deploy/.ssh
    chown -R deploy:deploy /home/deploy/.ssh
    echo "SSH key injected for deploy user"
fi

# Start sshd in foreground
exec /usr/sbin/sshd -D -e
