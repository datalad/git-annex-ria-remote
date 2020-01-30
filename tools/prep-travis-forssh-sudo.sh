#!/usr/bin/env bash

echo "127.0.0.1  datalad-test" >> /etc/hosts
apt-get install openssh-client
echo "MaxSessions 64" >> /etc/ssh/sshd_config
echo "AllowTcpForwarding yes" >> /etc/ssh/sshd_config
