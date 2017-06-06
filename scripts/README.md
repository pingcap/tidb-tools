## scripts

This directory contains a collection of auxiliary scripts.

- ``checkenv.sh``: Check if current server environment is suitable for a tidb deployment.

## ``checkenv.sh``

Check following requirements:

- OS, kernel version
- CPU cores, HT status
- Memory
- Swap turned off
- Disk space, mount options, rotational (may not be recognized correctly)
- sysctl.conf
- ulmit

NOTE: requires ``bash`` to run.

### How to use

```
cd ./tidb-tools/scripts
chmod a+x ./checkenv.sh # make sure the script is executable
./checkenv.sh
```

### Example output

```
> ./checkenv.sh
Kernel checking ... Linux ok
Archetecture ... x86_64 ok
Kernel version ... 3.19.0-69-generic ok
CPU checking ... ok
  Logical CPU Number  : 8
  Physical CPU Number : 1
  CPU Core Number     : 4
  HT Number           : 2
Total Memory  ... 16 GB ok
Swap off ... ok
Checking disks ...
  sda1 of size  [228GB] is mounted on /
    warning: mount options is (rw,errors=remount-ro), better add `noatime` for deploying pd/tikv
  sdb1 of size  [7452GB] is mounted on /data
    warning: not an SSD device? not suitable for deploying tikv
    warning: mount options is (rw), better add `noatime` for deploying pd/tikv
Checking net.core.somaxconn ... 32768 ok
Checking net.ipv4.tcp_syncookies ... 0 ok
Checking fs.file-max ... 65535 ok
Checking ulimit -c (core file size)... unlimited ok
Checking ulimit -n (fd num)... 1024 too low
  tune it in /etc/security/limits.conf
```

## License

Apache 2.0 license. See the [LICENSE](../LICENSE) file for details.
