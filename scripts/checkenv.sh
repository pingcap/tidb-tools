#!/bin/bash

# Copyright 2016 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

################################################################################
## Colors
################################################################################

RED="\033[0;31m"
GREEN="\033[0;32m"
YELLOW="\033[0;33m"
NC="\033[0m" # No Color

################################################################################
## Utility Functions
################################################################################
echo_info () {
    echo -e "${GREEN}$@${NC}"
}

echo_warning () {
    echo -e "${YELLOW}$@${NC}"
}

echo_error () {
    echo -e "${RED}$@${NC}"
}


################################################################################
## Check Functions
################################################################################

### Kernel
check_os () {
    local _uname_s=$(uname -s)
    echo -n "${_uname_s} "
    case "${_uname_s}" in
	Linux)
	    echo_info ok
	    ;;
	Darwin)
	    echo_warning "works, for test only"
	    echo_warning "happy hacking!"
	    exit 1
	    ;;
	*BSD|CYGWIN*|Haiku|MINGW*|*)
	    echo_error "bad os"
	    echo_warning "happy hacking!"
	    exit 1
	    ;;
    esac
}

check_kernel_arch () {
    local _uname_m=$(uname -m)
    echo -n "${_uname_m} "
    case "${_uname_m}" in
	x86_64)
	    echo_info ok
	    ;;
	i686)
	    echo_warning works, for test only
	    ;;
	*)
	    echo_error bad arch
	    echo_warning "happy hacking!"
	    ;;
    esac
}

check_kernel_version () {
    local _uname_r=$(uname -r)
    echo -n "${_uname_r} "
    case "${_uname_r}" in
        3.*|4.*)
	    echo_info ok
	    ;;
        2.6.*)
            echo_warning "outdated kernel version"
            ;;
	*)
	    echo_warning "unsupported version"
	    ;;
    esac
}

## CPU
check_cpu () {
    local _physicalNumber=0
    local _coreNumber=0
    local _logicalNumber=0
    local _HTNumber=0
    _logicalNumber=$(grep "processor" /proc/cpuinfo|sort -u|wc -l)
    _physicalNumber=$(grep "physical id" /proc/cpuinfo|sort -u|wc -l)
    _coreNumber=$(grep "cpu cores" /proc/cpuinfo|uniq|awk -F':' '{print $2}'|xargs)
    _HTNumber=$((_logicalNumber / (_physicalNumber * _coreNumber)))
    if [ "${_logicalNumber}" -lt 8 -o "${_coreNumber}" -lt 4 ]; then
	echo_warning "cpu cores are not sufficient"
    else
	echo_info ok
    fi
    echo "  Logical CPU Number  : ${_logicalNumber}"
    echo "  Physical CPU Number : ${_physicalNumber}"
    echo "  CPU Core Number     : ${_coreNumber}"
    echo "  HT Number           : ${_HTNumber}"
    if [ "${_HTNumber}" -eq 1 ]; then
	echo_warning "    better open HT"
    fi
}

## Memory
# >= 8G
check_memory_total () {
    local _memory_total=$(cat /proc/meminfo | grep -i MemTotal | awk '{print $2}')
    echo -n "$((${_memory_total} / 1000 / 1000)) GB "
    if [ "${_memory_total}" -lt $((1000 * 1000 * 16)) ]; then
	echo_warning "not sufficient"
    else
	echo_info ok
    fi
}

# make sure swap is off
check_swap () {
    local _swap=$(cat /proc/meminfo | grep -i SwapTotal | awk '{print $2}')
    local _swappiness=$(cat /proc/sys/vm/swappiness)
    if [ "${_swap}" -ne 0 ]; then
	echo_warning "\n  swap is on"
	echo_warning "  use [sudo swapoff -a] to close swap"
        if [ ${_swappiness} -gt 20 ]; then
	    echo_warning "  or tune [sys.vm.swappiness]"
        fi
    else
	echo_info ok
    fi
}

## Disk
check_disk () {
    echo
    mount | awk '$1 ~ /^\// {
       _dev = $1;
       sub(".*/", "", _dev);
       _partition = _dev;
       _mount_point = $3;
       _fstype = $5;
       _options = $6;
       ("cat /sys/class/block/" _partition "/size") | getline _size_in_block;
       _size_inG = int(_size_in_block * 512 / 1024 / 1024 / 1024);
       YELLOW="\033[0;33m";
       NC="\033[0m";

       sub(/[0-9]/, "", _dev);
       ("cat /sys/block/" _dev "/queue/rotational") | getline _rotational;
       print "  " _partition, "of size\t[" _size_inG  "GB]", "is mounted on", _mount_point;
       if (_size_inG < 200) {
           print YELLOW "    warning" NC ": insufficient disk space";
           print YELLOW "    warning" NC ": not suitable for deploying pd/tikv";
       }
       if (_rotational == 1) {
           print YELLOW "    warning" NC ": not an SSD device? not suitable for deploying tikv";
       }
       if (_options !~ /noatime/) {
           print YELLOW "    warning" NC ": mount options is", _options ", better add `noatime` for deploying pd/tikv";
       }
    }'
}


# Kernel parameter
check_sysctl () {
    local _somaxconn=$(cat /proc/sys/net/core/somaxconn)
    echo -n "Checking net.core.somaxconn ... ${_somaxconn} "
    if [ ${_somaxconn} -lt 2048 ]; then
	echo_warning "too low"
    else
	echo_info ok
    fi

    local _syncookies=$(cat /proc/sys/net/ipv4/tcp_syncookies)
    echo -n "Checking net.ipv4.tcp_syncookies ... ${_syncookies} "
    if [ ${_syncookies} -ne 0 ]; then
	echo_warning "should be turned off"
    else
	echo_info ok
    fi

    local _file_max=$(cat /proc/sys/fs/file-max)
    echo -n "Checking fs.file-max ... ${_file_max} "
    if [ ${_file_max} -lt 10000 ]; then
	echo_warning "too low"
    else
	echo_info ok
    fi
}


# ulimit corefile size / max fd
check_ulimit () {
    local _ulimit_c=$(ulimit -c)
    echo -n "Checking ulimit -c (core file size)... ${_ulimit_c} "
    if [ ${_ulimit_c} != unlimited ]; then
	echo_warning "should open coredump"
    else
	echo_info ok
    fi

    local _ulimit_n=$(ulimit -n)
    echo -n "Checking ulimit -n (fd num)... ${_ulimit_n} "
    if [ ${_ulimit_n} -lt 10000 ]; then
	echo_warning "too low"
	echo_warning "  tune it in /etc/security/limits.conf"
    else
	echo_info ok
    fi
}

################################################################################
## Run check
################################################################################

echo -n "Kernel checking ... "
check_os

echo -n "Archetecture ... "
check_kernel_arch

echo -n "Kernel version ... "
check_kernel_version

echo -n "CPU checking ... "
check_cpu

echo -n "Total Memory  ... "
check_memory_total

echo -n "Swap off ... "
check_swap

echo -n "Checking disks ... "
check_disk

# check Kernel parameters
check_sysctl

check_ulimit
