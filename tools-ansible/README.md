# Deploy TiDB enterprise tools using ansible

This guide describes how to deploy a TiDB enterprise tools(syncer/loader/mydumper,etc) using Ansible. For the production environment, it is recommended to deploy TiDB enterprise tools using Ansible.



## Overview 
Ansible is an IT automation tool that can configure systems, deploy software, and orchestrate more advanced IT tasks such as continuous deployments or zero downtime rolling updates.

Tools-Ansible is a tidb-enterprise-tools deployment tool developed by PingCAP, based on Ansible playbook. Tools-Ansible enables you to quickly deploy tidb-enterprise-tools which includes syncer, loader, mydumper.

Tools-Ansible **does not** provide components such as grafana/prometheus/node_exporter, which have been provide by [tidb-ansible](https://github.com/pingcap/tidb-ansible).

You can use tools-ansible to complete the following operation tasks:

- Download tidb-enterprise-tools package
- Deploy syncer/loader/mydumper independently or all of them together to multiplie servers.
- Start the syncer/loader/mydumper process indepently or start the integrated scripts in multiple servers.
- Stop the syncer/loader/mydumper process indepently or start the integrated scripts in multiple servers.

## Prerequisites

Before you start, make sure you have:

For target machines:

- CentOS 7.3 (64 bit) or later, x86_64 architecture (AMD64)
- Network between machines
- No SSD disk required, SAS disk is OK.
- A Control Machine that meets the following requirements:

For control mathine(can be one of the target machines, or a independent mathine).

- CentOS 7.3 (64 bit) or later with Python 2.7 installed
- Access to the Internet 


## Step 1: Install system dependencies on the Control Machine
Log in to the Control Machine using the root user account, and run the corresponding command according to your operating system.

If you use a Control Machine installed with CentOS 7, run the following command:

```
# yum -y install epel-release git curl sshpass
# yum -y install python-pip
```

If you use a Control Machine installed with Ubuntu, run the following command:

```
# apt-get -y install git curl sshpass python-pip
```

## Step 2: Create the tidb user on the Control Machine and generate the SSH key
Make sure you have logged in to the Control Machine using the root user account, and then run the following command.

Create the tidb user.

```
# useradd tidb
```

Set a password for the tidb user account.

```
# passwd tidb
```
Configure sudo without password for the tidb user account by adding tidb ALL=(ALL) NOPASSWD: ALL to the end of the sudo file:

```
# visudo
tidb ALL=(ALL) NOPASSWD: ALL
```

Generate the SSH key.

Execute the su command to switch the user from root to tidb. Create the SSH key for the tidb user account and hit the Enter key when Enter passphrase is prompted. After successful execution, the SSH private key file is /home/tidb/.ssh/id_rsa, and the SSH public key file is /home/tidb/.ssh/id_rsa.pub.

```
# su - tidb
$ ssh-keygen -t rsa
Generating public/private rsa key pair.
Enter file in which to save the key (/home/tidb/.ssh/id_rsa):
Created directory '/home/tidb/.ssh'.
Enter passphrase (empty for no passphrase):
Enter same passphrase again:
Your identification has been saved in /home/tidb/.ssh/id_rsa.
Your public key has been saved in /home/tidb/.ssh/id_rsa.pub.
The key fingerprint is:
SHA256:eIBykszR1KyECA/h0d7PRKz4fhAeli7IrVphhte7/So tidb@172.16.10.49
The key's randomart image is:
+---[RSA 2048]----+
|=+o+.o.          |
|o=o+o.oo         |
| .O.=.=          |
| . B.B +         |
|o B * B S        |
| * + * +         |
|  o + .          |
| o  E+ .         |
|o   ..+o.        |
+----[SHA256]-----+
```


## Step 3: Download tools-ansible to the Control Machine
Log in to the Control Machine using the tidb user account and enter the /home/tidb directory.

Tools-ansible playbook is a sub-directory of tidb-tools repository.

```
$ git clone  https://github.com/pingcap/tidb-tools.git
$ cd tidb-tools/tools-ansible 
```

Note: It is required to download tools-ansible to the /home/tidb directory using the tidb user account. If you download it to the /root directory, a privilege issue occurs.

## Step 4: Install Ansible and its dependencies on the Control Machine
Make sure you have logged in to the Control Machine using the `tidb` user account.

It is required to use `pip` to install Ansible and its dependencies, otherwise a compatibility issue occurs. 

Install Ansible and the dependencies on the Control Machine:

```
$ cd /home/tidb/tidb-ansible
$ sudo pip install -r ./requirements.txt
```
Ansible and the related dependencies are in the `tools-ansible/requirements.txt` file.

Check the version of Ansible:

```
$ ansible --version
ansible 2.5.0
```

## Step 5: Configure the SSH mutual trust and sudo rules on the Control Machine
Make sure you have logged in to the Control Machine using the tidb user account.

Add the IPs of your target machines to the [servers] section of the hosts.ini file.

$ cd /home/tidb/tidb-ansible
$ vi hosts.ini
[servers]
192.168.0.2
192.168.0.3
192.168.0.4
192.168.0.5
192.168.0.6
192.168.0.7
192.168.0.8

[all:vars]
username = tidb

Run the following command and input the root user account password of your target machines.

```
$ ansible-playbook -i hosts.ini create_users.yml -k
```

This step creates the `tidb` user account on the target machines, configures the sudo rules and the SSH mutual trust between the Control Machine and the target machines.


## Step 6: Edit the inventory.ini file to orchestrate the tidb-enterprise-tools


### Option 1: Deploy syncer/loader/mydumper and run it independenttly.

```
[syncer_servers]
# must specify job_name, and syncer_status_port. syncer_status_port must be same with status_addr(the port part) in the config file
syncer_1 ansible_host=172.16.10.103 job_name=1 syncer_status_port=10081

[loader_servers]
#must specify job_name, and loader_status_port. loader_status_port must be same with status_addr(the port part) in the config file
loader_1 ansible_host=172.16.10.103 job_name=1 loader_status_port=10084

[mydumper_servers]
# must specify job_name.
mydumper_1 ansible_host=172.16.10.103 job_name=1 master_host="127.0.0.1" master_port=3306 master_user="root" master_password="Pingcap1@#"



## Global variables

[all:vars]
# we will use deploy_dir/data to save mydumper data, please ensure deploy_dir is large enough to hold the dump data.
deploy_dir = /home/tidb/deploy
# ssh via normal user
ansible_user = tidb

# integrated_mode runs an integrated scripts which dumps data and loads it and finally synchronizes it.
integrated_mode = False
```



### Option 2: Deploy syncer/loader/mydumper and run them together(via integrated script).

```
[integrated_servers]
integrated_1 ansible_host=172.16.10.103 job_name=1 syncer_status_port=10081 loader_status_port=10084 master_user="root" master_password="Pingcap1@#" master_host="127.0.0.1" master_port=3306

## Global variables

[all:vars]
# we will use deploy_dir/data to save mydumper data, please ensure deploy_dir is large enough to hold the dump data.
deploy_dir = /home/tidb/deploy
# ssh via normal user
ansible_user = tidb

# integrated_mode runs an integrated scripts which dumps data and loads it and finally synchronizes it.
integrated_mode = True

```

## Step 7: Edit configuration files in the `conf` directory.

TODO


## Step 8: Deploy the tidb-enterprise-tools 

### Download the tidb-enterprise-tools 

```
ansible-playbook -i inventory.ini local_prepare.yml

```

### Deploy it 

```
ansible-playbook -i inventory.ini deploy.yml
```

If you want to deploy only syncer(or loader, mydumper), you can run it as follows:

```
ansible-playbook -i inventory.ini deploy.yml --tags syncer

```


### Start it 

By default, `start.yml` only start the integrated script.

```
ansible-playbook -i inventory.ini start.yml
```

If you want to start only syncer(or loader, mydumper), you can run it as follows:

```
ansible-playbook -i inventory.ini start.yml --tags syncer
```


### Stop it 

By default, `start.yml` only stop the integrated script.

```
ansible-playbook -i inventory.ini stop.yml
```

If you want to stop only syncer(or loader, mydumper), you can run it as follows:

```
ansible-playbook -i inventory.ini stop.yml --tags syncer
```


## Tips

Use `-k` for ssh password prompt

```
ansible-playbook -i inventory.ini -k deploy.yml
```

Use `-v` for verbose logging (`-vv`, `-vvv`, `-vvvv` for even more details)

```
ansible-playbook -i inventory.ini deploy.yml -vvv

```

### Limit

