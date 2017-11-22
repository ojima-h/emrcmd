# emrcmd

EMR utility command

## Install

```
go get github.com/ojima-h/emrcmd
```

or

```
curl -L https://github.com/ojima-h/emrcmd/releases/download/v0.0.1/emrcmd-0.0.1.linux-amd64 > emrcmd
chmod +x emrcmd
```

Next, put your EMR config template to ~/.emrcmd-cluster.yml.

This is sample configuration file:

```yaml

---
name: {{name}}

releaselabel: emr-5.9.0

servicerole: EMR_DefaultRole
jobflowrole: EMR_EC2_DefaultRole

instances:
  ec2subnetid: subnet-00000000
  keepjobflowalivewhennosteps: true

  instancegroups:
  - name: master
    instancerole: MASTER
    instancetype: m3.xlarge
    instancecount: 1
    market: SPOT
    bidprice: '0.5'
  - name: core
    instancerole: CORE
    instancetype: m3.xlarge
    instancecount: {{lookup "core" 1}}
    market: SPOT
    bidprice: '0.5'
  - name: task
    instancerole: TASK
    instancetype: m3.xlarge
    instancecount: 0
    market: SPOT
    bidprice: '0.5'

visibletoallusers: true
tags:
- key: Name
  value: EMR-{{name}}

applications:
- name: Hadoop
- name: Hive
- name: Tez

configurations:
- classification: hive-site
  properties:
    hive.exec.parallel: 'true'
    hive.exec.compress.output: 'true'
```

## Usage

```
NAME:
   emrcmd - An EMR utility command

USAGE:
   emrcmd [global options] command [command options] [arguments...]

VERSION:
   0.0.0

COMMANDS:
     start, up            start new EMR cluster
     list, ls             list EMR clusters
     resize               resize an EMR instance group
     terminate, rm, down  terminate EMR cluster
     ssh                  ssh to EMR cluster
     scp                  copy files from/to EMR cluster
     shell                set master uri to EMR_MASTER environment variable
     init                 print initialization script for shell helper
     help, h              Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --help, -h     show help
   --version, -v  print the version
```

## Example

```
# start new cluster named "foo"
emrcmd start foo

# start new cluster with 2 core instances
emrcmd start foo core=2

# resize task instance group size to 3
emrcmd resize foo task 3

# ssh to "foo" master
emrcmd ssh foo

# copy a local file to "foo" master
emrcmd scp foo localfile @:remotefile

# copy a remote directory on "foo" master to local
emrcmd scp foo -r @:remotedir .

# termiante the cluster
emrcmd terminate foo

# execute shell in the enviroment where the EMR master DNS name is set to EMR_MASTER.
emrcmd shell foo bash

# export the EMR master DNS name to EMR_MASTER
eval "$(emrcmd init)"
emrcmd shell foo
```

## Template

The following template functions are available:

- `name`

    returns cluster name

- `lookup "KEY" DEFAULT`

    fetch the value from given variables. If the `KEY` is not given, it returns `DEFAULT`.
