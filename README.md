aspike_discover
=====

Implementation of Aerospike Cluster Discovery in Erlang

Uses Aerospike Binary protocol, ```aspike-protocol```, https://github.com/vsavkov/aspike-protocol

Cluster Discovery is a process to:
- build list of the cluster nodes;
- build list of namespaces in the cluster;
- find out how the namespaces are replicated across nodes,
  i.e. how the namespaces are mapped (distributed) across the nodes.

#### Diagram 1. Namespace-Partitions-Replicas-Nodes

```
                             =========================================
                             |            Namespace 'Test'           |
                             |---------------------------------------|
                             | K1                                    |
                             | K2                                    |
                             =========================================
                                                |
                                   Assignment of keys to partitions
                                                |
                                                V
                             =========================================
                             |    | K1 |    |    | K2 |    |    |    |
                             |---------------------------------------|
                             | P0 | P1 | P2 | P3 | P4 | P5 | P6 | P7 |
                             =========================================
                                                |
                                Assignment of partitions to replicas
                                                |
                                -------------------------------------
                                |                                   |
                                V                                   V
[====== Replica1 =============================]   [============================= Replica2 ======]
================  ===========  ================   ===========  ================  ================
| P0 | P1 | P2 |  | P3 | P4 |  | P5 | P6 | P7 |   | P4 | P7 |  | P0 | P3 | P6 |  | P1 | P2 | P5 |
|--------------|  |---------|  |--------------|   |---------|  |--------------|  |--------------|
|    Node1     |  |  Node2  |  |    Node3     |   |  Node1  |  |    Node2     |  |    Node3     |
|--------------|  |---------|  |--------------|   |---------|  |--------------|  |--------------|
|    | K1 |    |  |    | K2 |  |    |    |    |   | K2 |    |  |    |    |    |  | K1 |    |    |
================  ===========  ================   ===========  ================  ================

Node1:
  Test1: - Namespace name
    Replica1: 11100000 - Node1 participates in partitions P0, P1, P2 for Replica1
    Replica2: 00001001 - Node1 participates in partitions P4, P7     for Replica2

Node2:
  Test1: - Namespace name
    Replica1: 00011000 - Node2 participates in partitions P3, P4     for Replica1
    Replica2: 10010010 - Node1 participates in partitions P0, P3, P6 for Replica2

Node3:
  Test1: - Namespace name
    Replica1: 00000111 - Node3 participates in partitions P5, P6, P7 for Replica1
    Replica2: 01100100 - Node1 participates in partitions P1, P2, P5 for Replica2

```

_Steps to perform the cluster discovery_, see:

    https://github.com/vsavkov/aspike-protocol/blob/master/doc/Aerospike_cluster_discovery.txt

Build
-----

    $ rebar3 compile

Examples
--------
```bash
$ rebar3 shell
Erlang/OTP...
```
### ``info`` function usage
``info`` is the main tool to query cluster for Configuration and Statistics of the Aerospike Cluster.

For the details about what queries are available, see https://aerospike.com/docs/server/reference/info

First, setup connection parameters: node IP address and port, user's name and password.

We will assume the Aerospike node is accessible on ``192.168.0.1:3000``, the user's name is ``User`` with password ``password``.

```erlang
1> Ip = "192.168.0.1", Port = 3000, User = "User", Password = "password", ok.
2> Encrypted_password = aspike_blowfish:crypt(Password).
<<"$2a$10$7EqJtq98hPqEX7fNZaFWoOqUH6KN8IKy3Yk5Kz..RHGKNAUCoP/LG">>
```
WARNING! ``aspike_blowfish:crypt/1`` is slow, could take 2-5 seconds.

##### Query Aerospike Server build
```erlang
3> aspike_discover:info(Ip, Port, User, Encrypted_password, ["build"]).
{ok,[{<<"build">>,<<"6.1.0.1">>},{<<>>}],<<>>}
```

##### Query Aerospike Node name
```erlang
4> aspike_discover:info(Ip, Port, User, Encrypted_password, ["node"]).
{ok,[{<<"node">>,<<"Name assigned to the node">>},{<<>>}],<<>>}
```

##### Query Aerospike Cluster namespaces
```erlang
5> aspike_discover:info(Ip, Port, User, Encrypted_password, ["namespaces"]).
{ok,[{<<"namespaces">>,
      <<"namespace-1;another-namespace;...;namespace-N">>},
     {<<>>}],
    <<>>}
```

## Cluster Discovery Process

as described in https://github.com/vsavkov/aspike-protocol/blob/master/doc/Aerospike_cluster_discovery.txt

##### Assumptions

Cluster Seed IP and port: 192.168.0.1:3000;

User/password: "User"/"password".

```erlang
1> Seed = "192.168.0.1", Port = 3000, User = "User", Password = "password", ok.
2> Encrypted_password = aspike_blowfish:crypt(Password).
<<"$2a$10$7EqJtq98hPqEX7fNZaFWoOqUH6KN8IKy3Yk5Kz..RHGKNAUCoP/LG">>
3> Access_node = aspike_discover:access_node(Seed, Port, User, Encrypted_password).
{ok,{<<"192.168.0.3">>,3000}}
4> {ok, {Access_node_Ip, Access_node_port}} = Access_node.
5> Nodes = aspike_discover:nodes(Access_node_Ip, Access_node_port, User, Encrypted_password).
[{<<"A2">>,<<"192.168.0.2">>,3000},
{<<"A5">>,<<"192.168.0.1">>,3000},
{<<"A1">>,<<"192.168.0.4">>,3000},
{<<"A3">>,<<"192.168.0.5">>,3000},
{<<"A4">>,<<"192.168.0.3">>,3000}]
6> Namespace_replica_node = aspike_discover:namespace_replica_node(Nodes, User, Encrypted_password).
#{
  <<"ns-1">> => {2,4096,
    [#{2902 => {<<"A1">>,<<"192.168.0.4">>,3000},
      3101 => {{<<"A2">>,<<"192.168.0.2">>,3000},
      3164 => {<<"A3">>,<<"192.168.0.5">>,3000},
      2957 => {<<"A4">>,<<"192.168.0.3">>,3000},
      1358 => {<<"A5">>,<<"192.168.0.1">>,3000},
      1196 => {<<"A2">>,<<"192.168.0.2">>,3000},
      747 => {<<"A4">>,<<"192.168.0.3">>,3000},
      3885 => {<<"A3">>,<<"192.168.0.5">>,3000},
      3755 => {<<"A5">>,<<"192.168.0.1">>,3000},...,
    #{2902 => {<<"A5">>,<<"192.168.0.1">>,3000},
      3101 => {<<"A1">>,<<"192.168.0.4">>,3000},
      3164 => {<<"A4">>,<<"192.168.0.3">>,3000},
      2957 => {<<"A3">>,<<"192.168.0.5">>,3000},
      1358 => {<<"A2">>,<<"192.168.0.2">>,3000},
      1196 => {<<"A1">>,<<"192.168.0.4">>,3000},
      747 => {<<"A4">>,<<"192.168.0.3">>,3000},
      3885 => {<<"A5">>,<<"192.168.0.1">>,3000},
      3755 => {<<"A3">>,<<"192.168.0.5">>,3000},...}]},
  <<"ns-2">> => ...}
```

## Cluster Discovery Result

``aspike_discover:namespace_replica_node/3`` output shows the cluster structure:
- Namespace ``ns-1`` has ``2`` replicas and ``4096`` partitions;
- Namespace ``ns-1``, ``replica 1``:
  - partition ``2902`` is mapped to node ``A1, 192.168.0.4:3000``;
  - partition ``3101`` is mapped to node ``A2, 192.168.0.2:3000``;
  - ...
- Namespace ``ns-1``, ``replica 2``:
  - partition ``2902`` is mapped to node ``A5, 192.168.0.1:3000``;
  - partition ``3101`` is mapped to node ``A1, 192.168.0.4:3000``;
  - ...
- Namespace ``ns-2`` ...
