# es-actors

## es-server
```
sbt "run-main com.broilogabriel.Server"
```

## es-client

```
Usage: es-client [options]

  -i, --indices <index1>,<index2>...
                           
  -d, --dateRange:<start_date>=<end_date>
                           Start date value should be lower than end date.
  -s, --source <source_address>
                           default value 'localhost'
  -p, --sourcePort <source_port>
                           default value 9300
  -c, --sourceCluster <source_cluster>
                           
  -t, --target <target_address>
                           default value 'localhost'
  -r, --targetPort <target_port>
                           default value 9301
  -u, --targetCluster <target_cluster>
                           
  --remoteAddress <remote_address>
                           
  --remotePort <remote_port>
                           
  --remoteName <remote_name>
                           
  --help                   Prints the usage text.
```

Example:
```

sbt "run-main com.broilogabriel.Client --help"

sbt "run-main com.broilogabriel.Client \
        --indices=index_name \
        --sourceCluster=cluster_name \
        --targetCluster=cluster_name"

sbt "run-main com.broilogabriel.Client \
        -d:2016-10-1=2016-10-25 \
        -c=cluster_name \
        -u=cluster_name"

```
