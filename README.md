# es-actors

## es-server
```
sbt "run-main com.broilogabriel.Server"
```

## es-client

Usage: es-client [options]

  -i, --index <index>      
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

```
sbt "run-main com.broilogabriel.Client \
        --index=index_name \
        --sourceCluster=cluster_name \
        --targetCluster=cluster_name"
```
