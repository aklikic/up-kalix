# Introduction
This is Kalix based implementation of uProtocol cloud platform (dispatcher, subscription manager, utwin and udiscovery)
# Start 
## Start the infrastructure components  
```shell
docker compose -f docker-compose-kafka.yml up
```
## Create kafka topic
Open [Kafka UI](http://localhost:8081/) in web brower
```
vehicle-in
```
```
vehicle-out
```
```
cloudservice-in
```
```
cloudservice-out-1
```
```
cloudservice-out-2
```

## Start the service
```shell
sbt run
```

# Test locally

## Provision new Cloud Service entity
### Add
```shell
grpcurl -plaintext -d '{
  "entityName": "myApp1",
  "outputId": 1
}' localhost:9000 com.lightbend.uprotocol.UEntityMetadatas/AddUEntityMetadata
```

```shell
grpcurl -plaintext -d '{
  "entityName": "myApp2",
  "outputId": 2
}' localhost:9000 com.lightbend.uprotocol.UEntityMetadatas/AddUEntityMetadata
```

### Validate provisioning
```shell
grpcurl -plaintext -d '{
  "entityName": "myApp1"
}' localhost:9000 com.lightbend.uprotocol.UEntityMetadatas/FetchUEntityMetadata
```
```shell
grpcurl -plaintext -d '{
  "entityName": "myApp2"
}' localhost:9000 com.lightbend.uprotocol.UEntityMetadatas/FetchUEntityMetadata
```

## Subscribe Cloud Service UEntity
### Send Subscribe Request

```json
{
  "specversion":"1.0",
  "id":"1ef4426e-a1f0-6f86-ace8-8997402d6307",
  "source":"//cs.uprotocol.kalix.io/myApp1",
  "type":"req.v1",
  "datacontenttype":"application/protobuf",
  "sink":"//111111/core.usubscription/3/rpc.Subscribe",
  "priority":"CS4",
  "ttl":6000000,
  "data_base64":"CjcKCAoGMTExMTExEg8KC2JvZHkuYWNjZXNzEAEaGgoEZG9vchIKZnJvbnRfbGVmdBoERG9vciABEicKJQoXChVjcy51cHJvdG9jb2wua2FsaXguaW8SCgoGbXlBcHAxEAE="
}
```
```json
{
  "specversion":"1.0",
  "id":"1ef4426e-a92e-60c8-9e09-112fdd67fd73",
  "source":"//cs.uprotocol.kalix.io/myApp2",
  "type":"req.v1",
  "datacontenttype":"application/protobuf",
  "sink":"//111111/core.usubscription/3/rpc.Subscribe",
  "priority":"CS4",
  "ttl":6000000,
  "data_base64":"CjcKCAoGMTExMTExEg8KC2JvZHkuYWNjZXNzEAEaGgoEZG9vchIKZnJvbnRfbGVmdBoERG9vciABEicKJQoXChVjcy51cHJvdG9jb2wua2FsaXguaW8SCgoGbXlBcHAyEAE="
}
```
### Check subscription
```shell
grpcurl -plaintext -d '{
  "topic": "//111111/body.access//door.front_left#Door",
  "offset": 0
}' localhost:9000 com.lightbend.uprotocol.Topics/FetchSubscribers
```


## Send Publish Message Cloud Service UEntity is subscribed to
```json
{
  "specversion":"1.0",
  "id":"1ef4426e-aaff-6492-bfa8-4fd02079965f",
  "source":"//111111/body.access//door.front_left#Door",
  "type":"pub.v1",
  "datacontenttype":"application/protobuf",
  "priority":"CS4",
  "ttl":6000000,
  "data_base64":"Q29kZTogT0s="
}
```
