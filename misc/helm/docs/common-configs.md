# Common configuration sections

## <a name="pull-settings" />Image pull settings 

| key                    | Description                                                                                                                                                                                                                          | Default value |
|------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `imagePullSecret.name` | (optional) if defined, this configuration key should refer to the name of a [docker pull secret](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry) to use to pull docker images to deploy on k8s |               |
| `imagePullPolicy`      | (optional) use this key to define an [image pull policy](https://kubernetes.io/docs/concepts/containers/images/#updating-images) for all pods deployed in this chart                                                                 |               |

### Example

```yaml
imagePullSecret:
  name: docker-registry

imagePullPolicy: Always
```

## <a name="security-settings" />Security settings

This section of the configuration is used to customize the user and
group used to run the container inside the pods deployed by a helm
chart.

| key                         | Description                                                                             | Default value    |
|-----------------------------|-----------------------------------------------------------------------------------------|------------------|
| `securityContext.enabled`   | If `true` kubernetes security context section is added to all pods created by the chart | `false`          |
| `securityContext.runAsUser` | User id to be used to run the processes inside pods                                     | *uid of metocqt* |
| `securityContext.fsGroup`   | Group id to be used to mount the volumes inside the pods                                | *gid of metocqt* |
| `securityContext.userName`  | (optional) User name to be used to run the processes inside pods                        |                  |
| `securityContext.groupName` | (optional) Group name to be used to mount the volumes inside the pods                   |                  |


### Example

```yaml
securityContext:
  enabled: true
  userName: metocdev
  groupName: metoc
  runAsUser: 11242
  fsGroup: 10646
```

## <a name="metoc-volume-settings" />Metoc Volume settings

### Persistent volume settings

| key                                | Description                                                                             | Default value      |
|------------------------------------|-----------------------------------------------------------------------------------------|--------------------|
| `volumes.nfsMetocVolume.name`      | Name of the NFS volume PVC                                                              | `pvc-nfs-metoc`    |
| `volumes.nfsMetocVolume.subPath`   | Subpath of the NFS volume that will be used (to use the root of the NFS volume use `.`) | `qt`               |
| `volumes.nfsMetocVolume.size`      | Size of the volume                                                                      | `5Gi`              |



## <a name="logging-settings" />Logging

| key                    | Description                                                                                                                               | Default value                  |
|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------|
| `log.root.level`       | Logging level to apply to the root logger                                                                                                 | `INFO`                         |
| `log.stdout.enabled`   | If `true` the standard output logs appender will be activated                                                                             | `true`                         |
| `log.stdout.level`     | Level of logs to apply on the standard output logs appender                                                                               | `INFO`                         |
| `log.logstash.enabled` | If `true` the logstash logs appender will be activated                                                                                    | `false`                        |
| `log.logstash.level`   | Level of logs to apply on the logstash logs appender                                                                                      | `INFO`                         |
| `log.logstash.host`    | Hostname of the logstash socket to send the logs to                                                                                       | `big-namenode1.bigdata.cls.fr` |
| `log.logstash.port`    | Port  of the logstash socket to send the logs to                                                                                          | `10515`                        |
| `log.loggers`          | Section of the configuration where you can restrict the logging level by package. In key (FQ package name), and value (log level) format. |                                |

### Example

```yaml
log:
  root:
    level: INFO
  logstash:
    enabled: false
    host: big-namenode1.bigdata.cls.fr
    port: 10515
    level: INFO
  stdout:
    enabled: true
    level: INFO
  loggers:
    "org.apache.parquet": INFO
```

## <a name="dataset-settings" />Datasets section

The `datasets` section is where the datasets managed by bigdata metoc
are listed.

Each dataset is identified by a unique id that we will refer to as
`(dataset-id)`, and will appear as a sub-section of `datasets`.

The following table describes the configuration keys applicable to every
dataset.

| key                                    | Description                                                                                                                                                                                | Default value                                                |
|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| `datasetsFolder`                       | Path in the distributed storage under which the parquet data and the indexes of the datasets can be found                                                                                  | `hdfs://big-namenode1.bigdata.cls.fr/qt/data/metoc/datasets` |
| `datasets.(dataset-id).relativeFolder` | Relative path to append to `datasetsFolder` in order to retrieve the full absolute data/index path of the datasets (the folder under which we can find the `data` and `index` directories) |                                                              |

### Example

```yaml
datasetsFolder: hdfs://big-namenode1.bigdata.cls.fr/qt/data/metoc/datasets
datasets:
  "dataset-1":
    relativeFolder: my-collection-1/dataset1
  "dataset-2":
    relativeFolder: my-collection-1/dataset2
  "dataset-3":
    relativeFolder: my-collection-2/dataset1
```

## <a name="component-settings" />Common components settings

These sections of configuration are always associated with kubernetes
deployments, they allow to customize the associated resources.

### <a name="resources-settings" />Resource settings

A section of the configuration where
[resources request/limit](https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/)
of a pod can be configured.

### <a name="service-settings" />Service settings

A section of the configuration where
[a kubernetes service](https://kubernetes.io/docs/concepts/services-networking/service/)
corresponding to a pod can be customized.

| key                             | Description                                                                                                        | Default value |
|---------------------------------|--------------------------------------------------------------------------------------------------------------------|---------------|
| `(...).service.type`            | [Service type](https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types) | `ClusterIP`   |
| `(...).service.sessionAffinity` | Kubernetes service session affinity                                                                                | `None`        |

## <a name="hadoop-settings" />Hadoop client settings

This section of the configuration is used to customize the hadoop client
configuration.

| key                      | Description                                                                                     | Default value |
|--------------------------|-------------------------------------------------------------------------------------------------|---------------|
| `hadoop.userName`        | (optional) Username to use to access HDFS (if not specified, the process username will be used) |               |
| `hadoop.configuration.*` | Hadoop configuration to pass to all hadoop clients using the chart                              |               |

### Example

```yaml
hadoop:
  userName: my-user-name
  configuration:
    "fs.hdfs.impl": "org.apache.hadoop.hdfs.DistributedFileSystem"
    "fs.file.impl": "org.apache.hadoop.fs.LocalFileSystem"
    "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider"
    "fs.s3a.endpoint": "s3.cls.fr:9443"
    "fs.s3a.connection.ssl.enabled": true
    "fs.s3a.path.style.access": true
```
