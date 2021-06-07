# *parquet-cube-storage* Helm Chart

This Helm chart will create the PV and PVC required for all the other
bigdata metoc component. As well as a FTP server that exposes the volume
to the outside so that the CLS datastore can push netcdf files.

## Dependencies

N/A

## Resources

This Helm chart will create the following resources:
+ If `volumes.nfsMetocVolume.pvcreate` is `true`, it will create a
  *Persistent Volume*, named `volumes.nfsMetocVolume.pvname`. The persistent
  volume will be mapped to a NFS shared folder, the mapping settings can
  be customized under `volumes.nfsMetocVolume.*`.
+ A *Persistent Volume Claim* named `volumes.nfsMetocVolume.name`,
  binding to the persistent volume `volumes.nfsMetocVolume.pvname`.
+ An FTP server as a kubernetes *Deployment* along with its *Service*.
  The FTP server exposes a control port and a range of data ports.<br>
  The FTP server allows users outside the cluster to access the folders.
  
  **Note:** Due to some limitations in the FTP protocol, these ports
  **cannot** be mapped to different port numbers when exposed outside
  the cluster.

## KDL Diagram

![](kdl.png)

## Hooks

When the helm chart is installed or upgraded, the following routines are
executed as kubernetes jobs.

### create folders

This routine will mount the NFS volume from the server
`volumes.nfsMetocVolume.nfsServer` and having the path
`volumes.nfsMetocVolume.path`, and will try to create the folder
`volumes.nfsMetocVolume.subPath` under the mounted volume using the
correct user/group ownership.

This hook is here mainly to fail fast in case the root folder
corresponding to the NFS metoc volume cannot be created or accessed due
to an error in configuration or a lack of permission.

## Configuration

### Image pull settings

See [common configuration
sections](../docs/common-configs.md#pull-settings)

### Security settings

See [common configuration
sections](../docs/common-configs.md#security-settings)

### Hadoop client settings

See [common configuration
sections](../docs/common-configs.md#hadoop-settings)

### FTP Server settings

| key                                                                 | Description                                                                                                                                    | Default value                                                   |
|---------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------|
| `ftpMetoc.ident`                                                    | Description of the FTP server                                                                                                                  | Kubernetes Bigdata metoc Ftp server. Use only with passive mode |
| `ftpMetoc.user.name`                                                | Username to be used to login to the FTP server                                                                                                 | `metoc`                                                         |
| `ftpMetoc.user.password`                                            | Password to be used to login to the FTP server                                                                                                 | `metoc_ftp_password`                                            |
| `ftpMetoc.user.uid`                                                 | User id that owns the persistent volume                                                                                                        | *uid of metocqt*                                                |
| `ftpMetoc.user.gid`                                                 | Group id that owns the persistent volume                                                                                                       | *gid of metocqt*                                                |
| `ftpMetoc.advertisedHostOrIp`                                       | External hostname of the FTP server                                                                                                            | `bigdata-vip.vlandata.cls.fr`                                   |
| `ftpMetoc.ports.control`                                            | Main control port of the FTP server                                                                                                            | `10121`                                                         |
| `ftpMetoc.ports.passiveRange.min`-`ftpMetoc.ports.passiveRange.max` | Range of port to allocate to FTP passive mode                                                                                                  | `10122-10124`                                                   |
| `ftpMetoc.resources`                                                | Section of the configuration where the min/max of memory/cpu usage can be customized, see [here](../docs/common-configs.md#logging-settings)   |                                                                 |
| `ftpMetoc.service`                                                  | Section of the configuration where the associated kubernetes service can be customized, see [here](../docs/common-configs.md#service-settings) |                                                                 |

### Persistent volume settings

See [common configuration
sections](../docs/common-configs.md#metoc-volume-settings)

Additionally the following configuration keys are added to this section
in the current helm chart.

| key                                       | Description                                                   | Default value  |
|-------------------------------------------|---------------------------------------------------------------|----------------|
| `volumes.nfsMetocVolume.pvname`           | Name of the NFS volume PV                                     | `pv-nfs-metoc` |
| `volumes.nfsMetocVolume.pvcreate`         | If set to false the PV won't be created                       | `true`         |
| `volumes.nfsMetocVolume.nfsServer`        | IP or hostname of the NFS server                              | `2.2.2.2`      |
| `volumes.nfsMetocVolume.path`             | NFS path to mount                                             | `/tomodif`     |
| `volumes.nfsMetocVolume.storageClassName` | If set a storage class name should be added to the pv and pvc | None           |



## Storage and volumes

### NFS Volume

For each environment QT, QO, PROD. An NFS volume is provisionned.

This volume serves as an exchange volume mainly with the datastore.

The datastore (or a manual action) will push raw netcdf files to this volume (using FTP protocol),
and the crawler will inspect it in order to retrieve new netcdf files to
process and transform to parquet format.

The helm package `bigdata-metoc-storage` is used to create kubernetes
 persistent volumes (pv) and persistent volume claims (pvc), so this NFS
 storage can be used inside the kubernetes cluster.
 It also deploys an instance of an FTP server that allows to access the volume
 from the outside.

### The creation of NFS volumes

The creation of a new NFS volume is the responsibility of the bigdata platform infrastructure team.
The request for the creation should go through a CLS Jira ticket in the project https://jira-ext.cls.fr/projects/PBC

The Jira ticket should mention the size of the volume to create. And the
 User id/Group id of the owner.

In return, the response of the infrastructure team should include:
+ Ip or hostname of the NFS server
+ Path to the NFS volume

### Deploying and configuring `bigdata-metoc-storage` helm package

The Helm package `bigdata-metoc-storage` allows to create kubernetes
 PV and PVC and link them to the volumes created by the procedure above.

#### Configuration

A configuration file should be provided during the installation, it allows
 the customization of parameters.

The configuration files for QT, QO and PROD are maintained under: https://gitlab.cls.fr/bigdata/bigdata-metoc-deployment

The configuration of the storage looks like this:
```yaml
ftpMetoc: # Configuration of the FTP server to expose metoc volume
  user:
    name: metoc # username to use when logging in to the FTP server
    password: metoc_ftp_password # password to use when logging in to the FTP server
    uid: 10069 # posix user id of the user that owns the files in the volume
    gid: 10072 # posix group id of the user that owns the files in the volume
  advertisedHostOrIp: bigdata-vip.vlandata.cls.fr # External hostname of the FTP server
  ports:
    control: 10121 # Main port for the FTP server
    passiveRange: # Range of ports to use in passive mode by the FTP server
      min: 10122
      max: 10124

volumes:
  nfsMetocVolume:
    name: pvc-nfs-metoc-qt # Name of the kubernetes PVC (should be different for each deployment)
    pvname: pv-nfs-metoc-qt # Name of the kubernetes PV (should be different for each deployment)
    nfsServer: 192.168.97.1 # IP or hostname of the NFS server
    path: /data/prod/ais # NFS mount path
    size: 5Gi # Size of the NFS volume
    subPath: metoc/qt # sub-folder inside the NFS volume to use by your deployment (allows to use the same concrete NFS volume for different deployments)
```

#### Deployment

```bash
helm install <bigdata-metoc-storage-helm-package> --wait --name <release-name> --namespace <namespace> -f <values-file>
```

### Exposing the FTP server outside of the kubernetes cluster

The FTP server deployed by the helm package `bigdata-metoc-storage` is
 natively accessible inside the kubernetes cluster (thanks to the addition
 of a kubernetes service).

In order for the FTP service to be accessible from the outside (so the
 datastore team can push data to it), we need to manually create an L4
 load balancer from the rancher UI.

+ Take note the ftp server service name, in order to do so, you can issue the following command:
  ```bash
  kubectl get svc -n <namespace>
  ```
  The service name is usually constructed this way: `<helm-release-name>-ftp-metoc-svc`

  For example: `bigdata-metoc-storage-qt-ftp-metoc-svc` in QT env.
+ Issue the following command:
  ```bash
   kubectl describe svc -n <namespace> <ftp-service-name> | grep '^TargetPort:.*/TCP$' | sed 's,TargetPort:[[:space:]]\+\([[:digit:]]\+\)/TCP,\1,'
  ```
  The command will print out the list of ports to configure in your load balancer.
+ Open rancher UI in a browser and log in with your credentials:
  http://big-namenode1.vlandata.cls.fr:9080/
+ Navigate in the UI to `Kubernetes (in the top menu) | Infrastructure Stacks | kubernetes-ingress-lbs | Add Service (drop down list) | Add Load Balancer`
+ Fill in the from as following:
  + `Scale`: `Always run one instance of this container on every host`
  + `Name`: Choose a significant name for the load balancer (example: `ftp-metoc-qt`)
  + Port Rule: Add as many `Service Rule` in this list as there are ports listed in the previous step.
    Each line should correspond to a port, with the following values:
    + `Access`: `Public`
    + `Protocol`: `TCP`
    + `Request Host`: `n/a`
    + `Port`: Corresponding Port
    + `Path`: `n/a`
    + `Target`: `<ftp-service-name>` (choose from the list)
    + `Port`: Corresponding Port (the same as the previous field)
  + In the tab `Custom haproxy.cfg`, put the following:
  ```
  defaults
      timeout client 1800000
      timeout connect 5000
      timeout server 1800000
  ```
  + In the tab `Scheduling`, add the following condition:
    + Condition: `must`
    + Field: `host label`
    + Key: `loadbalancer`
    + Value: `true`
  + Validate the form and wait for the load balancer to have the state `Active`
