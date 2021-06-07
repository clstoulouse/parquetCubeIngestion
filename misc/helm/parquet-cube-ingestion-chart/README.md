# *parquet-cube-ingestion-chart* Helm Chart

This Helm chart will deploy the netcdf ingestion pipeline.

## Dependencies

This helm chart depends on the following helm charts and assumes they
are already installed:
+ [parquet-cube-storage](../parquet-cube-storage)

## Resources

This Helm chart will create the following resources:
+ The parquet cube crawler as kubernetes
  *Deployments*.
  
### Quick refresher

The crawlers are components that consume netcdf files from the
`nc-input` folder of bigdata metoc NFS volume, and transform data to the
parquet format to write them in the HDFS distributed storage.

These netcdf files are typically pushed via ftp to the NFS volume by CLS
datastore.

The crawlers are scalable at will, and each instance will consume one
file at a time. A locking mechanism is used to ensure that the instances
are isolated and will not try to consume the same file at the same time.

For now there is one type of crawlers:
+ normal crawlers: those are simple mono-thread java processes, they are
  fine for most datasets.
  
## KDL Diagram

![](kdl.png)

## Hooks

The following routines are executed as a kubernetes job whenever the
helm chart is deployed or upgraded:

### Create folders

This routine will create the following input and output folders involved
in the ingestion process, if they don't exist:
+ The input folder for each dataset under `nc-input` directory (where
  the CLS datastore will put netcdf files) in the metoc NFS volume.
+ The output folder for each dataset where the parquet files and the
  index files will be written.
+ The `locks` folder in the metoc NFS volume, that will be used to make
  the locking mechanism between the crawlers work.

If the creation of any folder fails, the routine fails which will
interrupt the helm release installation/upgrade.

## Configuration


### Image pull settings

See [common configuration
sections](../docs/common-configs.md#pull-settings)
 
### Security settings

See [common configuration
sections](../docs/common-configs.md#security-settings)

### Logging

See [common configuration
sections](../docs/common-configs.md#logging-settings)

### Metoc Volume settings

See [common configuration
sections](../docs/common-configs.md#metoc-volume-settings)

Additionally the following configuration keys are added to this section
in the current helm chart.

| key                                         | Description                            | Default value |
|---------------------------------------------|----------------------------------------|---------------|
| `volumes.nfsMetocVolume.subFolders.ncInput` | Relative path of the `nc-input` folder | `nc-input`    |
| `volumes.nfsMetocVolume.subFolders.locks`   | Relative path of the `locks` folder    | `locks`       |

### Hadoop client settings

See [common configuration
sections](../docs/common-configs.md#hadoop-settings)

### Datasets

See [common configuration
sections](../docs/common-configs.md#dataset-settings)

In addition, this component will require additional configuration keys
for each dataset, to parametrize the ingestion.

| key                                                   | Description                                                                                                                                                                                                                                                                        | Default value |
|-------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `datasets.(dataset-id).ingestionMode`                 | type of crawler to use for the dataset. For now the only accepted value is `local`.                                                                                                                                                       |               |
| `datasets.(dataset-id).excludedVariables`             | (optional) list of variables to exclude as a yaml list                                                                                                                                                                                                                             | `[]`          |
| `datasets.(dataset-id).rounding.coordinatesPrecision` | (optional) coordinates precision (expressed in *digits after decimal*), basically the coordinates will be rounded to this precision when ingesting                                                                                                                                 | `5`           |
| `datasets.(dataset-id).rounding.roundingMode`         | (optional) Rounding type to use on coordinates during the ingestion, acceptable values are: RoundUp, RoundDown, RoundCeiling, RoundFloor, RoundHalfUp, RoundHalfDown, RoundHalfEven, RoundUnnecessary, see <https://docs.oracle.com/javase/7/docs/api/java/math/RoundingMode.html> | `RoundUp`     |

#### Example

```yaml
datasetsFolder: hdfs://big-namenode1.bigdata.cls.fr/qt/data/metoc/datasets
datasets:
  "dataset-1":
    relativeFolder: my-collection-1/dataset1
    ingestionMode: local
    rounding:
      coordinatesPrecision: 2
      roundingMode: RoundHalfUp
  "dataset-2":
    relativeFolder: my-collection-1/dataset2
    ingestionMode: local
    excludedVariables:
    - "var-1"
    - "var-2"
```

### Common ingestion configuration

Configuration keys common to both crawlers.

| key                                    | Description                                                                                                                                       | Default value |
|----------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `ingestion.shuffleDatasets`            | set true if you need the crawler to treat the datasets in random order                                                                            | `false`       |
| `ingestion.inProgressFolderName`       | Name of the sub-folder where in-progress datasets will be moved                                                                                   | `.inprogress` |
| `ingestion.rootSuccessFolder`          | URL-Path where successfully processed files can be moved (organized by dataset name), if empty those files will not be archived                   | ``            |
| `ingestion.removeSucceededSourceFiles` | If set to `true` the files will be deleted from the inprogress folder when the ingestion succeeds                                                 | `false`       |
| `ingestion.rootFailureFolder`          | URL-Path where files that failed to be process can be moved (organized by dataset name), if empty those files will not be archived                | ``            |
| `ingestion.removeFailedSourceFiles`    | If set to `true` the files will be deleted from the inprogress folder when the ingestion fails                                                    | `false`       |
| `ingestion.crawlingPeriod`             | duration between two crawling iterations, in the format described here <https://github.com/lightbend/config/blob/master/HOCON.md#duration-format> | `10 seconds`  |

### Regular crawlers configuration

| key                    | Description                                                                                                                | Default value |
|------------------------|----------------------------------------------------------------------------------------------------------------------------|---------------|
| `crawler.replicaCount` | Number of replicas to deploy                                                                                               | `1`           |
| `crawler.resources`    | (optional) Resources to allocate to each instance of the crawler, see [here](../docs/common-configs.md#resources-settings) |               |
