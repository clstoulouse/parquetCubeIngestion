# Convert NetCDF files to Parquet locally
You can run transformation of NetCDF files to parquet locally using the script [run-netcdf-to-parquet.sh](./dev-local/script/run-netcdf-to-parquet.sh)
 inside the dev-local/script folder.
 
 The script needs an [application.conf](script/conf/application.conf) configuration file that will be passed through as a script argument. 


### application.conf

The folder contains an [application.conf](script/conf/application.conf)
that contains the configuration of your application in the local
environment.
 
You first have to define the fr.cls.bigdata.dev-local-path parameter that has to point to the absolute path of your
`dev-local` folder. 

The local crawler is configured by default to look for netcdf files
inside [nc-input folder](./nc-input) and write the generated
parquet and index files to [nc-output folder](./nc-output). The
ingested netcdf files are archived in
[nc-raw/success](./nc-raw/success) or
[nc-raw/failure](./nc-raw/failure) depending on the outcome.

In the [application.conf](./script/conf/application.conf), you can find and
edit the list of datasets to crawl and support. In order to add a new
dataset:
+ Add its input folder under:
  `fr.cls.bigdata.metoc.netcdf.crawler.datasets.<dataset-id>`  
    + Add its `input-folder`
    + Add `mode=local` for your dataset.
+ Add its parquet and index folders under:
  `fr.cls.bigdata.metoc.datasets.<dataset-id>`
  
  + Add its `data-folder`
  + Add its `index-folder`

There are pre-configured datasets in the
[application.conf](./script/conf/application.conf) you can check their
configuration for examples.

*Some default parameters can be found in the crawler's [reference.conf](../core/parquet-cube-ingestion/src/main/resources/reference.conf)*

### logging configuration

A [logback.xml](.script/conf/logback.xml) is also available to configure
logging, by default everything is logged at `DEBUG` level on the
standard output.

## Running the netcdf crawler script locally

Before running the script, you have to :
+ [Build the project](../README.md#building-parquet-cube-project)
+ [Build the docker images](../README.md#building-the-docker-images)

Then, in a bash command window, run the script with the `-Dconfig.file=` parameter pointing to your application.conf
```
./run-netcdf-to-parquet.sh -Dconfig.file=./conf/application.conf
```

If you edited the log configuration in the logback.xml, you can pass it as well as a parameter :
```
./run-netcdf-to-parquet.sh -Dconfig.file=./conf/application.conf -Dlogback.configurationFile=./conf/logback.xml
```
 
Your input folders will automatically be created and the crawler will start crawling these folders according to the crawling period (crawling-period in the application.conf, 30 seconds by default). 

Add your NetCDF folder to your input folder.

When the next crawling period starts, your NetCDF file will be converted to parquet.

If the transformation is successful, you will find in the output data folder your parquet file. It is placed inside a tspartday folder corresponding to the daynumber of the NetCDF data file date. 
For example, if the NetCDF data date is 2021-01-01, its tspartday will be 2021001. For 2021-06-01, tspartday=2021152.
This tspartday is a column added to the parquet and can be used as a partitioning key.
The index folder contains the metadata linked to the ingested NetCDF folder.

The crawling will then go on to transform new files if needed.

