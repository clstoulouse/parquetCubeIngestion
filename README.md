# parquetCubeIngestion
This repository is providing the source code and documentation about the Parquet Cube Ingestion described in the GMD publication "A Parquet Cube alternative to store gridded data for data analytics and modeling".

# Overview
Parquet Cube's goal is to  allow the transformation of [NetCDF](https://en.wikipedia.org/wiki/NetCDF) 
data files into the [Apache Parquet](https://en.wikipedia.org/wiki/Apache_Parquet). 
format, and then store these parquet files in an Hadoop Distributed File System (HDFS)
 storage to make them available for further processing in a big data ecosystem.
 
This project contains the source code for the NetCDF to Parquet transformation, and the possibility to launch the transformation manually in a local environment
 
It also contains the ressources to deploy the transformation and ingestion of NetCDF files to Parquet 
 in an HDFS storage as a kubernetes deployments. Included are the the source code for building the according docker images and helm charts.

# Building Parquet Cube project
After cloning the project in your local directory, go inside the parent directory and compile the project :

```
cd parquet-cube-parent
mvn clean install
```

# Building the docker images
The project contains 3 docker images :
+ base-bigdata-java : the base image to interact with the big data platform
+ hadoop-tools : the base image to interact with HDFS
+ parquet-cube-crawler : the image containing the transformation and ingestion code

To build and push these images in your docker registry:
```
cd parquet-cube-parent
mvn docker:build docker:push
```

# Building the helm chart

```
cd misc/helm
mvn deploy
```

# Deploy using the helm chart
Change the values.yaml inside the deployment/dev folder to your configuration and launch:

```
cd deployment/dev
../cicd/deploy.sh upstall
```

#Set up local environment

