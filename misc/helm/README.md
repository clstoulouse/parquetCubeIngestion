# Helm Deployment for Bigdata Metoc Platform

This folder contains the HELM charts that can be used to deploy metoc
bigdata components.

**If you are lost and don't know what Helm is**, go
[here](/docs/architecture/helm-intro.md).

## Documentation

Each HELM module contains a `README.md` file that:
+ Describes the chart.
+ Lists all its dependencies
+ Describes the main supported configuration keys (the list is not
  exhaustive, you should checkout the `src/main/helm/values.yaml` to
  have the full list).
+ A diagram describing the components and kubernetes resources deployed
  by the chart. The diagram uses the
  [KDL notation](https://blog.openshift.com/kdl-notation-kubernetes-app-deploy/).

### KDL notation additions

In the KDL diagrams available here we use some additional notations and
formalism that can't be found in the standard description:

+ A resource with a dashed border, is a resource that the chart depends
  on but does not create.
  
+ Text in <span style="border: 5px solid red;font-style:
  italic;color:gray">gray and italic</span>, refers to bits of pods
  names that are configurable.
  
## Things you need to known

### Maven modules

Each sub-folder in the current directory is a maven module that is built
into a Helm chart (as a .tar.gz archive).

The [pom.xml](pom.xml) in this directory is a maven *parent module* pom
file, that references the other helm charts as sub-modules.

For each module, the content of the folder `src/main/helm` is considered
a the *source code* of the Helm chart. During the build: 
+ The content of this folder will be processed by maven to replace some
  tokens (like `${project.version}` by the current version of bigdata
  metoc).
+ Content from [/misc/helm/common](/misc/helm/common) will be
  pre-processed and added as well.
+ The preprocessed files will be packaged into a .tar.gz, that will be
  written to the folder `target` of the module.
  
In order to manually build a helm module, just cd into the module folder
and execute the following command in a git bash `mvn clean install` (or
run it on this folder to create the packages for all the sub-modules).

If you execute `mvn deploy`, the generated `.tar.gz` files will be
pushed to nexus raw repository under:
`http://todefine.host.fr/nexus/repository/packages-to-deploy/parquet-cube/helm/`.

The target url where the archives are pushed is configurable in
[pom.xml](pom.xml).

### Common configuration keys

#### The `deploymentName` config key

All charts have a by-default null configuration key called
`deploymentName`.

+ If null, all kubernetes resources created by the chart will be
  prefixed by the release name.
+ If not null, the value of this config key will be used as a prefix
  instead.
  
#### The `imagePullSecret.name` config key

The `imagePullSecret.name` config key can be defined to apply globally a
docker pull secret on all pods (by default no secret is used).

#### The `imagePullPolicy` config key

The `imagePullPolicy` config key can be defined to apply globally a
docker pull policy on all pods (by default `IfNotPresent` is used).
