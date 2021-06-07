# Quick introduction to HELM

[Helm](https://helm.sh/) is a package manager for kubernetes
applications.

It allows the creation of packages (called *Helm charts*) that are easy
to deploy using Helm command line client (something like: `helm install
<release-name> <package-name>`).

## What is it used for?

Without HELM we will have to maintain for each environment a bunch of
yaml files having hard coded environment specific values, and we have to
keep track of a mostly manual installation procedure to deploy the
bigdata metoc components on the bigdata cluster.

All this can lead to an increasingly heavy operational complexity, and
is too error prone.

So, in order to have a more manageable and maintainable deployment
process, we use Helm charts, as a way of factorizing the numerous YAML
files into neatly self-sufficient packages.

## What is a HELM chart/release

Basically, a **Helm chart** is either a folder or a `.tar.gz` archive
having a specific file structure:

```
Chart.yaml
values.yaml
templates/
  *.yaml
```

The file:
+ `Chart.yaml`: is a file containing the metadata of a package (the name
  of the application, the version...)
+ `templates/*.yaml`: The folder `templates` contains the yaml files
  corresponding to the kubernetes resources (deployment, services,
  ingresses, configmaps...) to create when the package is deployed.
  
  These yaml files are *templatized*. So basically all previously
  hard-coded values are externalized and replaced by place holders of
  the type `{{ .Values.foo.bar }}`. Here `foo.bar` is a configuration
  key, that the user can assign a concrete value to when he deploys the
  package.
+ `values.yaml`: Is the file that summarizes all the configuration keys
  supported by this package, and defines their default values in case
  the end user does not specify any specific value when deploying the
  package.

As said before, a Helm chart is a ready-to-deploy package, and the same
package can be deployed multiple time, eventually with multiple
configurations. Every time a helm chart is deployed the user is required
to give a name to its deployment, this named deployment (ie.
instantiation of a chart) is called a **HELM release**.

For more information about HELM charts please refer to
[HELM documentation](https://helm.sh/docs/topics/charts/).

## How to use the HELM cli

You can use HELM with its command line client. The installation
procedure for the HELM command line client can be found in the [official
documentation](https://helm.sh/docs/intro/install/).

### Listing all deployed releases

```bash
helm list -n metocqo # Lists all deployed HELM releases in the namespace metocqo
```

### Deploying a HELM chart (ie. creating a HELM release)

In order to deploy a HELM chart, you should have the following inputs:
+ Path or Url to the HELM chart to install
+ A `my-values.yaml` containing all the configuration keys that you want
  to customize during the installation (you can name the file however
  you want), this configuration will override the default values
  specified in Helm chart (in its `values.yaml`).
+ A kubernetes namespace where you want to install your HELM chart.
+ A name for your release (choose whatever name you want), this name
  should be unique and must not collide with the existing release names
  in your namespace (run a `helm list -n {name-space}` to check) .

Then the following command will allow you to create the Helm Release:

```bash
helm install \
    {release-name} \
    {path-or-url-of-the-chart} \
    --namespace {name-space} \
    -f my-values.yaml
```

### Upgrading a HELM release

Once a release is created, you can use a specific command to update it
if you need to change the configuration or the chart version after the
first installation.

The following command allows to apply a the new configuration or chart
version:

```bash
helm upgrade \
    {release-name} \
    {path-or-url-of-the-chart} \
    --namespace {name-space} \
    -f my-values.yaml
```

You can also add the `--install` option to the previous command if want
helm to create a new release if it does not exist.

### Deleting a HELM release

You can uninstall a previously deployed release using the following
command:

```bash
helm delete -n {name-space} release1 release2...
```

where `release1 release2...` are the names of the release to uninstall
(from the namespace `{name-space}`).

### Can I still use kubectl?

Yes, you can and you will have to.

Any other operation can be done using `kubectl` just like before. For
example:
+ Listing pods: `kubectl get pods`
+ Getting pod's log: `kubectl logs <pod-name>`
+ Printing pod's yaml: `kubectl get pod <pod-name> -o yaml`
+ Restarting a pod: `kubectl delete pod <pod-name>`

You just have to remember to **NEVER** edit existing
pods/services/ingress manually, the creation/edition is managed by HELM.
If something should change, you should change it in the HELM charts or
in the configuration files, and apply the change using `helm upgrade` or
`helm install`.

## Developing HELM charts

### HELM templates syntax

The templating syntax for the files under the folder `templates` in Helm
charts is a special flavour of Go templates syntax. You can find a
generic purpose documentation in
[Helm website](https://helm.sh/docs/topics/chart_template_guide/).

Basically, everything having the form `{{ ... }}` is pre-processed by
Helm during the deployment, and replaced by a corresponding value.

Here are some few highlights about the templating syntax:
+ `{{ .Values.key1.key2 }}` will be replaced at deployment time by the
  value of the configuration key `key1.key2`.
+ `{{ .Release.Name }}` and `{{ .Release.Namespace }}` will be replaced
  by the release name and namespace respectively, you can find the full
  list of predefined Helm objects in the
  [official documentation](https://helm.sh/docs/topics/chart_template_guide/builtin_objects/).
+ You can embed logic in the templates using constructs like:
  + `{{ if <condition> }}something{{ end }}`: To print a string only
    when a condition (or a value in the configuration) is true.
  + `{{ .Values.key1 | default "other value" }}`: To fallback to a
    default value when a configuration key is not defined.
  + `{{ range .Values.myList }}Element: {{ . }}{{ end }}`: To introduce
    a looping behaviour on lists.
  + The documentation of control structures can be found
    [here](https://helm.sh/docs/topics/chart_template_guide/control_structures/).
+ Be careful about the difference between `{{ ... }}`, `{{- ... }}`, `{{
  ... -}}`, `{{- ... -}}`:
  
  The `-` bits added to the right or left of the curly braces are meant
  to control white space. Basically, whitespace chars next to `-` are
  deleted.
  [This chapter](https://helm.sh/docs/topics/chart_template_guide/control_structures/#controlling-whitespace)
  of the documentation is a good read to understand what's going on.
+ Usually, you will find a file `_helpers.tpl` inside the `templates`
  folder, this file contains some repeated bits of yaml, that are put
  here to avoid repetition. Those are called
  [named templates](https://helm.sh/docs/topics/chart_template_guide/named_templates/).

The official Helm documentation, lacks a full list of all functions
supported in golang template. The best documentation we've found for
this [documentation](http://masterminds.github.io/sprig/), it was made for
another project but it is basically the same syntax.

### Debugging Helm templates

You can use HELM command line client, to ask helm to render the
templates of a chart without actually deploying the application. To do
so you just need to add the options `--dry-run --debug` to a `helm
install` or a `helm upgrade` command.

```bash
helm upgrade \
    {release-name} \
    {path-or-url-of-the-chart} \
    --namespace {name-space} \
    -f my-values.yaml \
    --dry-run \
    --debug
```