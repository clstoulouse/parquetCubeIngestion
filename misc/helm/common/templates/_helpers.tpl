{{- define "app.name" }}
{{- default .Chart.Name .Values.nameOverride }}
{{- end }}

{{- define "chart.fullname" }}
{{- .Chart.Version | replace "+" "_" | printf "%s-%s" .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "release.id" }}
{{- default .Release.Name .Values.deploymentName | trunc 21 | trimSuffix "-" }}
{{- end }}

{{- define "release.labels" }}
app: {{ template "app.name" . }}
chart: {{ template "chart.fullname" . }}
release: {{ .Release.Name }}
heritage: {{ .Release.Service }}
{{- end }}

{{- define "app.labels" }}
app: {{ template "app.name" . }}
release: {{ .Release.Name }}
{{- end }}

{{- define "security.context" }}
{{- if .Values.securityContext.enabled }}
securityContext:
  runAsUser: {{ .Values.securityContext.runAsUser }}
  fsGroup: {{ .Values.securityContext.fsGroup }}
{{- end }}
{{- end }}

{{- define "security.context.env" }}
{{- if .Values.securityContext.enabled }}
{{- with .Values.securityContext.userName }}
- name: "USER_NAME"
  value: {{ . | quote }}
{{- end}}
{{- with .Values.securityContext.groupName }}
- name: "GROUP_NAME"
  value: {{ . | quote }}
{{- end}}
{{- end }}
{{- end }}

{{- define "hadoop.env" }}
{{- with .Values.hadoop.userName }}
- name: "HADOOP_USER_NAME"
  value: {{ . | quote }}
{{- end }}
{{- end }}

{{- define "node.selector" }}
{{- with .nodeSelector }}
nodeSelector:
{{ toYaml . | indent 2 }}
{{- end }}
{{- end }}

{{- define "joinListWithComma" }}
{{- $local := dict "first" true }}
{{- range $k, $v := . }}
{{- if not $local.first }},{{ end }}
{{- $v }}
{{- $_ := set $local "first" false }}
{{- end }}
{{- end }}
