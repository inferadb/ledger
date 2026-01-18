{{/*
Expand the name of the chart.
*/}}
{{- define "inferadb-ledger.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "inferadb-ledger.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "inferadb-ledger.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "inferadb-ledger.labels" -}}
helm.sh/chart: {{ include "inferadb-ledger.chart" . }}
{{ include "inferadb-ledger.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "inferadb-ledger.selectorLabels" -}}
app.kubernetes.io/name: {{ include "inferadb-ledger.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name
*/}}
{{- define "inferadb-ledger.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "inferadb-ledger.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Headless service name (for discovery)
*/}}
{{- define "inferadb-ledger.headlessServiceName" -}}
{{- include "inferadb-ledger.fullname" . }}
{{- end }}

{{/*
Discovery domain (headless service FQDN)
*/}}
{{- define "inferadb-ledger.discoveryDomain" -}}
{{- printf "%s.%s.svc.cluster.local" (include "inferadb-ledger.headlessServiceName" .) .Release.Namespace }}
{{- end }}

{{/*
Image reference
*/}}
{{- define "inferadb-ledger.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}
