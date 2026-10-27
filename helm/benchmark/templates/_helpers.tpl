{{- define "bench.labels" -}}
app.kubernetes.io/part-of: aggbench
aggbench/run: {{ .Values.runId | quote }}
aggbench/variant: {{ .Values.variant | quote }}
{{- end -}}

{{- define "bench.ingressTopic" -}}aggbench.{{ .Values.runId }}.ingress{{- end -}}
{{- define "bench.actionsTopic" -}}aggbench.{{ .Values.runId }}.actions{{- end -}}
{{- define "bench.groupId" -}}aggbench-{{ .Values.variant }}-{{ .Values.runId }}{{- end -}}
{{- define "bench.isStreams" -}}{{ or (eq .Values.variant "a1") (eq .Values.variant "a2") }}{{- end -}}

{{/* Lag-monitored groups: the aggregation group first; Streams variants also expose the intent consumer group. */}}
{{- define "bench.lagGroups" -}}
{{- if eq (include "bench.isStreams" .) "true" -}}
{{ include "bench.groupId" . }}:{{ include "bench.ingressTopic" . }},{{ include "bench.groupId" . }}-intents:{{ include "bench.actionsTopic" . }}
{{- else -}}
{{ include "bench.groupId" . }}:{{ include "bench.ingressTopic" . }}
{{- end -}}
{{- end -}}

{{- define "bench.dbEnv" -}}
- name: DB_URL
  valueFrom: {secretKeyRef: {name: {{ .Values.postgresSecret }}, key: jdbc-url}}
- name: DB_USER
  valueFrom: {secretKeyRef: {name: {{ .Values.postgresSecret }}, key: user}}
- name: DB_PASSWORD
  valueFrom: {secretKeyRef: {name: {{ .Values.postgresSecret }}, key: password}}
{{- end -}}

{{- define "bench.metricsAnnotations" -}}
prometheus.io/scrape: "true"
prometheus.io/port: "8080"
prometheus.io/path: /q/metrics
{{- end -}}
