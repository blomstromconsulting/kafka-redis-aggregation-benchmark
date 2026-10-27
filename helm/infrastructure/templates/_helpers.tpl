{{- define "aggbench.labels" -}}
app.kubernetes.io/part-of: aggbench
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "aggbench.kafkaVoters" -}}
{{- $ns := .Release.Namespace -}}
{{- $voters := list -}}
{{- range $i, $e := until (int .Values.kafka.brokers) -}}
{{- $voters = append $voters (printf "%d@kafka-%d.kafka-headless.%s.svc.cluster.local:9093" $i $i $ns) -}}
{{- end -}}
{{- join "," $voters -}}
{{- end -}}
