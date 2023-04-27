
output "cluster" {
  value = google_container_cluster.primary.name
}

output "host" {
  value     = google_container_cluster.primary.endpoint
  sensitive = true
}

output "kubernetes_cluster_name" {
  value = google_container_cluster.primary.name
}

output "zone" {
  value       = var.zone
  description = "GCloud Zone"
}

output "cluster_ca_certificate" {
  value     = base64decode(google_container_cluster.primary.master_auth.0.cluster_ca_certificate)
  sensitive = true
}
