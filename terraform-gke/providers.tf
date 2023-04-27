provider "google" {
  # version     = "2.7.0"
  credentials = file("credentials.json")
  project     = var.project
  region      = var.region
}