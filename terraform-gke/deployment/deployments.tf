terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "3.52.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.0.1"
    }
  }
}

# Configuring path to get details for existing cluster
data "terraform_remote_state" "gke" {
  backend = "local"
  config = {
    path = "../terraform.tfstate"
  }
}

# Retrieve GKE cluster information
data "google_client_config" "default" {}

data "google_container_cluster" "zions-rfp-cluster" {
  name     = "zions-rfp-cluster"
  location = "us-central1"  
  project  = "mit-zions-rfp"
}

provider "kubernetes" {
  host = "https://${data.terraform_remote_state.gke.outputs.host}"

  token                  = "${data.google_client_config.default.access_token}"
  cluster_ca_certificate = "${data.terraform_remote_state.gke.outputs.cluster_ca_certificate}"
}


# Deployment configuration
resource "kubernetes_deployment" "zions-rfp" {
  metadata {
    name = "zions-rfp"
    labels = {
      App = "zions-rfp"
    }
  }

  spec {
    replicas = 2
    selector {
      match_labels = {
        App = "zions-demo"
      }
    }
    template {
      metadata {
        labels = {
          App = "zions-demo"
        }
      }
      spec {
        container {
          image = "gcr.io/mit-zions-rfp/zions-demo:latest"
          name  = "zions-demo"

          port {
            container_port = 5000
          }
      }
    }
    }
  }
}

# Service Configuration
resource "kubernetes_service" "zions-rfp" {
  metadata {
    name = "mit-lb"
  }
  spec {
    selector = {
      App = kubernetes_deployment.zions-rfp.spec.0.template.0.metadata[0].labels.App
    }
    port {
      port        = 80
      target_port = 5000
    }

    type = "LoadBalancer"
  }
}