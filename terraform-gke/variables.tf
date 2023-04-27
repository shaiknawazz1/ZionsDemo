variable "project" {
  default = "mit-zions-rfp"
}

variable "region" {
  default = "us-central1"
}

variable "zone" {
  default = "us-central1-a"
}

variable "cluster" {
  default = "zions-rfp-cluster"
}

variable "app_name" {
  default = "zions-demo"
}

variable "machine_type" {
  default = "g1-small"
}


variable "credentials" {
  default = "credentials.json"
}

variable "kubernetes_min_ver" {
  default = "latest"
}

variable "kubernetes_max_ver" {
  default = "latest"
}