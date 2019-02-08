variable "cluster_name" {
  default     = "feast"
  description = "Cluster name"
}

variable "project_name" {
  description = "Project name"
}

variable "region" {
  description = "Subnet region"
}

variable "network" {
  default = "default"
}

variable "subnetwork" {
  default = "default"
}

variable "bucket_name" {
  description = "Working storage for feast"
}

variable "feast_node_pool_min_size" {
  description = "Minimum number of nodes for node pool"
  default     = 1
}

variable "feast_node_pool_max_size" {
  description = "Maximum number of nodes for node pool"
  default     = 2
}
