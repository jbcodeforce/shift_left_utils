variable "region" {
    type = string
    description = "The cloud region to deploy the resource to"
    default = "us-west-2"
}
variable "main_vpc_cidr" {
    type = string
}
variable "public_subnets" {
    type = string
}
variable "private_subnets" {
    type = string
}

variable "ssh_api_key" {
    type = string
}