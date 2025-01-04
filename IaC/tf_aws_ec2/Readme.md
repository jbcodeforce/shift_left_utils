# AWS EC2 for Ollama and Shift Left tools

Using Ollama enables local access to large language models like Llama 2, , Mistral to ensure data privacy and security. For the Shift Left project we may want to run the migration, sink table by sink table, so a maximum of 2 days of work so having an EC2 with enough resources will be the best approach.

qwen2.5-coder seems to have the best performance among open-source models on multiple popular code generation benchmarks. It gaves quite good results for transforming SQL to Flink SQL. A future solution will include tuned model for Flink SQL as a target, or RAG. The full characteristic of the model [is nere](https://ollama.com/library/qwen2.5-coder:32b/blobs/ac3d1ba8aa77).

As AWS has limited EC2 types and VRAM sizing that are suitable to run LLM, even if Ollama offers convenient way to deploy multiple different LLM models, we deploy one LLM model per EC2 instance. This architecture decision is due to the EC2 VRAM limits, and the fact that loading a new model can take some time. For the shift left use case we do not need more than one model, but in the future, as the Agent workflow may involve more models we may change this configuration to run multiple EC2 instance. 


The EC2 Image unique identifier is for AWS us-west-2 region. If you change region look at the AWS console for equivalent ami_id.

## The EC2 instance

* Linux 2 from Amazon
* ami  = "ami-0c55b159cbfafe1f0"  # Amazon Linux 2 AMI with Nvidia support and deep learning library
* instance_type = g5.4xlarge   # 1.64 $ per hour

* vCPU: 4
* RAM: 64GB
* GPU: 1 (VRAM: 32GB)
* EBS: 100GB (gp3)
* SSH Key: Required for PuTTY login

The instance needs to be accessed via SSH, if the Python scripts are executed in this machinem while the dbt source project is cloned in this machine.

## The Terraform configuration

The Terraform creates:

* Creates a VPC
* Creates a public and private subnets
* Creates a NAT Gateway to enable private subnets to reach out to the internet without needing an externally routable IP address assigned to each resource.
* Creates an Internet Gateway and attaches it to the VPC to allow traffic within the VPC to be reachable by the outside world.
* Creates a route tables for the public and private subnets and associates the table with both subnets
* Create an Ollama Service: Deploys EC2 instances running the Ollama LLM, exposed via an API Gateway for secure access.
* Clone this repository to get access to python utilities for Shift Left Project


## Pre-requisites

* Install Terraform CLI [from here](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)

## Execution

* Run the `terraform init` command in the same directory. The terraform init command initializes the plugins and providers which are required to work with resources.
* Run the `terraform plan` command. This is an optional, yet recommended action to ensure your configuration’s syntax is correct and gives you an overview of which resources will be provisioned in your infrastructure.
* Provision the AWS VPC and resources using `terraform apply`.

When we need to stop this deployment use the command: `terraform destroy`