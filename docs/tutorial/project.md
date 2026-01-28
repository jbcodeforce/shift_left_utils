# Lab: Project Management

This lab focuses on starting a Confluent Flink project using best practices.

1. Prerequisite

	* You have followed [the setup lab](setup_lab.md) to get shift_left CLI configured and running.

1. Initialize a Project

	* Go to where you want to create the Flink project
	* Execute the project init command:
		```sh
		shift_left project init <project_name> <project_path> 
		# example for a default Kimball project
		shift_left project init dsp .
		# For a project more focused on developing data as a product
		shift_left project init dsp ../  --project-type data-product
		```

1. 
