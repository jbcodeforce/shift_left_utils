
## Create table structure

::: utils.create_table_folder_structure
    handler: python
    options:
        members:
            - main
            - create_folder_structure
        show_root_heading: true
        show_source: true

## Process Source Table for migration


::: utils.process_src_tables
    handler: python
    options:
        members:
            - process_from_table_name
        show_root_heading: true
        show_source: true


## Pipeline helper


::: utils.pipeline_helper
    handler: python
    options:
        members:
            - build_pipeline_definition_from_table
        show_root_heading: true
        show_source: true