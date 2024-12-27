# Migration process

This chapter is going over a simple fact to sources pipeline migration.

All the dbt source tables are defined in the examples folder. 

The method start from a fact table.

## 1 - Discover the current pipeline

Taking one target fact table the following command will bring the dependency hierarchy with some element to track the migration project:

```
python find_pipeline.py -f ../examples/models/facts/fct_training_doc.sql 
```

-- Process file: ../dbt-src/models/facts/qx/fct_training_document.sql
Process the table: fct_training_document
Dependencies found:
  - depends on : int_training_completed_release
  - depends on : int_non_abandond_training
  - depends on : int_unassigned_curriculum
  - depends on : int_courses_wo_trainingdata
  - depends on : int_docs_wo_trainingdata


-- Process file: ../dbt-src/models/intermediates/training_document/int_courses_wo_trainingdata.sql
Process the table: int_courses_wo_trainingdata
Dependencies found:
  - depends on : int_training_data_deduped

  ....

 NOT_TESTED | OK ('int_training_completed_release', '../dbt-src/models/intermediates/training_document/int_training_completed_release.sql')
 NOT_TESTED | OK ('src_tdc_doc_accesscattype', '../dbt-src/models/sources/src_tdc_doc_accesscattype.sql')
 NOT_TESTED | OK ('src_web_business_unit_entity_type', '../dbt-src/models/sources/src_web_business_unit_entity_type.sql')
 NOT_TESTED | OK ('int_training_doc_release_transform', '../dbt-src/models/intermediates/training_document/int_training_doc_release_transform.sql')
 NOT_TESTED | OK ('int_training_curriculum_deduped', '../dbt-src/models/intermediates/dedups/int_training_curriculum_deduped.sql')
 NOT_TESTED | OK ('src_tdc_doc_document_type', '../dbt-src/models/sources/src_tdc_doc_document_type.sql')
 NOT_TESTED | OK ('int_tdc_doc_document_type_deduped', '../dbt-src/models/intermediates/dedups/int_tdc_doc_document_type_deduped.sql')
 NOT_TESTED | OK ('int_unassigned_curriculum', '../dbt-src/models/intermediates/training_document/int_unassigned_curriculum.sql')
 NOT_TESTED | OK ('int_training_data_deduped', '../dbt-src/models/intermediates/dedups/int_training_data_deduped.sql')
 NOT_TESTED | OK ('int_training_course_deduped', '../dbt-src/models/intermediates/dedups/int_training_course_deduped.sql')
 NOT_TESTED | OK ('int_courses_wo_trainingdata', '../dbt-src/models/intermediates/training_document/int_courses_wo_trainingdata.sql')
 NOT_TESTED | OK ('src_training_curriculum', '../dbt-src/models/sources/src_training_curriculum.sql')
 NOT_TESTED | OK ('src_training_course', '../dbt-src/models/sources/src_training_course.sql')
 NOT_TESTED | OK ('int_docs_wo_trainingdata', '../dbt-src/models/intermediates/training_document/int_docs_wo_trainingdata.sql')
 NOT_TESTED | OK ('src_training_data', '../dbt-src/models/sources/src_training_data.sql')
 NOT_TESTED | OK ('int_web_business_unit_entity_deduped', '../dbt-src/models/intermediates/dedups/int_web_business_unit_entity_deduped.sql')
 NOT_TESTED | OK ('int_non_abandond_training', '../dbt-src/models/intermediates/training_document/int_non_abandond_training.sql')


