WITH training_completed_release AS (
  SELECT
    *
  FROM
    { { REF('int_training_completed_release') } }
),
non_abandond_training AS (
  SELECT
    *
  FROM
    { { REF('int_non_abandond_training') } }
),
unassigned_curriculum AS (
  SELECT
    *
  FROM
    { { REF('int_unassigned_curriculum') } }
),
completeTraining AS(
  SELECT
    training_id,
    course_id,
    course_name,
    course_number,
    course_revision,
    effective_date,
    release_date,
    is_completed,
    training_completion_status,
    user_id,
    ts_ms
  FROM
    training_completed_release
  UNION
  ALL
  SELECT
    training_id,
    course_id,
    course_name,
    course_number,
    course_revision,
    effective_date,
    release_date,
    is_completed,
    training_completion_status,
    user_id,
    ts_ms
  FROM
    non_abandond_training
),
union_all AS (
  SELECT
    training_id,
    course_id,
    course_name,
    course_number,
    CASE
        WHEN course_number IS NULL THEN ''
        ELSE course_number END AS training_document_course_number_blanked,
    course_revision,
    effective_date,
    release_date,
    is_completed,
    training_completion_status,
    user_id,
    ts_ms
  FROM
    completeTraining
  UNION
  ALL
  SELECT
    training_id,
    course_id,
    course_name,
    course_number,
    CASE
        WHEN course_number IS NULL THEN ''
        ELSE course_number END AS training_document_course_number_blanked,
    course_revision,
    effective_date,
    release_date,
    is_completed,
    training_completion_status,
    user_id,
    ts_ms
  FROM
    unassigned_curriculum
),
courses_wo_trainingdata AS (
  SELECT
    *
  FROM
    { { REF('int_courses_wo_trainingdata') } }
),
docs_wo_trainingdata AS (
  SELECT
    *
  FROM
    { { REF('int_docs_wo_trainingdata') } }
),
FINAL AS (
  SELECT
    { { dbt_utils.surrogate_key(
      [ 'course_id',
      'training_id',
      'user_id']
    ) } } AS training_document_sid,
    { { dbt_utils.surrogate_key([ 'training_id', 'tenant_id' ]) } } AS training_data_sid,
    { { dbt_utils.surrogate_key([ 'user_id', 'tenant_id' ]) } } AS user_sid,
    training_id,
    course_id,
    user_id,
    course_name,
    course_number,
    course_revision,
    training_document_course_number_blanked,
    effective_date :: TIMESTAMP AS effective_date,
    release_date :: TIMESTAMP AS release_date,
    is_completed,
    training_completion_status,
    ts_ms
  FROM
    union_all
  UNION
  ALL
  SELECT
    { { dbt_utils.surrogate_key(
      [ 'course_id',
      'training_id',
      'user_id']
    ) } } AS training_document_sid,
    { { dbt_utils.surrogate_key([ 'training_id', 'tenant_id' ]) } } AS training_data_sid,
    { { dbt_utils.surrogate_key([ 'user_id', 'tenant_id' ]) } } AS user_sid,
    *
  FROM
    courses_wo_trainingdata
  UNION
  ALL
  SELECT
    { { dbt_utils.surrogate_key(
      [ 'course_id',
      'training_id',
      'user_id']
    ) } } AS training_document_sid,
    { { dbt_utils.surrogate_key([ 'training_id', 'tenant_id' ]) } } AS training_data_sid,
    { { dbt_utils.surrogate_key([ 'user_id', 'tenant_id' ]) } } AS user_sid,
    *
  FROM
    docs_wo_trainingdata
)
SELECT * FROM FINAL