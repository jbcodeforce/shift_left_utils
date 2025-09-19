# Flink Project Pipelines - SQL Deployment Tracker

This table tracks the deployment status of all SQL scripts across the pipeline components.

## Legend
- ✅ **Deployed**: Successfully deployed to production
- ❌ **Failed**: Deployment failed, needs attention
- ⏸️ **Pending**: Not yet deployed
- 📝 **Review**: Pending code review
- SL deploy: shift_left pipeline deploy -table-name

## Integration tests

* [Happy path]() 

## Sources

| Component | DDL Script | DML Script |  Status | Type | Last Deployed | Notes |
|-----------|------------|------------|-----------------|-------------|---------------|-------|
| **Common** |  |  |  |  |  |  |
| src_tenant | ddl.src_common_tenant.sql | dml.src_common_tenant.sql | ✅ | SL deploy | - | Core tenant configuration |
| raw_tenants | tests/ddl.raw_tenants.sql | insert_raw_data.sql | ✅  | Make | 09/18/25 | 6 records - debezium format |
| **P1** |  |  |  |  |  |  |
| src_table_1 | ddl.src_table_1.sql | dml.src_table_1.sql | ⏸️ | - | - | Primary data source |
| src_table_2 | ddl.src_p1_table_2.sql | dml.src_p1_table_2.sql | ⏸️ | - | - | Secondary data source |
| src_table_3 | ddl.src_p1_table_3.sql | dml.src_p1_table_3.sql | ⏸️ | - | - | Tertiary data source |
| **P2** |  |  |  |  |  |  |
| src_a | ddl.src_p2_a.sql | dml.src_p2_a.sql | ⏸️ | - | - | Source A for P2 pipeline |
| src_b | ddl.src_b.sql | dml.src_b.sql | ⏸️ | - | - | Source B for P2 pipeline |
| src_x | ddl.src_x.sql | dml.src_x.sql | ⏸️ | - | - | Source X for P2 pipeline |
| src_y | ddl.src_y.sql | dml.src_y.sql | ⏸️ | - | - | Source Y for P2 pipeline |
| **P3** |  |  |  |  |  |  |
| src_roles | ddl.src_p3_roles.sql | dml.src_p3_roles.sql | ⏸️ | - | - | User roles data |
| src_tenants | ddl.src_p3_tenants.sql | dml.src_p3_tenants.sql | ⏸️ | - | - | Tenant configuration |
| src_users | ddl.src_p3_users.sql | dml.src_p3_users.sql | ⏸️ | - | - | User profile data |
| **c360** |  |  |  |  |  |  |
| src_groups | ddl.src_users_groups.sql | dml.src_users_groups.sql | ✅ | SL deploy | 09/18/25 | User groups data |
| raw_groups | tests/ddl.raw_groups.sql | dml.insert_groups.sql | ✅  | Make | 09/18/25 | |
| src_users | ddl.src_users_users.sql | dml.src_users_users.sql | ✅  | SL deploy | - | User profile data |
| raw_users | tests/ddl.raw_users.sql | dml.insert_users.sql | ✅  | Make | 09/18/25 | |

## Intermediates

| Component | DDL Script | DML Script | Pipeline Status | Environment | Last Deployed | Notes |
|-----------|------------|------------|-----------------|-------------|---------------|-------|
| **Common** |  |  |  |  |  |  |
| int_tenant | ddl.int_common_tenant.sql | dml.int_common_tenant.sql | ⏸️ | - | - | Tenant processing logic |
| **P1** |  |  |  |  |  |  |
| int_table_1 | ddl.int_table_1.sql | dml.int_table_1.sql | ⏸️ | - | - | Intermediate table 1 |
| int_table_2 | ddl.int_table_2.sql | dml.int_table_2.sql | ⏸️ | - | - | Intermediate table 2 |
| int_test | ddl.test.sql | dml.int_tag.sql | ⏸️ | - | - | Test intermediate |
| **P2** |  |  |  |  |  |  |
| int_a | ddl.a.sql | dml.a.sql | ⏸️ | - | - | Intermediate processing A |
| int_b | ddl.b.sql | dml.b.sql | ⏸️ | - | - | Intermediate processing B |
| int_c | ddl.c.sql | dml.c.sql | ⏸️ | - | - | Intermediate processing C |
| int_d | ddl.d.sql | dml.d.sql | ⏸️ | - | - | Intermediate processing D |
| int_x | ddl.x.sql | dml.x.sql | ⏸️ | - | - | Intermediate processing X |
| int_y | ddl.y.sql | dml.y.sql | ⏸️ | - | - | Intermediate processing Y |
| int_z | ddl.z.sql | dml.z.sql | ⏸️ | - | - | Intermediate processing Z |
| **P3** |  |  |  |  |  |  |
| int_it2 | ddl.int_p3_it2.sql | dml.int_p3_it2.sql | ⏸️ | - | - | IT2 intermediate |
| int_user_role | ddl.int_p3_user_role.sql | dml.int_p3_user_role.sql | ⏸️ | - | - | User role processing |
| int_p3_data_mesh | ddl.int_p3_data_mesh.sql | dml.int_p3_data_mesh.sql | ⏸️ | - | - | Data mesh integration |

## Facts

| Component | DDL Script | DML Script | Pipeline Status | Environment | Last Deployed | Notes |
|-----------|------------|------------|-----------------|-------------|---------------|-------|
| **P1** |  |  |  |  |  |  |
| fct_order | ddl.p1_fct_order.sql | dml.p1_fct_order.sql | ⏸️ | - | - | Order fact table |
| **P2** |  |  |  |  |  |  |
| fct_e | ddl.e.sql | dml.e.sql | ⏸️ | - | - | Event fact table |
| fct_f | ddl.f.sql | dml.f.sql | ⏸️ | - | - | F fact table |
| fct_p | ddl.p.sql | dml.p.sql | ⏸️ | - | - | P fact table |
| **c360** |  |  |  |  |  |  |
| fct_user_per_group | ddl.c360_fct_user_per_group.sql | dml.users_fct_user_per_group.sql | ⏸️ | - | - | User per group fact |

## Dimensions

| Component | DDL Script | DML Script | Pipeline Status | Environment | Last Deployed | Notes |
|-----------|------------|------------|-----------------|-------------|---------------|-------|
| **c360** |  |  |  |  |  |  |
| dim_groups | ddl.dim_groups.sql | dml.dim_users.sql | ⏸️ | - | - | User dimension table |
| dim_users | ddl.dim_users.sql | dml.dim_users.sql | ⏸️ | - | - | User dimension table |

## Special Scripts

| Script Name | Purpose | Status | Environment | Last Deployed | Notes |
|-------------|---------|--------|-------------|---------------|-------|
| alter_table_avro_debezium.sql | Avro/Debezium schema changes | ⏸️ | - | - | Database migration script |
| test_prepare_tables_integration.sql | Integration test setup | ⏸️ | - | - | Test infrastructure |

## Deployment Instructions

1. All raw_data for the sources table should be deployed with Make
    ```sh
    cd tests/data/flink-project/pipelines/sources/common/src_tenant
    make create_raw_data

    ```
2. Deploy fact table for c360 product
    ```
    shift_left deploy
    ```


## Notes

- Each table  has its own `Makefile` for automated deployment
- Test data and validation scripts are available in each table's `tests/` directory
- Pipeline definitions are stored in `pipeline_definition.json` files for automation

---
*Last Updated: {date}*
*Total SQL Scripts Tracked: 112*
