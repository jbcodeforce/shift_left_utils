## [0.1.47] - 2025-12-17

### Changes:
- 07d7e8a update version for children recursively
- a691a80 fix UT for project_mgr
- ee958cc Make update to allow --properties
- 7f03f95 Make update to allow --properties
- 79a8522 Make update to allow --properties
- 71a1ef0 Merge pull request #83 from jbcodeforce/make
- 74bcfc1 Make update to allow --properties - bugfix
- 874aaa2 Merge pull request #84 from jbcodeforce/make2
- 9c48028 update to remove apache-flink dependency

## [0.1.46] - 2025-11-30

### Changes:
- 55ac03c start to work on bg. transformer now support updating sql create and insert content with unique table to change
- 38c42d9 Merge branch 'main' into develop
- 8acfa36 update to changelog
- 8ffa802 Merge branch 'develop' into fix62bis
- 032fa78 update UT for b/g
- 6e054e8 finalize version update
- bb342a5 merge docs update
- 0116717 Upgrade housekeep_statement to pause statements in a pool
- 4bef2c4 Merge pull request #76 from jbcodeforce/upgrade_housekeep
- c60b349 Upgrade housekeep_statement to pause statements in a pool
- b126638 Merge pull request #77 from jbcodeforce/upgrade_housekeep2
- aae367d Merge branch 'develop' into fix62bis
- 207f1e1 deep refactoring of ai processing
- 72d555d refactoring of ai migration
- e527268 refactor ai migration code

## [0.1.45] - 2025-11-10

### Changes:
- b9f05bf healthcheck bug fixes
- c36d3c0 Merge pull request #70 from jbcodeforce/healthcheck2
- d20d005 work on b/g deployment
- fe7e91c improve robustness of list modified file from git, and fix some new issues in the UT
- 7e9617b modification to settings
- f598220 Merge branch 'fix62' into develop
- ef04806 update to sql parser
- 2d901fe update ksql
- 779a4dd work on excluding tables, and fixing some deployment management
- e89f6d2 finish exclude-file implementation, but also fix some internal function with Return None or optional field in the model. Fixed impacted unit tests
- 8072f50 add compute poolid to get_statement_list + new command in project so user can get the list of statements per compute pool
- 32f6f3c Merge pull request #74 from jbcodeforce/fix62
- 1edc048 fix report unit test

## [0.1.44] - 2025-10-22

### Changes:
- 63a3f46 post_fix parm for GHA, New feature healthcheck
- ec862ad post_fix parm for GHA, New feature healthcheck
- f918ae9 Merge pull request #68 from jbcodeforce/postfix_N_healthcheck
- c8f3348 post_fix parm for GHA, New feature healthcheck
- 5208606 Merge pull request #69 from jbcodeforce/postfix_N_healthcheck2

## [0.1.43] - 2025-10-16

### Changes:
- e5e5d66 Merge branch 'v0.1.42' into develop
- 5285fc0 Merge branch 'main' into develop
- c4d3ad0 fix processing statement result for unit test, by changing the scan.bounded.mode to latest-offset, so validation scripts will complete, and the results will be bounded

## [0.1.42] - 2025-10-15

### Changes:
- 3e7dce3 Merge branch 'v0.1.41' into develop
- 32dd4b7 update to ksqldb migration
- 03a2bd9 enhance explanation of test harness
- c384f55 test mgr delete flink statement improvement. Add MCP server
- 7b7eee0 work on making test_mgre more robust. Improve documentation for test harness with FAQs
- f2143a0 Logging Improvement and performance tracking
- 3cc20b7 Logging Improvement and performance tracking
- d64c316 Merge pull request #66 from jbcodeforce/logging_improvements
- 9ee59da continue improving test harness
- ac12f44 work on ut for cli - project one

