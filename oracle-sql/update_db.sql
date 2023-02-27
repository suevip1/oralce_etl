ALTER TABLE USER_ECS_REPORT.ASSIST_WHITELIST DROP COLUMN FLIGHT_NO;

ALTER TABLE USER_ECS_REPORT.ASSIST_WHITELIST_DETAIL ADD FLIGHT_NO VARCHAR2(10);
COMMENT ON COLUMN USER_ECS_REPORT.ASSIST_WHITELIST_DETAIL.FLIGHT_NO IS '航班号';

alter session set nls_date_format='YYYY-MM-DD HH24:MI:SS';

INSERT INTO xxl_job_group
(id, app_name, title, address_type, address_list, update_time)
VALUES(XXL_JOB_GROUP_ID.nextval, 'airline-feature-history', 'feat-hist', 1, 'http://10.76.61.27:9090/', '2023-02-14 14:23:49');

INSERT INTO xxl_job_info
(job_group, job_desc, add_time, update_time, author, alarm_email, schedule_type, schedule_conf, misfire_strategy, executor_route_strategy, executor_handler, executor_param, executor_block_strategy, executor_timeout, executor_fail_retry_count, glue_type, glue_source, glue_remark, glue_updatetime, child_jobid, trigger_status, trigger_last_time, trigger_next_time, id)
VALUES(XXL_JOB_GROUP_ID.currval, '节假日读取', '2023-02-16 09:49:02', '2023-02-16 09:49:02', '陈波', 'chenbo@aograph.com', 'CRON', '0 0 4 * * ?', 'DO_NOTHING', 'FIRST', 'readHolidayJob', '{"history":true}', 'SERIAL_EXECUTION', 0, 0, 'BEAN', '', 'GLUE代码初始化', '2023-02-16 09:49:02', '', 0, 0, 0, XXL_JOB_INFO_ID.nextval);

INSERT INTO xxl_job_info
(job_group, job_desc, add_time, update_time, author, alarm_email, schedule_type, schedule_conf, misfire_strategy, executor_route_strategy, executor_handler, executor_param, executor_block_strategy, executor_timeout, executor_fail_retry_count, glue_type, glue_source, glue_remark, glue_updatetime, child_jobid, trigger_status, trigger_last_time, trigger_next_time, id)
VALUES(XXL_JOB_GROUP_ID.currval, 'FLP读取', '2023-02-14 14:31:56', '2023-02-16 09:49:15', '陈波', 'chenbo@aograph.com', 'CRON', '0 0 4 * * ?', 'DO_NOTHING', 'FIRST', 'readFlpJob', '{}', 'SERIAL_EXECUTION', 0, 0, 'BEAN', '', 'GLUE代码初始化', '2023-02-14 14:31:56', XXL_JOB_INFO_ID.currval - 1, 0, 0, 0, XXL_JOB_INFO_ID.nextval);

INSERT INTO xxl_job_info
(job_group, job_desc, add_time, update_time, author, alarm_email, schedule_type, schedule_conf, misfire_strategy, executor_route_strategy, executor_handler, executor_param, executor_block_strategy, executor_timeout, executor_fail_retry_count, glue_type, glue_source, glue_remark, glue_updatetime, child_jobid, trigger_status, trigger_last_time, trigger_next_time, id)
VALUES(XXL_JOB_GROUP_ID.currval, '历史INV读取', '2023-02-14 14:31:26', '2023-02-14 14:35:20', '陈波', 'chenbo@aograph.com', 'CRON', '0 0 4 * * ?', 'DO_NOTHING', 'FIRST', 'readHisInvJob', '{"history":true}', 'SERIAL_EXECUTION', 0, 0, 'BEAN', '', 'GLUE代码初始化', '2023-02-14 14:31:26', XXL_JOB_INFO_ID.currval - 1, 0, 0, 0, XXL_JOB_INFO_ID.nextval);

INSERT INTO xxl_job_info
(job_group, job_desc, add_time, update_time, author, alarm_email, schedule_type, schedule_conf, misfire_strategy, executor_route_strategy, executor_handler, executor_param, executor_block_strategy, executor_timeout, executor_fail_retry_count, glue_type, glue_source, glue_remark, glue_updatetime, child_jobid, trigger_status, trigger_last_time, trigger_next_time, id)
VALUES(XXL_JOB_GROUP_ID.currval, 'ctrip价格读取', '2023-02-14 14:30:53', '2023-02-14 14:35:07', '陈波', 'chenbo@aograph.com', 'CRON', '0 0 4 * * ?', 'DO_NOTHING', 'FIRST', 'readCtripPriceJob', '{"history":true}', 'SERIAL_EXECUTION', 0, 0, 'BEAN', '', 'GLUE代码初始化', '2023-02-14 14:30:53', XXL_JOB_INFO_ID.currval - 1, 0, 0, 0, XXL_JOB_INFO_ID.nextval);

INSERT INTO xxl_job_info
(job_group, job_desc, add_time, update_time, author, alarm_email, schedule_type, schedule_conf, misfire_strategy, executor_route_strategy, executor_handler, executor_param, executor_block_strategy, executor_timeout, executor_fail_retry_count, glue_type, glue_source, glue_remark, glue_updatetime, child_jobid, trigger_status, trigger_last_time, trigger_next_time, id)
VALUES(XXL_JOB_GROUP_ID.currval, 'FD读取', '2023-02-14 14:29:32', '2023-02-16 17:17:50', '陈波', 'chenbo@aograph.com', 'CRON', '0 0 4 * * ?', 'DO_NOTHING', 'FIRST', 'readFareFdJob', '{"history":true,"add_hx":1}', 'SERIAL_EXECUTION', 0, 0, 'BEAN', '', 'GLUE代码初始化', '2023-02-14 14:29:32', XXL_JOB_INFO_ID.currval - 1, 0, 0, 0, XXL_JOB_INFO_ID.nextval);

INSERT INTO xxl_job_info
(job_group, job_desc, add_time, update_time, author, alarm_email, schedule_type, schedule_conf, misfire_strategy, executor_route_strategy, executor_handler, executor_param, executor_block_strategy, executor_timeout, executor_fail_retry_count, glue_type, glue_source, glue_remark, glue_updatetime, child_jobid, trigger_status, trigger_last_time, trigger_next_time, id)
VALUES(XXL_JOB_GROUP_ID.currval, '私有运价读取', '2023-02-14 14:28:57', '2023-02-16 17:17:58', '陈波', 'chenbo@aograph.com', 'CRON', '0 0 4 * * ?', 'DO_NOTHING', 'FIRST', 'readFarePrivateJob', '{"history":true,"add_hx":1}', 'SERIAL_EXECUTION', 0, 0, 'BEAN', '', 'GLUE代码初始化', '2023-02-14 14:28:57', XXL_JOB_INFO_ID.currval - 1, 0, 0, 0, XXL_JOB_INFO_ID.nextval);

INSERT INTO xxl_job_info
(job_group, job_desc, add_time, update_time, author, alarm_email, schedule_type, schedule_conf, misfire_strategy, executor_route_strategy, executor_handler, executor_param, executor_block_strategy, executor_timeout, executor_fail_retry_count, glue_type, glue_source, glue_remark, glue_updatetime, child_jobid, trigger_status, trigger_last_time, trigger_next_time, id)
VALUES(XXL_JOB_GROUP_ID.currval, 'PNR读取', '2023-02-14 14:28:11', '2023-02-14 14:34:11', '陈波', 'chenbo@aograph.com', 'CRON', '0 0 4 * * ?', 'DO_NOTHING', 'FIRST', 'readPnrTicketJob', '{"history":true}', 'SERIAL_EXECUTION', 0, 0, 'BEAN', '', 'GLUE代码初始化', '2023-02-14 14:28:11', XXL_JOB_INFO_ID.currval - 1, 0, 0, 0, XXL_JOB_INFO_ID.nextval);

INSERT INTO xxl_job_info
(job_group, job_desc, add_time, update_time, author, alarm_email, schedule_type, schedule_conf, misfire_strategy, executor_route_strategy, executor_handler, executor_param, executor_block_strategy, executor_timeout, executor_fail_retry_count, glue_type, glue_source, glue_remark, glue_updatetime, child_jobid, trigger_status, trigger_last_time, trigger_next_time, id)
VALUES(XXL_JOB_GROUP_ID.currval, '航班信息读取', '2023-02-14 14:27:20', '2023-02-14 14:34:01', '陈波', 'chenbo@aograph.com', 'CRON', '0 0 4 * * ?', 'DO_NOTHING', 'FIRST', 'readFlightInfoJob', '{"history":true}', 'SERIAL_EXECUTION', 0, 0, 'BEAN', '', 'GLUE代码初始化', '2023-02-14 14:27:20', XXL_JOB_INFO_ID.currval - 1, 0, 0, 0, XXL_JOB_INFO_ID.nextval);

INSERT INTO xxl_job_info
(job_group, job_desc, add_time, update_time, author, alarm_email, schedule_type, schedule_conf, misfire_strategy, executor_route_strategy, executor_handler, executor_param, executor_block_strategy, executor_timeout, executor_fail_retry_count, glue_type, glue_source, glue_remark, glue_updatetime, child_jobid, trigger_status, trigger_last_time, trigger_next_time, id)
VALUES(XXL_JOB_GROUP_ID.currval, '排班计划生成', '2023-02-14 14:26:46', '2023-02-14 14:33:50', '陈波', 'chenbo@aograph.com', 'CRON', '0 0 4 * * ?', 'DO_NOTHING', 'FIRST', 'readFlightScheduleJob', '{"history":true}', 'SERIAL_EXECUTION', 0, 0, 'BEAN', '', 'GLUE代码初始化', '2023-02-14 14:26:46', XXL_JOB_INFO_ID.currval - 1, 0, 0, 0, XXL_JOB_INFO_ID.nextval);

INSERT INTO xxl_job_info
(job_group, job_desc, add_time, update_time, author, alarm_email, schedule_type, schedule_conf, misfire_strategy, executor_route_strategy, executor_handler, executor_param, executor_block_strategy, executor_timeout, executor_fail_retry_count, glue_type, glue_source, glue_remark, glue_updatetime, child_jobid, trigger_status, trigger_last_time, trigger_next_time, id)
VALUES(XXL_JOB_GROUP_ID.currval, '航距信息读取', '2023-02-14 14:25:41', '2023-02-14 14:33:38', '陈波', 'chenbo@aograph.com', 'CRON', '0 0 4 * * ?', 'DO_NOTHING', 'FIRST', 'readDepArrPairJob', '{"history":true}', 'SERIAL_EXECUTION', 0, 0, 'BEAN', '', 'GLUE代码初始化', '2023-02-14 14:25:41', XXL_JOB_INFO_ID.currval - 1, 0, 0, 0, XXL_JOB_INFO_ID.nextval);

INSERT INTO xxl_job_info
(job_group, job_desc, add_time, update_time, author, alarm_email, schedule_type, schedule_conf, misfire_strategy, executor_route_strategy, executor_handler, executor_param, executor_block_strategy, executor_timeout, executor_fail_retry_count, glue_type, glue_source, glue_remark, glue_updatetime, child_jobid, trigger_status, trigger_last_time, trigger_next_time, id)
VALUES(XXL_JOB_GROUP_ID.currval, 'AV读取(START)', '2023-02-14 14:25:04', '2023-02-14 14:33:03', '陈波', 'chenbo@aograph.com', 'CRON', '0 0 4 * * ?', 'DO_NOTHING', 'FIRST', 'readAvJob', '{"history":true}', 'SERIAL_EXECUTION', 0, 0, 'BEAN', '', 'GLUE代码初始化', '2023-02-14 14:25:04', XXL_JOB_INFO_ID.currval - 1, 0, 0, 0, XXL_JOB_INFO_ID.nextval);
