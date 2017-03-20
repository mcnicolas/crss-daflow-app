package com.pemc.crss.dataflow.app.service.impl;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.dao.JdbcJobExecutionDao;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.Assert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DataFlowJdbcJobExecutionDao extends JdbcJobExecutionDao {
    private static final String FIND_CUSTOM_JOB_INSTANCE = "SELECT A.JOB_INSTANCE_ID, A.JOB_NAME from %PREFIX%JOB_INSTANCE A join %PREFIX%JOB_EXECUTION B on A.JOB_INSTANCE_ID = B.JOB_INSTANCE_ID join %PREFIX%JOB_EXECUTION_PARAMS C on B.JOB_EXECUTION_ID = C.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS D on B.JOB_EXECUTION_ID = D.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS E on B.JOB_EXECUTION_ID = E.JOB_EXECUTION_ID where JOB_NAME like ? and B.STATUS like ? and TO_CHAR(B.START_TIME, 'yyyy-mm-dd') like ? and COALESCE(TO_CHAR(B.END_TIME, 'yyyy-mm-dd'), '') like ? and (C.STRING_VAL like ? and C.KEY_NAME = 'mode') and (TO_CHAR(D.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and D.KEY_NAME = 'startDate') and (TO_CHAR(E.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and E.KEY_NAME = 'endDate') order by JOB_INSTANCE_ID desc";
    private static final String COUNT_JOBS_WITH_NAME = "SELECT COUNT(*) from %PREFIX%JOB_INSTANCE A join %PREFIX%JOB_EXECUTION B on A.JOB_INSTANCE_ID = B.JOB_INSTANCE_ID join %PREFIX%JOB_EXECUTION_PARAMS C on B.JOB_EXECUTION_ID = C.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS D on B.JOB_EXECUTION_ID = D.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS E on B.JOB_EXECUTION_ID = E.JOB_EXECUTION_ID where JOB_NAME like ? and B.STATUS like ? and TO_CHAR(B.START_TIME, 'yyyy-mm-dd') like ? and COALESCE(TO_CHAR(B.END_TIME, 'yyyy-mm-dd'), '') like ? and (C.STRING_VAL like ? and C.KEY_NAME = 'mode') and (TO_CHAR(D.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and D.KEY_NAME = 'startDate') and (TO_CHAR(E.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and E.KEY_NAME = 'endDate')";
    private static final String FIND_CUSTOM_JOB_EXECUTION = "SELECT A.JOB_EXECUTION_ID, A.START_TIME, A.END_TIME, A.STATUS, A.EXIT_CODE, A.EXIT_MESSAGE, A.CREATE_TIME, A.LAST_UPDATED, A.VERSION, A.JOB_CONFIGURATION_LOCATION from %PREFIX%JOB_EXECUTION A join %PREFIX%JOB_EXECUTION_PARAMS B on A.JOB_EXECUTION_ID = B.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS C on A.JOB_EXECUTION_ID = C.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS D on A.JOB_EXECUTION_ID = D.JOB_EXECUTION_ID where A.JOB_INSTANCE_ID = ? and A.STATUS like ? and TO_CHAR(A.START_TIME, 'yyyy-mm-dd') like ? and COALESCE(TO_CHAR(A.END_TIME, 'yyyy-mm-dd'), '') like ? and (B.STRING_VAL like ? and B.KEY_NAME = 'mode') and (TO_CHAR(C.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and C.KEY_NAME = 'startDate') and (TO_CHAR(D.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and D.KEY_NAME = 'endDate') order by JOB_EXECUTION_ID desc";

    public DataFlowJdbcJobExecutionDao() {
    }

    public List<JobExecution> findJobExecutions(JobInstance job, String status, String mode,
                                                String runStartDate, String runEndDate,
                                                String tradingStartDate, String tradingEndDate) {
        Assert.notNull(job, "Job cannot be null.");
        Assert.notNull(job.getId(), "Job Id cannot be null.");

        status = status.contains("*") ? status.replaceAll("\\*", "%") : status;
        mode = mode.contains("*") ? mode.replaceAll("\\*", "%") : mode;
        runStartDate = runStartDate.isEmpty() ? "%" : runStartDate;
        runEndDate = runEndDate.isEmpty() ? "%" : runEndDate;
        tradingStartDate = tradingStartDate.isEmpty() ? "%" : tradingStartDate;
        tradingEndDate = tradingEndDate.isEmpty() ? "%" : tradingEndDate;

        return this.getJdbcTemplate().query(this.getQuery(FIND_CUSTOM_JOB_EXECUTION), new DataFlowJdbcJobExecutionDao.JobExecutionRowMapper(job), new Object[]{job.getId(),
                status, runStartDate, runEndDate, mode, tradingStartDate, tradingEndDate});
    }

    public List<JobInstance> findJobInstancesByName(String jobName, final int start, final int count,
                                                    String status, String mode, String runStartDate, String runEndDate,
                                                    String tradingStartDate, String tradingEndDate) {
        ResultSetExtractor extractor = new ResultSetExtractor() {
            private List<JobInstance> list = new ArrayList();

            public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
                int rowNum;
                for (rowNum = 0; rowNum < start && rs.next(); ++rowNum) {
                    ;
                }

                while (rowNum < start + count && rs.next()) {
                    DataFlowJdbcJobExecutionDao.JobInstanceRowMapper rowMapper = DataFlowJdbcJobExecutionDao.this.new JobInstanceRowMapper();
                    this.list.add(rowMapper.mapRow(rs, rowNum));
                    ++rowNum;
                }

                return this.list;
            }
        };

        jobName = jobName.contains("*") ? jobName.replaceAll("\\*", "%") : jobName;
        status = status.contains("*") ? status.replaceAll("\\*", "%") : status;
        mode = mode.contains("*") ? mode.replaceAll("\\*", "%") : mode;
        runStartDate = runStartDate.isEmpty() ? "%" : runStartDate;
        runEndDate = runEndDate.isEmpty() ? "%" : runEndDate;
        tradingStartDate = tradingStartDate.isEmpty() ? "%" : tradingStartDate;
        tradingEndDate = tradingEndDate.isEmpty() ? "%" : tradingEndDate;

        return (List) this.getJdbcTemplate().query(this.getQuery(FIND_CUSTOM_JOB_INSTANCE), new Object[]{jobName, status, runStartDate,
                runEndDate, mode, tradingStartDate, tradingEndDate}, extractor);
    }

    public int getJobInstanceCount(String jobName, String status, String mode, String runStartDate, String runEndDate,
                                   String tradingStartDate, String tradingEndDate) throws NoSuchJobException {
        try {
            jobName = jobName.contains("*") ? jobName.replaceAll("\\*", "%") : jobName;
            status = status.contains("*") ? status.replaceAll("\\*", "%") : status;
            mode = mode.contains("*") ? mode.replaceAll("\\*", "%") : mode;
            runStartDate = runStartDate.isEmpty() ? "%" : runStartDate;
            runEndDate = runEndDate.isEmpty() ? "%" : runEndDate;
            tradingStartDate = tradingStartDate.isEmpty() ? "%" : tradingStartDate;
            tradingEndDate = tradingEndDate.isEmpty() ? "%" : tradingEndDate;

            return this.getJdbcTemplate().queryForObject(this.getQuery(COUNT_JOBS_WITH_NAME), Integer.class, new Object[]{jobName, status, runStartDate,
                    runEndDate, mode, tradingStartDate, tradingEndDate}).intValue();
        } catch (EmptyResultDataAccessException var3) {
            throw new NoSuchJobException("No job instances were found for job name " + jobName);
        }
    }

    private final class JobExecutionRowMapper implements RowMapper<JobExecution> {
        private JobInstance jobInstance;
        private JobParameters jobParameters;

        public JobExecutionRowMapper(JobInstance jobInstance) {
            this.jobInstance = jobInstance;
        }

        public JobExecution mapRow(ResultSet rs, int rowNum) throws SQLException {
            Long id = Long.valueOf(rs.getLong(1));
            String jobConfigurationLocation = rs.getString(10);
            if (this.jobParameters == null) {
                this.jobParameters = DataFlowJdbcJobExecutionDao.this.getJobParameters(id);
            }

            JobExecution jobExecution;
            if (this.jobInstance == null) {
                jobExecution = new JobExecution(id, this.jobParameters, jobConfigurationLocation);
            } else {
                jobExecution = new JobExecution(this.jobInstance, id, this.jobParameters, jobConfigurationLocation);
            }

            jobExecution.setStartTime(rs.getTimestamp(2));
            jobExecution.setEndTime(rs.getTimestamp(3));
            jobExecution.setStatus(BatchStatus.valueOf(rs.getString(4)));
            jobExecution.setExitStatus(new ExitStatus(rs.getString(5), rs.getString(6)));
            jobExecution.setCreateTime(rs.getTimestamp(7));
            jobExecution.setLastUpdated(rs.getTimestamp(8));
            jobExecution.setVersion(Integer.valueOf(rs.getInt(9)));
            return jobExecution;
        }
    }

    private final class JobInstanceRowMapper implements RowMapper<JobInstance> {
        public JobInstanceRowMapper() {
        }

        public JobInstance mapRow(ResultSet rs, int rowNum) throws SQLException {
            JobInstance jobInstance = new JobInstance(Long.valueOf(rs.getLong(1)), rs.getString(2));
            jobInstance.incrementVersion();
            return jobInstance;
        }
    }
}
