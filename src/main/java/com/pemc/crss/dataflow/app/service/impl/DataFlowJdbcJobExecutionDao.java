package com.pemc.crss.dataflow.app.service.impl;

import com.pemc.crss.dataflow.app.support.FinalizeJobQuery;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.support.StlJobQuery;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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
import java.util.Map;

@Slf4j
public class DataFlowJdbcJobExecutionDao extends JdbcJobExecutionDao {
    private static final String FIND_CUSTOM_JOB_INSTANCE = "SELECT A.JOB_INSTANCE_ID, A.JOB_NAME from %PREFIX%JOB_INSTANCE A join %PREFIX%JOB_EXECUTION B on A.JOB_INSTANCE_ID = B.JOB_INSTANCE_ID join %PREFIX%JOB_EXECUTION_PARAMS C on B.JOB_EXECUTION_ID = C.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS D on B.JOB_EXECUTION_ID = D.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS E on B.JOB_EXECUTION_ID = E.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS F on B.JOB_EXECUTION_ID = F.JOB_EXECUTION_ID where JOB_NAME like ? and B.STATUS like ? and TO_CHAR(B.START_TIME, 'yyyy-mm-dd') like ? and (C.STRING_VAL like ? and C.KEY_NAME = 'mode') and (TO_CHAR(D.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and D.KEY_NAME = 'startDate') and (TO_CHAR(E.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and E.KEY_NAME = 'endDate') and (F.STRING_VAL like ? and F.KEY_NAME = 'username') order by JOB_INSTANCE_ID desc";
    private static final String COUNT_JOBS_WITH_NAME = "SELECT COUNT(*) from %PREFIX%JOB_INSTANCE A join %PREFIX%JOB_EXECUTION B on A.JOB_INSTANCE_ID = B.JOB_INSTANCE_ID join %PREFIX%JOB_EXECUTION_PARAMS C on B.JOB_EXECUTION_ID = C.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS D on B.JOB_EXECUTION_ID = D.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS E on B.JOB_EXECUTION_ID = E.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS F on B.JOB_EXECUTION_ID = F.JOB_EXECUTION_ID where JOB_NAME like ? and B.STATUS like ? and TO_CHAR(B.START_TIME, 'yyyy-mm-dd') like ? and (C.STRING_VAL like ? and C.KEY_NAME = 'mode') and (TO_CHAR(D.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and D.KEY_NAME = 'startDate') and (TO_CHAR(E.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and E.KEY_NAME = 'endDate') and (F.STRING_VAL like ? and F.KEY_NAME = 'username')";
    private static final String FIND_CUSTOM_JOB_EXECUTION = "SELECT A.JOB_EXECUTION_ID, A.START_TIME, A.END_TIME, A.STATUS, A.EXIT_CODE, A.EXIT_MESSAGE, A.CREATE_TIME, A.LAST_UPDATED, A.VERSION, A.JOB_CONFIGURATION_LOCATION from %PREFIX%JOB_EXECUTION A join %PREFIX%JOB_EXECUTION_PARAMS B on A.JOB_EXECUTION_ID = B.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS C on A.JOB_EXECUTION_ID = C.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS D on A.JOB_EXECUTION_ID = D.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS E on A.JOB_EXECUTION_ID = E.JOB_EXECUTION_ID where A.JOB_INSTANCE_ID = ? and A.STATUS like ? and TO_CHAR(A.START_TIME, 'yyyy-mm-dd') like ? and (B.STRING_VAL like ? and B.KEY_NAME = 'mode') and (TO_CHAR(C.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and C.KEY_NAME = 'startDate') and (TO_CHAR(D.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and D.KEY_NAME = 'endDate') and (E.STRING_VAL like ? and E.KEY_NAME = 'username') order by JOB_EXECUTION_ID desc";

    public DataFlowJdbcJobExecutionDao() {
    }

    public List<JobExecution> findJobExecutions(JobInstance job, String status, String mode,
                                                String runStartDate, String tradingStartDate, String tradingEndDate, String username) {
        Assert.notNull(job, "Job cannot be null.");
        Assert.notNull(job.getId(), "Job Id cannot be null.");

        status = status.contains("*") ? status.replaceAll("\\*", "%") : status;
        mode = mode.contains("*") ? mode.replaceAll("\\*", "%") : mode;
        runStartDate = runStartDate.isEmpty() ? "%" : runStartDate;
        tradingStartDate = tradingStartDate.isEmpty() ? "%" : tradingStartDate;
        tradingEndDate = tradingEndDate.isEmpty() ? "%" : tradingEndDate;
        username = username.isEmpty() ? "%" : username;

        return this.getJdbcTemplate().query(this.getQuery(FIND_CUSTOM_JOB_EXECUTION), new DataFlowJdbcJobExecutionDao.JobExecutionRowMapper(job), new Object[]{job.getId(),
                status, runStartDate, mode, tradingStartDate, tradingEndDate, username});
    }

    public List<JobInstance> findJobInstancesByName(String jobName, final int start, final int count,
                                                    String status, String mode, String runStartDate,
                                                    String tradingStartDate, String tradingEndDate,
                                                    String username) {
        jobName = jobName.contains("*") ? jobName.replaceAll("\\*", "%") : jobName;
        status = status.contains("*") ? status.replaceAll("\\*", "%") : status;
        mode = mode.contains("*") ? mode.replaceAll("\\*", "%") : mode;
        runStartDate = runStartDate.isEmpty() ? "%" : runStartDate;
        tradingStartDate = tradingStartDate.isEmpty() ? "%" : tradingStartDate;
        tradingEndDate = tradingEndDate.isEmpty() ? "%" : tradingEndDate;
        username = username.isEmpty() ? "%" : username;

        return this.getJdbcTemplate().query(this.getQuery(FIND_CUSTOM_JOB_INSTANCE), new Object[]{jobName, status, runStartDate,
                mode, tradingStartDate, tradingEndDate, username}, getJobInstanceExtractor(start, count));
    }

    public int getJobInstanceCount(String jobName, String status, String mode, String runStartDate,
                                   String tradingStartDate, String tradingEndDate, String username) throws NoSuchJobException {
        try {
            jobName = jobName.contains("*") ? jobName.replaceAll("\\*", "%") : jobName;
            status = status.contains("*") ? status.replaceAll("\\*", "%") : status;
            mode = mode.contains("*") ? mode.replaceAll("\\*", "%") : mode;
            runStartDate = runStartDate.isEmpty() ? "%" : runStartDate;
            tradingStartDate = tradingStartDate.isEmpty() ? "%" : tradingStartDate;
            tradingEndDate = tradingEndDate.isEmpty() ? "%" : tradingEndDate;
            username = username.isEmpty() ? "%" : username;

            return this.getJdbcTemplate().queryForObject(this.getQuery(COUNT_JOBS_WITH_NAME), Integer.class, new Object[]{jobName, status, runStartDate,
                    mode, tradingStartDate, tradingEndDate, username});
        } catch (EmptyResultDataAccessException var3) {
            throw new NoSuchJobException("No job instances were found for job name " + jobName);
        }
    }

    // Stl Job Queries
    public Long countStlJobInstances(final PageableRequest pageableRequest) {
        String processType = resolveProcessType(pageableRequest.getMapParams());

        log.debug("Querying Stl Job Count query with processType: {}", processType);
        switch (processType) {
            case "ADJUSTED":
            case "PRELIM":
            case "FINAL":
                return this.getJdbcTemplate().queryForObject(StlJobQuery.stlFilterMonthlyCountQuery(), Long.class,
                        getStlMonthlyParams(pageableRequest.getMapParams()));
            case "DAILY":
                return this.getJdbcTemplate().queryForObject(StlJobQuery.stlFilterDailyCountQuery(), Long.class,
                        getStlDailyParams(pageableRequest.getMapParams()));
            default:
                return this.getJdbcTemplate().queryForObject(StlJobQuery.stlFilterAllCountQuery(), Long.class);
        }
    }

    public List<JobInstance> findStlJobInstances(final int start, final int count, final PageableRequest pageableRequest) {
        String processType = resolveProcessType(pageableRequest.getMapParams());

        log.debug("Querying Stl Job Select query with processType: {}", processType);
        switch (processType) {
            case "ADJUSTED":
            case "PRELIM":
            case "FINAL":
                return this.getJdbcTemplate().query(StlJobQuery.stlFilterMonthlySelectQuery(),
                        getStlMonthlyParams(pageableRequest.getMapParams()),
                        getJobInstanceExtractor(start, count));
            case "DAILY":
                return this.getJdbcTemplate().query(StlJobQuery.stlFilterDailySelectQuery(),
                        getStlDailyParams(pageableRequest.getMapParams()),
                        getJobInstanceExtractor(start, count));
            default:
                return this.getJdbcTemplate().query(StlJobQuery.stlFilterAllSelectQuery(),
                        getJobInstanceExtractor(start, count));
        }
    }

    public Long countFinalizeJobInstances(MeterProcessType type, String startDate, String endDate) {
        String jobName = MeterProcessType.ADJUSTED == type
                ? FinalizeJobQuery.FINALIZE_JOB_NAME_ADJ
                : FinalizeJobQuery.FINALIZE_JOB_NAME_FINAL;

        return this.getJdbcTemplate().queryForObject(FinalizeJobQuery.countQuery(), Long.class,
                new String[]{jobName, startDate, endDate, type.name()});
    }

    // Support methods
    private String resolveProcessType(final Map<String, String> mapParams) {
        String processType = mapParams.getOrDefault("processType", null);

        if (processType == null) {
            processType = "ALL";
        }

        return processType;
    }

    private String[] getStlMonthlyParams(final Map<String, String> mapParams) {
        String processType = resolveQueryParam(mapParams.getOrDefault("processType", null));
        String startDate = resolveQueryParam(mapParams.getOrDefault("startDate", null));
        String endDate = resolveQueryParam(mapParams.getOrDefault("endDate", null));

        return new String[]{processType, startDate, endDate};
    }

    private String[] getStlDailyParams(final Map<String, String> mapParams) {
        return new String[]{resolveQueryParam(mapParams.getOrDefault("tradingDate", null))};
    }

    private String resolveQueryParam(String param) {
        return StringUtils.isEmpty(param) ? "%" : param;
    }

    // ResultSetExtractors start
    private ResultSetExtractor<List<JobInstance>> getJobInstanceExtractor(int start, int count) {
        return new ResultSetExtractor<List<JobInstance>>() {
            private List<JobInstance> list = new ArrayList<>();

            @Override
            public List<JobInstance> extractData(ResultSet rs) throws SQLException, DataAccessException {
                int rowNum = 0;
                while (rowNum < start && rs.next()) {
                    ++rowNum;
                }

                while (rowNum < start + count && rs.next()) {
                    DataFlowJdbcJobExecutionDao.JobInstanceRowMapper rowMapper = DataFlowJdbcJobExecutionDao.this.new JobInstanceRowMapper();
                    this.list.add(rowMapper.mapRow(rs, rowNum));
                    ++rowNum;
                }

                return this.list;
            }
        };
    }
    // ResultSetExtractors end

    // RowMappers start
    private final class JobExecutionRowMapper implements RowMapper<JobExecution> {
        private JobInstance jobInstance;
        private JobParameters jobParameters;

        public JobExecutionRowMapper(JobInstance jobInstance) {
            this.jobInstance = jobInstance;
        }

        public JobExecution mapRow(ResultSet rs, int rowNum) throws SQLException {
            Long id = rs.getLong(1);
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
            jobExecution.setVersion(rs.getInt(9));
            return jobExecution;
        }
    }

    private final class JobInstanceRowMapper implements RowMapper<JobInstance> {
        public JobInstanceRowMapper() {

        }

        public JobInstance mapRow(ResultSet rs, int rowNum) throws SQLException {
            JobInstance jobInstance = new JobInstance(rs.getLong(1), rs.getString(2));
            jobInstance.incrementVersion();
            return jobInstance;
        }
    }
    // RowMappers end
}
