package com.pemc.crss.dataflow.app.service.impl;

import com.pemc.crss.dataflow.app.dto.DistinctAddtlCompDto;
import com.pemc.crss.dataflow.app.support.FinalizeJobQuery;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.support.StlCalculationQuery;
import com.pemc.crss.dataflow.app.support.StlJobQuery;
import com.pemc.crss.dataflow.app.support.StlQueryProcessType;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.DateUtil;
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
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.pemc.crss.dataflow.app.support.AddtlCompensationQuery.ADDTL_COMP_COMPLETE_FINALIZE_QUERY;
import static com.pemc.crss.dataflow.app.support.AddtlCompensationQuery.ADDTL_COMP_DISTINCT_COUNT_QUERY;
import static com.pemc.crss.dataflow.app.support.AddtlCompensationQuery.ADDTL_COMP_DISTINCT_QUERY;
import static com.pemc.crss.dataflow.app.support.AddtlCompensationQuery.ADDTL_COMP_INTS_QUERY;

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
        StlQueryProcessType processType = resolveProcessType(pageableRequest.getMapParams());

        log.debug("Querying Stl Job Count query with processType: {}", processType);
        switch (processType) {
            case ADJUSTED:
            case PRELIM:
            case FINAL:
            case ALL_MONTHLY:
                return this.getJdbcTemplate().queryForObject(StlJobQuery.stlFilterMonthlyCountQuery(), Long.class,
                        getStlMonthlyParams(pageableRequest.getMapParams(), processType));
            case DAILY:
                return this.getJdbcTemplate().queryForObject(StlJobQuery.stlFilterDailyCountQuery(), Long.class,
                        getStlDailyParams(pageableRequest.getMapParams()));
            default:
                return this.getJdbcTemplate().queryForObject(StlJobQuery.stlFilterAllCountQuery(), Long.class);
        }
    }

    public List<JobInstance> findStlJobInstances(final int start, final int count, final PageableRequest pageableRequest) {
        StlQueryProcessType processType = resolveProcessType(pageableRequest.getMapParams());

        log.debug("Querying Stl Job Select query with processType: {}", processType);
        switch (processType) {
            case ADJUSTED:
            case PRELIM:
            case FINAL:
            case ALL_MONTHLY:
                return this.getJdbcTemplate().query(StlJobQuery.stlFilterMonthlySelectQuery(),
                        getStlMonthlyParams(pageableRequest.getMapParams(), processType),
                        getJobInstanceExtractor(start, count));
            case DAILY:
                return this.getJdbcTemplate().query(StlJobQuery.stlFilterDailySelectQuery(),
                        getStlDailyParams(pageableRequest.getMapParams()),
                        getJobInstanceExtractor(start, count));
            default:
                return this.getJdbcTemplate().query(StlJobQuery.stlFilterAllSelectQuery(),
                        getJobInstanceExtractor(start, count));
        }
    }

    // Additional Compensation Queries
    public List<JobInstance> findAddtlCompJobInstances(final int start, final int count, final DistinctAddtlCompDto dto) {
        log.debug("Querying Additional Compensation Job Select query with processType: {}");
        return this.getJdbcTemplate().query(this.getQuery(ADDTL_COMP_INTS_QUERY),
                new String[]{"%", DateUtil.convertToString(dto.getStartDate(), DateUtil.DEFAULT_DATE_FORMAT),
                        DateUtil.convertToString(dto.getEndDate(), DateUtil.DEFAULT_DATE_FORMAT), dto.getPricingCondition()},
                getJobInstanceExtractor(start, count));
    }

    public List<JobInstance> findAddtlCompCompleteFinalizeInstances(final int start, final int count,
        final String startDate, final String endDate, final String pricingCondition) {
        return this.getJdbcTemplate().query(this.getQuery(ADDTL_COMP_COMPLETE_FINALIZE_QUERY),
                new String[]{startDate, endDate, pricingCondition}, getJobInstanceExtractor(start, count));
    }

    public Long countDistinctAddtlCompJobInstances(final Map<String, String> mapParams) {
        log.debug("Querying Additional Compensation Job Distinct Count query with params: {}", mapParams);
        return this.getJdbcTemplate().queryForObject(this.getQuery(ADDTL_COMP_DISTINCT_COUNT_QUERY), Long.class,
                resolveAddtlCompFilterParams(mapParams));
    }

    public List<DistinctAddtlCompDto> findDistinctAddtlCompJobInstances(final int start, final int count,
                                                                        final Map<String, String> mapParams) {
        log.debug("Querying Additional Compensation Job Distinct Select query with params: {}", mapParams);

        return this.getJdbcTemplate().query(this.getQuery(ADDTL_COMP_DISTINCT_QUERY),
                resolveAddtlCompFilterParams(mapParams), getAddtlCompExtractor(start, count));
    }

    private String[] resolveAddtlCompFilterParams(final Map<String, String> mapParams) {
        String status = "%"; // filter all statuses
        String startDate = resolveQueryParam(mapParams.getOrDefault("startDate", null));
        String endDate = resolveQueryParam(mapParams.getOrDefault("endDate", null));
        String pricingCondition = resolveQueryParam(mapParams.getOrDefault("pricingCondition", null));

        return new String[]{status, startDate, endDate, pricingCondition};
    }

    public Long countFinalizeJobInstances(MeterProcessType type, String startDate, String endDate) {
        String jobName = MeterProcessType.ADJUSTED == type
                ? FinalizeJobQuery.FINALIZE_JOB_NAME_ADJ
                : FinalizeJobQuery.FINALIZE_JOB_NAME_FINAL;

        return this.getJdbcTemplate().queryForObject(FinalizeJobQuery.countQuery(), Long.class,
                new String[]{jobName, startDate, endDate, type.name()});
    }

    public List<JobExecution> findStlCalcJobInstances(String parentGroup, MeterProcessType type, String startDate, String endDate) {
        String jobName;
        switch (type) {
            case PRELIM:
            case PRELIMINARY:
                jobName = StlCalculationQuery.CALC_JOB_NAME_PRELIM;
                break;
            case FINAL:
                jobName = StlCalculationQuery.CALC_JOB_NAME_FINAL;
                break;
            case ADJUSTED:
                jobName = StlCalculationQuery.CALC_JOB_NAME_ADJ;
                break;
            default:
                jobName = StlCalculationQuery.CALC_JOB_NAME_DAILY;
        }

        return this.getJdbcTemplate().query(StlCalculationQuery.executionQuery(), new DataFlowJdbcJobExecutionDao.JobExecutionColNameRowMapper(null),
                new String[]{jobName.concat(parentGroup), startDate, endDate, startDate, endDate, type != null ? type.name() : ""});
    }

    // Support methods
    private StlQueryProcessType resolveProcessType(final Map<String, String> mapParams) {
        String processType = mapParams.getOrDefault("processType", null);

        if (processType == null) {
            processType = "ALL";
        }

        return StlQueryProcessType.valueOf(processType);
    }

    private String[] getStlMonthlyParams(final Map<String, String> mapParams, StlQueryProcessType processTypeEnum) {
        String processType = StlQueryProcessType.MONTHLY_PROCESS_TYPES.contains(processTypeEnum)
                ? processTypeEnum.toString() : "%";
        String startDate = resolveQueryParam(mapParams.getOrDefault("startDate", null));
        String endDate = resolveQueryParam(mapParams.getOrDefault("endDate", null));

        return new String[]{processType, startDate, endDate};
    }

    private String[] getStlDailyParams(final Map<String, String> mapParams) {
        String tradingDateStart = getStringValFromMap(mapParams, "tradingDateStart", StlJobQuery.DEFAULT_TRADING_DATE_START);
        String tradingDateEnd = getStringValFromMap(mapParams, "tradingDateEnd", StlJobQuery.DEFAULT_TRADING_DATE_END);

        return new String[]{tradingDateStart, tradingDateEnd};
    }

    private String resolveQueryParam(String param) {
        return StringUtils.isEmpty(param) ? "%" : param;
    }

    private String getStringValFromMap(final Map<String, String> mapParams, final String key, final String defaultValue) {
        return mapParams.containsKey(key) && mapParams.get(key) != null ? mapParams.get(key) : defaultValue;
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

    private ResultSetExtractor<List<DistinctAddtlCompDto>> getAddtlCompExtractor(int start, int count) {
        return rs -> {
            List<DistinctAddtlCompDto> list = new ArrayList<>();

            int rowNum = 0;
            while (rowNum < start && rs.next()) {
                ++rowNum;
            }

            while (rowNum < start + count && rs.next()) {
                Date startDate = rs.getDate("start_date");
                Date endDate = rs.getDate("end_date");
                String pricingCondition = rs.getString("pricing_condition");
                list.add(DistinctAddtlCompDto.create(startDate, endDate, pricingCondition));
                ++rowNum;
            }

            return list;
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

    private final class JobExecutionColNameRowMapper implements RowMapper<JobExecution> {
        private JobInstance jobInstance;
        private JobParameters jobParameters;

        private final String[] columns = new String[]{
                "job_execution_id",
                "version",
                "job_instance_id",
                "create_time",
                "start_time",
                "end_time",
                "status",
                "exit_code",
                "exit_message",
                "last_updated",
                "job_configuration_location"
        };

        public JobExecutionColNameRowMapper(JobInstance jobInstance) {
            this.jobInstance = jobInstance;
        }

        public JobExecution mapRow(ResultSet rs, int rowNum) throws SQLException {
            Long id = rs.getLong(columns[0]);
            String jobConfigurationLocation = rs.getString(columns[10]);
            this.jobParameters = DataFlowJdbcJobExecutionDao.this.getJobParameters(id);

            JobExecution jobExecution;
            if (this.jobInstance == null) {
                jobExecution = new JobExecution(id, this.jobParameters, jobConfigurationLocation);
            } else {
                jobExecution = new JobExecution(this.jobInstance, id, this.jobParameters, jobConfigurationLocation);
            }

            jobExecution.setStartTime(rs.getTimestamp(columns[4]));
            jobExecution.setEndTime(rs.getTimestamp(columns[5]));
            jobExecution.setStatus(BatchStatus.valueOf(rs.getString(columns[6])));
            jobExecution.setExitStatus(new ExitStatus(rs.getString(columns[7]), rs.getString(columns[8])));
            jobExecution.setCreateTime(rs.getTimestamp(columns[3]));
            jobExecution.setLastUpdated(rs.getTimestamp(columns[9]));
            jobExecution.setVersion(rs.getInt(columns[1]));
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
