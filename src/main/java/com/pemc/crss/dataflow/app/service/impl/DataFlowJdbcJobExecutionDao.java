package com.pemc.crss.dataflow.app.service.impl;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.dao.AbstractJdbcBatchMetadataDao;
import org.springframework.batch.core.repository.dao.NoSuchObjectException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;
import org.springframework.util.Assert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

public class DataFlowJdbcJobExecutionDao extends AbstractJdbcBatchMetadataDao implements DataFlowJobExecutionDao, InitializingBean {
    private static final Log logger = LogFactory.getLog(org.springframework.batch.core.repository.dao.JdbcJobExecutionDao.class);
    private static final String SAVE_JOB_EXECUTION = "INSERT into %PREFIX%JOB_EXECUTION(JOB_EXECUTION_ID, JOB_INSTANCE_ID, START_TIME, END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE, VERSION, CREATE_TIME, LAST_UPDATED, JOB_CONFIGURATION_LOCATION) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String CHECK_JOB_EXECUTION_EXISTS = "SELECT COUNT(*) FROM %PREFIX%JOB_EXECUTION WHERE JOB_EXECUTION_ID = ?";
    private static final String GET_STATUS = "SELECT STATUS from %PREFIX%JOB_EXECUTION where JOB_EXECUTION_ID = ?";
    private static final String UPDATE_JOB_EXECUTION = "UPDATE %PREFIX%JOB_EXECUTION set START_TIME = ?, END_TIME = ?,  STATUS = ?, EXIT_CODE = ?, EXIT_MESSAGE = ?, VERSION = ?, CREATE_TIME = ?, LAST_UPDATED = ? where JOB_EXECUTION_ID = ? and VERSION = ?";
    private static final String FIND_JOB_EXECUTIONS = "SELECT JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE, CREATE_TIME, LAST_UPDATED, VERSION, JOB_CONFIGURATION_LOCATION from %PREFIX%JOB_EXECUTION where JOB_INSTANCE_ID = ? order by JOB_EXECUTION_ID desc";
    private static final String GET_LAST_EXECUTION = "SELECT JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE, CREATE_TIME, LAST_UPDATED, VERSION, JOB_CONFIGURATION_LOCATION from %PREFIX%JOB_EXECUTION E where JOB_INSTANCE_ID = ? and JOB_EXECUTION_ID in (SELECT max(JOB_EXECUTION_ID) from %PREFIX%JOB_EXECUTION E2 where E2.JOB_INSTANCE_ID = ?)";
    private static final String GET_EXECUTION_BY_ID = "SELECT JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE, CREATE_TIME, LAST_UPDATED, VERSION, JOB_CONFIGURATION_LOCATION from %PREFIX%JOB_EXECUTION where JOB_EXECUTION_ID = ?";
    private static final String GET_RUNNING_EXECUTIONS = "SELECT E.JOB_EXECUTION_ID, E.START_TIME, E.END_TIME, E.STATUS, E.EXIT_CODE, E.EXIT_MESSAGE, E.CREATE_TIME, E.LAST_UPDATED, E.VERSION, E.JOB_INSTANCE_ID, E.JOB_CONFIGURATION_LOCATION from %PREFIX%JOB_EXECUTION E, %PREFIX%JOB_INSTANCE I where E.JOB_INSTANCE_ID=I.JOB_INSTANCE_ID and I.JOB_NAME=? and E.END_TIME is NULL order by E.JOB_EXECUTION_ID desc";
    private static final String CURRENT_VERSION_JOB_EXECUTION = "SELECT VERSION FROM %PREFIX%JOB_EXECUTION WHERE JOB_EXECUTION_ID=?";
    private static final String FIND_PARAMS_FROM_ID = "SELECT JOB_EXECUTION_ID, KEY_NAME, TYPE_CD, STRING_VAL, DATE_VAL, LONG_VAL, DOUBLE_VAL, IDENTIFYING from %PREFIX%JOB_EXECUTION_PARAMS where JOB_EXECUTION_ID = ?";
    private static final String CREATE_JOB_PARAMETERS = "INSERT into %PREFIX%JOB_EXECUTION_PARAMS(JOB_EXECUTION_ID, KEY_NAME, TYPE_CD, STRING_VAL, DATE_VAL, LONG_VAL, DOUBLE_VAL, IDENTIFYING) values (?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String FIND_CUSTOM_JOB_INSTANCE = "SELECT A.JOB_INSTANCE_ID, A.JOB_NAME from %PREFIX%JOB_INSTANCE A join %PREFIX%JOB_EXECUTION B on A.JOB_INSTANCE_ID = B.JOB_INSTANCE_ID join %PREFIX%JOB_EXECUTION_PARAMS C on B.JOB_EXECUTION_ID = C.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS D on B.JOB_EXECUTION_ID = D.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS E on B.JOB_EXECUTION_ID = E.JOB_EXECUTION_ID where JOB_NAME like ? and B.STATUS like ? and TO_CHAR(B.START_TIME, 'yyyy-mm-dd hh24:mi') like ? and TO_CHAR(B.END_TIME, 'yyyy-mm-dd hh24:mi') like ? and (C.STRING_VAL like ? and C.KEY_NAME = 'mode') and (TO_CHAR(D.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and D.KEY_NAME = 'startDate') and (TO_CHAR(E.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and E.KEY_NAME = 'endDate') order by JOB_INSTANCE_ID desc";
    private static final String COUNT_JOBS_WITH_NAME = "SELECT COUNT(*) from %PREFIX%JOB_INSTANCE A join %PREFIX%JOB_EXECUTION B on A.JOB_INSTANCE_ID = B.JOB_INSTANCE_ID join %PREFIX%JOB_EXECUTION_PARAMS C on B.JOB_EXECUTION_ID = C.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS D on B.JOB_EXECUTION_ID = D.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS E on B.JOB_EXECUTION_ID = E.JOB_EXECUTION_ID where JOB_NAME like ? and B.STATUS like ? and TO_CHAR(B.START_TIME, 'yyyy-mm-dd hh24:mi') like ? and TO_CHAR(B.END_TIME, 'yyyy-mm-dd hh24:mi') like ? and (C.STRING_VAL like ? and C.KEY_NAME = 'mode') and (TO_CHAR(D.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and D.KEY_NAME = 'startDate') and (TO_CHAR(E.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and E.KEY_NAME = 'endDate')";
    private static final String FIND_CUSTOM_JOB_EXECUTION = "SELECT A.JOB_EXECUTION_ID, A.START_TIME, A.END_TIME, A.STATUS, A.EXIT_CODE, A.EXIT_MESSAGE, A.CREATE_TIME, A.LAST_UPDATED, A.VERSION, A.JOB_CONFIGURATION_LOCATION from %PREFIX%JOB_EXECUTION A join %PREFIX%JOB_EXECUTION_PARAMS B on A.JOB_EXECUTION_ID = B.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS C on A.JOB_EXECUTION_ID = C.JOB_EXECUTION_ID join %PREFIX%JOB_EXECUTION_PARAMS D on A.JOB_EXECUTION_ID = D.JOB_EXECUTION_ID where A.JOB_INSTANCE_ID = ? and A.STATUS like ? and TO_CHAR(A.START_TIME, 'yyyy-mm-dd hh24:mi') like ? and TO_CHAR(A.END_TIME, 'yyyy-mm-dd hh24:mi') like ? and (B.STRING_VAL like ? and B.KEY_NAME = 'mode') and (TO_CHAR(C.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and C.KEY_NAME = 'startDate') and (TO_CHAR(D.DATE_VAL, 'yyyy-mm-dd hh24:mi') like ? and D.KEY_NAME = 'endDate') order by JOB_EXECUTION_ID desc";
    private int exitMessageLength = 2500;
    private DataFieldMaxValueIncrementer jobExecutionIncrementer;

    public DataFlowJdbcJobExecutionDao() {
    }

    public void setExitMessageLength(int exitMessageLength) {
        this.exitMessageLength = exitMessageLength;
    }

    public void setJobExecutionIncrementer(DataFieldMaxValueIncrementer jobExecutionIncrementer) {
        this.jobExecutionIncrementer = jobExecutionIncrementer;
    }

    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        Assert.notNull(this.jobExecutionIncrementer, "The jobExecutionIncrementer must not be null.");
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
                for(rowNum = 0; rowNum < start && rs.next(); ++rowNum) {
                    ;
                }

                while(rowNum < start + count && rs.next()) {
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

        List result = (List)this.getJdbcTemplate().query(this.getQuery(FIND_CUSTOM_JOB_INSTANCE), new Object[]{jobName, status, runStartDate,
                runEndDate, mode, tradingStartDate, tradingEndDate}, extractor);
        return result;
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

            return ((Integer)this.getJdbcTemplate().queryForObject(this.getQuery(COUNT_JOBS_WITH_NAME), Integer.class, new Object[]{jobName, status, runStartDate,
                    runEndDate, mode, tradingStartDate, tradingEndDate})).intValue();
        } catch (EmptyResultDataAccessException var3) {
            throw new NoSuchJobException("No job instances were found for job name " + jobName);
        }
    }

    public void saveJobExecution(JobExecution jobExecution) {
        this.validateJobExecution(jobExecution);
        jobExecution.incrementVersion();
        jobExecution.setId(Long.valueOf(this.jobExecutionIncrementer.nextLongValue()));
        Object[] parameters = new Object[]{jobExecution.getId(), jobExecution.getJobId(), jobExecution.getStartTime(), jobExecution.getEndTime(), jobExecution.getStatus().toString(), jobExecution.getExitStatus().getExitCode(), jobExecution.getExitStatus().getExitDescription(), jobExecution.getVersion(), jobExecution.getCreateTime(), jobExecution.getLastUpdated(), jobExecution.getJobConfigurationName()};
        this.getJdbcTemplate().update(this.getQuery("INSERT into %PREFIX%JOB_EXECUTION(JOB_EXECUTION_ID, JOB_INSTANCE_ID, START_TIME, END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE, VERSION, CREATE_TIME, LAST_UPDATED, JOB_CONFIGURATION_LOCATION) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"), parameters, new int[]{-5, -5, 93, 93, 12, 12, 12, 4, 93, 93, 12});
        this.insertJobParameters(jobExecution.getId(), jobExecution.getJobParameters());
    }

    private void validateJobExecution(JobExecution jobExecution) {
        Assert.notNull(jobExecution);
        Assert.notNull(jobExecution.getJobId(), "JobExecution Job-Id cannot be null.");
        Assert.notNull(jobExecution.getStatus(), "JobExecution status cannot be null.");
        Assert.notNull(jobExecution.getCreateTime(), "JobExecution create time cannot be null");
    }

    public void updateJobExecution(JobExecution jobExecution) {
        this.validateJobExecution(jobExecution);
        Assert.notNull(jobExecution.getId(), "JobExecution ID cannot be null. JobExecution must be saved before it can be updated");
        Assert.notNull(jobExecution.getVersion(), "JobExecution version cannot be null. JobExecution must be saved before it can be updated");
        synchronized(jobExecution) {
            Integer version = Integer.valueOf(jobExecution.getVersion().intValue() + 1);
            String exitDescription = jobExecution.getExitStatus().getExitDescription();
            if(exitDescription != null && exitDescription.length() > this.exitMessageLength) {
                exitDescription = exitDescription.substring(0, this.exitMessageLength);
                if(logger.isDebugEnabled()) {
                    logger.debug("Truncating long message before update of JobExecution: " + jobExecution);
                }
            }

            Object[] parameters = new Object[]{jobExecution.getStartTime(), jobExecution.getEndTime(), jobExecution.getStatus().toString(), jobExecution.getExitStatus().getExitCode(), exitDescription, version, jobExecution.getCreateTime(), jobExecution.getLastUpdated(), jobExecution.getId(), jobExecution.getVersion()};
            if(((Integer)this.getJdbcTemplate().queryForObject(this.getQuery("SELECT COUNT(*) FROM %PREFIX%JOB_EXECUTION WHERE JOB_EXECUTION_ID = ?"), Integer.class, new Object[]{jobExecution.getId()})).intValue() != 1) {
                throw new NoSuchObjectException("Invalid JobExecution, ID " + jobExecution.getId() + " not found.");
            } else {
                int count = this.getJdbcTemplate().update(this.getQuery("UPDATE %PREFIX%JOB_EXECUTION set START_TIME = ?, END_TIME = ?,  STATUS = ?, EXIT_CODE = ?, EXIT_MESSAGE = ?, VERSION = ?, CREATE_TIME = ?, LAST_UPDATED = ? where JOB_EXECUTION_ID = ? and VERSION = ?"), parameters, new int[]{93, 93, 12, 12, 12, 4, 93, 93, -5, 4});
                if(count == 0) {
                    int curentVersion = ((Integer)this.getJdbcTemplate().queryForObject(this.getQuery("SELECT VERSION FROM %PREFIX%JOB_EXECUTION WHERE JOB_EXECUTION_ID=?"), Integer.class, new Object[]{jobExecution.getId()})).intValue();
                    throw new OptimisticLockingFailureException("Attempt to update job execution id=" + jobExecution.getId() + " with wrong version (" + jobExecution.getVersion() + "), where current version is " + curentVersion);
                } else {
                    jobExecution.incrementVersion();
                }
            }
        }
    }

    public JobExecution getLastJobExecution(JobInstance jobInstance) {
        Long id = jobInstance.getId();
        List executions = this.getJdbcTemplate().query(this.getQuery("SELECT JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE, CREATE_TIME, LAST_UPDATED, VERSION, JOB_CONFIGURATION_LOCATION from %PREFIX%JOB_EXECUTION E where JOB_INSTANCE_ID = ? and JOB_EXECUTION_ID in (SELECT max(JOB_EXECUTION_ID) from %PREFIX%JOB_EXECUTION E2 where E2.JOB_INSTANCE_ID = ?)"), new DataFlowJdbcJobExecutionDao.JobExecutionRowMapper(jobInstance), new Object[]{id, id});
        Assert.state(executions.size() <= 1, "There must be at most one latest job execution");
        return executions.isEmpty()?null:(JobExecution)executions.get(0);
    }

    public JobExecution getJobExecution(Long executionId) {
        try {
            JobExecution e = (JobExecution)this.getJdbcTemplate().queryForObject(this.getQuery("SELECT JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE, CREATE_TIME, LAST_UPDATED, VERSION, JOB_CONFIGURATION_LOCATION from %PREFIX%JOB_EXECUTION where JOB_EXECUTION_ID = ?"), new DataFlowJdbcJobExecutionDao.JobExecutionRowMapper(), new Object[]{executionId});
            return e;
        } catch (EmptyResultDataAccessException var3) {
            return null;
        }
    }

    public Set<JobExecution> findRunningJobExecutions(String jobName) {
        final HashSet result = new HashSet();
        RowCallbackHandler handler = new RowCallbackHandler() {
            public void processRow(ResultSet rs) throws SQLException {
                DataFlowJdbcJobExecutionDao.JobExecutionRowMapper mapper = DataFlowJdbcJobExecutionDao.this.new JobExecutionRowMapper();
                result.add(mapper.mapRow(rs, 0));
            }
        };
        this.getJdbcTemplate().query(this.getQuery("SELECT E.JOB_EXECUTION_ID, E.START_TIME, E.END_TIME, E.STATUS, E.EXIT_CODE, E.EXIT_MESSAGE, E.CREATE_TIME, E.LAST_UPDATED, E.VERSION, E.JOB_INSTANCE_ID, E.JOB_CONFIGURATION_LOCATION from %PREFIX%JOB_EXECUTION E, %PREFIX%JOB_INSTANCE I where E.JOB_INSTANCE_ID=I.JOB_INSTANCE_ID and I.JOB_NAME=? and E.END_TIME is NULL order by E.JOB_EXECUTION_ID desc"), new Object[]{jobName}, handler);
        return result;
    }

    public void synchronizeStatus(JobExecution jobExecution) {
        int currentVersion = ((Integer)this.getJdbcTemplate().queryForObject(this.getQuery("SELECT VERSION FROM %PREFIX%JOB_EXECUTION WHERE JOB_EXECUTION_ID=?"), Integer.class, new Object[]{jobExecution.getId()})).intValue();
        if(currentVersion != jobExecution.getVersion().intValue()) {
            String status = (String)this.getJdbcTemplate().queryForObject(this.getQuery("SELECT STATUS from %PREFIX%JOB_EXECUTION where JOB_EXECUTION_ID = ?"), String.class, new Object[]{jobExecution.getId()});
            jobExecution.upgradeStatus(BatchStatus.valueOf(status));
            jobExecution.setVersion(Integer.valueOf(currentVersion));
        }

    }

    private void insertJobParameters(Long executionId, JobParameters jobParameters) {
        Iterator i$ = jobParameters.getParameters().entrySet().iterator();

        while(i$.hasNext()) {
            Map.Entry entry = (Map.Entry)i$.next();
            JobParameter jobParameter = (JobParameter)entry.getValue();
            this.insertParameter(executionId, jobParameter.getType(), (String)entry.getKey(), jobParameter.getValue(), jobParameter.isIdentifying());
        }

    }

    private void insertParameter(Long executionId, JobParameter.ParameterType type, String key, Object value, boolean identifying) {
        Object[] args = new Object[0];
        int[] argTypes = new int[]{-5, 12, 12, 12, 93, -5, 8, 1};
        String identifyingFlag = identifying?"Y":"N";
        if(type == JobParameter.ParameterType.STRING) {
            args = new Object[]{executionId, key, type, value, new Timestamp(0L), Long.valueOf(0L), Double.valueOf(0.0D), identifyingFlag};
        } else if(type == JobParameter.ParameterType.LONG) {
            args = new Object[]{executionId, key, type, "", new Timestamp(0L), value, new Double(0.0D), identifyingFlag};
        } else if(type == JobParameter.ParameterType.DOUBLE) {
            args = new Object[]{executionId, key, type, "", new Timestamp(0L), Long.valueOf(0L), value, identifyingFlag};
        } else if(type == JobParameter.ParameterType.DATE) {
            args = new Object[]{executionId, key, type, "", value, Long.valueOf(0L), Double.valueOf(0.0D), identifyingFlag};
        }

        this.getJdbcTemplate().update(this.getQuery("INSERT into %PREFIX%JOB_EXECUTION_PARAMS(JOB_EXECUTION_ID, KEY_NAME, TYPE_CD, STRING_VAL, DATE_VAL, LONG_VAL, DOUBLE_VAL, IDENTIFYING) values (?, ?, ?, ?, ?, ?, ?, ?)"), args, argTypes);
    }

    protected JobParameters getJobParameters(Long executionId) {
        final HashMap map = new HashMap();
        RowCallbackHandler handler = new RowCallbackHandler() {
            public void processRow(ResultSet rs) throws SQLException {
                JobParameter.ParameterType type = JobParameter.ParameterType.valueOf(rs.getString(3));
                JobParameter value = null;
                if(type == JobParameter.ParameterType.STRING) {
                    value = new JobParameter(rs.getString(4), rs.getString(8).equalsIgnoreCase("Y"));
                } else if(type == JobParameter.ParameterType.LONG) {
                    value = new JobParameter(Long.valueOf(rs.getLong(6)), rs.getString(8).equalsIgnoreCase("Y"));
                } else if(type == JobParameter.ParameterType.DOUBLE) {
                    value = new JobParameter(Double.valueOf(rs.getDouble(7)), rs.getString(8).equalsIgnoreCase("Y"));
                } else if(type == JobParameter.ParameterType.DATE) {
                    value = new JobParameter(rs.getTimestamp(5), rs.getString(8).equalsIgnoreCase("Y"));
                }

                map.put(rs.getString(2), value);
            }
        };
        this.getJdbcTemplate().query(this.getQuery("SELECT JOB_EXECUTION_ID, KEY_NAME, TYPE_CD, STRING_VAL, DATE_VAL, LONG_VAL, DOUBLE_VAL, IDENTIFYING from %PREFIX%JOB_EXECUTION_PARAMS where JOB_EXECUTION_ID = ?"), new Object[]{executionId}, handler);
        return new JobParameters(map);
    }

    private final class JobExecutionRowMapper implements RowMapper<JobExecution> {
        private JobInstance jobInstance;
        private JobParameters jobParameters;

        public JobExecutionRowMapper() {
        }

        public JobExecutionRowMapper(JobInstance jobInstance) {
            this.jobInstance = jobInstance;
        }

        public JobExecution mapRow(ResultSet rs, int rowNum) throws SQLException {
            Long id = Long.valueOf(rs.getLong(1));
            String jobConfigurationLocation = rs.getString(10);
            if(this.jobParameters == null) {
                this.jobParameters = DataFlowJdbcJobExecutionDao.this.getJobParameters(id);
            }

            JobExecution jobExecution;
            if(this.jobInstance == null) {
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
