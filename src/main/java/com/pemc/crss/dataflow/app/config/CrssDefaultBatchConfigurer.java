package com.pemc.crss.dataflow.app.config;

import org.springframework.batch.core.configuration.BatchConfigurationException;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.support.DatabaseType;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

/**
 * Created by jdimayuga on 09/06/2017.
 */
@Component
public class CrssDefaultBatchConfigurer implements BatchConfigurer {

    private DataSource dataSource;
    private PlatformTransactionManager transactionManager;

    private JobRepository jobRepository;
    private JobLauncher jobLauncher;
    private JobExplorer jobExplorer;


    public CrssDefaultBatchConfigurer(DataSource dataSource) {
        this.setDataSource(dataSource);
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        this.transactionManager = new DataSourceTransactionManager(dataSource);
    }


    @PostConstruct
    public void initialize() {
        try {
            this.jobRepository = this.createJobRepository();
            JobExplorerFactoryBean e1 = new JobExplorerFactoryBean();
            e1.setDataSource(this.dataSource);
            e1.afterPropertiesSet();
            this.jobExplorer = e1.getObject();
            this.jobLauncher = this.createJobLauncher();
        } catch (Exception var3) {
            throw new BatchConfigurationException(var3);
        }
    }


    private JobRepository createJobRepository() throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(this.dataSource);
        factory.setTransactionManager(new DataSourceTransactionManager(this.dataSource));
        factory.setDatabaseType(DatabaseType.POSTGRES.name());
        factory.afterPropertiesSet();
        return factory.getObject();
    }

    protected JobLauncher createJobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(this.jobRepository);
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }

    @Override
    public JobRepository getJobRepository() throws Exception {
        return this.jobRepository;
    }

    @Override
    public PlatformTransactionManager getTransactionManager() throws Exception {
        return this.transactionManager;
    }

    @Override
    public JobLauncher getJobLauncher() throws Exception {
        return this.jobLauncher;
    }

    @Override
    public JobExplorer getJobExplorer() throws Exception {
        return this.jobExplorer;
    }
}
