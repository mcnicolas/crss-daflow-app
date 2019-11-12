package com.pemc.crss.dataflow.app.config;


import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.pemc.crss.dataflow.app.listener.TaskRetryListener;
import com.pemc.crss.dataflow.app.service.impl.DataFlowJdbcJobExecutionDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.repository.ExecutionContextSerializer;
import org.springframework.batch.core.repository.dao.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.orm.jpa.JpaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.incrementer.AbstractDataFieldMaxValueIncrementer;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;
import org.springframework.orm.jpa.JpaVendorAdapter;
import org.springframework.orm.jpa.persistenceunit.PersistenceUnitManager;
import org.springframework.orm.jpa.vendor.AbstractJpaVendorAdapter;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableBatchProcessing
@EnableConfigurationProperties({JpaProperties.class})
public class ApplicationConfig extends WebMvcConfigurerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationConfig.class);

    private String tablePrefix = AbstractJdbcBatchMetadataDao.DEFAULT_TABLE_PREFIX;

    private DataFieldMaxValueIncrementer incrementer = new AbstractDataFieldMaxValueIncrementer() {
        @Override
        protected long getNextKey() {
            throw new IllegalStateException("JobExplorer is read only.");
        }
    };

    @Autowired
    private JpaProperties properties;

    @Autowired
    private JedisConnectionFactory jedisConnectionFactory;

    @Autowired
    private RedisTemplate<String, Long> redisTemplate;

    @Bean
    public JpaVendorAdapter jpaVendorAdapter() {
        AbstractJpaVendorAdapter adapter = new HibernateJpaVendorAdapter();
        adapter.setShowSql(this.properties.isShowSql());
        adapter.setDatabase(this.properties.getDatabase());
        adapter.setDatabasePlatform(this.properties.getDatabasePlatform());
        adapter.setGenerateDdl(this.properties.isGenerateDdl());
        return adapter;
    }

    @Bean
    public EntityManagerFactoryBuilder entityManagerFactoryBuilder(
            JpaVendorAdapter jpaVendorAdapter,
            ObjectProvider<PersistenceUnitManager> persistenceUnitManagerProvider) {
        return new EntityManagerFactoryBuilder(
                jpaVendorAdapter, this.properties.getProperties(),
                persistenceUnitManagerProvider.getIfAvailable());
    }

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        converters.add(jsonConverter());
        converters.add(byteArrayConverter());
    }

    /**
     * Json marshalling configuration.
     *
     * @return the {@link MappingJackson2HttpMessageConverter}
     */
    @Bean
    public MappingJackson2HttpMessageConverter jsonConverter() {
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.getObjectMapper().registerModule(new JodaModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return converter;
    }


    @Bean
    public ByteArrayHttpMessageConverter byteArrayConverter() {
        return new ByteArrayHttpMessageConverter();
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean
    public JdbcOperations jdbcOperations(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }


    @Bean(name = "jsonContextSerializer")
    public ExecutionContextSerializer jsonContextSerializer() {
        return new FakeSerializer();
    }

    public class FakeSerializer implements ExecutionContextSerializer {
        @Override
        public Map<String, Object> deserialize(InputStream inputStream) throws IOException {
            return new HashMap<>();
        }
        @Override
        public void serialize(Map<String, Object> object, OutputStream outputStream) throws IOException {
            outputStream.write("fake".getBytes(StandardCharsets.US_ASCII));
        }
    }

    @Bean
    public ExecutionContextDao executionContextDao(JdbcOperations jdbcOperations,
                                                   @Qualifier("jsonContextSerializer") ExecutionContextSerializer jsonContextSerializer) throws Exception {
        JdbcExecutionContextDao dao = new JdbcExecutionContextDao();
        dao.setJdbcTemplate(jdbcOperations);
        dao.setTablePrefix(tablePrefix);
        dao.setSerializer(jsonContextSerializer);
        dao.afterPropertiesSet();
        return dao;
    }

    @Bean
    public JobInstanceDao jobInstanceDao(JdbcOperations jdbcOperations) throws Exception {
        JdbcJobInstanceDao dao = new JdbcJobInstanceDao();
        dao.setJdbcTemplate(jdbcOperations);
        dao.setJobIncrementer(incrementer);
        dao.setTablePrefix(tablePrefix);
        dao.afterPropertiesSet();
        return dao;
    }

    @Bean
    public JobExecutionDao jobExecutionDao(JdbcOperations jdbcOperations) throws Exception {
        JdbcJobExecutionDao dao = new JdbcJobExecutionDao();
        dao.setJdbcTemplate(jdbcOperations);
        dao.setJobExecutionIncrementer(incrementer);
        dao.setTablePrefix(tablePrefix);
        dao.afterPropertiesSet();
        return dao;
    }

    @Bean
    public DataFlowJdbcJobExecutionDao dataFlowJobExecutionDao(JdbcOperations jdbcOperations) throws Exception {
        DataFlowJdbcJobExecutionDao dao = new DataFlowJdbcJobExecutionDao();
        dao.setJdbcTemplate(jdbcOperations);
        dao.setJobExecutionIncrementer(incrementer);
        dao.setTablePrefix(tablePrefix);
        dao.afterPropertiesSet();
        return dao;
    }

    @Bean
    public StepExecutionDao stepExecutionDao(JdbcOperations jdbcOperations) throws Exception {
        JdbcStepExecutionDao dao = new JdbcStepExecutionDao();
        dao.setJdbcTemplate(jdbcOperations);
        dao.setStepExecutionIncrementer(incrementer);
        dao.setTablePrefix(tablePrefix);
        dao.afterPropertiesSet();
        return dao;
    }

    @Bean
    public ChannelTopic topic() {
        // increment retryQueueSubscriberCount as instance count on every startup
        String topic = "pubsub:retryQueue" + redisTemplate.opsForValue().increment("retryQueueSubscriberCount", 1);
        return new ChannelTopic(topic);
    }

    @Bean
    public MessageListenerAdapter messageListener() {
        return new MessageListenerAdapter(taskRetryListener());
    }

    @Bean
    public MessageListener taskRetryListener() {
        return new TaskRetryListener();
    }

    @Bean
    public RedisMessageListenerContainer redisContainer() {
        final RedisMessageListenerContainer container = new RedisMessageListenerContainer();

        container.setConnectionFactory(jedisConnectionFactory);
        container.addMessageListener(messageListener(), topic());

        return container;
    }

}