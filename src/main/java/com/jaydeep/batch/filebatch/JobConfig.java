package com.jaydeep.batch.filebatch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.net.MalformedURLException;

@Configuration
@EnableBatchProcessing
public class JobConfig {
    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Value("${input-file}")
    private String fileName;

    @Value("${thread-size}")
    private int threadSize;

    @Bean
    public Partitioner partitioner() {
        try {
            MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
            ClassLoader cl = this.getClass().getClassLoader();
            ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(cl);
            Resource[] resources = resolver.getResources("file:input/input.txt");
            partitioner.setResources(resources);
            partitioner.partition(10);
            return partitioner;
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(threadSize);
        taskExecutor.afterPropertiesSet();
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setQueueCapacity(8);
        return taskExecutor;
    }

    @Bean(name = "partitionerJob")
    public Job partitionerJob(Step masterStep) throws UnexpectedInputException, ParseException {
        return jobBuilderFactory.get("partitionerJob")
          .start(masterStep)
          .build();
    }

    @Bean
    @Qualifier("masterStep")
    public Step masterStep(Step processData) {
        return stepBuilderFactory.get("masterStep")
                .partitioner(processData)
                .partitioner("ProcessDataStep",partitioner())
                .taskExecutor(taskExecutor())
                .build();
    }


    @Bean
    @Qualifier("processData")
    public Step processData(ItemReader<String> reader) {
        return stepBuilderFactory.get("processData")
                .<String, String> chunk(5)
                .reader(reader)
                .processor(applyCaesar())
                .writer(itemWriter(null))
                .build();
    }

    @Bean(destroyMethod = "")
    @StepScope
    public FlatFileItemWriter<String> itemWriter(@Value("#{stepExecutionContext[opFileName]}") String filename) {
        FlatFileItemWriter<String> csvFileWriter = new FlatFileItemWriter<>();

        String exportFilePath = "output/encrypted.txt";
        csvFileWriter.setResource(new FileSystemResource(exportFilePath));
        csvFileWriter.setAppendAllowed(false);

        LineAggregator<String> lineAggregator = new PassThroughLineAggregator<>();
        csvFileWriter.setLineAggregator(lineAggregator);

        return csvFileWriter;
    }

    @Bean(name="reader")
    @StepScope
    public FlatFileItemReader<String> reader(@Value("#{stepExecutionContext['fileName']}") String filename) throws MalformedURLException {
        FlatFileItemReader<String> reader = new FlatFileItemReader<>();
        reader.setResource(new UrlResource(filename));
        reader.setLineMapper(new PassThroughLineMapper());
        return reader;
    }

    @Bean
    public ItemProcessor<String, String> applyCaesar() {
        return new CaesarProcessor();
    }
}
