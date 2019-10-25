package com.prashanth.os.spring.batch.gs;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.Map;

@EnableBatchProcessing
@SpringBootApplication
public class SpringBatchGsApplication {

    public static class Person {
        private int age;
        private String firstName, email;

        public Person(){}

        public Person(int age, String firstName, String email) {
            this.age = age;
            this.firstName = firstName;
            this.email = email;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }
    }

    @Configuration
    public static class Step1Configuration {
        @Bean
        FlatFileItemReader <Person> fileItemReader(@Value("${input}") Resource in) {
            return new FlatFileItemReaderBuilder<Person>()
                    .name("file-reader")
                    .resource(in)
                    .targetType(Person.class)
                    .delimited().delimiter(",").names(new String[]{"firstName", "age", "email"})
                    .build();
        }

        @Bean
        JdbcBatchItemWriter<Person> jdbcBatchItemWriter(DataSource ds) {
            return new JdbcBatchItemWriterBuilder<Person>()
                    .dataSource(ds)
                    .sql("insert into PEOPLE(AGE, FIRST_NAME, EMAIL) values (:age, :firstName, :email)")
                    .beanMapped()
                    .build();
        }
    }

    @Configuration
    public static class Step2Configuration {
        @Bean
        ItemReader <Map<Integer, Integer>> jdbcReader (DataSource dataSource) {
            return new JdbcCursorItemReaderBuilder<Map<Integer, Integer>>()
                    .dataSource(dataSource)
                    .name("jdbc-reader")
                    .sql("select COUNT(age) c, age a from PEOPLE group by age")
                    .rowMapper((resultSet, i) -> Collections.singletonMap(
                            resultSet.getInt("a"),
                            resultSet.getInt("c")
                    ))
                    .build();
        }

        @Bean
        ItemWriter<Map<Integer, Integer>> fileWriter(@Value("${output}") Resource out) {
            return new FlatFileItemWriterBuilder<Map<Integer, Integer>>()
                    .name("file-writer")
                    .resource(out)
                    .lineAggregator(new DelimitedLineAggregator<Map<Integer, Integer>>() {
                        {
                            setDelimiter(",");
                            setFieldExtractor(integerMap -> {
                                Map.Entry<Integer, Integer> next = integerMap.entrySet().iterator().next();
                                return new Object[] {next.getKey(), next.getValue()};
                            });
                        }
                    })
                    .build();
        }
    }

    @Bean
    Job job(JobBuilderFactory jobBuilderFactory,
            StepBuilderFactory stepBuilderFactory,
            Step1Configuration step1Configuration,
            Step2Configuration step2Configuration) {

        Step step1 = stepBuilderFactory.get("file-db")
                .<Person, Person>chunk(100)
                .reader(step1Configuration.fileItemReader(null))
                .writer(step1Configuration.jdbcBatchItemWriter(null))
                .build();

        Step step2 = stepBuilderFactory.get("db-file")
                .<Map<Integer, Integer>, Map<Integer, Integer>>chunk(100)
                .reader(step2Configuration.jdbcReader(null))
                .writer(step2Configuration.fileWriter(null))
                .build();

        return jobBuilderFactory.get("etl") // Create job
                .incrementer(new RunIdIncrementer())
                .start(step1)
                .next(step2)
                .build();
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringBatchGsApplication.class, args);
    }
}
