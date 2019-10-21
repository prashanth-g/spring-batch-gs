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
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;

@EnableBatchProcessing
@SpringBootApplication
public class SpringBatchGsApplication {

    static class Person {
        private int age;
        private String firstName, email;

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
    JdbcBatchItemWriter <Person> jdbcBatchItemWriter(DataSource ds) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .dataSource(ds)
                .sql("insert into PEOPLE(AGE, FIRST_NAME, EMAIL) values (:age, :firstName, :email)")
                .beanMapped()
                .build();
    }

    @Bean
    Job job(JobBuilderFactory jobBuilderFactory,
            StepBuilderFactory stepBuilderFactory,
            ItemReader<? extends Person> itemReader,
            ItemWriter<? super Person> itemWriter) {

        Step step1 = stepBuilderFactory.get("file-db")
                .<Person, Person> chunk(100)
                .reader(itemReader)
                .writer(itemWriter)
                .build();

        return jobBuilderFactory.get("etl") // Create job
                .incrementer(new RunIdIncrementer())
                .start(step1)
                .build();
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringBatchGsApplication.class, args);
    }
}
