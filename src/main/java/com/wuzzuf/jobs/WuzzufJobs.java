/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.wuzzuf.jobs;

import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import static org.apache.spark.sql.functions.col;

/**
 *
 * @author fady
 */
public final class WuzzufJobs implements DAOJobs {
    
    private static WuzzufJobs instance;
    private Dataset<Row> df;
    private final String PATH = "Wuzzuf_Jobs.csv";
    private final JavaRDD<Job> jobsRDD;
    private final SparkSession sparkSession;
    
    private WuzzufJobs() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        sparkSession = SparkSession
                .builder()
                .appName("Wuzzuf Jobs")
                .master("local[4]")
                .getOrCreate ();
        final DataFrameReader dfFrameReader = sparkSession.read();
        dfFrameReader.option ("header", "true");
        df = dfFrameReader.csv(PATH);
        jobsRDD = df.toJavaRDD().map(Utilities::toJob).cache();
        clean();
    }
    
    public static WuzzufJobs getInstance(){
       if(instance == null)
           instance = new WuzzufJobs();
       return instance;
    }

    @Override
    public void clean() {
        System.out.println ("Count : " + df.count());
        System.out.println("Remove Duplicates");
        df = df.dropDuplicates();
        System.out.println ("Count : " + df.count());
        System.out.println("Remove Nulls");
        df = df.na().drop();
        System.out.println ("Count : " + df.count());
    }

    @Override
    public JavaRDD<Job> getJobs() {
        return jobsRDD;
    }
    
    @Override
    public List<Job> getJobs(int n) {
        return jobsRDD.take(n);
    }

    @Override
    public Dataset<Row> summary() {
        df.printSchema();
        df.summary("count", "min", "25%", "75%", "max").show();
        return df.summary("count", "min", "25%", "75%", "max");
    }
    
    @Override
    public void show(int n) {
        df.show(n);
    }

    @Override
    public List<List<Object>> jobsPerCompany() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<List<Object>> mostJobTitles() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<List<Object>> mostPopularAreas() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Map<String, Integer> getSkillList() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public void CreatMinYearsExp() {
        jobsRDD.foreach(j -> j.setMinYear(Utilities.minYear(j.getYearsExp())));
        df = sparkSession.createDataFrame(jobsRDD, Job.class);

    }
    
}
