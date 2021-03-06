
import com.wuzzuf.jobs.DAOJobs;
import com.wuzzuf.jobs.Job;
import com.wuzzuf.jobs.WuzzufJobs;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author fady
 */
public class Main {
    public static void main(String[] args) {
        DAOJobs data = WuzzufJobs.getInstance();
        List<Job> jobs = data.getJobs(5);
        jobs.forEach(j -> System.out.println(j));
        data.show(5);
        data.CreatMinYearsExp();
        jobs = data.getJobs(5);
        jobs.forEach(j -> System.out.println(j));
        data.show(5);
        Dataset<Row> summary = data.summary();
        summary.foreach(s -> {
            System.out.println(s);
        });
        List<List<Object>> res = data.jobsPerCompany();
        System.out.println(res);
        res = data.mostJobTitles();
        System.out.println(res);
        res = data.mostPopularAreas();
        System.out.println(res);
        List<Map.Entry<String, Long>> map = data.getSkillList();
        map.forEach(i -> System.out.println(i.getKey() + " : " + i.getValue())); 
    }
}
