package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class HadoopConf {

    public static Job generateConf(Class mainClass, Class mapperClass, Class reducerClass,
                                       String jobName, Class mapOutputKeyClass,
                                       Class mapOutputValueClass, Class outputKeyClass,
                                       Class outputValueClass, Class inputFormatClass
                                       ) throws IOException{
        Configuration conf = new Configuration();
        //conf.set("mapred.child.java.opts", "-Xmx200m");
        Job job = Job.getInstance(conf, jobName);

        job.setJarByClass(mainClass);
        job.setMapperClass(mapperClass);
        //job.setCombinerClass(reducerClass);
        job.setReducerClass(reducerClass);
        job.setMapOutputKeyClass(mapOutputKeyClass);
        job.setMapOutputValueClass(mapOutputValueClass);
        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);
        job.setInputFormatClass(inputFormatClass);


        return job;
    }
}
