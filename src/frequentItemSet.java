import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class frequentItemSet {
    private static int support;

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {


        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            ArrayList<String> items = new ArrayList<>();
            //seperate and sort
            String[] commaDelim = line.split(",");
            int len = commaDelim.length;
            sort(commaDelim, items);
            //backtracking algorithm
            if (len==1){
                support=Integer.parseInt(commaDelim[0]);
                output.collect(new Text("support:"),new IntWritable(support));
            }else {
                int nCnt = items.size();

                int nBit = (0xFFFFFFFF >>> (32 - nCnt));

                for (int i = 1; i <= nBit; i++) {
                    String candidate="{";
                    for (int j = 0; j < nCnt; j++) {
                        if ((i<<(31-j)>>31) == -1) {
                            candidate=candidate+items.get(j)+",";
                        }
                    }
                    candidate=candidate.substring(0,candidate.length()-1);
                    candidate=candidate+"}:";
                    output.collect(new Text(candidate),new IntWritable(1));
                }
            }
        }
        private void sort(String[] commaDelim, ArrayList<String> items) {
            for (int i = 1; i <commaDelim.length; i++) {
                items.add(commaDelim[i]);
            }
            Collections.sort(items);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            if (sum>=support) {
                output.collect(key, new IntWritable(sum));
            }
        }
    }



    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        long starTime=System.currentTimeMillis();
        JobClient.runJob(conf);
        long endTime=System.currentTimeMillis();
        System.out.println(endTime-starTime+"ms");
    }
}