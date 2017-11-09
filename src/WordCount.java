	
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WordCount {
  private static int support;

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {


    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      HashMap<String, Integer> frequency = new HashMap<>();
      String line = value.toString();
      String tabDelim[] = line.split("\\s+");
      ArrayList<String> items = new ArrayList<>();
//      for (String s : tabDelim) {
      String[] commaDelim = tabDelim[0].split(",");
      int len = commaDelim.length;
        sort(commaDelim, items);
//           output.collect(new Text(commaDelim[i]), new IntWritable(1));
//      }
      //backtracking algorithm and brute force
      if (len==1){
        support=Integer.parseInt(commaDelim[0]);
        output.collect(new Text("support:"),new IntWritable(support));
      }else {
        LinkedList<String> stack = new LinkedList<>();
        for (int i = 2; i <= len - 1; i++) {
          int k = 1;
          bruteforce(k, i, stack, tabDelim, items, 0,frequency);
        }
        for (HashMap.Entry<String, Integer> entry : frequency.entrySet()) {
          int i = 0;
          output.collect(new Text(entry.getKey()), new IntWritable(entry.getValue()));
        }
      }
    }

    private void bruteforce(int k, int i, LinkedList<String> stack, String[] commaDelim, ArrayList<String> items, int n,HashMap<String,Integer> frequency) {
      if (k > i) {
        int count = 0;
        for (String s : commaDelim) {
          if (find(stack, s)) {
            count++;
          }
          String frequenSet = new String();
          for (String str : stack) {
            if (str.equals(stack.getLast())) {
              frequenSet = frequenSet + str;
            } else {
              frequenSet = frequenSet + str + ",";
            }
          }
          frequency.put(frequenSet, count);
        }
      } else {
        for (int j = n; j < items.size(); j++) {
          stack.add(items.get(j));
          bruteforce(k + 1, i, stack, commaDelim, items, j + 1,frequency);
        }
      }
      if (stack.size() > 0) {
        stack.remove(stack.size() - 1);
      }
    }

    private boolean find(LinkedList<String> stack, String s) {
      String[] split = s.split(",");
      for (String str : stack) {
        boolean find = false;
        for (int i = 1; i < split.length; i++) {
          if (str.equals(split[i])) {
            find = true;
          }
        }
        if (!find) {
          return false;
        }
      }
      return true;
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