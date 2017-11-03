	
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WordCount {

   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
     HashMap<String,Integer> frequency=new HashMap<>();
     int support=3;
     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
       String line = value.toString();
       String tabDelim[]= line.split("\\t");
       int maxLength=0;
       ArrayList<String> items=new ArrayList<>();
       for(String s : tabDelim) {
         String[] commaDelim= s.split(",");
         int len = commaDelim.length;
         if (len>maxLength){
           maxLength=len;
         }
         for (int i = 1; i < len; ++i) {
           statistic(commaDelim[i],items);
           output.collect(new Text(commaDelim[i]), new IntWritable(1));
         }
       }
       //backtracking algorithm and brute force
       LinkedList<String> stack=new LinkedList<>();
       for (int i =2; i <=maxLength; i++) {
         int k=1;
         bruteforce(k,i,stack,tabDelim,items,0);
       }
       for (HashMap.Entry<String,Integer> entry:frequency.entrySet()){
         int i=0;
         output.collect(new Text(entry.getKey()),new IntWritable(entry.getValue()));
       }
     }
     private  void bruteforce(int k,int i,LinkedList<String> stack,String[] commaDelim,ArrayList<String> items,int n){
       if (k>=i){
         int count=0;
         for (String s:commaDelim){
           if (find(stack,s)){
             count++;
           }
           if (count>support){
             String frequenSet=new String();
             for (String str:stack){
               if (str.equals(stack.getLast())){
                 frequenSet=frequenSet+str;
               }else {
                 frequenSet=frequenSet+str+",";
               }
             }
             frequency.put(frequenSet,count);
           }
         }
       }else {
         for (int j = n; j <items.size(); j++) {
           stack.add(items.get(j));
           k++;
           bruteforce(k,i,stack,commaDelim,items,j+1);
         }
       }
       if (stack.size()>0) {
         stack.remove(stack.size() - 1);
       }
     }

     private boolean find(LinkedList<String> stack,String s){
       String[] split=s.split(",");
       for (String str:stack) {
         boolean find=false;
         for (int i = 1; i < split.length; i++) {
           if (str.equals(split[i])){
             find=true;
           }
         }
         if (!find){
           return false;
         }
       }
       return true;
     }

     private void statistic(String commaDelim,ArrayList<String> items){
       boolean judge=true;
       for (String s:items){
         if (s.equals(commaDelim)){
           judge=false;
         }
       }
       if (judge){
         items.add(commaDelim);
       }
     }
   }

   public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
       int sum = 0;
       while (values.hasNext()) {
         sum += values.next().get();
       }
       output.collect(key, new IntWritable(sum));
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