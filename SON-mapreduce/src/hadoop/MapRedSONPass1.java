package hadoop;

import apriori.Apriori;
import apriori.Database;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import static apriori.Params.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;


public class MapRedSONPass1 {
	static double startTime;
	double endTime;

    public static class Pass1Map extends Mapper<LongWritable, Text,
            Text, IntWritable> {

        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int minSupport = conf.getInt(MINIMUM_SUPPORT.toString(), 0);
            int totalTransactions = conf.getInt(TOTAL_TRANSACTIONS.toString(), 0);

            if (minSupport == 0 || totalTransactions == 0) {
                throw new InterruptedException("Support or Total Transactions is 0");
            }

            Database db = new Database(value.toString());
            int numTransactions = db.dbSize();

            int supportForMap = (int) Math.floor((((double) numTransactions)/((double) totalTransactions)) * minSupport);

            Apriori apriori = new Apriori("Map1-Apriori", db, supportForMap);
            Apriori.debugger = false;

            apriori.start();
            
            apriori.join();

            List<List<Integer>> frequentItemsets = apriori.getFrequentItemsets();
            for (List<Integer> itemset : frequentItemsets) {
                String output = "";
                Collections.sort(itemset);
                for (Integer item : itemset) {
                    output += String.valueOf(item) + " ";
                }
                context.write(new Text(output.trim()), new IntWritable(1));
            }
        }
    }

    public static class Pass1Reduce extends Reducer<Text, IntWritable,
            Text, NullWritable> {

        public void reduce(Text text, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            context.write(new Text(text.toString()), NullWritable.get());
        }
    }
}
