import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwitterSentimentAnalysis {

    
    public static class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text sentiment = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] fields = value.toString().split(",");

            
            if (fields.length > 3) {
               
                String tweetSentiment = fields[2].trim().toLowerCase();

                
                if (tweetSentiment.equals("positive") || tweetSentiment.equals("negative") || tweetSentiment.equals("neutral")) {
                    sentiment.set(tweetSentiment);
                    context.write(sentiment, one);
                }
            }
        }
    }

    
    public static class SentimentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

   
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Twitter Sentiment Analysis");

       
        job.setJarByClass(TwitterSentimentAnalysis.class);

        
        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);

        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
