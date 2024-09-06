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

    // 1. Mapper Class
    public static class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text sentiment = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the CSV line by commas (assuming it's a CSV file)
            String[] fields = value.toString().split(",");

            // Ensure the line has the correct number of fields
            if (fields.length > 3) {
                // Extract the sentiment (assuming it's the third column)
                String tweetSentiment = fields[2].trim().toLowerCase();

                // Output sentiment and count as key-value pair
                if (tweetSentiment.equals("positive") || tweetSentiment.equals("negative") || tweetSentiment.equals("neutral")) {
                    sentiment.set(tweetSentiment);
                    context.write(sentiment, one);
                }
            }
        }
    }

    // 2. Reducer Class
    public static class SentimentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // Sum up the counts for each sentiment type (positive, negative, neutral)
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // 3. Driver/Main Class
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Twitter Sentiment Analysis");

        // Set Jar class
        job.setJarByClass(TwitterSentimentAnalysis.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);

        // Set output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
