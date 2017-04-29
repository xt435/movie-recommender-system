import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

/**
 * Created by Tom on 4/28/17.
 */
public class RemoveRatedMovies {

    public static class SumResultMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input: movieB \t movieA=relation

            String[] line = value.toString().split("\t");
            String user_movie = line[0];
            String movieScore = line[1];
            context.write(new Text(user_movie), new Text(movieScore));

        }
    }

    public static class RatedMoviesMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //input: movie, user, rating, timestamp
            String[] movie_user_rating = value.toString().trim().split(",");
            String userID = movie_user_rating[1];
            String movieID = movie_user_rating[0];
            String ratedMark = "isRated";
            context.write(new Text(userID + ":" + movieID), new Text(ratedMark));
        }
    }

    public static class RemoveRatedMoviesReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //key = userID:movieID value = <score... "isRated"...>
            boolean isRated = false;
            double score = 0d;
            for (Text value : values) {
                if (value.toString().equals("isRated")) {
                    isRated = true;
                } else {
                    score = Double.parseDouble(value.toString());
                }
            }
            if (isRated) {
                score = 0d;
            }
            context.write(key, new DoubleWritable(score));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(RemoveRatedMovies.class);

        ChainMapper.addMapper(job,SumResultMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, RatedMoviesMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

        job.setMapperClass(RatedMoviesMapper.class);
        job.setMapperClass(SumResultMapper.class);

        job.setReducerClass(RemoveRatedMoviesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatedMoviesMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SumResultMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
