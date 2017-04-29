import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * Created by Tom on 4/28/17.
 */
public class Top10MoviesRecom {
    public static class UserMovieScoreMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //input is "UserID:movieID  score"
            String[] userMovie_score = value.toString().trim().split("\t");
            String score = userMovie_score[1];
            String[] userMovie = userMovie_score[0].split(":");
            int userID = Integer.parseInt(userMovie[0]);
            String movieID = userMovie[1];
            context.write(new IntWritable(userID), new Text(movieID + ":" + score));
        }
    }

    public static class Top10MoviesReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        // reduce method
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {



            List<String> valuesList = new ArrayList<>();
            for (Text value : values) {
                valuesList.add(value.toString());
            }

            Collections.sort(valuesList, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    String score1 = o1.trim().split(":")[1];
                    String score2 = o2.trim().split(":")[1];
                    int diff = Double.compare(Double.parseDouble(score2), Double.parseDouble(score1));
                    if (diff > 0) {
                        return 1;
                    } else if (diff == 0) {
                        return 0;
                    } else {
                        return -1;
                    }
                }
            });

            StringBuilder sb = new StringBuilder();
            int count = 9;
            for (String value: valuesList) {
                if (count >= 0) {
                    sb.append("," + value.toString().trim().split(":")[0]);
                    count--;
                }

            }
            context.write(key, new Text(sb.toString().replaceFirst(",", "")));

            //key = UserID   value = <movie:score, movie:score...>
//            Queue<Text> minHeap = new PriorityQueue<Text>(5, new Comparator<>() {
//                @Override
//                public int compare(Text o1, Text o2) {
//                    String score1 = o1.toString().trim().split(":")[1];
//                    String score2 = o2.toString().trim().split(":")[1];
//                    int diff = Double.compare(Double.parseDouble(score1), Double.parseDouble(score2));
//                    if (diff > 0) {
//                        return 1;
//                    } else if (diff == 0) {
//                        return 0;
//                    } else {
//                        return -1;
//                    }
//                }
//            });
//            for (int i = 0; i < 5; i++) {
//                sb.append("," + valuesList.get(i).toString().trim().split(":")[0]);
//            }
//
//            context.write(key, new Text(sb.toString().replaceFirst(",", "")));

//            for (Text value: values) {
//                if (minHeap.size() < 5) {
//                    minHeap.offer(value);
//                } else {
//                    String minInHeap = minHeap.peek().toString();
//                    double minScoreInHeap = Double.parseDouble(minInHeap.toString().split(":")[1]);
//                    String[] movieAndScore = value.toString().split(":");
//                    double comingScore = Double.parseDouble(movieAndScore[1]);
//                    int dif = Double.compare(comingScore, minScoreInHeap);
//                    if (dif <= 0) {
//                        continue;
//                    }
//                    minHeap.poll();
//                    minHeap.offer(value);
//                }
//            }

//            String[] top10Movies = new String[5];
//            int index = 4;
//            while (!minHeap.isEmpty()) {
//                String[] movie_score = minHeap.poll().toString().trim().split(":");
//                String movieID = movie_score[0];
//                top10Movies[index--] = movieID;
//            }

//            // reverse the result
//            StringBuilder sb = new StringBuilder();
//            for (int i = 0; i <= top10Movies.length - 1 ; i++) {
//                sb.append("," + top10Movies[i]);
//            }
//            // key = userID   value =movie1, movie2, movie3, ...
//            context.write(key, new Text(sb.toString().replaceFirst(",", "")));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(UserMovieScoreMapper.class);
        job.setReducerClass(Top10MoviesReducer.class);

        job.setJarByClass(Top10MoviesRecom.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
