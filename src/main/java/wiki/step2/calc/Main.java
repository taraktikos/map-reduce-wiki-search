package wiki.step2.calc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Main extends Configured implements Tool {
    enum MoreIterations {
        numberOfIterations
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Main(), args));
    }

    public int run(String[] strings) throws Exception {
        int iterationCount = 0;

        Job job;
        long terminationValue = 1;
        while (terminationValue > 0) {
            job = setupJob();
            String input = "data/output/parsing";
            String output = "data/output/step";
            if (iterationCount > 0) {
                input = output + iterationCount;
            }
            output = output + (iterationCount + 1);
            FileInputFormat.setInputPaths(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.waitForCompletion(true);
            Counters counters = job.getCounters();
            terminationValue = counters.findCounter(MoreIterations.numberOfIterations).getValue();
            iterationCount++;
        }
        return 0;
    }

    private Job setupJob() throws IOException {
        Job job = Job.getInstance(new Configuration(), "parseXml");
        job.setJarByClass(Main.class);

        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(3);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    public static class Mapper extends SearchMapper {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                Node inNode = new Node(value.toString());
                super.map(key, value, context, inNode);
            } catch (IllegalArgumentException e) {
            }
        }
    }

    public static class Reducer extends SearchReducer {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Node outNode = new Node();
            outNode = super.reduce(key, values, context, outNode);
            if (outNode.getColor() == Node.Color.GRAY) {
                context.getCounter(MoreIterations.numberOfIterations).increment(1L);
            }
        }
    }
}
