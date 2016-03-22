package wiki.step3.findResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Result extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Result(), args));
    }

    public int run(String[] strings) throws Exception {
        return searchResult("data/output/parsing", "data/output/result") ? 0 : 1;
    }

    private boolean searchResult(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job searchResult = Job.getInstance(conf, "searchResult");
        searchResult.setJarByClass(Result.class);

        searchResult.setOutputKeyClass(Text.class);
        searchResult.setOutputValueClass(Text.class);

        searchResult.setMapperClass(SearchMapper.class);

        FileInputFormat.setInputPaths(searchResult, new Path(inputPath));
        FileOutputFormat.setOutputPath(searchResult, new Path(outputPath));

        searchResult.setInputFormatClass(TextInputFormat.class);
        searchResult.setOutputFormatClass(TextOutputFormat.class);

        return searchResult.waitForCompletion(true);
    }

    private static class SearchMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String name = value.toString().split("\t")[0];
            if (name.equals("Adolf_Hitler")) {
                context.write(new Text(name), value);
            }
        }
    }
}
