package wiki;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import wiki.step1.parseXml.WikiLinksReducer;
import wiki.step1.parseXml.WikiPageLinksMapper;
import wiki.step1.parseXml.XmlInputFormat;
import wiki.step2.calc.Node;
import wiki.step2.calc.SearchMapper;
import wiki.step2.calc.SearchReducer;
import wiki.step3.findResult.ResultMapper;

import java.io.IOException;

public class Application extends Configured implements Tool {

//    public static String SOURCE_PAGE = "William_Shakespeare";
//    public static String TARGET_PAGE = "Adolf_Hitler";
    public static String SOURCE_PAGE = "Taras_Shevchenko";
    public static String TARGET_PAGE = "Freddie_Mercury";

    private enum MoreIterations {
        numberOfIterations
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Application(), args));
    }

    public int run(String[] strings) throws Exception {
        String input = "data/input";
        String output = "data/" + SOURCE_PAGE;
        boolean isCompleted = runXmlParsing(input, output + "/parsing");
        if (isCompleted) {
            String calculatedPath = runCalculateDistance(output + "/parsing", output + "/step");
            if (calculatedPath != null) {
                isCompleted = searchResult(calculatedPath, output + "/result");
            } else {
                isCompleted = false;
            }
        }
        if (!isCompleted) return 1;

        return 0;
    }

    private boolean runXmlParsing(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

//        FileSystem fs = FileSystem.get(conf);
//        fs.delete(new Path(outputPath), true);

        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        Job parseXml = Job.getInstance(conf, "parseXml");
        parseXml.setJarByClass(Application.class);

        FileInputFormat.addInputPath(parseXml, new Path(inputPath));
        parseXml.setInputFormatClass(XmlInputFormat.class);
        parseXml.setMapperClass(WikiPageLinksMapper.class);
        parseXml.setMapOutputKeyClass(Text.class);

        FileOutputFormat.setOutputPath(parseXml, new Path(outputPath));
        parseXml.setOutputFormatClass(TextOutputFormat.class);

        parseXml.setOutputKeyClass(Text.class);
        parseXml.setOutputValueClass(Text.class);
        parseXml.setReducerClass(WikiLinksReducer.class);

        return parseXml.waitForCompletion(true);
    }

    private String runCalculateDistance(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        int iterationCount = 0;

        Job job;
        long terminationValue = 1;
        String input = inputPath;
        String output = outputPath;
        while (terminationValue > 0) {
            job = setupCalculateDistanceJob();

            if (iterationCount > 0) {
                input = outputPath + iterationCount;
            }
            output = outputPath + (iterationCount + 1);
            FileInputFormat.setInputPaths(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.waitForCompletion(true);
            Counters counters = job.getCounters();
            terminationValue = counters.findCounter(MoreIterations.numberOfIterations).getValue();
            iterationCount++;
        }
        return iterationCount > 0 ? output : null;
    }

    private boolean searchResult(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job searchResult = Job.getInstance(conf, "searchResult");
        searchResult.setJarByClass(Application.class);

        searchResult.setOutputKeyClass(Text.class);
        searchResult.setOutputValueClass(Text.class);

        searchResult.setMapperClass(ResultMapper.class);

        FileInputFormat.setInputPaths(searchResult, new Path(inputPath));
        FileOutputFormat.setOutputPath(searchResult, new Path(outputPath));

        searchResult.setInputFormatClass(TextInputFormat.class);
        searchResult.setOutputFormatClass(TextOutputFormat.class);

        return searchResult.waitForCompletion(true);
    }

    private Job setupCalculateDistanceJob() throws IOException {
        Job job = Job.getInstance(new Configuration(), "calculateDistance");
        job.setJarByClass(Application.class);

        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(3);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    private static class Mapper extends SearchMapper {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                Node inNode = new Node(value.toString());
                super.map(key, value, context, inNode);
            } catch (NumberFormatException e) {
//                System.out.println(value.toString());
            } catch (IllegalArgumentException e) {
//                System.out.println(value.toString());
            }
        }
    }

    private static class Reducer extends SearchReducer {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            try {
            Node outNode = new Node();
            outNode = super.reduce(key, values, context, outNode);
            if (outNode.getColor() == Node.Color.GRAY) {
                context.getCounter(MoreIterations.numberOfIterations).increment(1L);
            }
//            } catch (ArrayIndexOutOfBoundsException e) {
//                System.out.println(e.getMessage());
//            }
        }
    }
}
