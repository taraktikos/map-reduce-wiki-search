package wiki;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import wiki.step1.parseXml.WikiLinksReducer;
import wiki.step1.parseXml.WikiPageLinksMapper;
import wiki.step1.parseXml.XmlInputFormat;

import java.io.IOException;

public class Application extends Configured implements Tool {

//    public static final String DELIMITER = "\\|\\|";

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Application(), args));
    }

    public int run(String[] strings) throws Exception {
        boolean isCompleted = runXmlParsing("data/input", "data/output/parsing");
        if (!isCompleted) return 1;

        return 0;
    }

    private boolean runXmlParsing(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(outputPath), true);

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
}
