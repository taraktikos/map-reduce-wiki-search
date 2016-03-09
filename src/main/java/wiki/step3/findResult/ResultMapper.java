package wiki.step3.findResult;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import wiki.Application;

import java.io.IOException;

public class ResultMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String name = value.toString().split("\t")[0];
//            String name = key.toString();
        if (name.equals(Application.TARGET_PAGE)) {
            context.write(new Text(name), value);
        }
    }
}
