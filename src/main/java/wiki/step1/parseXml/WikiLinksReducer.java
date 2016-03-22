package wiki.step1.parseXml;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import wiki.Application;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class WikiLinksReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String result = "";
        Set<String> set = new HashSet<String>();
        for (Text value : values) {
            if (!set.contains(value.toString())) {
                set.add(value.toString());
                result += value.toString();
                result += ",";
            }
        }
        if (result.length() > 0) {
            result = result.substring(0, result.length() - 1);
        }
        if (key.toString().equals(Application.SOURCE_PAGE)) {
            result += "|0|GRAY|source";
        } else {
            result += "|Integer.MAX_VALUE|WHITE|null";
        }
        context.write(key, new Text(result));
    }
}
