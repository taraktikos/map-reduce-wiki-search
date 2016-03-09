package wiki.step2.calc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SearchReducer extends Reducer<Text, Text, Text, Text> {

    public Node reduce(Text key, Iterable<Text> values, Context context, Node outNode) throws IOException, InterruptedException {
        outNode.setId(key.toString());

        for (Text value : values) {
            Node inNode = new Node(key.toString() + "\t" + value.toString());

            if (inNode.getEdges().size() > 0) {
                outNode.setEdges(inNode.getEdges());
            }
            if (inNode.getDistance() < outNode.getDistance()) {
                outNode.setDistance(inNode.getDistance());
                outNode.getParents().addAll(inNode.getParents());
            }
            if (inNode.getColor().ordinal() > outNode.getColor().ordinal()) {
                outNode.setColor(inNode.getColor());
            }
        }
        context.write(key, new Text(outNode.getNodeInfo()));
        return outNode;
    }
}
