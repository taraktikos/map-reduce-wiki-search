package wiki.step2.calc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SearchMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context, Node inNode) throws IOException, InterruptedException {
        if (inNode.getColor() == Node.Color.GRAY) {
            for (String neighbor : inNode.getEdges()) {
                Node adjacentNode = new Node();
                adjacentNode.setId(neighbor);
                adjacentNode.setDistance(inNode.getDistance() + 1);
                adjacentNode.setColor(Node.Color.GRAY);
                adjacentNode.getParents().addAll(inNode.getParents());
                adjacentNode.getParents().add(inNode.getId());
                context.write(new Text(adjacentNode.getId()), adjacentNode.getNodeInfo());
            }
            inNode.setColor(Node.Color.BLACK);
        }
        context.write(new Text(inNode.getId()), inNode.getNodeInfo());
    }
}
