package wiki.step2.calc;

import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.Integer.parseInt;

public class Node {
    public static enum Color {WHITE, GRAY, BLACK};

    private String id;
    private List<String> parents = new ArrayList<String>();
    private List<String> edges = new ArrayList<String>();
    private Color color = Color.WHITE;
    private int distance = Integer.MAX_VALUE;

    public Node() {
        edges = new ArrayList<String>();
        distance = Integer.MAX_VALUE;
        color = Color.WHITE;
        parents = new ArrayList<String>();
    }

    public Node(String nodeInfo) {
        String[] inputLine = nodeInfo.split("\t");
        String key = "", value = "";
        try {
            key = inputLine[0]; // node id
            value = inputLine[1]; // adjacent nodes, distance, color, parent
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        String[] tokens = value.split("\\|");
        //tokens[0] = list of adjacent nodes, tokens[1]= distance, tokens[2]= color, tokens[3]= parent

        this.id = key;

        for (String s : tokens[0].split(",")) {
            if (s.length() > 0) {
                this.edges.add(s);
            }
        }
        if (tokens[1].equals("Integer.MAX_VALUE")) {
            this.distance = Integer.MAX_VALUE;
        } else {
            try {
                this.distance = parseInt(tokens[1]);
            } catch (NumberFormatException e) {
                throw new NumberFormatException(nodeInfo);
            }
        }
        this.color = Color.valueOf(tokens[2]);
        if (tokens.length == 4 && !tokens[3].equals("null")) {
            Collections.addAll(this.parents, tokens[3].split(","));
        }
    }

    public Text getNodeInfo() {
        StringBuffer s = new StringBuffer();
        try {
            for (String v : edges) {
                s.append(v).append(",");
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
            System.exit(1);
        }

        s.append("|");

        if (this.distance < Integer.MAX_VALUE) {
            s.append(this.distance).append("|");
        } else {
            s.append("Integer.MAX_VALUE").append("|");
        }
        s.append(color.toString()).append("|");
        for (String p : parents) {
            s.append(p).append(",");
        }
        return new Text(s.toString());
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getParents() {
        return parents;
    }

    public void setParents(List<String> parents) {
        this.parents = parents;
    }

    public int getDistance() {
        return distance;
    }

    public void setDistance(int distance) {
        this.distance = distance;
    }

    public List<String> getEdges() {
        return edges;
    }

    public void setEdges(List<String> edges) {
        this.edges = edges;
    }

    public Color getColor() {
        return color;
    }

    public void setColor(Color color) {
        this.color = color;
    }
}
