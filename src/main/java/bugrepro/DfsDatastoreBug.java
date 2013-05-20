package bugrepro;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import com.backtype.cascading.tap.PailTap;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.PailStructure;

import java.io.*;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class DfsDatastoreBug {
    public static void main(String[] args) {
        Tap source = new Hfs(new TextLine(), "input/test.txt");
//        Tap sink = new Hfs(new TextLine(), "output/test.txt");
        PailTap.PailTapOptions options = new PailTap.PailTapOptions();
        options.spec = new PailSpec();
        options.spec.setStructure(new IntStructure());
        Tap sink = new PailTap("output/test.txt", options);

        Pipe pipe = new Pipe("copy");
        pipe = new Each(pipe, new Fields("line"), new Identity(Integer.TYPE), Fields.RESULTS);

        FlowDef flowDef = FlowDef.flowDef()
                .addSource(pipe, source)
                .addTailSink(pipe, sink)
                ;

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, DfsDatastoreBug.class);
        FlowConnector flowConnector = new HadoopFlowConnector(properties);

        flowConnector.connect(flowDef).complete();
    }

    public static class IntStructure implements PailStructure<Integer> {

        public boolean isValidTarget(String... dirs) {
            return true;
        }

        public Integer deserialize(byte[] serialized) {
            ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
            DataInputStream dis = new DataInputStream(bais);
            try {
                int x = dis.readInt();
                dis.close();
                return x;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return 0;
        }

        public byte[] serialize(Integer object) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            try {
                dos.writeInt(object);
                dos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return baos.toByteArray();
        }

        public List<String> getTarget(Integer object) {
            return Collections.EMPTY_LIST;
        }

        public Class getType() {
            return Integer.class;
        }
    }
}
