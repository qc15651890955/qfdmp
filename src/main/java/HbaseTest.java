import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Test;

import java.io.IOException;

public class HbaseTest {
    @Test
    public void test() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.110.111:2181,192.168.110.112:2181,192.168.110.113:2181");
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        NamespaceDescriptor ns1 = NamespaceDescriptor.create("ns1").build();
        ns1.setConfiguration("name","qianchen");
        hBaseAdmin.createNamespace(ns1);
    }
}
