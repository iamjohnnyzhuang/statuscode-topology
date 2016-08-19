package com.cnc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.cnc.bolts.*;
import com.cnc.spouts.CodeSpout;

import java.io.*;
import java.util.Properties;

public class TopologyMain {
    public static void main(String[] args) throws Exception {
        //读入路径配置文件
        InputStream in = new BufferedInputStream(ClassLoader.getSystemResourceAsStream("config.properties"));
        Properties p = new Properties();
        p.load(in);
        String dir = (String) p.get("dir");

        //Topology
        String spoutId = "reader";
        String parseId = "parse";
        String channelId = "channel";
        String totalId = "total";
        String singleId = "singleId";

        TopologyBuilder builder = new TopologyBuilder();
        //spout读取文件随机分发
        builder.setSpout(spoutId, new CodeSpout());
        //parseBolt 解析内容
        builder.setBolt(parseId,new ParseBolt(),4).shuffleGrouping(spoutId,"default").shuffleGrouping(spoutId,"signal");
        //ChannelBolt 整合同频道发送给TOTAL打印 、 整合同文件并且同频道发送给文件Bolt打印
        builder.setBolt(channelId,new ChannelBolt(),1).fieldsGrouping(parseId,"default",new Fields("channel")).allGrouping(parseId,"signal");
        //TotalBolt 整合不同文件同频道后打印统计结果（总体结果）
        builder.setBolt(totalId,new TotalBolt(),1).globalGrouping(channelId,"total");
        //FileBolt 整合相同文件后打印统计结果
        builder.setBolt(singleId,new SingleBolt(),4).fieldsGrouping(channelId,"file",new Fields("file"));

        Config conf = new Config();
        conf.put("dir", dir);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("StatusCode-Topology", conf, builder.createTopology());
    }
}
