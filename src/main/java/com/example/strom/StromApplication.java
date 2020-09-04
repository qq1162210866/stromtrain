package com.example.strom;

import com.example.strom.bolt.CountBolt;
import com.example.strom.bolt.SegmentationBolt;
import com.example.strom.spout.ExampleSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class StromApplication {

    public static void main(String[] args) {
        /**
         * 新建一个拓扑
         */
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        /**
         * 设置Spout的Executor数量参数parallelism_hint
         * 放置一个Spout，设置Spout的ID
         */
        topologyBuilder.setSpout("ExampleSpout", new ExampleSpout(), 1);
        /**
         * 设置分割Bolt同时设置数据的来源为随机分组，来源为实体名为ExampleSpout。流的名称为"default"
         */
        topologyBuilder.setBolt("SegmentationBolt", new SegmentationBolt(), 1).setNumTasks(2).fieldsGrouping("ExampleSpout", new Fields("word"));
        /**
         * 设置统计Bolt，设置为字段分组，根据count字段来获取SegmentationBolt中的tuple
         */
        topologyBuilder.setBolt("CountBolt", new CountBolt(), 1).setNumTasks(1).fieldsGrouping("SegmentationBolt", new Fields("count"));
        /**
         * 初始化Strom配置
         */
        Config config = new Config();
        config.put("test", "test");
        try {
            if (args != null && args.length > 0) {
                /**
                 * 向集群提交作业
                 */
                System.out.println("运行远程模式");
                /**
                 * 把第一个参数当作
                 */
                StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
            } else {
                System.err.println("运行本地模式");
                LocalCluster localCluster = new LocalCluster();
                localCluster.submitTopology("wordcount", config, topologyBuilder.createTopology());
                Thread.sleep(20000);
                localCluster.shutdown();
            }
        } catch (Exception exception) {
            System.err.println(exception);
            exception.printStackTrace();
        }
    }
}
