package com.example.strom.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * ExampleBolt.java
 * Description:  Bolt练习，处理流数据的地方
 *
 * @author Peng Shiquan
 * @date 2020/7/7
 */
public class SegmentationBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private TopologyContext topologyContext;

    /**
     * Description: 在Bolt启动前执行，提供启动的环境。
     *
     * @param map
     * @param topologyContext
     * @param outputCollector
     * @return void
     * @Author: Peng Shiquan
     * @Date: 2020/7/7
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        System.err.println("进入SegmentationBolt的prepare");
        this.outputCollector = outputCollector;
        this.topologyContext = topologyContext;
    }

    /**
     * Description: Bolt的核心,执行的方法。每次接收一个tuple，就会调用一次
     *
     * @param tuple
     * @return void
     * @Author: Peng Shiquan
     * @Date: 2020/7/7
     */
    @Override
    public void execute(Tuple tuple) {

        /**
         * Spout已经定义了这个field
         */
        String msg = tuple.getStringByField("word");
        System.err.println("开始执行处理方法,Task的ID为：" + topologyContext.getThisTaskId() + ".处理消息为：" + msg);
        String[] words = msg.toLowerCase().split(" ");

        for (String word : words) {
            /**
             * 发送到下个bolt
             */
            this.outputCollector.emit(new Values(word));
        }
        outputCollector.ack(tuple);
    }


    /**
     * Description: 声明数据格式，即输出的tuple中，包含了几个字段。Bolt执行完毕也会输出一个流
     *
     * @param outputFieldsDeclarer
     * @return void
     * @Author: Peng Shiquan
     * @Date: 2020/7/7
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        System.err.println("SegmentationBolt开始声明数据格式");
        outputFieldsDeclarer.declare(new Fields("count"));
    }

    /**
     * Description: 释放该Bolt占用的资源，Strom在终止时会调用这个方法
     *
     * @param
     * @return void
     * @Author: Peng Shiquan
     * @Date: 2020/7/7
     */
    @Override
    public void cleanup() {
        System.err.println("SegmentationBolt资源释放");
        super.cleanup();
    }
}
