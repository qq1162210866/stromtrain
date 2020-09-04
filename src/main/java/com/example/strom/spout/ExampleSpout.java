package com.example.strom.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * ExampleSpout.java
 * Description: 练习Spout，数据当源头。也叫壶嘴
 *
 * @author Peng Shiquan
 * @date 2020/7/6
 */
public class ExampleSpout extends BaseRichSpout {

    /**
     * Spout输出收集器
     */
    private SpoutOutputCollector spo;
    /**
     *
     */
    private static final String filed = "word";
    /**
     * 计数变量
     */
    private int count = 1;
    /**
     * 用数组模拟流
     */
    private String[] messages = {"aaa", "bbb", "ccc", "ddd", "eee", "aaa"};


    /**
     * Description: 在spout组件初始化时被调用
     *
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     * @return void
     * @Author: Peng Shiquan
     * @Date: 2020/7/6
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        System.err.println("开始调用ExampleSpout的open方法");
        this.spo = spoutOutputCollector;
    }

    /**
     * Description: spout的核心，主要执行方法，用于输出信息
     *
     * @param
     * @return void
     * @Author: Peng Shiquan
     * @Date: 2020/7/6
     */
    @Override
    public void nextTuple() {
        if (count <= messages.length) {
            System.err.println("第" + count + "次模拟发送数据...");
            /**
             * Values类所作的优化主要是提供若干个支持可变列表的构造方法，包括全为String的参数类型、全为Integer的参数类型以及通用的Object类型
             * Values类直接继承自Java中的ArrayList类，因为ArrayList类恰好能够很好地满足描述“一行”值的需要——有序、不去重、可伸缩。
             */
            /**
             * 必须设置messageId，才能调用ack方法
             */
            this.spo.emit(new Values(messages[count - 1], "test" + count), count);
        }
        count++;
    }

    /**
     * Description: 声明数据格式，即输出的tuple中，包含了几个字段
     *
     * @param outputFieldsDeclarer
     * @return void
     * @Author: Peng Shiquan
     * @Date: 2020/7/7
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        System.err.println("ExampleSpout开始声明数据格式");
        /**
         * 这里Fields声明了几个字段，传tuple就要传多少个Values
         */
        /**
         * 这里指定了stream的名称
         */
        outputFieldsDeclarer.declare(new Fields("word", "test"));
    }

    /**
     * Description: 当Topology停止时，会调用这个方法
     *
     * @param
     * @return void
     * @Author: Peng Shiquan
     * @Date: 2020/7/7
     */
    @Override
    public void close() {
        System.err.println("Topology停止");
        super.close();
    }

    /**
     * Description: 处理成功时，会调用这个方法
     *
     * @param msgId
     * @return void
     * @Author: Peng Shiquan
     * @Date: 2020/7/7
     */
    @Override
    public void ack(Object msgId) {
        System.err.println("消息处理成功，消息ID" + msgId);
        super.ack(msgId);
    }

    /**
     * Description: 当tuple处理失败时，会调用这个方法
     *
     * @param msgId
     * @return void
     * @Author: Peng Shiquan
     * @Date: 2020/7/7
     */
    @Override
    public void fail(Object msgId) {
        System.err.println("消息处理失败");
        super.fail(msgId);
    }
}
