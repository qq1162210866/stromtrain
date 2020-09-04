package com.example.strom.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * CountBolt.java
 * Description: Bolt练习，计数的Bolt
 *
 * @author Peng Shiquan
 * @date 2020/7/7
 */
public class CountBolt extends BaseRichBolt {

    /**
     * 计数
     */
    private long count;
    /**
     * 保存单词和对应的计数
     */
    private HashMap<String, Integer> counts = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        System.err.println("进入CountBolt的prepare");
        this.counts = new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple tuple) {
        String msg = tuple.getStringByField("count");
        System.err.println("第" + count + "次统计单词次数");
        if (!counts.containsKey(msg)) {
            counts.put(msg, 1);
        } else {
            counts.put(msg, counts.get(msg) + 1);
        }
        count++;
    }

    /**
     * Description: 最后执行，打印统计的单词次数
     *
     * @param
     * @return void
     * @Author: Peng Shiquan
     * @Date: 2020/7/7
     */
    @Override
    public void cleanup() {
        System.err.println("下面就是单词次数================");
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            System.err.println("单词" + entry.getKey() + "出现" + entry.getValue() + "次");
        }
        System.err.println("输出结束=======================");
        System.err.println("释放资源");
        super.cleanup();
    }

    /**
     * Description: 不在往下一个Bolt输出，所以没有代码
     *
     * @param outputFieldsDeclarer
     * @return void
     * @Author: Peng Shiquan
     * @Date: 2020/7/7
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        System.err.println("CountBolt开始声明数据格式");
    }
}
