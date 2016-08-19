package com.cnc.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cnc.tools.Cleanable;
import com.cnc.tools.CodeCompare;
import com.cnc.tools.Common;
import com.cnc.tools.StatusCode;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by zhuangjy on 2016/8/18.
 */
public class ChannelBolt extends BaseBasicBolt implements Cleanable{
    /**
     * key: fileName
     * value-key: channel
     * value: statusCode
     */
    private Map<String, Map<String, StatusCode>> statusCodeMap = new HashedMap();
    private static final Logger LOGGER = LoggerFactory.getLogger(ParseBolt.class);


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //收到发送信号
        if (tuple.getSourceStreamId().equals("signal")) {
            LOGGER.info("received signal");
            //按文件发送给所有文件打印bolt
            for (Map.Entry entry : statusCodeMap.entrySet()) {
                String fileName = (String) entry.getKey();
                basicOutputCollector.emit("file",new Values(fileName,entry.getValue()));
            }

            //分别将三种情况TOP N的Channel发送至全局 先整合所有
            Map<String, StatusCode> total = new HashedMap();
            List<StatusCode> current = new ArrayList<>();
            for (Map<String, StatusCode> map : statusCodeMap.values()) {
                for (Map.Entry entry : map.entrySet()) {
                    String channel = (String) entry.getKey();
                    if (total.containsKey(channel)) {
                        StatusCode sc = total.get(channel);
                        sc.fold((StatusCode) entry.getValue());
                    } else {
                        StatusCode sc = ((StatusCode) entry.getValue()).clone();
                        total.put(channel, sc);
                        current.add(sc);
                    }
                }
            }
            //整合所有元素后排序发送TOP N
            Collections.sort(current, new CodeCompare(Common.ALL));
            ImmutableList all = ImmutableList.copyOf(current.subList(0, Math.min(Common.TOP,current.size())));
            Collections.sort(current, new CodeCompare(Common.HIT));
            ImmutableList hit = ImmutableList.copyOf(current.subList(0,Math.min(Common.TOP,current.size())));
            Collections.sort(current, new CodeCompare(Common.MISS));
            ImmutableList miss = ImmutableList.copyOf(current.subList(0, Math.min(Common.TOP,current.size())));
            ImmutableList result = ImmutableList.of(all, hit, miss);
            //发送所有结果准备打印
            basicOutputCollector.emit("total", new Values(result));

            clean();
        } else {
            String channel = tuple.getString(0);
            String file = tuple.getString(1);
            StatusCode sc = (StatusCode) tuple.getValueByField("sc");
            if (statusCodeMap.containsKey(file) && statusCodeMap.get(file).containsKey(channel)) {
                //合并同文件、同频道数据
                StatusCode statusCode = statusCodeMap.get(file).get(channel);
                statusCode.fold(sc);
            } else {
                //创建新数据
                Map<String, StatusCode> channelStatus = null;
                //判断是否为该文件一次创建如果是要新建一个HashMap
                if(!statusCodeMap.containsKey(file)){
                    channelStatus = new HashedMap();
                }else {
                    channelStatus = statusCodeMap.get(file);
                }
                channelStatus.put(channel,sc);
                statusCodeMap.put(file, channelStatus);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("total", new Fields("channel"));
        outputFieldsDeclarer.declareStream("file", new Fields("file","code"));
    }

    @Override
    public void clean() {
        this.statusCodeMap = new HashedMap();
    }
}
