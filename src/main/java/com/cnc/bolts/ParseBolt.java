package com.cnc.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cnc.tools.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhuangjy on 2016/8/18.
 */
public class ParseBolt extends BaseBasicBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParseBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //收到发送信号
        if (tuple.getSourceStreamId().equals("signal")) {
            LOGGER.info("get Message,preparing to send data");
            basicOutputCollector.emit("signal", new Values("end"));
            return;
        }
        String line = tuple.getString(0);
        String file = tuple.getString(1);
        StatusCode sc = new StatusCode(line);
        if(sc.getChannel() == null)
            return;
        basicOutputCollector.emit("default",new Values(sc.getChannel(), file, sc));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("default", new Fields("channel", "file", "sc"));
        outputFieldsDeclarer.declareStream("signal", new Fields("signal"));
    }
}
