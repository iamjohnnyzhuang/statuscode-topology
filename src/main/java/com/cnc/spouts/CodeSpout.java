package com.cnc.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.cnc.tools.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CodeSpout extends BaseRichSpout {
    private static final Logger LOGGER = LoggerFactory.getLogger(CodeSpout.class);
    private SpoutOutputCollector collector;
    private File director;
    private Long endLine;
    private Long curentNum;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        String dir = conf.get("dir").toString();
        this.director = new File(dir);
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        String[] files = director.list();
        endLine = readLines(director.listFiles());
        curentNum = 0l;
        long num = 1;
        for (int i = 0; i < files.length; i++) {
            try (BufferedReader br = new BufferedReader(new FileReader(director.getAbsolutePath() + "\\" + files[i]))) {
                String line;
                while ((line = br.readLine()) != null) {
                    this.collector.emit("default", new Values(line, files[i]), num++);
                }
            } catch (IOException e) {
                LOGGER.error("", e);
            }
        }

//        this.collector.emit("signal", new Values("end"));
        try {
            LOGGER.info("Spout读取所有文件完成...进入睡眠状态");
            TimeUnit.SECONDS.sleep(Common.WAIT_TIME);
        } catch (InterruptedException e) {
            LOGGER.error("", e);
        }

    }

    @Override
    public void ack(Object msgId) {
        curentNum++;
        if (endLine.equals(curentNum)) {
            LOGGER.info("所有文件处理发送完毕，发送信号");
            this.collector.emit("signal", new Values("end"));
        }
    }

    @Override
    public void fail(Object msgId) {
        curentNum++;
        if (endLine.equals(curentNum)) {
            LOGGER.info("所有文件处理发送完毕，发送信号");
            this.collector.emit("signal", new Values("end"));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("default", new Fields("line", "file"));
        declarer.declareStream("signal", new Fields("signal"));
    }

    /**
     * 返回该目录下总行数
     */
    public long readLines(File[] files) {
        Long num = 0l;
        try {
            for (File f : files) {
                BufferedReader br = new BufferedReader(new FileReader(f));
                while (br.readLine() != null) {
                    num++;
                }
            }
        } catch (IOException e) {
            LOGGER.error("", e);
        }
        return num;
    }
}
