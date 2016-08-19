package com.cnc.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.cnc.tools.Cleanable;
import com.cnc.tools.CodeCompare;
import com.cnc.tools.Common;
import com.cnc.tools.StatusCode;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.functors.ExceptionClosure;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhuangjy on 2016/8/18.
 */
public class TotalBolt extends BaseBasicBolt implements Cleanable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TotalBolt.class);
    private List<StatusCode> allList = new ArrayList<>();
    private List<StatusCode> hitList = new ArrayList<>();
    private List<StatusCode> missList = new ArrayList<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                //每两分钟输出结果:
                while (true) {
                    try {
                        TimeUnit.SECONDS.sleep(Common.WAIT_TIME);
                    } catch (InterruptedException e) {
                        LOGGER.error("", e);
                    }
                    LOGGER.info("Total线程启动,准备打印全局信息...");
                    File file = new File(Common.outputFile);
                    try {
                        RandomAccessFile out = new RandomAccessFile(file, "rw");
                        FileChannel fileChannel = out.getChannel();
                        FileLock fileLock;
                        while (true) {
                            try {
                                fileLock = fileChannel.lock();
                                break;
                            } catch (Exception e) {
                                LOGGER.info("其他进程正在写文件...");
                                TimeUnit.SECONDS.sleep(1);
                            }
                        }
                        StringBuffer sb = new StringBuffer(new Date() + " 统计总体结果: \n");
                        sb.append("TOP" + Common.TOP + " ALL:\n");
                        for (StatusCode sc : allList.subList(0, Math.min(Common.TOP, allList.size())))
                            sb.append(sc.toString() + "\n");
                        sb.append("TOP" + Common.TOP + " HIT:\n");
                        for (StatusCode sc : hitList.subList(0, Math.min(Common.TOP, hitList.size())))
                            sb.append(sc.toString() + "\n");
                        sb.append("TOP" + Common.TOP + " MISS:\n");
                        for (StatusCode sc : missList.subList(0, Math.min(Common.TOP, missList.size())))
                            sb.append(sc.toString() + "\n");
                        sb.append("\n");
                        out.seek(file.length());
                        out.write(sb.toString().getBytes());

//                        FileUtils.write(file, new Date() + " 统计总体结果: \n", true);
//                        FileUtils.write(file, "TOP" + Common.TOP + " ALL:\n", true);
//                        FileUtils.writeLines(file, allList.subList(0, Math.min(Common.TOP, allList.size())), true);
//                        FileUtils.write(file, "TOP" + Common.TOP + " HIT:\n", true);
//                        FileUtils.writeLines(file, hitList.subList(0, Math.min(Common.TOP, hitList.size())), true);
//                        FileUtils.write(file, "TOP" + Common.TOP + " MISS:\n", true);
//                        FileUtils.writeLines(file, missList.subList(0, Math.min(Common.TOP, missList.size())), true);
//                        FileUtils.write(file, "统计结束...\n\n", true);

                        fileLock.release();
                        fileChannel.close();
                        out.close();
                        clean();
                    } catch (IOException e) {
                        LOGGER.error("", e);
                    } catch (InterruptedException e) {
                        LOGGER.error("", e);
                    }
                }
            }
        }).start();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        ImmutableList<ImmutableList<ImmutableList>> channel = (ImmutableList) tuple.getValueByField("channel");
        List channelAll = channel.get(Common.ALL).asList();
        List channelHit = channel.get(Common.HIT).asList();
        List channelMiss = channel.get(Common.MISS).asList();
        allList.addAll(channelAll);
        hitList.addAll(channelHit);
        missList.addAll(channelMiss);
        //排序
        Collections.sort(allList, new CodeCompare(Common.ALL));
        Collections.sort(hitList, new CodeCompare(Common.HIT));
        Collections.sort(missList, new CodeCompare(Common.MISS));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void clean() {
        this.allList = new ArrayList<>();
        this.hitList = new ArrayList<>();
        this.missList = new ArrayList<>();
    }
}
