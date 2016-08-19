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
import com.google.common.collect.Lists;
import com.sun.javafx.collections.ImmutableObservableList;
import com.sun.org.apache.xerces.internal.util.Status;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.metrics.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class SingleBolt extends BaseBasicBolt implements Cleanable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleBolt.class);
    Map<String, List<List<StatusCode>>> result = new HashedMap();

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        TimeUnit.SECONDS.sleep(Common.WAIT_TIME);
                    } catch (InterruptedException e) {
                        LOGGER.error("", e);
                    }
                    LOGGER.info("Single线程启动,准备打印部分文件信息...");
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

                        for (Map.Entry entry : result.entrySet()) {
                            List<List<StatusCode>> list = (List<List<StatusCode>>) entry.getValue();
                            List<StatusCode> allList = list.get(Common.ALL);
                            List<StatusCode> hitList = list.get(Common.HIT);
                            List<StatusCode> missList = list.get(Common.MISS);

                            StringBuffer sb = new StringBuffer(new Date() + " 文件统计结果:" + entry.getKey() + " \n");
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
//                            FileUtils.write(file, new Date() + " 统计文件:" + entry.getKey(), true);
//                            FileUtils.write(file, "TOP" + Common.TOP + " ALL:\n", true);
//                            FileUtils.writeLines(file, allList, true);
//                            FileUtils.write(file, "TOP" + Common.TOP + " HIT:\n", true);
//                            FileUtils.writeLines(file, hitList, true);
//                            FileUtils.write(file, "TOP" + Common.TOP + " MISS:\n", true);
//                            FileUtils.writeLines(file, missList, true);
//                            FileUtils.write(file, "统计结束...\n\n", true);
                        }

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
    public void execute(Tuple input, BasicOutputCollector collector) {
        String file = input.getString(0);
        Map<String, StatusCode> map = (Map<String, StatusCode>) input.getValueByField("code");
        if (result.containsKey(file)) {
            List<List<StatusCode>> list = result.get(file);
            List<StatusCode> allList = list.get(Common.ALL);
            List<StatusCode> hitList = list.get(Common.HIT);
            List<StatusCode> missList = list.get(Common.MISS);

            List<StatusCode> mapList = new LinkedList<>(map.values());
            Set<StatusCode> set = new HashSet<>();
            set.addAll(new ArrayList<>(allList));
            set.addAll(new ArrayList<>(hitList));
            set.addAll(new ArrayList<>(missList));
            mapList.addAll(set);
            result.put(file, sortAndSub(mapList));
        } else {
            List<StatusCode> list = new LinkedList<>();
            for (StatusCode sc : map.values())
                list.add(sc);
            result.put(file, sortAndSub(list));
        }

    }


    /**
     * 将一个数组按三种模式排序并且返回对应0-N子串
     *
     * @param list
     * @return
     */
    public List<List<StatusCode>> sortAndSub(List<StatusCode> list) {
        List<List<StatusCode>> res = Lists.newArrayListWithCapacity(3);
        res.add(sortAndSub(list, Common.ALL));
        res.add(sortAndSub(list, Common.HIT));
        res.add(sortAndSub(list, Common.MISS));
        return res;
    }

    /**
     * 将一个数组排序并且返回0-N子串
     *
     * @param list
     * @param mode
     * @return
     */
    public List<StatusCode> sortAndSub(List<StatusCode> list, int mode) {
        Collections.sort(list, new CodeCompare(mode));
        return new LinkedList<>(list.subList(0, Math.min(Common.TOP, list.size())));
    }

    @Override
    public void clean() {
        this.result = new HashedMap();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
