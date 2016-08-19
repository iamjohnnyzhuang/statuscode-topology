package com.cnc.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.cnc.tools.Cleanable;
import com.cnc.tools.Common;
import com.cnc.tools.StatusCode;
import org.apache.commons.collections.map.HashedMap;

import java.util.*;

public class RankBolt extends BaseBasicBolt implements Cleanable{
    //String:文件名 Values:各种数据的TOP5
    Map<String, LinkedList<StatusCode>> allMap;
    Map<String, LinkedList<StatusCode>> hitMap;
    Map<String, LinkedList<StatusCode>> missMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.allMap = new HashedMap(Common.TOP);
        this.hitMap = new HashedMap(Common.TOP);
        this.missMap = new HashedMap(Common.TOP);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ranked"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
//        //结束将结果发送到Merge
//        if (input.getSourceStreamId().equals("signals")) {
//            //按文件整理分发
//            Map<String, List<LinkedList<StatusCode>>> result = new HashedMap();
//            for (String f : CodeSpout.getFiles()) {
//                if (allMap.containsKey(f) || hitMap.containsKey(f) || missMap.containsKey(f)) {
//                    List<LinkedList<StatusCode>> list = new ArrayList<>();
//                    addIfContains(allMap, list, f);
//                    addIfContains(hitMap, list, f);
//                    addIfContains(missMap, list, f);
//                    result.put(f, list);
//                }
//            }
//            collector.emit(new Values(result));
//            clean();
//        } else {
//            String line = input.getString(0);
//            String file = input.getString(1);
//            String[] contents = line.split(" ");
//            String[] s = contents[1].split(":");
//            StatusCode sc = new StatusCode(line, Integer.valueOf(s[1]), Integer.valueOf(s[2]), Integer.valueOf(s[3]));
//            insertCode(allMap, file, sc, 0);
//            insertCode(hitMap, file, sc, 1);
//            insertCode(missMap, file, sc, 2);
//        }
    }

    public void addIfContains(Map<String, LinkedList<StatusCode>> map, List<LinkedList<StatusCode>> list, String s) {
        if (map.containsKey(s))
            list.add(map.get(s));
    }


    /**
     * 判断是否需要插入对应的状态码信息
     *
     * @param map  目标插入的Map
     * @param file 当前文件
     * @param sc   line信息
     * @param mode 模式—— 0:All 1：Hit 2：Miss
     * @return
     */
    public void insertCode(Map<String, LinkedList<StatusCode>> map, String file, StatusCode sc, final Integer mode) {
        LinkedList<StatusCode> list = null;
        //当前操作的目标值: 如all 或者 hit
        Integer target = mode == 0 ? sc.getAll() : mode == 1 ? sc.getHit() : sc.getMiss();
        boolean flag = false;
        if (!map.containsKey(file)) {
            list = new LinkedList<>();
            map.put(file, list);
        } else {
            list = map.get(file);
        }

        //获取当前链表TOP 最小值
        Integer[] mins = getMins(list);
        Integer min = mode == 0 ? mins[0] : mode == 1 ? mins[1] : mins[2];

        if (list.size() < Common.TOP) {
            list.add(sc);
            map.put(file, list);
            flag = true;
        } else if (min < target) {
            for(int i = list.size()-1;i>=0;i--){
                Integer curMin = mode == 0 ? list.get(i).getAll() : mode == 1 ? list.get(i).getHit() : list.get(i).getMiss();
                if (curMin <= min) {
                    list.remove(i);
                    list.add(sc);
                    flag = true;
                    break;
                }
            }
        }
        //有变化过更新序列以及最小值
        if (flag) {
            Collections.sort(list, new compare(mode));
        }
    }

    /**
     * 返回一个List中三个属性的最小值
     *
     * @param list
     * @return
     */
    public Integer[] getMins(List<StatusCode> list) {
        Integer minAll = Integer.MAX_VALUE;
        Integer minHit = Integer.MAX_VALUE;
        Integer minMiss = Integer.MAX_VALUE;

        for (StatusCode sc : list) {
            minAll = Math.min(minAll, sc.getAll());
            minHit = Math.min(minHit, sc.getHit());
            minMiss = Math.min(minMiss, sc.getMiss());
        }

        Integer[] res = new Integer[3];
        res[0] = minAll;
        res[1] = minHit;
        res[2] = minMiss;
        return res;
    }

    @Override
    public void clean() {
        this.allMap = new HashedMap(Common.TOP);
        this.hitMap = new HashedMap(Common.TOP);
        this.missMap = new HashedMap(Common.TOP);
    }

    class compare implements Comparator<StatusCode> {
        Integer mode;

        compare(Integer mode) {
            this.mode = mode;
        }

        @Override
        public int compare(StatusCode o1, StatusCode o2) {
            return mode == 0 ? o2.getAll() - o1.getAll() : mode == 1 ? o2.getHit() - o1.getHit() : o2.getMiss() - o1.getMiss();
        }
    }
}
