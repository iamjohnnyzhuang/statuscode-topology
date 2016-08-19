package com.cnc.tools;

import java.io.Serializable;
import java.util.*;

/**
 * Created by zhuangjy on 2016/8/15.
 */
public class StatusCode implements Serializable {
    private String channel;
    private int all;
    private int hit;
    private int miss;
    private int origin;

    private Map<String, List<Integer>> code = new LinkedHashMap<>();

    public StatusCode(String content) {
        String[] str = content.split(" ");
        if (str.length == 4) {
            this.channel = str[0];
            String[] num = str[1].split(":");
            this.all = Integer.valueOf(num[1]);
            this.hit = Integer.valueOf(num[2]);
            this.miss = Integer.valueOf(num[3]);
            this.origin = Integer.valueOf(num[4]);

            //将其余所有状态码转入code
            for (int i = 2; i < str.length; i++) {
                String[] c = str[i].split(":");
                List<Integer> list = new LinkedList<>();
                for (int j = 1; j < c.length; j++) {
                    list.add(Integer.valueOf(c[j]));
                }
                code.put(c[0], list);
            }
        }
    }

    public StatusCode(String channel, int all, int hit, int miss, int origin) {
        this.channel = channel;
        this.all = all;
        this.hit = hit;
        this.miss = miss;
        this.origin = origin;
    }

    public Integer getAll() {
        return all;
    }

    public void setAll(Integer all) {
        this.all = all;
    }

    public Integer getHit() {
        return hit;
    }

    public void setHit(Integer hit) {
        this.hit = hit;
    }

    public Integer getMiss() {
        return miss;
    }

    public void setMiss(Integer miss) {
        this.miss = miss;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public void setAll(int all) {
        this.all = all;
    }

    public void setHit(int hit) {
        this.hit = hit;
    }

    public void setMiss(int miss) {
        this.miss = miss;
    }

    public Map<String, List<Integer>> getCode() {
        return code;
    }

    public void setCode(Map<String, List<Integer>> code) {
        this.code = code;
    }

    public int getOrigin() {
        return origin;
    }

    public void setOrigin(int origin) {
        this.origin = origin;
    }

    public void fold(StatusCode sc) {
        this.all += sc.getAll();
        this.hit += sc.getHit();
        this.miss += sc.getMiss();
        this.origin += sc.getOrigin();

        Map<String, List<Integer>> map = sc.getCode();
        for (Map.Entry entry : map.entrySet()) {
            String key = (String) entry.getKey();
            List<Integer> newList = (List<Integer>) entry.getValue();
            if (code.containsKey(key)) {
                List<Integer> originList = code.get(key);
                for (int i = 0; i < 4; i++) {
                    originList.set(i, originList.get(i) + newList.get(i));
                }
            } else {
                code.put(key, newList);
            }
        }
    }

    //深复制
    @Override
    public StatusCode clone() {
        StatusCode sc = new StatusCode(this.channel, this.all, this.hit, this.miss, this.origin);
        Map<String, List<Integer>> map = new LinkedHashMap<>(this.code);
        for (Map.Entry entry : code.entrySet()) {
            String key = (String) entry.getKey();
            List<Integer> value = (List<Integer>) entry.getValue();
            map.put(new String(key), new ArrayList<>(value));
        }
        sc.setCode(map);
        return sc;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer(this.channel);
        sb.append(" " + "all:" + all + ":" + hit + ":" + miss + ":" + origin);
        for (Map.Entry entry : code.entrySet()) {
            String key = (String) entry.getKey();
            List<Integer> value = (List<Integer>) entry.getValue();
            sb.append(" " + key);
            for (int i : value) {
                sb.append(":" + i);
            }
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        StatusCode that = (StatusCode) o;
        if(that.getChannel().equals(this.channel))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = channel != null ? channel.hashCode() : 0;
        result = 31 * result + all;
        result = 31 * result + hit;
        result = 31 * result + miss;
        result = 31 * result + origin;
        result = 31 * result + (code != null ? code.hashCode() : 0);
        return result;
    }
}
