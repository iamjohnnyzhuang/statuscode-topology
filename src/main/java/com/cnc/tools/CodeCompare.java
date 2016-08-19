package com.cnc.tools;

import java.util.Comparator;

/**
 * Created by zhuangjy on 2016/8/18.
 * 为状态码数组排序 倒序输出TOP N
 * mode: 0以all为基准 1以hit为基准 2以miss为基准
 */
public class CodeCompare implements Comparator<StatusCode>{
    private int mode;

    public CodeCompare(int mode){
        this.mode = mode;
    }

    @Override
    public int compare(StatusCode o1, StatusCode o2) {
        return mode == 0 ? o2.getAll() - o1.getAll() : mode == 1 ? o2.getHit() - o1.getHit() : o2.getMiss() - o1.getMiss();
    }
}
