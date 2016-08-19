import com.cnc.tools.Common;
import com.cnc.tools.StatusCode;
import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.common.metrics.Stat;

import java.io.*;
import java.util.*;

/**
 * Created by zhuangjy on 2016/8/17.
 */
public class TestFile {

    public static void main(String[] args) throws Exception {
        //读入路径配置文件
        InputStream in = new BufferedInputStream(ClassLoader.getSystemResourceAsStream("config.properties"));
        Properties p = new Properties();
        p.load(in);
        String dir = (String) p.get("dir");
        File director = new File(dir);
        String[] files = director.list();
        Map<String, List<LinkedList<StatusCode>>> result = new HashedMap();
        System.out.println("打印文件...");
        for (int i = 0; i < files.length; i++) {
            Map<String, StatusCode> map = new HashedMap();
            LinkedList<StatusCode> all = new LinkedList<>();
            LinkedList<StatusCode> hit = new LinkedList<>();
            LinkedList<StatusCode> miss = new LinkedList<>();
            BufferedReader br = new BufferedReader(new FileReader(director.getAbsolutePath() + "\\" + files[i]));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.split(" ").length != 4)
                    continue;
                StatusCode sc = new StatusCode(line);
                if (map.containsKey(sc.getChannel())) {
                    StatusCode c = map.get(sc.getChannel());
                    c.fold(sc);
                } else {
                    map.put(sc.getChannel(), sc);
                    all.add(sc);
                    hit.add(sc);
                    miss.add(sc);
                }

            }
            Collections.sort(all, new compare(0));
            Collections.sort(hit, new compare(1));
            Collections.sort(miss, new compare(2));

            //按文件输出
            System.out.println(new Date() + " 文件统计结果:" + files[i]);
            System.out.println("TOP " + Common.TOP + " ALL:");
            outPutList(all.subList(0, Common.TOP));
            System.out.println("TOP " + Common.TOP + " HIT:");
            outPutList(hit.subList(0, Common.TOP));
            System.out.println("TOP " + Common.TOP + " MISS:");
            outPutList(miss.subList(0, Common.TOP));
            System.out.println();
        }
    }

    //遍历输出List
    public static void outPutList(List<StatusCode> list) {
        for (StatusCode s : list) {
            System.out.println(s);
        }
    }

    static class compare implements Comparator<StatusCode> {
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
