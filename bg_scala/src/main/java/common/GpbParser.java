package common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * GPB 解析总入口
 * Created by sk-qianxiao on 2018/7/10.
 */
public class GpbParser {
    private static final Logger logger = LoggerFactory.getLogger(GpbParser.class);

    /**
     * GPB 解析入口函数
     *
     * @param bytes
     * @return
     */
    public static List<Tables> parseData(byte[] bytes) {
        List<Tables> tables = new ArrayList<>();
        return tables;
    }
}
