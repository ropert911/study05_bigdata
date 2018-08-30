package common;

import java.io.Serializable;

/**
 * Hive 表的父类
 * Created By sk-tengfeiwang on 2018/7/10.
 */
public class Tables implements Serializable {

    /**
     * 子表类型
     **/
    private int tableType;
    /**
     * 是否是AP的表项
     */
    private boolean isAp;
    /**
     * 设置mac地址
     */
    private String devMac;
    /**
     * 一级区域ID
     */
    public long topAreaid;

    public Tables(int tableType, boolean isAp, String devMac) {
        this.tableType = tableType;
        this.isAp = isAp;
        this.devMac = devMac;
    }

    public int getTableType() {
        return tableType;
    }

    public void setTableType(int tableType) {
        this.tableType = tableType;
    }

    public boolean isAp() {
        return isAp;
    }

    public void setAp(boolean ap) {
        isAp = ap;
    }

    public String getDevMac() {
        return devMac;
    }

    public long getTopAreaid() {
        return topAreaid;
    }

    public void setTopAreaid(long topAreaid) {
        this.topAreaid = topAreaid;
    }
}
