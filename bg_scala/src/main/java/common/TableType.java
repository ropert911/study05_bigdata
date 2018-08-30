package common;

/**
 * 数据类型，根据不同的数据类型写入不同的HIVE表
 * Created By sk-tengfeiwang on 2018/7/12.
 */
public enum TableType {
    ClientAll(100),
    SessionAll(101),
    SessionAssocAll(102),
    ApAll(103),
    ApWanAll(104),
    ApRadioAll(105),
    ApVapInfoAll(106),
    ApVapTrafficAll(107),
    ApVapAssocAll(108),
    ApVapSnrAll(109),
    AcAll(110),
    ApOnacAll(111),
    AcDhcpAll(112),
    AcInterfaceAll(113),
    Others(999);

    private final int tableType;

    private TableType(int tableType) {
        this.tableType = tableType;
    }

    public int getTableType() {
        return tableType;
    }

}
