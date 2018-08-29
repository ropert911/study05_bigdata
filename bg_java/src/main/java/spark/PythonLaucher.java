package spark;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author sk-ziconglu on 2017/5/3.
 */
public class PythonLaucher {
    private final static Logger logger = LoggerFactory.getLogger(PythonLaucher.class);

    public static void main(String[] args) {
        logger.info("begin to running task,task id is:");
        try {
            //spark任务配置相关
            SparkLauncher launcher = new SparkLauncher();
            launcher.setAppName("test_AppName");
            launcher.setSparkHome("/opt/spark");
            launcher.setMaster("yarn");
            launcher.setDeployMode("cluster");

            //设置参数
            launcher.setConf("spark.executor.memory", "2g");
            launcher.setConf("spark.driver.memory", "1g");
            launcher.setConf("spark.driver.cores", "2");
            launcher.setConf("spark.executor.cores", "2");
            launcher.setConf("spark.executor.memoryOverhead", "1g");
            launcher.setConf("spark.executor.instances", "2");

            //添加自定义任务的参数
            List<String> scriptParams = new ArrayList(10);
            scriptParams.add("param_name1=param1");
            scriptParams.add("param_name2=param2");
            launcher.addAppArgs(scriptParams.toArray(new String[scriptParams.size()]));

            //设置脚本依赖库
            launcher.addPyFile("/opt/data/pyspark/default/skslib.zip");
            //设置运行脚本
            launcher.setAppResource("/opt/data/pyspark/default/ap_statistic.py");

            //启动spark任务
            SparkAppHandle handle = launcher.startApplication(new SparkAppHandle.Listener() {
                boolean isRunning = false;

                @Override
                public void stateChanged(SparkAppHandle sparkAppHandle) {
                    String status = sparkAppHandle.getState().toString().toUpperCase();
                    if (!isRunning && status.equalsIgnoreCase("RUNNING")) {
                        isRunning = true;
                    }
                }

                @Override
                public void infoChanged(SparkAppHandle sparkAppHandle) {
                    logger.info("infoChanged:appId-->" + sparkAppHandle.getAppId() + "\tstatus-->" + sparkAppHandle.getState().toString());
                }
            });

            //等待任务结束
            while (!"FINISHED".equalsIgnoreCase(handle.getState().toString())
                    && !"FAILED".equalsIgnoreCase(handle.getState().toString())
                    && !"KILLED".equalsIgnoreCase(handle.getState().toString())) {
                logger.info("wait for:appId-->" + handle.getAppId() + "\tstatus-->" + handle.getState().toString());

                try {
                    Thread.sleep(1000 * 5);
                } catch (InterruptedException e) {
                    logger.error("EtlTaskRunnable sleep error:", e);
                }
            }
        } catch (Exception e) {
            logger.error("SparkLauncher error:", e);
        }
    }
}
