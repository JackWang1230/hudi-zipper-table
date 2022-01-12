import org.apache.flink.api.java.Utils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;

import java.io.Serializable;

/**
 * @author RWang
 * @Date 2021/12/2
 */

public class MyStreamExecutionEnvironment  extends StreamExecutionEnvironment implements Serializable {
    private static final long serialVersionUID = 4933980714272600064L;
    private static final ThreadLocal<StreamExecutionEnvironmentFactory> threadLocalContextEnvironmentFactory = null;
    private static StreamExecutionEnvironmentFactory contextEnvironmentFactory;
    public MyStreamExecutionEnvironment(){
        super();
    }
    public static StreamExecutionEnvironment getExecutionEnvironment() {
        return getExecutionEnvironment(new Configuration());
    }

    public static StreamExecutionEnvironment getExecutionEnvironment(Configuration configuration) {
        return (StreamExecutionEnvironment) Utils.resolveFactory(threadLocalContextEnvironmentFactory, contextEnvironmentFactory).map((factory) -> {
            return factory.createExecutionEnvironment(configuration);
        }).orElseGet(() -> {
            return createLocalEnvironment(configuration);
        });
    }

}
