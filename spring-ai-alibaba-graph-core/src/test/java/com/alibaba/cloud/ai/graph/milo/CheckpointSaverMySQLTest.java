package com.alibaba.cloud.ai.graph.milo;

import com.alibaba.cloud.ai.graph.CompileConfig;
import com.alibaba.cloud.ai.graph.CompiledGraph;
import com.alibaba.cloud.ai.graph.KeyStrategy;
import com.alibaba.cloud.ai.graph.KeyStrategyFactory;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.RunnableConfig;
import com.alibaba.cloud.ai.graph.StateGraph;
import com.alibaba.cloud.ai.graph.checkpoint.config.SaverConfig;
import com.alibaba.cloud.ai.graph.checkpoint.savers.mysql.MysqlSaver;
import com.alibaba.cloud.ai.graph.exception.GraphStateException;
import com.alibaba.cloud.ai.graph.state.StateSnapshot;
import com.alibaba.cloud.ai.graph.state.strategy.ReplaceStrategy;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.cloud.ai.graph.StateGraph.END;
import static com.alibaba.cloud.ai.graph.StateGraph.START;
import static com.alibaba.cloud.ai.graph.action.AsyncNodeAction.node_async;

/**
 * @author milo
 * @since 2026/2/4
 */
public class CheckpointSaverMySQLTest {

    @Test
    public void test01() throws GraphStateException {
        // 定义状态策略
        KeyStrategyFactory keyStrategyFactory = () -> {
            Map<String, KeyStrategy> keyStrategyMap = new HashMap<>();
            keyStrategyMap.put("input", new ReplaceStrategy());
            keyStrategyMap.put("agent_1:prop1", new ReplaceStrategy());
            return keyStrategyMap;
        };

        // 定义节点
        var agent1 = node_async(state -> {
            System.out.println("agent_1 执行中");
            return Map.of("agent_1:prop1", "agent_1:test");
        });

        // 构建图
        StateGraph stateGraph = new StateGraph(keyStrategyFactory)
                .addNode("agent_1", agent1)
                .addEdge(START, "agent_1")
                .addEdge("agent_1", END);


        // RedisSaver
        //RedisSaver saver = RedisSaver.builder().redisson(redisson).build();

        // 创建 MySQL DataSource
        //CREATE DATABASE spring_ai_graph CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setURL("jdbc:mysql://localhost:3306/spring_ai_graph?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=Asia/Shanghai");
        dataSource.setUser("root");
        dataSource.setPassword("test1234");

        //MysqlSaver
        MysqlSaver mysqlSaver = MysqlSaver.builder().dataSource(dataSource).build();


        // SaverConfig
        SaverConfig saverConfig = SaverConfig.builder()
                .register(mysqlSaver)
                .build();

        // 使用检查点编译图 CompileConfig
        CompiledGraph workflow = stateGraph.compile(
                CompileConfig.builder()
                        .saverConfig(saverConfig)
                        .build()
        );

        // 执行工作流
        RunnableConfig runnableConfig = RunnableConfig.builder()
                .threadId("test-thread-1")
                .build();

        Map<String, Object> inputs = Map.of("input", "test1");
        OverAllState result = workflow.invoke(inputs, runnableConfig).orElseThrow();

        // 获取检查点历史
        List<StateSnapshot> history = (List<StateSnapshot>) workflow.getStateHistory(runnableConfig);

        System.out.println("检查点历史数量: " + history.size());

        // 获取最后保存的检查点
        StateSnapshot lastSnapshot = workflow.getState(runnableConfig);

        System.out.println("最后检查点节点: " + lastSnapshot.node());

    }

}
