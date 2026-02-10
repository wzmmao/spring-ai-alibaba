package com.alibaba.cloud.ai.graph.milo;

import com.alibaba.cloud.ai.graph.CompileConfig;
import com.alibaba.cloud.ai.graph.CompiledGraph;
import com.alibaba.cloud.ai.graph.KeyStrategy;
import com.alibaba.cloud.ai.graph.KeyStrategyFactory;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.RunnableConfig;
import com.alibaba.cloud.ai.graph.StateGraph;
import com.alibaba.cloud.ai.graph.checkpoint.config.SaverConfig;
import com.alibaba.cloud.ai.graph.checkpoint.savers.MemorySaver;
import com.alibaba.cloud.ai.graph.exception.GraphStateException;
import com.alibaba.cloud.ai.graph.state.strategy.AppendStrategy;
import com.alibaba.cloud.ai.graph.state.strategy.ReplaceStrategy;
import com.alibaba.cloud.ai.graph.store.Store;
import com.alibaba.cloud.ai.graph.store.StoreItem;
import com.alibaba.cloud.ai.graph.store.stores.MemoryStore;
import org.junit.Test;
import org.springframework.ai.chat.client.ChatClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.cloud.ai.graph.StateGraph.END;
import static com.alibaba.cloud.ai.graph.StateGraph.START;
import static com.alibaba.cloud.ai.graph.action.AsyncNodeAction.node_async;

/**
 * @author milo
 * @since 2026/2/4
 */
public class StoreTest {

    @Test
    public void test03LongSave() throws GraphStateException {
        // 在节点中使用 Store 存储用户信息
        var userProfileNode = node_async((state) -> {
            String userId = (String) state.value("userId").orElse("");

            if (userId.isEmpty()) {
                return Map.of("userProfile", Map.of("name", "Unknown", "preferences", "default"));
            }

            // 从 Store 获取用户配置
            Store store = state.getStore();
            if (store != null) {
                Optional<StoreItem> itemOpt = store.getItem(List.of("user_profiles"), userId);
                if (itemOpt.isPresent()) {
                    Map<String, Object> userProfile = itemOpt.get().getValue();
                    return Map.of("userProfile", userProfile);
                }
            }

            // 如果未找到，返回默认值
            Map<String, Object> userProfile = Map.of("name", "User", "preferences", "default");
            return Map.of("userProfile", userProfile);
        });

// 创建图
        KeyStrategyFactory keyStrategyFactory = () -> {
            Map<String, KeyStrategy> keyStrategyMap = new HashMap<>();
            keyStrategyMap.put("userId", new ReplaceStrategy());
            keyStrategyMap.put("userProfile", new ReplaceStrategy());
            return keyStrategyMap;
        };

        StateGraph stateGraph = new StateGraph(keyStrategyFactory)
                .addNode("load_profile", userProfileNode)
                .addEdge(START, "load_profile")
                .addEdge("load_profile", END);

        CompiledGraph graph = stateGraph.compile(CompileConfig.builder().build());

// 创建长期记忆存储并预填充数据
        MemoryStore memoryStore = new MemoryStore();
        Map<String, Object> profileData = new HashMap<>();
        profileData.put("name", "张三");
        profileData.put("preferences", "喜欢编程");
        StoreItem profileItem = StoreItem.of(List.of("user_profiles"), "user_001", profileData);
        memoryStore.putItem(profileItem);

// 运行图
        RunnableConfig config = RunnableConfig.builder()
                .threadId("profile_thread")
                .store(memoryStore)
                .build();

        Optional<OverAllState> stateOptional = graph.invoke(Map.of("userId", "user_001"), config);
        Map<String, Object> result = stateOptional.get().data();
        System.out.println("加载的用户配置: " + result.get("userProfile"));
    }


    @Test
    public void test04ShortAndLongSaver() throws GraphStateException {
        // 定义状态
        KeyStrategyFactory keyStrategyFactory = () -> {
            Map<String, KeyStrategy> keyStrategyMap = new HashMap<>();
            keyStrategyMap.put("userId", new ReplaceStrategy());
            keyStrategyMap.put("messages", new AppendStrategy());
            keyStrategyMap.put("userPreferences", new ReplaceStrategy());
            return keyStrategyMap;
        };

// 加载用户偏好（长期内存）
        var loadUserPreferences = node_async((state) -> {
            String userId = (String) state.value("userId").orElse("");

            if (userId.isEmpty()) {
                return Map.of("userPreferences", Map.of("theme", "default", "language", "zh"));
            }

            // 从 Store 加载用户偏好
            Store store = state.getStore();
            if (store != null) {
                Optional<StoreItem> itemOpt = store.getItem(List.of("user_preferences"), userId);
                if (itemOpt.isPresent()) {
                    Map<String, Object> preferences = itemOpt.get().getValue();
                    return Map.of("userPreferences", preferences);
                }
            }

            // 如果未找到，返回默认偏好
            Map<String, Object> preferences = Map.of("theme", "dark", "language", "zh");
            return Map.of("userPreferences", preferences);
        });

// 聊天节点（使用短期和长期内存）
        var chatNode = node_async(state -> {
            List<Map<String, String>> messages =
                    (List<Map<String, String>>) state.value("messages").orElse(List.of());
            Map<String, Object> preferences =
                    (Map<String, Object>) state.value("userPreferences").orElse(Map.of());

            // 构建包含用户偏好的提示
            String userPrompt = messages.get(messages.size() - 1).get("content");
            String enhancedPrompt = "用户偏好: " + preferences + " 用户问题: " + userPrompt;

            // 调用 AI
            ChatClient.Builder chatClientBuilder = null; // to do
            ChatClient chatClient = chatClientBuilder.build();
            String response = chatClient.prompt()
                    .user(enhancedPrompt)
                    .call()
                    .content();

            return Map.of("messages", List.of(
                    Map.of("role", "assistant", "content", response)
            ));
        });

// 构建图
        StateGraph stateGraph = new StateGraph(keyStrategyFactory)
                .addNode("load_preferences", loadUserPreferences)
                .addNode("chat", chatNode)
                .addEdge(START, "load_preferences")
                .addEdge("load_preferences", "chat")
                .addEdge("chat", END);

// 配置检查点（短期内存）
        SaverConfig saverConfig = SaverConfig.builder()
                .register(new MemorySaver())
                .build();

// 编译图
        CompiledGraph graph = stateGraph.compile(
                CompileConfig.builder()
                        .saverConfig(saverConfig)
                        .build()
        );

// 创建长期记忆存储并预填充用户偏好
        MemoryStore memoryStore = new MemoryStore();
        Map<String, Object> preferencesData = new HashMap<>();
        preferencesData.put("theme", "dark");
        preferencesData.put("language", "zh");
        preferencesData.put("timezone", "Asia/Shanghai");
        StoreItem preferencesItem = StoreItem.of(List.of("user_preferences"), "user_002", preferencesData);
        memoryStore.putItem(preferencesItem);

// 运行图
        RunnableConfig config = RunnableConfig.builder()
                .threadId("combined_thread")
                .store(memoryStore)
                .build();

// 第一轮对话（加载偏好并开始对话）
        graph.invoke(Map.of(
                "userId", "user_002",
                "messages", List.of(Map.of("role", "user", "content", "你好"))
        ), config);

// 第二轮对话（使用短期和长期记忆）
        graph.invoke(Map.of(
                "userId", "user_002",
                "messages", List.of(Map.of("role", "user", "content", "根据我的偏好给我一些建议"))
        ), config);
    }
}
