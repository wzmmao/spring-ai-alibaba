/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cloud.ai.graph;

import com.alibaba.cloud.ai.graph.action.AsyncNodeActionWithConfig;
import com.alibaba.cloud.ai.graph.action.Command;
import com.alibaba.cloud.ai.graph.internal.edge.EdgeValue;
import com.alibaba.cloud.ai.graph.checkpoint.Checkpoint;
import com.alibaba.cloud.ai.graph.exception.RunnableErrors;
import com.alibaba.cloud.ai.graph.internal.node.ParallelNode;
import com.alibaba.cloud.ai.graph.internal.node.ResumableSubGraphAction;
import com.alibaba.cloud.ai.graph.state.StateSnapshot;
import com.alibaba.cloud.ai.graph.streaming.OutputType;
import com.alibaba.cloud.ai.graph.streaming.StreamingOutput;
import com.alibaba.cloud.ai.graph.utils.SystemClock;
import com.alibaba.cloud.ai.graph.utils.TypeRef;

import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.metadata.Usage;

import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.cloud.ai.graph.StateGraph.END;
import static com.alibaba.cloud.ai.graph.StateGraph.NODE_AFTER;
import static com.alibaba.cloud.ai.graph.StateGraph.NODE_BEFORE;
import static com.alibaba.cloud.ai.graph.StateGraph.START;
import static com.alibaba.cloud.ai.graph.StateGraph.ERROR;
import static java.lang.String.format;
import static java.util.Optional.ofNullable;

/**
 * Context class to manage the state during graph execution
 */
public class GraphRunnerContext {

	public static final String INTERRUPT_AFTER = "__INTERRUPTED__";

	private static final Logger log = LoggerFactory.getLogger(GraphRunner.class);

	final CompiledGraph compiledGraph;

	final AtomicInteger iteration = new AtomicInteger(0);

	OverAllState overallState;

	RunnableConfig config;

	String currentNodeId;

	String nextNodeId;

	Usage tokenUsage;

	String resumeFrom;

	ReturnFromEmbed returnFromEmbed;

	public GraphRunnerContext(OverAllState initialState, RunnableConfig config, CompiledGraph compiledGraph)
			throws Exception {
		this.compiledGraph = compiledGraph;
		this.config = config;

        // HUMAN_FEEDBACK key
		if (config.metadata(RunnableConfig.HUMAN_FEEDBACK_METADATA_KEY).isPresent() || config.checkPointId().isPresent()) {
			initializeFromResume(initialState, config);
		} else {
			initializeFromStart(initialState, config);
		}
	}

	private void initializeFromResume(OverAllState initialState, RunnableConfig config) {
		log.trace("RESUME REQUEST");

		var saver = compiledGraph.compileConfig.checkpointSaver()
				.orElseThrow(() -> new IllegalStateException("Resume request without a configured checkpoint saver!"));
		var checkpoint = saver.get(config)
				.orElseThrow(() -> new IllegalStateException("Resume request without a valid checkpoint!"));

		var startCheckpointNextNodeAction = compiledGraph.getNodeAction(checkpoint.getNextNodeId());
		if (startCheckpointNextNodeAction instanceof ResumableSubGraphAction resumableAction) {
			// RESUME FORM SUBGRAPH DETECTED
			this.config = RunnableConfig.builder(config)
					.checkPointId(null) // Reset checkpoint id
					.addMetadata(resumableAction.getResumeSubGraphId(), true) // add metadata for
					// sub graph
					.build();
			this.config.clearContext();
		} else {
			// Reset checkpoint id
			this.config = config.withCheckPointId(null);
		}

		this.currentNodeId = null;
		this.nextNodeId = checkpoint.getNextNodeId();
		this.overallState = initialState.input(checkpoint.getState());
		this.resumeFrom = checkpoint.getNodeId();

		log.trace("RESUME FROM {}", checkpoint.getNodeId());
	}

	private void initializeFromStart(OverAllState initialState, RunnableConfig config) {
		log.trace("START");

		Map<String, Object> inputs = initialState.data();
		if (!CollectionUtils.isEmpty(inputs)) {
			// Simple validation without accessing protected method
			log.debug("Initializing with inputs: {}", inputs.keySet());
		}

		// Use CompiledGraph's getInitialState method
		this.overallState = stateCreate(compiledGraph.getInitialState(inputs, config), initialState);
		this.currentNodeId = START;
		this.nextNodeId = null;
	}

	// FIXME, duplicated method with CompiledGraph.stateCreate, need to have a
	// unified way of when and how to do OverallState creation.
	// This temporary fix is to make sure the message provided by user is always the
	// last element in the messages list.
	private OverAllState stateCreate(Map<String, Object> inputs, OverAllState initialState) {
		// Creates a new OverAllState instance using key strategies from the graph and
		// provided input data.
		return OverAllStateBuilder.builder()
				.withKeyStrategies(initialState.keyStrategies())
				.withData(inputs)
				.withStore(initialState.getStore())
				.build();
	}

	// Helper methods
	public boolean shouldStop() {
		return nextNodeId == null && currentNodeId == null;
	}

	public boolean isMaxIterationsReached() {
		return iteration.incrementAndGet() > compiledGraph.getMaxIterations();
	}

	public boolean isStartNode() {
		return START.equals(currentNodeId);
	}

	public boolean isEndNode() {
		return END.equals(nextNodeId);
	}

	public boolean shouldInterrupt() {
		return shouldInterruptBefore(nextNodeId, currentNodeId) || shouldInterruptAfter(currentNodeId, nextNodeId);
	}

	private boolean shouldInterruptBefore(String nodeId, String previousNodeId) {
		if (previousNodeId == null)
			return false;
		return compiledGraph.compileConfig.interruptsBefore().contains(nodeId);
	}

	private boolean shouldInterruptAfter(String nodeId, String previousNodeId) {
		if (nodeId == null || Objects.equals(nodeId, previousNodeId))
			return false;
		return (compiledGraph.compileConfig.interruptBeforeEdge() && Objects.equals(nodeId, INTERRUPT_AFTER))
				|| compiledGraph.compileConfig.interruptsAfter().contains(nodeId);
	}

	// ================================================================================================================
	// Node Action Methods
	// ================================================================================================================

	public AsyncNodeActionWithConfig getNodeAction(String nodeId) {
		return compiledGraph.getNodeAction(nodeId);
	}

	public Command getEntryPoint() throws Exception {
		var entryPoint = compiledGraph.getEdge(START);
		return nextNodeId(entryPoint, overallState.data(), "entryPoint");
	}

	public Command nextNodeId(String nodeId, Map<String, Object> state) throws Exception {
		return nextNodeId(compiledGraph.getEdge(nodeId), state, nodeId);
	}

	private Command nextNodeId(EdgeValue route, Map<String, Object> state,
			String nodeId) throws Exception {
		if (route == null) {
			throw RunnableErrors.missingEdge.exception(nodeId);
		}
		if (route.id() != null) {
			return new Command(route.id(), state);
		}
		if (route.value() != null) {
			var edgeCondition = route.value();
			
			// Check if this is a multi-command action
			if (edgeCondition.isMultiCommand()) {
				// Multi-command action - route to ConditionalParallelNode
				// The ConditionalParallelNode is dynamically created in CompiledGraph
				String conditionalParallelNodeId = ParallelNode.formatNodeId(nodeId);
				// Return Command pointing to ConditionalParallelNode
				// The ConditionalParallelNode will handle the MultiCommand internally
				return new Command(conditionalParallelNodeId, state);
			} else {
				// Single Command action
				var singleAction = edgeCondition.singleAction();
				var command = singleAction.apply(this.overallState, config).get();
				
				// Single Command case
				var newRoute = command.gotoNode();
				String result = route.value().mappings().get(newRoute);
				if (result == null) {
					throw RunnableErrors.missingNodeInEdgeMapping.exception(nodeId, newRoute);
				}
				this.mergeIntoCurrentState(command.update());
				return new Command(result, state);
			}
		}
		throw RunnableErrors.executionError.exception(format("invalid edge value for nodeId: [%s] !", nodeId));
	}

	// ================================================================================================================
	// Checkpoint Methods
	// ================================================================================================================

	/**
	 * 添加一个新的检查点到检查点历史中。
	 *
	 * <p>此方法在节点执行完成后被调用，用于保存当前图的状态快照。
	 * 检查点包含以下关键信息：
	 * <ul>
	 *   <li><b>nodeId</b>：刚刚执行完成的节点 ID</li>
	 *   <li><b>state</b>：当前图的完整状态数据（深拷贝）</li>
	 *   <li><b>nextNodeId</b>：下一个将要执行的节点 ID</li>
	 * </ul>
	 *
	 * <p>检查点的作用：
	 * <ul>
	 *   <li><b>持久化</b>：将图的执行状态保存到存储介质</li>
	 *   <li><b>恢复执行</b>：从中断点恢复图的执行</li>
	 *   <li><b>时间旅行</b>：回溯到历史状态重新执行</li>
	 *   <li><b>调试审计</b>：记录完整的执行轨迹</li>
	 * </ul>
	 *
	 * <p>重要设计决策：
	 * 此方法<b>强制将 checkPointId 设置为 null</b>，确保总是追加新的检查点，
	 * 而不是替换现有的检查点。这样可以保留完整的执行历史，支持时间旅行功能。
	 *
	 * <p>调用时机：
	 * <ul>
	 *   <li>每个节点执行完成后（通过 {@link #buildNodeOutputAndAddCheckpoint}）</li>
	 *   <li>中断点处（保存中断前的状态）</li>
	 *   <li>关键节点处（用户显式创建快照）</li>
	 * </ul>
	 *
	 * @param nodeId 刚刚执行完成的节点 ID，用于标识检查点的位置
	 * @param nextNodeId 下一个将要执行的节点 ID，用于恢复执行时知道从哪里继续
	 * @return 包含新创建的检查点的 Optional，如果没有配置检查点保存器则返回 empty
	 * @throws Exception 如果检查点保存失败（如存储不可用、序列化错误等）
	 */
	public Optional<Checkpoint> addCheckpoint(String nodeId, String nextNodeId) throws Exception {
		// 1. 检查是否配置了检查点保存器
		// 如果没有配置，则不保存检查点（适用于不需要持久化的场景）
		if (compiledGraph.compileConfig.checkpointSaver().isPresent()) {

			// 2. 构建检查点对象
			// - nodeId: 当前执行完成的节点
			// - state: 克隆当前状态（深拷贝，避免后续修改影响检查点）
			// - nextNodeId: 下一个要执行的节点（用于恢复时继续执行）
			var cp = Checkpoint.builder()
					.nodeId(nodeId)
					.state(cloneState(overallState.data()))
					.nextNodeId(nextNodeId)
					.build();

			// 3. 强制将 checkPointId 设置为 null，确保追加新检查点
			// 这是一个关键设计决策：
			// - 如果保留 checkPointId，会替换现有检查点（更新模式）
			// - 设置为 null，会创建新检查点（插入模式）
			// - 追加模式保留完整历史，支持时间旅行和审计
			RunnableConfig appendConfig = RunnableConfig.builder(config)
					.checkPointId(null)
					.build();

			// 4. 调用检查点保存器保存检查点
			// put() 方法返回更新后的配置，包含新生成的 checkPointId
			this.config = compiledGraph.compileConfig.checkpointSaver().get().put(appendConfig, cp);

			// 5. 返回新创建的检查点
			return Optional.of(cp);
		}

		// 6. 如果没有配置检查点保存器，返回空 Optional
		return Optional.empty();
	}

	// ================================================================================================================
	// Output Building Methods
	// ================================================================================================================

	public NodeOutput buildOutput(String nodeId, Optional<Checkpoint> checkpoint) throws Exception {
		if (checkpoint.isPresent() && config.streamMode() == CompiledGraph.StreamMode.SNAPSHOTS) {
			return StateSnapshot.of(getKeyStrategyMap(), checkpoint.get(), config,
					compiledGraph.stateGraph.getStateSerializer().stateFactory());
		}
		return buildNodeOutput(nodeId);
	}

	public StreamingOutput<?> buildStreamingOutput(Message message, Object originData, String nodeId, boolean streaming) {
		// Create StreamingOutput with chunk and originData
		OutputType outputType = OutputType.from(streaming, nodeId);
		StreamingOutput<?> output = new StreamingOutput<>(message, originData, nodeId,
				(String) config.metadata("_AGENT_").orElse(""), this.overallState, outputType);
		output.setSubGraph(true);
		return output;
	}

	public StreamingOutput<?> buildStreamingOutput(Object originData, String nodeId, boolean streaming) {
		// Create StreamingOutput with chunk only
		OutputType outputType = OutputType.from(streaming, nodeId);
		StreamingOutput<?> output = new StreamingOutput<>(originData, nodeId, (String) config.metadata("_AGENT_").orElse(""),
				this.overallState, outputType);
		output.setSubGraph(true);
		return output;
	}

	// Normal NodeOutput builders for nodes with normal message output.

	public NodeOutput buildNodeOutput(String nodeId) throws Exception {
		return NodeOutput.of(
				nodeId,
				(String) config.metadata("_AGENT_").orElse(""),
				cloneState(this.overallState.data()),
				this.tokenUsage);
	}

	public OverAllState cloneState(Map<String, Object> data) throws Exception {
		return compiledGraph.cloneState(data);
	}

	// ================================================================================================================
	// Lifecycle Methods
	// ================================================================================================================

	public void doListeners(String scene, Exception e) {
		for (GraphLifecycleListener listener : compiledGraph.compileConfig.lifecycleListeners()) {
			try {
				switch (scene) {
					case START:
						listener.onStart(getCurrentNodeId(), getCurrentStateData(), config);
						break;
					case END:
						listener.onComplete(END, getCurrentStateData(), config);
						break;
					case NODE_BEFORE:
						listener.before(getCurrentNodeId(), getCurrentStateData(), config, SystemClock.now());
						break;
					case NODE_AFTER:
						listener.after(getCurrentNodeId(), getCurrentStateData(), config, SystemClock.now());
						break;
					case ERROR:
						listener.onError(getCurrentNodeId(), getCurrentStateData(), e, config);
						break;
				}
			} catch (Exception ex) {
				log.error("Error in listener", ex);
			}
		}
	}

	/**
	 * This method updates both the current state data and the overall state.
	 *
	 * @param updateState the state updates to apply
	 */
	public void mergeIntoCurrentState(Map<String, Object> updateState) {
		// Create a new map and filter out ChatResponse entries
		Map<String, Object> filteredState = findTokenUsageInDeltaState(updateState);

		this.overallState.updateState(filteredState);
	}

	/**
	 * FIXME, this method is a temporary fix to separate Usage from state updates.
	 * works together with AgentLlmNode non-stream node.
	 */
	private Map<String, Object> findTokenUsageInDeltaState(Map<String, Object> updateState) {
		Map<String, Object> filteredState = new HashMap<>();
		for (Map.Entry<String, Object> entry : updateState.entrySet()) {
			Object value = entry.getValue();
			if (value instanceof Usage && entry.getKey().equals("_TOKEN_USAGE_")) {
				// Assign ChatResponse to this.chatResponse
				this.tokenUsage = (Usage) value;
			} else {
				// Add non-ChatResponse entries to the filtered map
				filteredState.put(entry.getKey(), value);
			}
		}
		return filteredState;
	}

	// ================================================================================================================
	// Getter and Setter Methods
	// ================================================================================================================

	public String getCurrentNodeId() {
		return currentNodeId;
	}

	public void setCurrentNodeId(String nodeId) {
		this.currentNodeId = nodeId;
	}

	public String getNextNodeId() {
		return nextNodeId;
	}

	public void setNextNodeId(String nodeId) {
		this.nextNodeId = nodeId;
	}

	public Map<String, Object> getCurrentStateData() {
		return overallState.data();
	}

	public OverAllState getOverallState() {
		return overallState;
	}

	public void setOverallState(OverAllState state) {
		this.overallState = state;
	}

	public Map<String, KeyStrategy> getKeyStrategyMap() {
		return compiledGraph.getKeyStrategyMap();
	}

	public CompiledGraph getCompiledGraph() {
		return compiledGraph;
	}

	public RunnableConfig getConfig() {
		return config;
	}

	public void setConfig(RunnableConfig config) {
		this.config = config;
	}

	public String getResumeFrom() {
		return resumeFrom;
	}

	public void setResumeFrom(String resumeFrom) {
		this.resumeFrom = resumeFrom;
	}

	public Optional<String> getResumeFromAndReset() {
		final var result = ofNullable(resumeFrom);
		resumeFrom = null;
		return result;
	}

	public Optional<ReturnFromEmbed> getReturnFromEmbedAndReset() {
		var result = ofNullable(returnFromEmbed);
		returnFromEmbed = null;
		return result;
	}

	public void setReturnFromEmbedWithValue(Object value) {
		returnFromEmbed = new ReturnFromEmbed(value);
	}

	public record ReturnFromEmbed(Object value) {
		public <T> Optional<T> value(TypeRef<T> ref) {
			return ofNullable(value).flatMap(ref::cast);
		}
	}

	/**
	 * FIXME
	 * Below are duplicated methods. Need to have a unified way of streaming output
	 * to end user.
	 */
	public NodeOutput buildNodeOutputAndAddCheckpoint(Map<String, Object> updateStates) throws Exception {
		Optional<Checkpoint> cp = addCheckpoint(currentNodeId, nextNodeId);
		return buildOutput(currentNodeId, updateStates, cp, false);
	}

	public NodeOutput buildOutput(String nodeId, Map<String, Object> updateStates, Optional<Checkpoint> checkpoint, boolean streaming)
			throws Exception {
		if (checkpoint.isPresent() && config.streamMode() == CompiledGraph.StreamMode.SNAPSHOTS) {
			return StateSnapshot.of(getKeyStrategyMap(), checkpoint.get(), config,
					compiledGraph.stateGraph.getStateSerializer().stateFactory());
		}
		return buildNodeOutput(nodeId, updateStates, streaming);
	}

	/**
	 * 构建节点输出对象。
	 *
	 * <p>此方法根据节点的状态更新数据构建一个 {@link NodeOutput} 对象，
	 * 用于在图执行流程中传递节点的执行结果。输出对象包含节点 ID、消息、
	 * 状态快照、Token 使用情况等信息。
	 *
	 * <p>核心功能：
	 * <ul>
	 *   <li><b>消息提取</b>：从状态更新中提取最新的消息（如 LLM 响应）</li>
	 *   <li><b>输出类型确定</b>：根据是否流式和节点 ID 确定输出类型</li>
	 *   <li><b>状态快照</b>：克隆当前状态，避免后续修改影响输出</li>
	 *   <li><b>元数据附加</b>：附加 Agent 名称、Token 使用等元数据</li>
	 * </ul>
	 *
	 * <h2>消息提取逻辑</h2>
	 *
	 * <p>方法会尝试从 {@code updateStates} 中提取 "messages" 字段的最新消息：
	 *
	 * <h3>场景 1：messages 是消息列表</h3>
	 * <pre>{@code
	 * updateStates = {
	 *     "messages": [
	 *         new UserMessage("你好"),
	 *         new AssistantMessage("你好！有什么可以帮助你的？")  // ← 提取这个
	 *     ]
	 * }
	 * }</pre>
	 * <p>提取列表中的<b>最后一条消息</b>（通常是最新的 LLM 响应）
	 *
	 * <h3>场景 2：messages 是单个消息</h3>
	 * <pre>{@code
	 * updateStates = {
	 *     "messages": new AssistantMessage("你好！")  // ← 直接使用
	 * }
	 * }</pre>
	 * <p>直接使用这个消息对象
	 *
	 * <h3>场景 3：没有 messages 字段</h3>
	 * <pre>{@code
	 * updateStates = {
	 *     "result": "some data",
	 *     "count": 42
	 * }
	 * }</pre>
	 * <p>message 为 null，输出对象不包含消息内容
	 *
	 * <h2>输出类型（OutputType）</h2>
	 *
	 * <p>输出类型由两个因素决定：
	 * <ul>
	 *   <li><b>streaming</b>：是否是流式输出
	 *       <ul>
	 *         <li>true：流式输出（如 LLM 流式响应的中间块）</li>
	 *         <li>false：完整输出（如节点执行完成后的最终结果）</li>
	 *       </ul>
	 *   </li>
	 *   <li><b>nodeId</b>：节点 ID，用于标识输出来源</li>
	 * </ul>
	 *
	 * <h2>StreamingOutput 构造</h2>
	 *
	 * <p>根据是否提取到消息，使用不同的构造器：
	 *
	 * <h3>包含消息的输出</h3>
	 * <pre>{@code
	 * new StreamingOutput<>(
	 *     message,           // 提取的消息对象（如 AssistantMessage）
	 *     nodeId,            // 节点 ID（如 "llm_node"）
	 *     agentName,         // Agent 名称（从配置元数据中获取）
	 *     stateSnapshot,     // 当前状态的克隆快照
	 *     tokenUsage,        // Token 使用统计
	 *     outputType         // 输出类型（流式/完整）
	 * )
	 * }</pre>
	 *
	 * <h3>不包含消息的输出</h3>
	 * <pre>{@code
	 * new StreamingOutput<>(
	 *     nodeId,            // 节点 ID
	 *     agentName,         // Agent 名称
	 *     stateSnapshot,     // 状态快照
	 *     tokenUsage,        // Token 使用统计
	 *     outputType         // 输出类型
	 * )
	 * }</pre>
	 *
	 * <h2>使用场景</h2>
	 *
	 * <ul>
	 *   <li><b>流式 LLM 响应</b>：
	 *       <ul>
	 *         <li>streaming = true</li>
	 *         <li>每个流式块都会调用此方法构建输出</li>
	 *         <li>message 包含当前块的内容</li>
	 *       </ul>
	 *   </li>
	 *   <li><b>节点执行完成</b>：
	 *       <ul>
	 *         <li>streaming = false</li>
	 *         <li>构建节点的最终输出</li>
	 *         <li>message 包含完整的响应</li>
	 *       </ul>
	 *   </li>
	 *   <li><b>非消息节点</b>：
	 *       <ul>
	 *         <li>节点不产生消息（如数据处理节点）</li>
	 *         <li>message = null</li>
	 *         <li>输出仅包含状态快照</li>
	 *       </ul>
	 *   </li>
	 * </ul>
	 *
	 * @param nodeId 节点 ID，标识输出来源的节点
	 * @param updateStates 节点的状态更新数据，可能包含 "messages" 字段
	 * @param streaming 是否是流式输出
	 *                  - true: 流式输出（LLM 流式响应的中间块）
	 *                  - false: 完整输出（节点执行完成后的最终结果）
	 * @return 构建的节点输出对象，包含消息、状态快照、元数据等信息
	 * @throws Exception 如果状态克隆失败或构建输出时发生错误
	 */
	public NodeOutput buildNodeOutput(String nodeId, Map<String, Object> updateStates, boolean streaming) throws Exception {
		// 用于存储提取的消息对象
		Message message = null;

		// 1. 尝试从状态更新中提取消息
		// 只有当 updateStates 不为空时才进行提取
		if (updateStates != null && !updateStates.isEmpty()) {
			// 1.1 获取 "messages" 字段的值
			// 这个字段通常由 LLM 节点或消息处理节点设置
			Object messagesObj = updateStates.get("messages");

			// 1.2 检查 messages 是否是消息列表
			if (messagesObj instanceof List<?> messagesList && !messagesList.isEmpty()) {
				// 场景 1：messages 是一个非空列表
				// 例如：[UserMessage, AssistantMessage, ToolMessage]

				// 获取列表中的最后一个元素（最新的消息）
				// 在对话场景中，最后一条消息通常是最新的 LLM 响应
				Object lastElement = messagesList.get(messagesList.size() - 1);

				// 检查最后一个元素是否是 Message 类型
				if (lastElement instanceof Message) {
					message = (Message) lastElement;
				}
			} else if (messagesObj instanceof Message singleMessage) {
				// 场景 2：messages 是单个 Message 对象
				// 例如：messages = new AssistantMessage("你好")
				// 直接使用这个消息
				message = singleMessage;
			}
			// 场景 3：messages 不存在或不是 Message 类型
			// message 保持为 null
		}

		// 2. 确定输出类型
		// OutputType 根据是否流式和节点 ID 来确定
		// 例如：
		// - streaming=true, nodeId="llm" → STREAMING
		// - streaming=false, nodeId="llm" → COMPLETE
		OutputType outputType = OutputType.from(streaming, nodeId);

		// 3. 构建并返回 StreamingOutput 对象
		if (message != null) {
			// 3.1 包含消息的输出（适用于 LLM 节点、消息处理节点等）
			return new StreamingOutput<>(
					message,                                              // 提取的消息对象
					nodeId,                                               // 节点 ID
					(String) config.metadata("_AGENT_").orElse(""),      // Agent 名称（从配置元数据获取）
					cloneState(this.overallState.data()),                // 当前状态的深拷贝快照
					tokenUsage,                                           // Token 使用统计
					outputType                                            // 输出类型
			);
		} else {
			// 3.2 不包含消息的输出（适用于数据处理节点、工具节点等）
			return new StreamingOutput<>(
					nodeId,                                               // 节点 ID
					(String) config.metadata("_AGENT_").orElse(""),      // Agent 名称
					cloneState(this.overallState.data()),                // 状态快照
					tokenUsage,                                           // Token 使用统计
					outputType                                            // 输出类型
			);
		}
	}

}
