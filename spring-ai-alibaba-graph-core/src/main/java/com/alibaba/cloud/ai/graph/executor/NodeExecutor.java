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
package com.alibaba.cloud.ai.graph.executor;

import com.alibaba.cloud.ai.graph.GraphResponse;
import com.alibaba.cloud.ai.graph.GraphRunnerContext;
import com.alibaba.cloud.ai.graph.NodeOutput;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.RunnableConfig;
import com.alibaba.cloud.ai.graph.action.AsyncNodeActionWithConfig;
import com.alibaba.cloud.ai.graph.action.Command;
import com.alibaba.cloud.ai.graph.action.InterruptableAction;
import com.alibaba.cloud.ai.graph.action.InterruptionMetadata;
import com.alibaba.cloud.ai.graph.exception.RunnableErrors;
import com.alibaba.cloud.ai.graph.streaming.GraphFlux;
import com.alibaba.cloud.ai.graph.streaming.ParallelGraphFlux;
import com.alibaba.cloud.ai.graph.streaming.StreamingOutput;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.AssistantMessage.ToolCall;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.Executor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.alibaba.cloud.ai.graph.GraphRunnerContext.INTERRUPT_AFTER;
import static com.alibaba.cloud.ai.graph.StateGraph.*;
import static com.alibaba.cloud.ai.graph.internal.node.ParallelNode.getExecutor;

/**
 * 节点执行器 - 负责普通节点的执行和结果处理。
 *
 * <p>这是图执行引擎的节点级别执行器，负责处理普通业务节点的执行细节。
 * 它处理节点动作的调用、中断钩子、流式响应、状态更新等复杂逻辑，
 * 并在执行完成后递归调用 {@link MainGraphExecutor} 继续执行下一个节点。
 *
 * <h2>在三层执行器架构中的定位</h2>
 *
 * <p>NodeExecutor 是三层执行器架构的<b>节点执行层</b>：
 * <ul>
 *   <li>继承自 {@link BaseGraphExecutor}，获得公共基础能力</li>
 *   <li>被 {@link MainGraphExecutor} 组合，接收节点执行委托</li>
 *   <li>反向持有 {@link MainGraphExecutor} 引用，递归调用继续执行</li>
 * </ul>
 *
 * <h2>核心职责</h2>
 *
 * <h3>1. 节点动作执行（{@link #executeNode}）</h3>
 * <ul>
 *   <li><b>获取节点动作</b>：
 *       <ul>
 *         <li>从上下文中获取当前节点的 {@link AsyncNodeActionWithConfig}</li>
 *         <li>如果节点不存在，返回错误响应</li>
 *       </ul>
 *   </li>
 *   <li><b>执行前中断检查</b>：
 *       <ul>
 *         <li>如果节点实现了 {@link InterruptableAction}</li>
 *         <li>调用 {@code interrupt()} 方法检查是否需要在执行前中断</li>
 *         <li>支持人工审核节点输入、修改状态等场景</li>
 *       </ul>
 *   </li>
 *   <li><b>触发监听器</b>：
 *       <ul>
 *         <li>执行前触发 NODE_BEFORE 事件</li>
 *         <li>用于观测、日志、指标收集等</li>
 *       </ul>
 *   </li>
 *   <li><b>异步执行</b>：
 *       <ul>
 *         <li>调用 {@code action.apply(state, config)}</li>
 *         <li>返回 {@code CompletableFuture<Map<String, Object>>}</li>
 *         <li>支持异步操作（如调用外部 API、数据库查询）</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h3>2. 执行结果处理（{@link #handleActionResult}）</h3>
 * <ul>
 *   <li><b>流式响应处理</b>：
 *       <ul>
 *         <li><b>Flux</b>：原始的 Reactor Flux，通常来自 LLM 流式输出</li>
 *         <li><b>GraphFlux</b>：包装的图流，支持自定义流式处理逻辑</li>
 *         <li><b>ParallelGraphFlux</b>：并行图流，同时执行多个流</li>
 *         <li>详见 {@link #handleEmbeddedFlux}、{@link #handleGraphFlux}、{@link #handleParallelGraphFlux}</li>
 *       </ul>
 *   </li>
 *   <li><b>执行后中断检查</b>：
 *       <ul>
 *         <li>调用 {@code interruptAfter()} 方法</li>
 *         <li>在状态合并前检查，允许人工审核节点输出</li>
 *         <li>支持人工修改输出、决定是否继续等场景</li>
 *       </ul>
 *   </li>
 *   <li><b>状态更新</b>：
 *       <ul>
 *         <li>将节点返回的状态更新合并到图的整体状态</li>
 *         <li>使用 {@code context.mergeIntoCurrentState(updateState)}</li>
 *       </ul>
 *   </li>
 *   <li><b>下一个节点确定</b>：
 *       <ul>
 *         <li>调用 {@code context.nextNodeId()} 根据路由逻辑确定下一个节点</li>
 *         <li>支持条件路由、动态路由等</li>
 *       </ul>
 *   </li>
 *   <li><b>检查点创建</b>：
 *       <ul>
 *         <li>调用 {@code context.buildNodeOutputAndAddCheckpoint()}</li>
 *         <li>保存当前状态快照，支持恢复和时间旅行</li>
 *       </ul>
 *   </li>
 *   <li><b>触发监听器</b>：
 *       <ul>
 *         <li>执行后触发 NODE_AFTER 事件</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h3>3. 流式响应处理</h3>
 *
 * <h4>3.1 嵌入 Flux 处理（{@link #handleEmbeddedFlux}）</h4>
 * <ul>
 *   <li>处理节点返回的原始 Flux（如 LLM 流式输出）</li>
 *   <li>将 Flux 元素转换为 GraphResponse</li>
 *   <li>聚合流式数据（如聚合 ChatResponse）</li>
 *   <li>在流结束后合并状态并继续执行</li>
 * </ul>
 *
 * <h4>3.2 GraphFlux 处理（{@link #handleGraphFlux}）</h4>
 * <ul>
 *   <li>处理包装的图流，支持自定义流式逻辑</li>
 *   <li>保留节点 ID，用于正确的观测和调试</li>
 *   <li>支持自定义结果映射函数</li>
 * </ul>
 *
 * <h4>3.3 ParallelGraphFlux 处理（{@link #handleParallelGraphFlux}）</h4>
 * <ul>
 *   <li>并行执行多个 GraphFlux</li>
 *   <li>使用 Reactor 的 Scheduler 实现真正的并行</li>
 *   <li>合并所有并行流的结果</li>
 *   <li>适用于并行调用多个 LLM、并行处理多个任务等场景</li>
 * </ul>
 *
 * <h3>4. 中断钩子处理</h3>
 *
 * <p>NodeExecutor 支持两种中断钩子，实现人工干预（Human-in-the-Loop）：
 *
 * <h4>4.1 执行前中断（interrupt）</h4>
 * <ul>
 *   <li><b>时机</b>：在节点动作执行前</li>
 *   <li><b>用途</b>：审核输入、修改状态、决定是否执行</li>
 *   <li><b>实现</b>：在 {@link #executeNode} 中调用</li>
 * </ul>
 *
 * <h4>4.2 执行后中断（interruptAfter）</h4>
 * <ul>
 *   <li><b>时机</b>：在节点动作执行后、状态合并前</li>
 *   <li><b>用途</b>：审核输出、修改结果、决定是否继续</li>
 *   <li><b>实现</b>：在 {@link #handleActionResult} 中调用</li>
 *   <li><b>特殊处理</b>：流式节点使用 {@link #interruptAfterForStreaming}</li>
 * </ul>
 *
 * <h2>与 MainGraphExecutor 的协作</h2>
 *
 * <p>NodeExecutor 和 MainGraphExecutor 通过<b>相互组合</b>实现递归执行：
 *
 * <pre>{@code
 * // MainGraphExecutor 委托给 NodeExecutor
 * public class MainGraphExecutor {
 *     private final NodeExecutor nodeExecutor;
 *
 *     public Flux<GraphResponse<NodeOutput>> execute(...) {
 *         if (普通节点) {
 *             return nodeExecutor.execute(context, resultValue);  // 委托
 *         }
 *     }
 * }
 *
 * // NodeExecutor 递归回 MainGraphExecutor
 * public class NodeExecutor {
 *     private final MainGraphExecutor mainGraphExecutor;
 *
 *     private Flux<GraphResponse<NodeOutput>> handleActionResult(...) {
 *         // 执行节点逻辑...
 *         return Flux.just(output)
 *             .concatWith(mainGraphExecutor.execute(context, resultValue));  // 递归
 *     }
 * }
 * }</pre>
 *
 * <h2>执行流程示例</h2>
 *
 * <pre>{@code
 * // MainGraphExecutor 委托给 NodeExecutor
 * MainGraphExecutor.execute()
 *   └─ nodeExecutor.execute(context, resultValue)
 *        ↓
 *      NodeExecutor.execute()
 *        └─ executeNode(context, resultValue)
 *             ├─ 1. 获取节点动作
 *             ├─ 2. 检查执行前中断 (interrupt)
 *             │    └─ 如果需要中断，返回中断元数据
 *             ├─ 3. 触发 NODE_BEFORE 监听器
 *             ├─ 4. 执行节点动作 action.apply()
 *             └─ 5. 处理执行结果
 *                  └─ handleActionResult(context, updateState, resultValue)
 *                       ├─ 检查是否是流式响应
 *                       │    ├─ Flux → handleEmbeddedFlux()
 *                       │    ├─ GraphFlux → handleGraphFlux()
 *                       │    └─ ParallelGraphFlux → handleParallelGraphFlux()
 *                       ├─ 检查执行后中断 (interruptAfter)
 *                       │    └─ 如果需要中断，返回中断元数据
 *                       ├─ 合并状态 mergeIntoCurrentState()
 *                       ├─ 确定下一个节点 nextNodeId()
 *                       ├─ 创建检查点 buildNodeOutputAndAddCheckpoint()
 *                       ├─ 触发 NODE_AFTER 监听器
 *                       └─ 递归调用 mainGraphExecutor.execute()
 *                            ↓
 *                          MainGraphExecutor.execute() [继续执行下一个节点]
 * }</pre>
 *
 * <h2>为什么需要 NodeExecutor？</h2>
 *
 * <ol>
 *   <li><b>关注点分离</b>：
 *       <ul>
 *         <li>NodeExecutor 专注于"节点如何执行"（How）</li>
 *         <li>MainGraphExecutor 专注于"图应该执行什么"（What）</li>
 *       </ul>
 *   </li>
 *   <li><b>复杂度管理</b>：
 *       <ul>
 *         <li>节点执行逻辑复杂（动作执行、中断、流式处理）约 600 行</li>
 *         <li>独立成类后职责清晰，易于理解和维护</li>
 *       </ul>
 *   </li>
 *   <li><b>流式处理专业化</b>：
 *       <ul>
 *         <li>处理 Flux、GraphFlux、ParallelGraphFlux 需要大量代码</li>
 *         <li>集中在 NodeExecutor 中，避免污染 MainGraphExecutor</li>
 *       </ul>
 *   </li>
 *   <li><b>扩展性</b>：
 *       <ul>
 *         <li>可以独立扩展节点执行逻辑（如支持新的流式类型）</li>
 *         <li>可以通过继承提供不同的节点执行策略</li>
 *       </ul>
 *   </li>
 * </ol>
 *
 * @see BaseGraphExecutor 执行器基类，提供公共能力
 * @see MainGraphExecutor 主图执行器，处理图级别控制
 * @see InterruptableAction 可中断动作接口，支持人工干预
 */
public class NodeExecutor extends BaseGraphExecutor {

	private static final Logger log = LoggerFactory.getLogger(NodeExecutor.class);

	private final MainGraphExecutor mainGraphExecutor;

	public NodeExecutor(MainGraphExecutor mainGraphExecutor) {
		this.mainGraphExecutor = mainGraphExecutor;
	}

	/**
	 * Implementation of the execute method. This demonstrates polymorphism as it provides
	 * a specific implementation for node execution.
	 * @param context the graph runner context
	 * @param resultValue the atomic reference to store the result value
	 * @return Flux of GraphResponse with execution result
	 */
	@Override
	public Flux<GraphResponse<NodeOutput>> execute(GraphRunnerContext context, AtomicReference<Object> resultValue) {
		return executeNode(context, resultValue);
	}

	/**
	 * 执行节点并处理其结果。
	 * 这是节点执行的核心方法，负责：
	 * 1. 设置当前节点ID并获取节点动作
	 * 2. 处理可中断动作的中断逻辑（执行前中断）
	 * 3. 触发节点执行前的监听器
	 * 4. 异步执行节点动作
	 * 5. 处理执行结果或错误
	 *
	 * @param context 图运行上下文，包含图的状态和配置信息
	 * @param resultValue 原子引用，用于存储节点执行的结果值
	 * @return 包含节点执行结果的 Flux 流
	 */
	private Flux<GraphResponse<NodeOutput>> executeNode(GraphRunnerContext context,
			AtomicReference<Object> resultValue) {
		try {
			// 1. 将下一个要执行的节点ID设置为当前节点ID
			context.setCurrentNodeId(context.getNextNodeId());
			String currentNodeId = context.getCurrentNodeId();

			// 2. 获取当前节点对应的动作（Action）
			AsyncNodeActionWithConfig action = context.getNodeAction(currentNodeId);

			// 3. 如果节点动作不存在，返回错误响应
			if (action == null) {
				return Flux.just(GraphResponse.error(RunnableErrors.missingNode.exception(currentNodeId)));
			}

			// 4. 处理可中断动作（InterruptableAction）
			// 这是执行前的中断检查，允许在节点执行前进行人工干预
			if (action instanceof InterruptableAction) {
				// 4.1 从配置中获取状态更新元数据（通常来自人工反馈）
				context.getConfig().metadata(RunnableConfig.STATE_UPDATE_METADATA_KEY).ifPresent(updateFromFeedback -> {
					if (updateFromFeedback instanceof Map<?, ?>) {
						// 将反馈数据合并到当前状态中
						context.mergeIntoCurrentState((Map<String, Object>) updateFromFeedback);
					} else {
						throw new RuntimeException();
					}
				});

				// 4.2 调用 interrupt 方法检查是否需要在执行前中断
				Optional<InterruptionMetadata> interruptMetadata = ((InterruptableAction) action)
					.interrupt(currentNodeId, context.cloneState(context.getCurrentStateData()), context.getConfig());

				// 4.3 如果需要中断，保存中断元数据并返回
				if (interruptMetadata.isPresent()) {
					resultValue.set(interruptMetadata.get());
					return Flux.just(GraphResponse.done(interruptMetadata.get()));
				}
			}

			// 5. 触发节点执行前的监听器（用于观测、日志等）
			context.doListeners(NODE_BEFORE, null);

			// 6. 异步执行节点动作
			// action.apply() 返回一个 CompletableFuture，包含节点执行后的状态更新
			CompletableFuture<Map<String, Object>> future = action.apply(context.getOverallState(),
					context.getConfig());

			// 7. 将 CompletableFuture 转换为 Mono，然后处理结果
			return Mono.fromFuture(future)
					// 7.1 当节点执行完成后，处理动作结果
					.flatMapMany(updateState -> handleActionResult(context, updateState, resultValue))
					// 7.2 如果执行过程中发生错误，触发错误监听器并返回错误响应
					.onErrorResume(error -> {
						context.doListeners(ERROR, new Exception(error));
						return Flux.just(GraphResponse.error(error));
					});

		}
		catch (Exception e) {
			// 8. 捕获同步异常并返回错误响应
			return Flux.just(GraphResponse.error(e));
		}
	}

	/**
	 * 处理节点动作执行后的结果并返回相应的响应。
	 *
	 * <p>这是节点执行流程中的关键方法，负责处理节点动作返回的状态更新。
	 * 该方法会根据返回结果的类型采取不同的处理策略：
	 *
	 * <p>处理流程：
	 * <ol>
	 *   <li>检查是否包含流式响应（Flux）- 用于处理 LLM 流式输出</li>
	 *   <li>检查是否包含并行图流（ParallelGraphFlux）- 用于并行执行多个子流</li>
	 *   <li>检查是否包含图流（GraphFlux）- 用于向后兼容</li>
	 *   <li>检查执行后中断（interruptAfter）- 允许在状态合并前进行人工干预</li>
	 *   <li>合并状态更新到当前状态</li>
	 *   <li>确定下一个要执行的节点</li>
	 *   <li>构建节点输出并创建检查点</li>
	 *   <li>触发节点执行后的监听器</li>
	 *   <li>递归调用主执行器继续执行下一个节点</li>
	 * </ol>
	 *
	 * @param context 图运行上下文，包含图的状态和配置信息
	 * @param updateState 节点动作返回的状态更新，包含需要合并到整体状态的数据
	 * @param resultValue 原子引用，用于存储最终的结果值
	 * @return 包含动作结果处理响应的 Flux 流
	 */
	private Flux<GraphResponse<NodeOutput>> handleActionResult(GraphRunnerContext context,
			Map<String, Object> updateState, AtomicReference<Object> resultValue) {
		try {

			// 1. 检查是否包含嵌入的 Flux（流式响应）
			// 这通常用于处理 LLM 的流式输出，如 ChatModel.stream() 返回的 Flux<ChatResponse>
			Optional<Flux<GraphResponse<NodeOutput>>> embedFlux = getEmbedFlux(context, updateState);
			if (embedFlux.isPresent()) {
				// 处理嵌入的流式响应，将流式数据转换为 GraphResponse 并逐步发送
				return handleEmbeddedFlux(mainGraphExecutor, context, embedFlux.get(), updateState, resultValue);
			}

			// 2. 检查是否包含并行图流（ParallelGraphFlux）
			// 这是从 ParallelNode 返回的，用于并行执行多个子图流
			Optional<ParallelGraphFlux> embedParallelGraphFlux = getEmbedParallelGraphFlux(updateState);
			if (embedParallelGraphFlux.isPresent()) {
				// 处理并行图流，同时执行多个流并合并结果
				return handleParallelGraphFlux(context, embedParallelGraphFlux.get(), updateState, resultValue);
			}

			// 3. 检查是否包含图流（GraphFlux）
			// 这是为了向后兼容旧版本的 API
			Optional<GraphFlux<?>> embedGraphFlux = getEmbedGraphFlux(updateState,context);
			if (embedGraphFlux.isPresent()) {
				// 处理图流，支持自定义的流式处理逻辑
				return handleGraphFlux(context, embedGraphFlux.get(), updateState, resultValue);
			}

			// 4. 检查执行后中断钩子（interruptAfter）
			// 这个钩子在节点执行完成后、状态合并前触发
			// 允许在看到节点执行结果后决定是否需要人工干预
			String currentNodeId = context.getCurrentNodeId();
			AsyncNodeActionWithConfig action = context.getNodeAction(currentNodeId);
			if (action instanceof InterruptableAction) {
				// 调用 interruptAfter 方法，传入执行前的状态和执行结果
				Optional<InterruptionMetadata> interruptMetadata = ((InterruptableAction) action)
					.interruptAfter(currentNodeId, context.cloneState(context.getCurrentStateData()),
						updateState, context.getConfig());

				if (interruptMetadata.isPresent()) {
					// 如果需要中断，执行以下步骤：

					// 4.1 先合并状态，确保恢复时状态正确
					context.mergeIntoCurrentState(updateState);

					// 4.2 确定下一个节点，在创建检查点前
					Command nextCommand = context.nextNodeId(currentNodeId, context.getCurrentStateData());
					context.setNextNodeId(nextCommand.gotoNode());

					// 4.3 构建检查点，包含正确的 nextNodeId
					context.buildNodeOutputAndAddCheckpoint(updateState);

					// 4.4 触发节点执行后的监听器
					context.doListeners(NODE_AFTER, null);

					// 4.5 保存中断元数据并返回中断响应
					resultValue.set(interruptMetadata.get());
					return Flux.just(GraphResponse.done(interruptMetadata.get()));
				}
			}

			// 5. 将节点返回的状态更新合并到当前的整体状态中
			context.mergeIntoCurrentState(updateState);

			// 6. 确定下一个要执行的节点
			// 检查是否配置了在边之前中断，并且当前节点在中断列表中
			if (context.getCompiledGraph().compileConfig.interruptBeforeEdge()
					&& context.getCompiledGraph().compileConfig.interruptsAfter()
						.contains(context.getCurrentNodeId())) {
				// 设置特殊的 INTERRUPT_AFTER 标记，表示在下一个节点执行前需要中断
				context.setNextNodeId(INTERRUPT_AFTER);
			}
			else {
				// 根据当前节点和状态，通过路由逻辑确定下一个节点
				Command nextCommand = context.nextNodeId(context.getCurrentNodeId(), context.getCurrentStateData());
				context.setNextNodeId(nextCommand.gotoNode());
			}

			// 7. 构建节点输出并添加检查点
			// 检查点用于支持图的持久化和恢复功能
			NodeOutput output = context.buildNodeOutputAndAddCheckpoint(updateState);

			// 8. 触发节点执行后的监听器（用于观测、日志、指标收集等）
			context.doListeners(NODE_AFTER, null);

			// 9. 返回当前节点的输出，并递归调用主执行器继续执行下一个节点
			// 使用 concatWith 确保当前节点输出先发送，然后再执行下一个节点
			return Flux.just(GraphResponse.of(output))
				.concatWith(Flux.defer(() -> mainGraphExecutor.execute(context, resultValue)));
		}
		catch (Exception e) {
			// 10. 捕获异常并返回错误响应
			return Flux.just(GraphResponse.error(e));
		}
	}

	/**
	 * Transforms a raw Flux to Flux<GraphResponse<NodeOutput>> with embedded flux processing logic.
	 * This is the core transformation logic extracted from getEmbedFlux for reuse.
	 * @param context the graph runner context
	 * @param rawFlux the raw flux to transform
	 * @param key the key associated with the flux (for logging and completion result)
	 * @param nodeId the node ID to use for building streaming output
	 * @return Flux of GraphResponse with transformed elements
	 */
	private Flux<GraphResponse<NodeOutput>> transformFluxToGraphResponse(
			GraphRunnerContext context, Flux<?> rawFlux, String key, String nodeId) {
		var lastChatResponseRef = new AtomicReference<ChatResponse>(null);
		var lastGraphResponseRef = new AtomicReference<GraphResponse<NodeOutput>>(null);

		return rawFlux.filter(element -> {
				// skip ChatResponse.getResult() == null
				if (element instanceof ChatResponse response) {
					return response.getResult() != null &&  response.getResult().getOutput() != null;
				}
				// Don't filter out Exception/Throwable - we need to handle them
				return true;
			})
			.switchIfEmpty(Flux.error(new IllegalStateException(
				"Empty flux detected for key '" + key + "'. This may indicate an LLM API error with null result.")))
			.map(element -> {
				// Handle Exception/Throwable as data elements (not error signals)
				if (element instanceof Throwable throwable) {
					log.error("Exception emitted as data element in embedded Flux stream for key '{}': {}",
						key, throwable.getMessage(), throwable);
					GraphResponse<NodeOutput> errorResponse = GraphResponse.error(throwable);
					lastGraphResponseRef.set(errorResponse);
					return errorResponse;
				}
				if (element instanceof ChatResponse response) {
					ChatResponse lastResponse = lastChatResponseRef.get();
					final var currentMessage = response.getResult().getOutput();

					if (lastResponse == null) {
						lastChatResponseRef.set(response);
					} else {
						var lastMessageText = "";
						if (lastResponse.getResult().getOutput().getText() != null) {
							lastMessageText = lastResponse.getResult().getOutput().getText();
						}

						final var currentMessageText = currentMessage.getText();

						var newMessage = AssistantMessage.builder()
								.content(currentMessageText != null ? lastMessageText.concat(currentMessageText) : lastMessageText)
								.properties(currentMessage.getMetadata()) // TODO, reasoningContent in metadata is not aggregated
								.toolCalls(mergeToolCalls(lastResponse.getResult().getOutput().getToolCalls(),
										currentMessage.getToolCalls()))
								.media(currentMessage.getMedia())
								.build();

						var newGeneration = new Generation(newMessage,
								response.getResult().getMetadata());

						ChatResponse newResponse = new ChatResponse(
								List.of(newGeneration), response.getMetadata());
						lastChatResponseRef.set(newResponse);
					}
					GraphResponse<NodeOutput> lastGraphResponse = GraphResponse
						.of(context.buildStreamingOutput(response.getResult().getOutput(), response, nodeId, true));
					 lastGraphResponseRef.set(lastGraphResponse);
					return lastGraphResponse;
				}
				else if (element instanceof GraphResponse) {
					GraphResponse<NodeOutput> graphResponse = (GraphResponse<NodeOutput>) element;
					lastGraphResponseRef.set(graphResponse);
					return graphResponse;
				} else if (element instanceof NodeOutput nodeOutput) {
					GraphResponse<NodeOutput> graphResponse = GraphResponse.of(nodeOutput);
					lastGraphResponseRef.set(graphResponse);
					return graphResponse;
				}
				else {
					try {
						log.info("Received element of type '{}' in embedded Flux for key '{}', wrapping in StreamingOutput.",
							element.getClass().getName(), key);
						StreamingOutput<?> streamingOutput = context.buildStreamingOutput(element, nodeId, true);
						GraphResponse<NodeOutput> graphResponse = GraphResponse.of(streamingOutput);
						lastGraphResponseRef.set(graphResponse);
						return graphResponse;
					}
					catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			})
			.onErrorResume(error -> {
				// Handle actual error signals from the Flux
				log.error("Error signal occurred in embedded Flux stream for key '{}': {}",
					key, error.getMessage());
				GraphResponse<NodeOutput> errorResponse = GraphResponse.error(error);
				lastGraphResponseRef.set(errorResponse);
				return Flux.just(errorResponse);
			})
			.concatWith(Flux.defer(() -> {
				if (lastChatResponseRef.get() == null) {
					GraphResponse<NodeOutput> lastGraphResponse = lastGraphResponseRef.get();
					if (lastGraphResponse != null && lastGraphResponse.resultValue().isPresent()) {
						Object result = lastGraphResponse.resultValue().get();

						// don't re-emit InterruptionMetadata, it will be handled by MainGraphExecutor
						if (result instanceof InterruptionMetadata) {
							return Flux.empty();
						}

						if (result instanceof Map resultMap) {
							if (!resultMap.containsKey(key) && resultMap.containsKey("messages")) {
								List<Object> messages = (List<Object>) resultMap.get("messages");
								Object lastMessage = messages.get(messages.size() - 1);
								if (lastMessage instanceof AssistantMessage lastAssistantMessage) {
									resultMap.put(key, lastAssistantMessage.getText());
								}
							}
						}
						return Flux.just(lastGraphResponse);
					}
					return Flux.empty();
				} else {
					ChatResponse lastChatResponse = lastChatResponseRef.get();
					// First emit a GraphResponse containing the aggregated ChatResponse
					GraphResponse<NodeOutput> aggregatedResponse = GraphResponse
						.of(context.buildStreamingOutput(lastChatResponse.getResult().getOutput(), lastChatResponse, nodeId, false));
					// Then emit the completion response
					Map<String, Object> completionResult = new HashMap<>();
					completionResult.put(key, lastChatResponse.getResult().getOutput());
					if (!key.equals("messages")) {
						completionResult.put("messages", lastChatResponse.getResult().getOutput());
					}
					GraphResponse<NodeOutput> doneResponse = GraphResponse.done(completionResult);
					return Flux.just(aggregatedResponse, doneResponse);
				}
			}));
	}

  /**
   * Merges tool calls from two messages.
   * Tool calls with the same id will be merged.
   *
   * @return the merged list of tool calls
   */
  private List<ToolCall> mergeToolCalls(List<ToolCall> lastToolCalls, List<ToolCall> currentToolCalls) {

	  if (lastToolCalls == null || lastToolCalls.isEmpty()) {
		  return currentToolCalls != null ? currentToolCalls : List.of();
	  }
	  if (currentToolCalls == null || currentToolCalls.isEmpty()) {
		  return lastToolCalls;
	  }


	  Map<String, ToolCall> toolCallMap = new LinkedHashMap<>();

	  List<AssistantMessage.ToolCall> resultCalls = new ArrayList<>();
	  currentToolCalls.forEach(tc -> toolCallMap.put(tc.id(), tc));

	  // remove duplicate while keep order
	  lastToolCalls.forEach(tc->{
		  if( !toolCallMap.containsKey(tc.id()) ) {
			  resultCalls.add(tc);
		  }
	  });

	  resultCalls.addAll(currentToolCalls);

	  return resultCalls;
  }

	/**
	 * Processes a Flux<GraphResponse<NodeOutput>> with embedded flux handling logic.
	 * This is the core processing logic extracted from handleEmbeddedFlux for reuse.
	 * @param mainGraphExecutor the main graph executor
	 * @param context the graph runner context
	 * @param embedFlux the embedded flux to process
	 * @param partialState the partial state
	 * @param resultValue the atomic reference to store the result value
	 * @return Flux of GraphResponse with processed result
	 */
	private Flux<GraphResponse<NodeOutput>> processGraphResponseFlux(
			MainGraphExecutor mainGraphExecutor, GraphRunnerContext context,
			Flux<GraphResponse<NodeOutput>> embedFlux, Map<String, Object> partialState,
			AtomicReference<Object> resultValue) {
		AtomicReference<GraphResponse<NodeOutput>> lastData = new AtomicReference<>();

		Flux<GraphResponse<NodeOutput>> processedFlux = embedFlux.map(data -> {
				if (data.getOutput() != null && !data.getOutput().isCompletedExceptionally()) {
					var output = data.getOutput().join();
					output.setSubGraph(true);
					GraphResponse<NodeOutput> newData = GraphResponse.of(output);
					lastData.set(newData);
					return newData;
				}
				lastData.set(data);
				return data;
			})
			// filter out InterruptionMetadata emitted directly by upstream to avoid duplicate sending
			// retain regular procedural events
			.filter(data -> {
				var value = data.resultValue();
				return value.isEmpty() || !(value.get() instanceof InterruptionMetadata);
			});

		Mono<Void> updateContextMono = Mono.fromRunnable(() -> {
			var data = lastData.get();
			if (data == null) {
				log.error("No data returned from last streaming node execution '{}', will goto END node directly.", context.getCurrentNodeId());
				context.setNextNodeId(END);
				context.doListeners(NODE_AFTER, null);
				return;
			}

			var nodeResultValue = data.resultValue();

			if (nodeResultValue.isPresent() && nodeResultValue.get() instanceof InterruptionMetadata) {
				context.setReturnFromEmbedWithValue(nodeResultValue.get());
				return;
			}

			Map<String, Object> partialStateWithoutFlux = partialState.entrySet()
					.stream()
					.filter(e -> !(e.getValue() instanceof Flux) 
							&& !(e.getValue() instanceof GraphFlux)
							&& !(e.getValue() instanceof ParallelGraphFlux))
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

			Map<String, Object> updateState = new HashMap<>();
			if (nodeResultValue.isPresent()) {
				Object value = nodeResultValue.get();
				if (value instanceof Map<?, ?>) {
					updateState = (Map<String, Object>) value;
				}
				else {
					throw new IllegalArgumentException("Node stream must return Map result using Data.done(),");
				}
			}

			Map<String, Object> combinedUpdateState = new HashMap<>(partialStateWithoutFlux);
			combinedUpdateState.putAll(updateState);
			Optional<InterruptionMetadata> interruptAfterMetadata = interruptAfterForStreaming(context, combinedUpdateState);

			context.mergeIntoCurrentState(partialStateWithoutFlux);
			context.mergeIntoCurrentState(updateState);

			try {
				Command nextCommand = context.nextNodeId(context.getCurrentNodeId(), context.getCurrentStateData());
				context.setNextNodeId(nextCommand.gotoNode());

				context.buildNodeOutputAndAddCheckpoint(updateState);

				context.doListeners(NODE_AFTER, null);
				interruptAfterMetadata.ifPresent(context::setReturnFromEmbedWithValue);
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

		return processedFlux
			.concatWith(updateContextMono.thenMany(Flux.defer(() -> mainGraphExecutor.execute(context, resultValue))));
	}

	/**
	 * Gets embed flux from partial state.
	 * @param context the graph runner context
	 * @param partialState the partial state containing flux instances
	 * @return an Optional containing Data with the flux if found, empty otherwise
	 */
	public Optional<Flux<GraphResponse<NodeOutput>>> getEmbedFlux(GraphRunnerContext context,
			Map<String, Object> partialState) {
		return partialState.entrySet().stream().filter(e -> e.getValue() instanceof Flux<?>).findFirst().map(e -> {
			var chatFlux = (Flux<?>) e.getValue();
			return transformFluxToGraphResponse(context, chatFlux, e.getKey(), context.getCurrentNodeId());
		});
	}

	/**
	 * Handles embedded flux processing.
	 * @param context the graph runner context
	 * @param embedFlux the embedded flux to handle
	 * @param partialState the partial state
	 * @param resultValue the atomic reference to store the result value
	 * @return Flux of GraphResponse with embedded flux handling result
	 */
	public Flux<GraphResponse<NodeOutput>> handleEmbeddedFlux(MainGraphExecutor mainGraphExecutor, GraphRunnerContext context,
			Flux<GraphResponse<NodeOutput>> embedFlux, Map<String, Object> partialState,
			AtomicReference<Object> resultValue) {
		return processGraphResponseFlux(mainGraphExecutor, context, embedFlux, partialState, resultValue);
	}

	/**
	 * Gets GraphFlux from partial state.
	 * @param partialState the partial state containing GraphFlux instances
	 * @return an Optional containing GraphFlux if found, empty otherwise
	 */
	private Optional<GraphFlux<?>> getEmbedGraphFlux(Map<String, Object> partialState, GraphRunnerContext context) {
		return partialState.entrySet()
				.stream()
				.filter(e -> e.getValue() instanceof GraphFlux)
				.findFirst()
				.map(e -> {
					GraphFlux<Object> graphFlux = (GraphFlux<Object>) e.getValue();
					return GraphFlux.of(StringUtils.hasText(graphFlux.getNodeId()) ? graphFlux.getNodeId() : context.getCurrentNodeId(),
							StringUtils.hasText(graphFlux.getKey()) ? graphFlux.getKey() : e.getKey(),
							graphFlux.getFlux(),
							graphFlux.getMapResult(),
							graphFlux.getChunkResult());
				});
	}

	/**
	 * Gets ParallelGraphFlux from partial state.
	 * @param partialState the partial state containing ParallelGraphFlux instances
	 * @return an Optional containing ParallelGraphFlux if found, empty otherwise
	 */
	private Optional<ParallelGraphFlux> getEmbedParallelGraphFlux(Map<String, Object> partialState) {
		return partialState.entrySet()
				.stream()
				.filter(e -> e.getValue() instanceof ParallelGraphFlux)
				.findFirst()
				.map(e -> (ParallelGraphFlux) e.getValue());
	}

	/**
	 * Handles GraphFlux processing with combined embedded flux transformation and processing.
	 * This method applies both getEmbedFlux transformation logic and handleEmbeddedFlux processing logic.
	 * @param context the graph runner context
	 * @param graphFlux the GraphFlux to handle
	 * @param partialState the partial state
	 * @param resultValue the atomic reference to store the result value
	 * @return Flux of GraphResponse with GraphFlux handling result
	 */
	private Flux<GraphResponse<NodeOutput>> transformGraphFluxToFlux(GraphRunnerContext context,
			GraphFlux<?> graphFlux, Map<String, Object> partialState,
			AtomicReference<Object> resultValue) {
		// Use nodeId from GraphFlux instead of context to preserve real node identity
		String effectiveNodeId = graphFlux.getNodeId();
		String key = graphFlux.getKey() != null ? graphFlux.getKey() : "result";

		// Step 1: Apply getEmbedFlux transformation logic to graphFlux.getFlux()
		Flux<GraphResponse<NodeOutput>> transformedFlux = transformFluxToGraphResponse(
				context, graphFlux.getFlux(), key, effectiveNodeId);

		// Step 2: Apply handleEmbeddedFlux processing logic (directly implemented)

		return transformedFlux.map(data -> {
				if (data.getOutput() != null && !data.getOutput().isCompletedExceptionally()) {
					var output = data.getOutput().join();
					output.setSubGraph(true);
					GraphResponse<NodeOutput> newData = GraphResponse.of(output);
					resultValue.set(newData);
					return newData;
				}
				resultValue.set(data);
				return data;
			})
			// filter out InterruptionMetadata emitted directly by upstream to avoid duplicate sending
			// retain regular procedural events
			.filter(data -> {
				var value = data.resultValue();
				return value.isEmpty() || !(value.get() instanceof InterruptionMetadata);
			});
	}

	/**
	 * Handles GraphFlux processing with node ID preservation.
	 * @param context the graph runner context
	 * @param graphFlux the GraphFlux to handle
	 * @param partialState the partial state
	 * @param resultValue the atomic reference to store the result value
	 * @return Flux of GraphResponse with GraphFlux handling result
	 */
	private Flux<GraphResponse<NodeOutput>> handleGraphFlux(GraphRunnerContext context,
															GraphFlux<?> graphFlux, Map<String, Object> partialState,
															AtomicReference<Object> resultValue) {

		// Use nodeId from GraphFlux instead of context to preserve real node identity
		String effectiveNodeId = graphFlux.getNodeId();
		AtomicReference<Object> lastDataRef = new AtomicReference<>();

		// Process the GraphFlux stream with preserved node ID
		Flux<GraphResponse<NodeOutput>> processedFlux = transformGraphFluxToFlux(context, graphFlux, partialState, lastDataRef);

		// Handle completion and result mapping
		Mono<Void> updateContextMono = Mono.fromRunnable(() -> {
			Object lastData = lastDataRef.get();

			if (lastData == null) {
				log.error("No data returned from last streaming node execution '{}', will goto END node directly.", context.getCurrentNodeId());
				context.setNextNodeId(END);
				return;
			}

			// Apply mapResult function if available
			Map<String, Object> resultMap = new HashMap<>();
			resultMap.put(graphFlux.getKey(), lastData);

			// Merge non-GraphFlux state
			Map<String, Object> partialStateWithoutGraphFlux = partialState.entrySet()
					.stream()
					.filter(e -> !(e.getValue() instanceof GraphFlux))
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

			Map<String, Object> combinedUpdateState = new HashMap<>(partialStateWithoutGraphFlux);
			combinedUpdateState.putAll(resultMap);
			Optional<InterruptionMetadata> interruptAfterMetadata = interruptAfterForStreaming(context, combinedUpdateState);

			context.mergeIntoCurrentState(partialStateWithoutGraphFlux);

			// Merge the result from GraphFlux processing
			if (!resultMap.isEmpty()) {
				context.mergeIntoCurrentState(resultMap);
			}

			try {
				Command nextCommand = context.nextNodeId(context.getCurrentNodeId(), context.getCurrentStateData());
				context.setNextNodeId(nextCommand.gotoNode());

				context.buildNodeOutputAndAddCheckpoint(partialStateWithoutGraphFlux);

				context.doListeners(NODE_AFTER, null);
				interruptAfterMetadata.ifPresent(context::setReturnFromEmbedWithValue);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

		return processedFlux
				.concatWith(updateContextMono.thenMany(Flux.defer(() -> mainGraphExecutor.execute(context, resultValue))));
	}

	/**
	 * Checks interruptAfter hook for streaming nodes using the pre-merge state.
	 * <p>
	 * This method must be called <strong>before</strong> the streaming state updates are
	 * merged into the {@link OverAllState} to keep semantics consistent with the
	 * non-streaming interruptAfter hook.
	 * @param context the graph runner context
	 * @param actionResult the streaming node action result (state delta) passed to interruptAfter
	 * @return interruption metadata if the hook triggers
	 */
	private Optional<InterruptionMetadata> interruptAfterForStreaming(GraphRunnerContext context,
			Map<String, Object> actionResult) {
		String currentNodeId = context.getCurrentNodeId();
		AsyncNodeActionWithConfig action = context.getNodeAction(currentNodeId);

		if (!(action instanceof InterruptableAction interruptableAction)) {
			return Optional.empty();
		}

		try {
			OverAllState stateBeforeMerge = context.cloneState(context.getCurrentStateData());
			return interruptableAction.interruptAfter(currentNodeId, stateBeforeMerge, actionResult,
					context.getConfig());
		}
		catch (Exception e) {
			context.doListeners(ERROR, e);
			throw new RuntimeException("Failed to check interruptAfter hook for streaming node", e);
		}
	}

	/**
	 * Handles ParallelGraphFlux processing with node ID preservation for all parallel streams.
	 * @param context the graph runner context
	 * @param parallelGraphFlux the ParallelGraphFlux to handle
	 * @param partialState the partial state
	 * @param resultValue the atomic reference to store the result value
	 * @return Flux of GraphResponse with ParallelGraphFlux handling result
	 */
	private Flux<GraphResponse<NodeOutput>> handleParallelGraphFlux(GraphRunnerContext context,
																	ParallelGraphFlux parallelGraphFlux, Map<String, Object> partialState,
																	AtomicReference<Object> resultValue) throws Exception {

		if (parallelGraphFlux.isEmpty()) {
			// Handle empty ParallelGraphFlux
			return handleNonStreamingResult(context, partialState, resultValue);
		}

		Map<String, AtomicReference<Object>> nodeDataRefs = new HashMap<>();

		// Get executor from context, fallback to Schedulers.parallel() if not available
		// Note: DEFAULT_EXECUTOR from ParallelNode is private, so we use Schedulers.parallel() as fallback
		Executor executor = getExecutor(context.getConfig(), context.getCurrentNodeId());
		
		// Convert Executor to Scheduler for Reactor, use Schedulers.parallel() as fallback
		Scheduler scheduler = executor != null ? Schedulers.fromExecutor(executor) : Schedulers.parallel();

		// Create merged flux from all GraphFlux instances with preserved node IDs
		// Use subscribeOn(scheduler) to ensure each Flux executes in parallel on the scheduler
		List<Flux<GraphResponse<NodeOutput>>> fluxList = parallelGraphFlux.getGraphFluxes()
				.stream()
				.map(graphFlux -> {
					String nodeId = graphFlux.getNodeId();
					AtomicReference<Object> nodeDataRef = new AtomicReference<>();
					nodeDataRefs.put(nodeId, nodeDataRef);

					return transformGraphFluxToFlux(context, graphFlux, partialState, nodeDataRef)
							.subscribeOn(scheduler);
				}).collect(Collectors.toList());
		
		// Merge all parallel streams while preserving node identities
		// Each Flux is already subscribed on the scheduler, so they will execute in parallel
		Flux<GraphResponse<NodeOutput>> mergedFlux = Flux.merge(fluxList);

		// Handle completion and result mapping for all nodes
		Mono<Void> updateContextMono = Mono.fromRunnable(() -> {
			Map<String, Object> combinedResultMap = new HashMap<>();

			// Process results from each GraphFlux with node-specific prefixes
			for (GraphFlux<?> graphFlux : parallelGraphFlux.getGraphFluxes()) {
				String nodeId = graphFlux.getNodeId();
				Object nodeData = nodeDataRefs.get(nodeId).get();

				combinedResultMap.put(graphFlux.getKey(),nodeData);
			}

			// Merge non-ParallelGraphFlux state
			Map<String, Object> partialStateWithoutParallelGraphFlux = partialState.entrySet()
					.stream()
					.filter(e -> !(e.getValue() instanceof ParallelGraphFlux))
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

			// Check interruptAfter hook for streaming nodes using the pre-merge state.
			Map<String, Object> combinedUpdateState = new HashMap<>(partialStateWithoutParallelGraphFlux);
			combinedUpdateState.putAll(combinedResultMap);
			Optional<InterruptionMetadata> interruptAfterMetadata = interruptAfterForStreaming(context, combinedUpdateState);

			context.mergeIntoCurrentState(partialStateWithoutParallelGraphFlux);

			// Merge the combined results from ParallelGraphFlux processing
			if (!combinedResultMap.isEmpty()) {
				context.mergeIntoCurrentState(combinedResultMap);
			}

			try {
				Command nextCommand = context.nextNodeId(context.getCurrentNodeId(), context.getCurrentStateData());
				context.setNextNodeId(nextCommand.gotoNode());

				context.buildNodeOutputAndAddCheckpoint(partialStateWithoutParallelGraphFlux);

				context.doListeners(NODE_AFTER, null);
				interruptAfterMetadata.ifPresent(context::setReturnFromEmbedWithValue);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

		return mergedFlux
				.concatWith(updateContextMono.thenMany(Flux.defer(() -> mainGraphExecutor.execute(context, resultValue))));
	}

	/**
	 * Handles non-streaming result processing.
	 * @param context the graph runner context
	 * @param partialState the partial state
	 * @param resultValue the atomic reference to store the result value
	 * @return Flux of GraphResponse with non-streaming result
	 */
	private Flux<GraphResponse<NodeOutput>> handleNonStreamingResult(GraphRunnerContext context,
																	 Map<String, Object> partialState, AtomicReference<Object> resultValue) throws Exception {
		if (context.getCompiledGraph().compileConfig.interruptBeforeEdge()
				&& context.getCompiledGraph().compileConfig.interruptsAfter()
				.contains(context.getCurrentNodeId())) {
			context.setNextNodeId(INTERRUPT_AFTER);
		}
		else {
			Command nextCommand = context.nextNodeId(context.getCurrentNodeId(), context.getCurrentStateData());
			context.setNextNodeId(nextCommand.gotoNode());
		}

		NodeOutput output = context.buildNodeOutputAndAddCheckpoint(partialState);
		// Recursively call the main execution handler
		return Flux.just(GraphResponse.of(output))
				.concatWith(Flux.defer(() -> mainGraphExecutor.execute(context, resultValue)));
	}
}
