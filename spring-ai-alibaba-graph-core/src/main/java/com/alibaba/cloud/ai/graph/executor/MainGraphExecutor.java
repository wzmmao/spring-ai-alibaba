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

import com.alibaba.cloud.ai.graph.GraphRunnerContext;
import com.alibaba.cloud.ai.graph.GraphResponse;
import com.alibaba.cloud.ai.graph.NodeOutput;
import com.alibaba.cloud.ai.graph.action.Command;
import com.alibaba.cloud.ai.graph.action.InterruptionMetadata;
import com.alibaba.cloud.ai.graph.checkpoint.Checkpoint;
import com.alibaba.cloud.ai.graph.utils.TypeRef;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.cloud.ai.graph.GraphRunnerContext.INTERRUPT_AFTER;
import static com.alibaba.cloud.ai.graph.StateGraph.END;
import static com.alibaba.cloud.ai.graph.StateGraph.ERROR;
import static com.alibaba.cloud.ai.graph.StateGraph.START;

/**
 * 主图执行器 - 负责图级别的流程控制和特殊节点处理。
 *
 * <p>这是图执行引擎的核心控制器，负责编排整个图的执行流程。它处理图级别的
 * 控制逻辑，如停止条件、中断恢复、特殊节点（START/END）等，并将普通节点
 * 的执行委托给 {@link NodeExecutor}。
 *
 * <h2>在三层执行器架构中的定位</h2>
 *
 * <p>MainGraphExecutor 是三层执行器架构的<b>中间控制层</b>：
 * <ul>
 *   <li>继承自 {@link BaseGraphExecutor}，获得公共基础能力</li>
 *   <li>组合 {@link NodeExecutor}，委托节点级别的执行</li>
 *   <li>被 {@link NodeExecutor} 反向引用，形成递归执行循环</li>
 * </ul>
 *
 * <h2>核心职责</h2>
 *
 * <h3>1. 图级别流程控制</h3>
 * <ul>
 *   <li><b>停止条件检查</b>：
 *       <ul>
 *         <li>检查 {@code context.shouldStop()} - 是否手动停止</li>
 *         <li>检查 {@code context.isMaxIterationsReached()} - 是否达到最大迭代次数</li>
 *         <li>满足条件时调用 {@link #handleCompletion} 结束执行</li>
 *       </ul>
 *   </li>
 *   <li><b>迭代控制</b>：
 *       <ul>
 *         <li>防止无限循环（如循环节点配置错误）</li>
 *         <li>支持有限次数的循环执行</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h3>2. 特殊节点处理</h3>
 * <ul>
 *   <li><b>START 节点</b>（{@link #handleStartNode}）：
 *       <ul>
 *         <li>触发 START 事件监听器</li>
 *         <li>获取图的入口点（第一个实际节点）</li>
 *         <li>创建初始检查点</li>
 *         <li>递归调用 execute() 开始执行第一个节点</li>
 *       </ul>
 *   </li>
 *   <li><b>END 节点</b>（{@link #handleEndNode}）：
 *       <ul>
 *         <li>触发 END 事件监听器</li>
 *         <li>构建 END 节点输出</li>
 *         <li>调用 handleCompletion() 生成最终结果</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h3>3. 中断和恢复机制</h3>
 * <ul>
 *   <li><b>子图返回处理</b>：
 *       <ul>
 *         <li>检查 {@code context.getReturnFromEmbedAndReset()}</li>
 *         <li>处理嵌入子图执行完成后的返回值</li>
 *         <li>提取中断元数据或构建节点输出</li>
 *       </ul>
 *   </li>
 *   <li><b>节点恢复</b>：
 *       <ul>
 *         <li>检查 {@code config.isInterrupted(nodeId)}</li>
 *         <li>恢复之前被中断的节点执行</li>
 *         <li>标记节点为已恢复状态</li>
 *       </ul>
 *   </li>
 *   <li><b>中断点恢复</b>：
 *       <ul>
 *         <li>检查 {@code context.getResumeFromAndReset()}</li>
 *         <li>从检查点恢复执行</li>
 *         <li>处理 INTERRUPT_AFTER 特殊标记</li>
 *       </ul>
 *   </li>
 *   <li><b>中断检查</b>：
 *       <ul>
 *         <li>调用 {@code context.shouldInterrupt()}</li>
 *         <li>在配置的中断点暂停执行</li>
 *         <li>构建中断元数据并返回</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h3>4. 执行流程编排</h3>
 * <ul>
 *   <li><b>节点类型判断</b>：
 *       <ul>
 *         <li>判断当前是 START、END 还是普通节点</li>
 *         <li>根据节点类型选择不同的处理策略</li>
 *       </ul>
 *   </li>
 *   <li><b>委托执行</b>：
 *       <ul>
 *         <li>对于普通节点，委托给 {@link NodeExecutor#execute}</li>
 *         <li>NodeExecutor 执行完成后会递归调用回 MainGraphExecutor</li>
 *         <li>形成 Main → Node → Main → Node 的执行循环</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h2>与 NodeExecutor 的协作</h2>
 *
 * <p>MainGraphExecutor 和 NodeExecutor 通过<b>相互组合</b>实现递归执行：
 *
 * <pre>{@code
 * public class MainGraphExecutor extends BaseGraphExecutor {
 *     private final NodeExecutor nodeExecutor;  // 持有 NodeExecutor 引用
 *
 *     public Flux<GraphResponse<NodeOutput>> execute(...) {
 *         // ... 图级别控制逻辑 ...
 *         return nodeExecutor.execute(context, resultValue);  // 委托给 NodeExecutor
 *     }
 * }
 *
 * public class NodeExecutor extends BaseGraphExecutor {
 *     private final MainGraphExecutor mainGraphExecutor;  // 持有 MainGraphExecutor 引用
 *
 *     private Flux<GraphResponse<NodeOutput>> handleActionResult(...) {
 *         // ... 节点执行逻辑 ...
 *         return mainGraphExecutor.execute(context, resultValue);  // 递归回 MainGraphExecutor
 *     }
 * }
 * }</pre>
 *
 * <h2>执行流程示例</h2>
 *
 * <pre>{@code
 * // 1. 客户端调用
 * graph.stream(input).subscribe(...);
 *   ↓
 * // 2. MainGraphExecutor 开始执行
 * MainGraphExecutor.execute(context, resultValue)
 *   ├─ 检查停止条件 ✓
 *   ├─ 检查子图返回 ✗
 *   ├─ 检查节点恢复 ✗
 *   ├─ 判断节点类型 → START 节点
 *   └─ handleStartNode()
 *        ├─ 触发 START 监听器
 *        ├─ 获取入口点 → "node1"
 *        ├─ 创建检查点
 *        └─ 递归调用 execute() → 执行 node1
 *             ↓
 *          MainGraphExecutor.execute() [第2次]
 *             ├─ 判断节点类型 → 普通节点
 *             └─ 委托给 NodeExecutor.execute()
 *                  ↓
 *               NodeExecutor.execute()
 *                  ├─ 执行节点动作
 *                  ├─ 合并状态
 *                  ├─ 确定下一个节点 → "node2"
 *                  └─ 递归调用 MainGraphExecutor.execute() → 执行 node2
 *                       ↓
 *                    MainGraphExecutor.execute() [第3次]
 *                       └─ ... 继续执行 ...
 * }</pre>
 *
 * <h2>为什么需要 MainGraphExecutor？</h2>
 *
 * <ol>
 *   <li><b>关注点分离</b>：
 *       <ul>
 *         <li>MainGraphExecutor 关注"图应该执行什么"（What）</li>
 *         <li>NodeExecutor 关注"节点如何执行"（How）</li>
 *       </ul>
 *   </li>
 *   <li><b>复杂度管理</b>：
 *       <ul>
 *         <li>图级别控制逻辑（停止、中断、特殊节点）约 200 行</li>
 *         <li>节点级别执行逻辑（动作执行、流式处理）约 600 行</li>
 *         <li>拆分后每个类职责清晰，易于维护</li>
 *       </ul>
 *   </li>
 *   <li><b>扩展性</b>：
 *       <ul>
 *         <li>可以独立扩展图级别控制逻辑（如添加新的停止条件）</li>
 *         <li>可以独立扩展节点级别执行逻辑（如支持新的流式类型）</li>
 *       </ul>
 *   </li>
 * </ol>
 *
 * @see BaseGraphExecutor 执行器基类，提供公共能力
 * @see NodeExecutor 节点执行器，处理节点级别逻辑
 */
public class MainGraphExecutor extends BaseGraphExecutor {

	private final NodeExecutor nodeExecutor;

	public MainGraphExecutor() {
		this.nodeExecutor = new NodeExecutor(this);
	}

	/**
     * 停止条件检查 - 检查是否应该停止执行或达到最大迭代次数
     * 嵌入图返回处理 - 处理子图执行完成后的返回结果
     * 节点恢复 - 处理之前被中断的节点恢复执行
     * 起始/结束节点 - 特殊处理图的起始和结束节点
     * 中断点恢复 - 从之前的中断点恢复执行
     * 中断检查 - 检查是否需要在当前节点处中断
     * 节点执行 - 最终执行当前节点的逻辑
     *
	 * Implementation of the execute method. This demonstrates polymorphism as it provides
	 * a specific implementation for main execution flow.
	 * @param context the graph runner context
	 * @param resultValue the atomic reference to store the result value
	 * @return Flux of GraphResponse with execution result
	 */
	@Override
	public Flux<GraphResponse<NodeOutput>> execute(GraphRunnerContext context, AtomicReference<Object> resultValue) {
		try {
			// 检查是否应该停止执行或已达到最大迭代次数
			if (context.shouldStop() || context.isMaxIterationsReached()) {
				return handleCompletion(context, resultValue);
			}

			// 处理从嵌入图返回的情况
			// 当子图执行完成后，会通过 returnFromEmbed 返回结果
			final var returnFromEmbed = context.getReturnFromEmbedAndReset();
			if (returnFromEmbed.isPresent()) {
				// 尝试从返回值中提取中断元数据
				var interruption = returnFromEmbed.get().value(new TypeRef<InterruptionMetadata>() {
				});
				if (interruption.isPresent()) {
					// 如果存在中断元数据，返回中断响应
					return Flux.just(GraphResponse.done(interruption.get()));
				}
				// 否则构建节点输出并添加检查点
				return Flux.just(GraphResponse.done(context.buildNodeOutputAndAddCheckpoint(Map.of())));
			}

			// 处理节点恢复执行的情况
			// 如果当前节点之前被中断，现在需要恢复执行
			if (context.getCurrentNodeId() != null && context.getConfig().isInterrupted(context.getCurrentNodeId())) {
				context.getConfig().withNodeResumed(context.getCurrentNodeId());
				return Flux.just(GraphResponse.done(GraphResponse.done(context.getCurrentStateData())));
			}

			// 处理起始节点
			if (context.isStartNode()) {
				return handleStartNode(context);
			}

			// 处理结束节点
			if (context.isEndNode()) {
				return handleEndNode(context, resultValue);
			}

			// 处理从中断点恢复执行的情况
			final var resumeFrom = context.getResumeFromAndReset();
			if (resumeFrom.isPresent()) {
				// 如果配置了在边之前中断，并且下一个节点是 INTERRUPT_AFTER
				if (context.getCompiledGraph().compileConfig.interruptBeforeEdge()
						&& java.util.Objects.equals(context.getNextNodeId(), INTERRUPT_AFTER)) {
					// 计算下一个要执行的节点
					var nextNodeCommand = context.nextNodeId(resumeFrom.get(), context.getCurrentStateData());
					context.setNextNodeId(nextNodeCommand.gotoNode());
					context.setCurrentNodeId(null);
				}
			}

			// 检查是否应该在当前节点处中断执行
			if (context.shouldInterrupt()) {
				try {
					// 构建中断元数据，包含当前节点ID和状态快照
					InterruptionMetadata metadata = InterruptionMetadata
						.builder(context.getCurrentNodeId(), context.cloneState(context.getCurrentStateData()))
						.build();
					return Flux.just(GraphResponse.done(metadata));
				}
				catch (Exception e) {
					return Flux.just(GraphResponse.error(e));
				}
			}

			// 执行当前节点
			return nodeExecutor.execute(context, resultValue);
		}
		catch (Exception e) {
			context.doListeners(ERROR, e);
			org.slf4j.LoggerFactory.getLogger(com.alibaba.cloud.ai.graph.GraphRunner.class)
				.error("Error during graph execution", e);
			return Flux.just(GraphResponse.error(e));
		}
	}

	/**
	 * Handles the start node execution.
	 * @param context the graph runner context
	 * @return Flux of GraphResponse with start node handling result
	 */
	/**
	 * 处理图的起始节点（START 节点）。
	 *
	 * <p>START 节点是图执行的入口点，它不是一个实际的业务节点，而是一个虚拟节点，
	 * 用于标记图执行的开始。此方法负责初始化图的执行流程，并确定第一个要执行的
	 * 实际业务节点。
	 *
	 * <p>执行流程：
	 * <ol>
	 *   <li>触发 START 事件监听器（用于观测、日志、指标收集等）</li>
	 *   <li>获取图的入口点（第一个要执行的实际节点）</li>
	 *   <li>设置下一个要执行的节点 ID</li>
	 *   <li>创建 START 节点的检查点（记录图执行的起点）</li>
	 *   <li>构建 START 节点的输出</li>
	 *   <li>更新当前节点 ID 为下一个节点</li>
	 *   <li>递归调用主执行器，开始执行第一个实际节点</li>
	 * </ol>
	 *
	 * <p>检查点的作用：
	 * 在 START 节点创建检查点可以记录图执行的初始状态，这对于以下场景很有用：
	 * <ul>
	 *   <li>从头重新执行图（时间旅行到起点）</li>
	 *   <li>审计和调试（查看图的初始输入）</li>
	 *   <li>恢复执行（如果第一个节点就失败了）</li>
	 * </ul>
	 *
	 * <p>与普通节点的区别：
	 * <ul>
	 *   <li>START 节点不执行任何业务逻辑</li>
	 *   <li>START 节点不更新状态（状态保持为初始输入）</li>
	 *   <li>START 节点的主要作用是确定第一个实际节点</li>
	 * </ul>
	 *
	 * @param context 图运行上下文，包含图的状态和配置信息
	 * @return 包含 START 节点输出和后续节点执行结果的 Flux 流
	 */
	private Flux<GraphResponse<NodeOutput>> handleStartNode(GraphRunnerContext context) {
		try {
			// 1. 触发 START 事件监听器
			// 这允许观测系统记录图执行的开始时间、初始状态等信息
			context.doListeners(START, null);

			// 2. 获取图的入口点（第一个要执行的实际节点）
			// getEntryPoint() 返回一个 Command 对象，包含要跳转到的节点 ID
			// 入口点通常在图编译时确定，可以是：
			// - 显式指定的起始节点
			// - 图中第一个添加的节点
			// - 通过条件路由确定的节点
			Command nextCommand = context.getEntryPoint();

			// 3. 设置下一个要执行的节点 ID
			// 这个节点将是第一个实际执行业务逻辑的节点
			context.setNextNodeId(nextCommand.gotoNode());

			// 4. 创建 START 节点的检查点
			// 参数说明：
			// - START: 当前节点 ID（虚拟的起始节点）
			// - context.getNextNodeId(): 下一个要执行的节点 ID（第一个实际节点）
			// 这个检查点记录了图执行的起点和初始状态
			Optional<Checkpoint> cp = context.addCheckpoint(START, context.getNextNodeId());

			// 5. 构建 START 节点的输出
			// 虽然 START 节点不执行业务逻辑，但仍需要构建输出对象
			// 用于在流中传递，并可能被观测系统捕获
			NodeOutput output = context.buildOutput(START, cp);

			// 6. 更新当前节点 ID 为下一个要执行的节点
			// 这样在递归调用 execute() 时，会执行第一个实际的业务节点
			context.setCurrentNodeId(context.getNextNodeId());

			// 7. 返回 START 节点的输出，并递归调用主执行器继续执行
			// 使用 concatWith 确保：
			// - 先发送 START 节点的输出（让观测系统知道图已开始）
			// - 然后执行第一个实际节点（通过递归调用 execute）
			// 使用 Flux.defer 延迟执行，确保在订阅时才开始执行
			return Flux.just(GraphResponse.of(output))
				.concatWith(Flux.defer(() -> execute(context, new AtomicReference<>())));
		}
		catch (Exception e) {
			// 8. 捕获异常并返回错误响应
			// 如果在处理 START 节点时发生错误（如检查点保存失败），
			// 返回错误响应，终止图的执行
			return Flux.just(GraphResponse.error(e));
		}
	}

	/**
	 * Handles the end node execution.
	 * @param context the graph runner context
	 * @param resultValue the atomic reference to store the result value
	 * @return Flux of GraphResponse with end node handling result
	 */
	/**
	 * 处理图的结束节点（END 节点）。
	 *
	 * <p>END 节点是图执行的终点，它不是一个实际的业务节点，而是一个虚拟节点，
	 * 用于标记图执行的正常结束。此方法负责完成图的执行流程，触发结束事件，
	 * 并准备最终的执行结果。
	 *
	 * <p>执行流程：
	 * <ol>
	 *   <li>触发 END 事件监听器（用于观测、日志、指标收集等）</li>
	 *   <li>构建 END 节点的输出（包含最终状态）</li>
	 *   <li>发送 END 节点输出到响应流</li>
	 *   <li>调用完成处理器，生成最终的执行结果</li>
	 * </ol>
	 *
	 * <p>到达 END 节点的方式：
	 * <ul>
	 *   <li><b>自然结束</b>：所有节点按照路由逻辑执行完毕，最后一个节点指向 END</li>
	 *   <li><b>显式跳转</b>：某个节点通过 Command.END 显式跳转到 END 节点</li>
	 *   <li><b>条件路由</b>：路由函数根据状态判断，返回 END 作为下一个节点</li>
	 * </ul>
	 *
	 * <p>与 START 节点的对称性：
	 * <ul>
	 *   <li>START 节点标记图的开始，END 节点标记图的结束</li>
	 *   <li>START 节点确定第一个实际节点，END 节点收集最终结果</li>
	 *   <li>两者都是虚拟节点，不执行实际的业务逻辑</li>
	 *   <li>两者都会触发相应的事件监听器（START/END）</li>
	 * </ul>
	 *
	 * <p>与普通节点的区别：
	 * <ul>
	 *   <li>END 节点不执行任何业务逻辑</li>
	 *   <li>END 节点不更新状态（状态保持为最后一个实际节点的输出）</li>
	 *   <li>END 节点不创建检查点（因为执行已经结束）</li>
	 *   <li>END 节点之后调用完成处理器，而不是继续执行下一个节点</li>
	 * </ul>
	 *
	 * <p>完成处理器的作用：
	 * {@link #handleCompletion} 方法会：
	 * <ul>
	 *   <li>提取最终的执行结果（从 resultValue 或当前状态）</li>
	 *   <li>触发 FINISH 事件监听器</li>
	 *   <li>生成包含最终结果的 GraphResponse.done() 响应</li>
	 *   <li>标记图执行流程的完全结束</li>
	 * </ul>
	 *
	 * @param context 图运行上下文，包含图的最终状态和配置信息
	 * @param resultValue 原子引用，用于存储图执行的最终结果值
	 * @return 包含 END 节点输出和最终完成结果的 Flux 流
	 */
	private Flux<GraphResponse<NodeOutput>> handleEndNode(GraphRunnerContext context,
			AtomicReference<Object> resultValue) {
		try {
			// 1. 触发 END 事件监听器
			// 这允许观测系统记录图执行的结束时间、最终状态、执行统计等信息
			// 监听器可以用于：
			// - 记录执行时长（从 START 到 END 的时间）
			// - 收集性能指标（节点执行次数、状态大小等）
			// - 发送完成通知（如 webhook、消息队列等）
			// - 清理资源（如临时文件、缓存等）
			context.doListeners(END, null);

			// 2. 构建 END 节点的输出
			// 虽然 END 节点不执行业务逻辑，但仍需要构建输出对象
			// 输出包含：
			// - 节点 ID: END（虚拟节点标识）
			// - 当前状态：最后一个实际节点执行后的状态
			// - 无检查点：因为执行已经结束，不需要保存检查点
			// 这个输出主要用于：
			// - 在响应流中标记 END 节点的到达
			// - 被观测系统捕获，用于可视化和调试
			NodeOutput output = context.buildNodeOutput(END);

			// 3. 返回 END 节点的输出，并调用完成处理器
			// 使用 concatWith 确保执行顺序：
			// - 先发送 END 节点的输出（让订阅者知道图已到达终点）
			// - 然后调用 handleCompletion 生成最终结果
			//
			// 使用 Flux.defer 延迟执行完成处理器，确保：
			// - END 节点输出先被发送
			// - 在订阅时才执行完成逻辑
			// - 避免过早计算最终结果
			//
			// 完成处理器会：
			// - 提取最终结果（从 resultValue 或当前状态）
			// - 触发 FINISH 事件
			// - 返回 GraphResponse.done() 标记流的结束
			return Flux.just(GraphResponse.of(output))
				.concatWith(Flux.defer(() -> handleCompletion(context, resultValue)));
		}
		catch (Exception e) {
			// 4. 捕获异常并返回错误响应
			// 如果在处理 END 节点时发生错误（如监听器抛出异常），
			// 返回错误响应，让订阅者知道图执行异常结束
			//
			// 可能的异常场景：
			// - 监听器执行失败（如网络错误、数据库错误）
			// - 构建输出失败（如状态序列化错误）
			// - 资源清理失败
			return Flux.just(GraphResponse.error(e));
		}
	}

}
