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
import com.alibaba.cloud.ai.graph.checkpoint.BaseCheckpointSaver;

import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 图执行器基类 - 提供公共基础能力和接口规范。
 *
 * <p>这是图执行器架构的基础抽象类，定义了所有执行器必须遵循的接口规范，
 * 并提供了可被子类共享的公共功能。通过继承机制，确保不同类型的执行器
 * 具有一致的行为和接口。
 *
 * <h2>架构设计 - 三层执行器模式</h2>
 *
 * <p>Spring AI Alibaba 的图执行引擎采用三层执行器架构，通过职责分离实现：
 * <ul>
 *   <li><b>关注点分离</b>：每个执行器专注于特定层次的逻辑</li>
 *   <li><b>代码复用</b>：公共功能在基类中实现，避免重复</li>
 *   <li><b>易于扩展</b>：新增执行器类型只需继承基类</li>
 *   <li><b>循环依赖解耦</b>：通过组合模式实现相互调用</li>
 * </ul>
 *
 * <h3>1. BaseGraphExecutor（本类）- 基础层</h3>
 * <p><b>职责</b>：提供公共基础能力和接口规范
 * <ul>
 *   <li>定义抽象的 {@link #execute} 方法，强制子类实现</li>
 *   <li>提供 {@link #handleCompletion} 完成处理逻辑</li>
 *   <li>封装检查点释放和结果提取的公共代码</li>
 *   <li>作为多态的基础，允许统一处理不同类型的执行器</li>
 * </ul>
 *
 * <h3>2. MainGraphExecutor - 图级别控制层</h3>
 * <p><b>职责</b>：处理图级别的流程控制和特殊节点
 * <ul>
 *   <li><b>停止条件检查</b>：判断是否达到停止条件或最大迭代次数</li>
 *   <li><b>START/END 节点处理</b>：处理虚拟的起始和结束节点</li>
 *   <li><b>中断恢复逻辑</b>：处理从中断点恢复执行的场景</li>
 *   <li><b>子图返回处理</b>：处理嵌入子图执行完成后的返回</li>
 *   <li><b>迭代控制</b>：管理循环节点的迭代次数</li>
 *   <li><b>流程编排</b>：决定何时调用 NodeExecutor 执行普通节点</li>
 * </ul>
 *
 * <h3>3. NodeExecutor - 节点级别执行层</h3>
 * <p><b>职责</b>：处理普通节点的执行和结果处理
 * <ul>
 *   <li><b>节点动作执行</b>：调用节点的业务逻辑（AsyncNodeAction）</li>
 *   <li><b>中断钩子处理</b>：执行 interrupt 和 interruptAfter 钩子</li>
 *   <li><b>流式响应处理</b>：处理 Flux、GraphFlux、ParallelGraphFlux</li>
 *   <li><b>状态更新和合并</b>：将节点输出合并到图状态中</li>
 *   <li><b>检查点创建</b>：在节点执行后创建状态快照</li>
 *   <li><b>下一个节点确定</b>：根据路由逻辑确定下一个要执行的节点</li>
 *   <li><b>监听器触发</b>：触发 NODE_BEFORE 和 NODE_AFTER 事件</li>
 * </ul>
 *
 * <h2>为何如此拆分？</h2>
 *
 * <h3>1. 单一职责原则（SRP）</h3>
 * <ul>
 *   <li><b>MainGraphExecutor</b>：只关心"图应该执行什么"（流程控制）</li>
 *   <li><b>NodeExecutor</b>：只关心"节点如何执行"（执行细节）</li>
 *   <li><b>BaseGraphExecutor</b>：只关心"公共能力是什么"（代码复用）</li>
 * </ul>
 *
 * <h3>2. 复杂度管理</h3>
 * <ul>
 *   <li>如果合并到一个类，会有 1000+ 行代码，难以维护</li>
 *   <li>拆分后每个类 200-400 行，职责清晰，易于理解</li>
 *   <li>降低认知负担：阅读时只需关注当前层次的逻辑</li>
 * </ul>
 *
 * <h3>3. 循环依赖解耦</h3>
 * <ul>
 *   <li><b>问题</b>：图执行需要递归（Main → Node → Main → Node...）</li>
 *   <li><b>解决</b>：通过组合模式实现相互引用，而不是继承</li>
 *   <li>MainGraphExecutor 持有 NodeExecutor 引用</li>
 *   <li>NodeExecutor 持有 MainGraphExecutor 引用</li>
 *   <li>两者通过接口（execute 方法）相互调用，形成执行循环</li>
 * </ul>
 *
 * <h3>4. 扩展性</h3>
 * <ul>
 *   <li>未来可以添加新的执行器类型（如 SubGraphExecutor）</li>
 *   <li>可以为不同场景提供不同的执行策略（如并行执行器）</li>
 *   <li>可以通过装饰器模式增强执行器功能（如缓存、重试）</li>
 * </ul>
 *
 * <h3>5. 测试友好</h3>
 * <ul>
 *   <li>可以单独测试图级别逻辑（MainGraphExecutor）</li>
 *   <li>可以单独测试节点级别逻辑（NodeExecutor）</li>
 *   <li>可以通过 Mock 隔离依赖，提高测试覆盖率</li>
 * </ul>
 *
 * <h2>执行流程示例</h2>
 * <pre>{@code
 * 客户端调用
 *   ↓
 * MainGraphExecutor.execute()
 *   ├─ 检查停止条件
 *   ├─ 处理 START 节点 → 递归调用 execute()
 *   ├─ 处理 END 节点 → 调用 handleCompletion()
 *   └─ 普通节点 → 委托给 NodeExecutor.execute()
 *        ↓
 *      NodeExecutor.execute()
 *        ├─ 执行节点动作
 *        ├─ 处理中断钩子
 *        ├─ 合并状态
 *        ├─ 创建检查点
 *        └─ 递归调用 MainGraphExecutor.execute() 执行下一个节点
 * }</pre>
 *
 * @see MainGraphExecutor 图级别流程控制执行器
 * @see NodeExecutor 节点级别执行执行器
 */
public abstract class BaseGraphExecutor {

	/**
	 * Abstract method to be implemented by subclasses. This demonstrates polymorphism as
	 * each subclass will provide its own implementation.
	 * @param context the graph runner context
	 * @param resultValue the atomic reference to store the result value
	 * @return Flux of GraphResponse with execution result
	 */
	public abstract Flux<GraphResponse<NodeOutput>> execute(GraphRunnerContext context,
			AtomicReference<Object> resultValue);

	/**
	 * Protected method that can be used by subclasses. This demonstrates encapsulation by
	 * providing controlled access to common functionality.
	 * @param context the graph runner context
	 * @param resultValue the atomic reference to store the result value
	 * @return Flux of GraphResponse with completion handling result
	 */
	protected Flux<GraphResponse<NodeOutput>> handleCompletion(GraphRunnerContext context,
			AtomicReference<Object> resultValue) {
		return Flux.defer(() -> {
			try {
				if (context.getCompiledGraph().compileConfig.releaseThread()
						&& context.getCompiledGraph().compileConfig.checkpointSaver().isPresent()) {
					BaseCheckpointSaver.Tag tag = context
						.getCompiledGraph().compileConfig.checkpointSaver()
						.get()
						.release(context.getConfig());
					resultValue.set(tag);
				} else {
					resultValue.set(new HashMap<>(context.getOverallState().data()));
				}
				return Flux.just(GraphResponse.done(resultValue.get()));
			}
			catch (Exception e) {
				return Flux.just(GraphResponse.error(e));
			}
		});
	}

}
