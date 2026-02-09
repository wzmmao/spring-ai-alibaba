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
package com.alibaba.cloud.ai.graph.action;

import com.alibaba.cloud.ai.graph.OverAllState;
import io.opentelemetry.context.Context;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * 表示一个异步节点动作，操作智能体状态并返回状态更新。
 *
 * <p>这是一个函数式接口，用于定义图中节点的异步执行逻辑。
 * 节点动作接收当前的整体状态（OverAllState），执行业务逻辑后，
 * 返回一个包含状态更新的 CompletableFuture。
 *
 * <p>使用场景：
 * <ul>
 *   <li>需要执行异步操作的节点（如调用外部API、数据库查询等）</li>
 *   <li>需要并行执行多个任务的节点</li>
 *   <li>需要非阻塞执行的长时间运行任务</li>
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * AsyncNodeAction action = state -> CompletableFuture.supplyAsync(() -> {
 *     // 执行异步业务逻辑
 *     String result = callExternalAPI(state.get("input"));
 *     return Map.of("output", result);
 * });
 * }</pre>
 *
 */
@FunctionalInterface
public interface AsyncNodeAction extends Function<OverAllState, CompletableFuture<Map<String, Object>>> {

	/**
	 * 将此动作应用于给定的智能体状态。
	 *
	 * <p>这是接口的核心方法，实现节点的具体业务逻辑。
	 * 方法接收当前的整体状态，执行相应的操作，并返回一个
	 * CompletableFuture，其中包含需要更新到状态中的键值对。
	 *
	 * @param state 智能体的整体状态，包含图执行过程中的所有状态数据
	 * @return 一个 CompletableFuture，完成时包含状态更新的 Map
	 *         键为状态字段名，值为要更新的数据
	 */
	CompletableFuture<Map<String, Object>> apply(OverAllState state);

	/**
	 * 从同步节点动作创建异步节点动作的工厂方法。
	 *
	 * <p>这个静态方法提供了一个便捷的方式，将同步的 {@link NodeAction}
	 * 转换为异步的 {@link AsyncNodeAction}。转换后的动作会在当前线程中
	 * 同步执行原始的同步动作，但返回值被包装在 CompletableFuture 中。
	 *
	 * <p>注意：虽然返回类型是异步的，但实际执行仍然是同步的。
	 * 如果需要真正的异步执行，应该直接实现 AsyncNodeAction 并使用
	 * CompletableFuture.supplyAsync() 等方法。
	 *
	 * <p>使用场景：
	 * <ul>
	 *   <li>将现有的同步节点动作适配到异步接口</li>
	 *   <li>简单的、不需要真正异步执行的节点逻辑</li>
	 *   <li>保持 API 一致性，统一使用异步接口</li>
	 * </ul>
	 *
	 * <p>示例：
	 * <pre>{@code
	 * NodeAction syncAction = state -> Map.of("result", "processed");
	 * AsyncNodeAction asyncAction = AsyncNodeAction.node_async(syncAction);
	 * }</pre>
	 *
	 * @param syncAction 要转换的同步节点动作
	 * @return 包装后的异步节点动作
	 */
	static AsyncNodeAction node_async(NodeAction syncAction) {
		return state -> {
			// 获取当前的 OpenTelemetry 上下文（用于分布式追踪）
			Context context = Context.current();

			// 创建一个新的 CompletableFuture 来包装同步执行的结果
			CompletableFuture<Map<String, Object>> result = new CompletableFuture<>();

			try {
				// 同步执行原始的节点动作并完成 Future
				result.complete(syncAction.apply(state));
			}
			catch (Exception e) {
				// 如果执行过程中发生异常，将异常传递给 Future
				result.completeExceptionally(e);
			}

			return result;
		};
	}

}
