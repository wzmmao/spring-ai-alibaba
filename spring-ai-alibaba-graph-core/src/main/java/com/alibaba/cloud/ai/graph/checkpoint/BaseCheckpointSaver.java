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
package com.alibaba.cloud.ai.graph.checkpoint;

import com.alibaba.cloud.ai.graph.RunnableConfig;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static java.util.Optional.ofNullable;

/**
 * 检查点保存器基础接口。
 *
 * <p>检查点（Checkpoint）是图执行过程中的状态快照，用于支持以下功能：
 * <ul>
 *   <li><b>持久化</b>：将图的执行状态保存到存储介质（数据库、文件等）</li>
 *   <li><b>恢复执行</b>：从之前保存的检查点恢复图的执行</li>
 *   <li><b>时间旅行</b>：回溯到历史状态，查看或重新执行</li>
 *   <li><b>人工干预</b>：在中断点暂停执行，等待人工审核或修改后继续</li>
 *   <li><b>调试和审计</b>：记录图执行的完整历史轨迹</li>
 * </ul>
 *
 * <p>实现类需要提供具体的存储机制，如：
 * <ul>
 *   <li>MemoryCheckpointSaver - 内存存储（用于测试和开发）</li>
 *   <li>PostgresCheckpointSaver - PostgreSQL 数据库存储</li>
 *   <li>RedisCheckpointSaver - Redis 存储</li>
 *   <li>MongoDBCheckpointSaver - MongoDB 存储</li>
 *   <li>FileCheckpointSaver - 文件系统存储</li>
 * </ul>
 *
 * <p>线程（Thread）概念：
 * 在 Spring AI Alibaba 中，"线程"（Thread）不是操作系统线程，而是一个逻辑概念，
 * 表示一个独立的对话会话或执行上下文。每个线程有唯一的 threadId，用于隔离不同
 * 会话的状态和检查点。
 *
 * @see Checkpoint
 * @see RunnableConfig
 */
public interface BaseCheckpointSaver {

	/**
	 * 默认线程 ID 常量。
	 *
	 * <p>当没有显式指定线程 ID 时使用此默认值。
	 * 适用于单会话场景或不需要区分多个执行上下文的情况。
	 */
	String THREAD_ID_DEFAULT = "$default";

	/**
	 * 从检查点列表中获取最后一个（最新的）检查点。
	 *
	 * <p>这是一个默认方法，提供了从 LinkedList 中获取最新检查点的便捷实现。
	 * LinkedList 的 peek() 方法返回头部元素，在检查点列表中通常是最新的检查点。
	 *
	 * <p>使用场景：
	 * <ul>
	 *   <li>恢复执行时，从最新的检查点继续</li>
	 *   <li>获取当前图的最新状态</li>
	 *   <li>检查图是否已经执行过</li>
	 * </ul>
	 *
	 * @param checkpoints 检查点列表，通常按时间倒序排列（最新的在前）
	 * @param config 运行配置，包含线程 ID 等上下文信息
	 * @return 包含最后一个检查点的 Optional，如果列表为空则返回 empty
	 */
	default Optional<Checkpoint> getLast(LinkedList<Checkpoint> checkpoints, RunnableConfig config) {
		return (checkpoints.isEmpty()) ? Optional.empty() : ofNullable(checkpoints.peek());
	}

	/**
	 * 列出指定配置下的所有检查点。
	 *
	 * <p>返回与给定配置（主要是 threadId）关联的所有检查点。
	 * 检查点通常按时间倒序排列，最新的在前。
	 *
	 * <p>使用场景：
	 * <ul>
	 *   <li>查看图的完整执行历史</li>
	 *   <li>实现时间旅行功能，选择特定的历史状态</li>
	 *   <li>调试和审计，分析图的执行轨迹</li>
	 *   <li>可视化展示执行过程</li>
	 * </ul>
	 *
	 * @param config 运行配置，包含 threadId 用于过滤检查点
	 * @return 检查点集合，如果没有检查点则返回空集合（不返回 null）
	 */
	Collection<Checkpoint> list(RunnableConfig config);

	/**
	 * 获取指定配置下的最新检查点。
	 *
	 * <p>这是最常用的方法，用于获取特定线程的最新状态。
	 * 实现类应该根据 config 中的 threadId 和 checkpointId（如果有）
	 * 来定位正确的检查点。
	 *
	 * <p>查找逻辑：
	 * <ul>
	 *   <li>如果 config 中指定了 checkpointId，返回该特定检查点</li>
	 *   <li>如果只指定了 threadId，返回该线程的最新检查点</li>
	 *   <li>如果都没有指定，使用默认线程 ID</li>
	 * </ul>
	 *
	 * <p>使用场景：
	 * <ul>
	 *   <li>恢复图执行：从上次中断的地方继续</li>
	 *   <li>获取当前状态：查看图的最新状态数据</li>
	 *   <li>条件执行：根据历史状态决定是否执行</li>
	 * </ul>
	 *
	 * @param config 运行配置，包含 threadId 和可选的 checkpointId
	 * @return 包含检查点的 Optional，如果不存在则返回 empty
	 */
	Optional<Checkpoint> get(RunnableConfig config);

	/**
	 * 保存一个新的检查点。
	 *
	 * <p>将图执行过程中的状态快照持久化到存储介质。
	 * 每次节点执行完成后，框架会调用此方法保存当前状态。
	 *
	 * <p>实现要求：
	 * <ul>
	 *   <li>生成唯一的检查点 ID（如果 checkpoint 中没有）</li>
	 *   <li>记录创建时间戳</li>
	 *   <li>关联到正确的 threadId</li>
	 *   <li>确保原子性，避免并发冲突</li>
	 *   <li>返回更新后的配置，包含新的 checkpointId</li>
	 * </ul>
	 *
	 * <p>检查点内容通常包括：
	 * <ul>
	 *   <li>图的状态数据（OverAllState）</li>
	 *   <li>当前执行的节点 ID</li>
	 *   <li>下一个要执行的节点 ID</li>
	 *   <li>元数据（时间戳、版本号等）</li>
	 * </ul>
	 *
	 * <p>使用场景：
	 * <ul>
	 *   <li>每个节点执行后自动保存状态</li>
	 *   <li>中断点处保存状态，等待人工干预</li>
	 *   <li>关键节点处创建快照，支持回滚</li>
	 * </ul>
	 *
	 * @param config 运行配置，包含 threadId 等信息
	 * @param checkpoint 要保存的检查点对象，包含状态快照
	 * @return 更新后的运行配置，包含新生成的 checkpointId
	 * @throws Exception 如果保存失败（如存储不可用、序列化错误等）
	 */
	RunnableConfig put(RunnableConfig config, Checkpoint checkpoint) throws Exception;

	/**
	 * 释放（删除）指定配置下的所有检查点。
	 *
	 * <p>此方法用于清理不再需要的检查点数据，释放存储空间。
	 * 通常在会话结束、数据过期或用户主动删除时调用。
	 *
	 * <p>实现要求：
	 * <ul>
	 *   <li>删除与 threadId 关联的所有检查点</li>
	 *   <li>返回被删除的检查点信息（用于确认或审计）</li>
	 *   <li>确保删除操作的原子性</li>
	 *   <li>处理不存在的情况（返回空集合而不是抛异常）</li>
	 * </ul>
	 *
	 * <p>使用场景：
	 * <ul>
	 *   <li>会话结束后清理数据</li>
	 *   <li>实现数据保留策略（如只保留最近 N 天的数据）</li>
	 *   <li>用户请求删除历史记录</li>
	 *   <li>测试环境清理</li>
	 * </ul>
	 *
	 * @param config 运行配置，包含要删除的 threadId
	 * @return Tag 对象，包含被删除的 threadId 和检查点集合
	 * @throws Exception 如果删除失败（如存储不可用、权限不足等）
	 */
	Tag release(RunnableConfig config) throws Exception;

	/**
	 * 标签记录，表示一组检查点的删除结果。
	 *
	 * <p>这是一个不可变的记录类（Java 14+ record），用于封装
	 * release() 方法的返回结果。包含被删除的线程 ID 和检查点集合。
	 *
	 * <p>设计目的：
	 * <ul>
	 *   <li>提供删除操作的确认信息</li>
	 *   <li>支持审计和日志记录</li>
	 *   <li>允许调用者验证删除结果</li>
	 *   <li>保持不可变性，确保线程安全</li>
	 * </ul>
	 *
	 * @param threadId 被删除检查点所属的线程 ID
	 * @param checkpoints 被删除的检查点集合（不可变副本）
	 */
	record Tag(String threadId, Collection<Checkpoint> checkpoints) {
		/**
		 * 规范构造器，确保检查点集合的不可变性。
		 *
		 * <p>实现细节：
		 * <ul>
		 *   <li>如果传入的 checkpoints 为 null，创建空的不可变列表</li>
		 *   <li>如果传入的 checkpoints 不为 null，创建其不可变副本</li>
		 *   <li>使用 List.copyOf() 确保防御性复制，避免外部修改</li>
		 * </ul>
		 *
		 * @param threadId 线程 ID
		 * @param checkpoints 检查点集合（可为 null）
		 */
		public Tag(String threadId, Collection<Checkpoint> checkpoints) {
			this.threadId = threadId;
			this.checkpoints = ofNullable(checkpoints).map(List::copyOf).orElseGet(List::of);
		}
	}

}
