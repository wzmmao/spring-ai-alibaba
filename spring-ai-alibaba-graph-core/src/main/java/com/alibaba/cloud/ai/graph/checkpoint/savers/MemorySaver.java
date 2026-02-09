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
package com.alibaba.cloud.ai.graph.checkpoint.savers;

import com.alibaba.cloud.ai.graph.RunnableConfig;
import com.alibaba.cloud.ai.graph.checkpoint.BaseCheckpointSaver;
import com.alibaba.cloud.ai.graph.checkpoint.Checkpoint;
import com.alibaba.cloud.ai.graph.utils.TryFunction;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

import static java.lang.String.format;

public class MemorySaver implements BaseCheckpointSaver {

	final Map<String, LinkedList<Checkpoint>> _checkpointsByThread = new HashMap<>();
	private final ReentrantLock _lock = new ReentrantLock();

	/**
	 * Protected constructor for MemorySaver.
	 * Use {@link #builder()} to create instances.
	 */
	public MemorySaver() {
	}

	/**
	 * Creates a new builder for MemorySaver.
	 * @return a new Builder instance
	 */
	public static Builder builder() {
		return new Builder();
	}

	protected LinkedList<Checkpoint> loadedCheckpoints(RunnableConfig config, LinkedList<Checkpoint> checkpoints) throws Exception {
		return checkpoints;
	}

	protected void insertedCheckpoint(RunnableConfig config, LinkedList<Checkpoint> checkpoints, Checkpoint checkpoint) throws Exception {
	}

	protected void updatedCheckpoint(RunnableConfig config, LinkedList<Checkpoint> checkpoints, Checkpoint checkpoint) throws Exception {
	}

	protected void releasedCheckpoints(RunnableConfig config, LinkedList<Checkpoint> checkpoints, Tag releaseTag) throws Exception {
	}

	protected final <T> T loadOrInitCheckpoints(RunnableConfig config,
			TryFunction<LinkedList<Checkpoint>, T, Exception> transformer) throws Exception {
		_lock.lock();
		try {
			var threadId = config.threadId().orElse(THREAD_ID_DEFAULT);
			return transformer.tryApply(loadedCheckpoints(config, _checkpointsByThread.computeIfAbsent(threadId, k -> new LinkedList<>())));

		}
		finally {
			_lock.unlock();
		}
	}

	protected final Collection<Checkpoint> remove(String threadId) {
		return _checkpointsByThread.remove(Objects.requireNonNull(threadId));
	}

	@Override
	public final Collection<Checkpoint> list(RunnableConfig config) {
		try {
			return loadOrInitCheckpoints(config, Collections::unmodifiableCollection);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final Optional<Checkpoint> get(RunnableConfig config) {

		try {
			return loadOrInitCheckpoints(config, checkpoints -> {
				if (config.checkPointId().isPresent()) {
					return config.checkPointId()
							.flatMap(id -> checkpoints.stream()
									.filter(checkpoint -> checkpoint.getId().equals(id))
									.findFirst());
				}
				return getLast(checkpoints, config);

			});
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 保存检查点到内存中。
	 *
	 * <p>此方法支持两种操作模式：
	 * <ol>
	 *   <li><b>更新模式</b>：如果 config 中包含 checkPointId，则替换现有的检查点</li>
	 *   <li><b>插入模式</b>：如果 config 中没有 checkPointId，则添加新的检查点到列表头部</li>
	 * </ol>
	 *
	 * <p>更新模式的使用场景：
	 * <ul>
	 *   <li>人工干预后更新检查点状态（如修改了某个节点的输出）</li>
	 *   <li>时间旅行后从历史检查点重新执行，需要更新该检查点</li>
	 *   <li>修正错误的检查点数据</li>
	 * </ul>
	 *
	 * <p>插入模式的使用场景：
	 * <ul>
	 *   <li>节点执行完成后自动保存新的状态快照</li>
	 *   <li>创建新的执行分支</li>
	 *   <li>正常的图执行流程中的状态持久化</li>
	 * </ul>
	 *
	 * <p>线程安全性：
	 * 此方法通过 {@link #loadOrInitCheckpoints} 使用 ReentrantLock 确保线程安全，
	 * 避免并发修改导致的数据不一致。
	 *
	 * @param config 运行配置，包含 threadId 和可选的 checkPointId
	 * @param checkpoint 要保存的检查点对象，包含状态快照和元数据
	 * @return 更新后的运行配置，对于新插入的检查点会包含新生成的 checkPointId
	 * @throws Exception 如果检查点不存在（更新模式）或其他保存错误
	 */
	@Override
	public final RunnableConfig put(RunnableConfig config, Checkpoint checkpoint) throws Exception {

		// 使用 loadOrInitCheckpoints 加载或初始化检查点列表，并在锁保护下执行操作
		return loadOrInitCheckpoints(config, checkpoints -> {

			// 模式判断：检查 config 中是否包含 checkPointId
			if (config.checkPointId().isPresent()) {
				// ========== 更新模式：替换现有检查点 ==========

				// 1. 获取要替换的检查点 ID
				String checkPointId = config.checkPointId().get();

				// 2. 在检查点列表中查找匹配的检查点索引
				// 使用 IntStream 遍历索引，找到 ID 匹配的检查点位置
				int index = IntStream.range(0, checkpoints.size())
						.filter(i -> checkpoints.get(i).getId().equals(checkPointId))
						.findFirst()
						// 如果找不到匹配的检查点，抛出异常
						.orElseThrow(() -> (new NoSuchElementException(
								format("Checkpoint with id %s not found!", checkPointId))));

				// 3. 用新的检查点替换指定位置的旧检查点
				checkpoints.set(index, checkpoint);

				// 4. 调用钩子方法，允许子类在检查点更新后执行额外操作
				// 例如：触发事件、更新索引、记录日志等
				updatedCheckpoint(config, checkpoints, checkpoint);

				// 5. 返回原始配置（checkPointId 保持不变）
				return config;
			}

			// ========== 插入模式：添加新检查点 ==========

			// 1. 将新检查点添加到列表头部（最新的检查点在前）
			// 使用 push() 而不是 add()，确保新检查点在列表开头
			checkpoints.push(checkpoint);

			// 2. 调用钩子方法，允许子类在检查点插入后执行额外操作
			// 例如：持久化到数据库、发送通知、更新缓存等
			insertedCheckpoint(config, checkpoints, checkpoint);

			// 3. 构建并返回新的配置，包含新检查点的 ID
			// 这样调用者可以知道新创建的检查点的 ID，用于后续引用
			return RunnableConfig.builder(config)
					.checkPointId(checkpoint.getId())
					.build();

		});
	}

	@Override
	public final Tag release(RunnableConfig config) throws Exception {

		return loadOrInitCheckpoints(config, checkpoints -> {

			var threadId = config.threadId().orElse(THREAD_ID_DEFAULT);

			var tag = new Tag(threadId, remove(threadId));

			releasedCheckpoints(config, checkpoints, tag);

			return tag;
		});
	}

	/**
	 * Builder class for MemorySaver.
	 */
	public static class Builder {
		/**
		 * Builds a new MemorySaver instance.
		 * @return a new MemorySaver instance
		 */
		public MemorySaver build() {
			return new MemorySaver();
		}
	}
}
