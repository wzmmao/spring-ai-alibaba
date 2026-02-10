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
package com.alibaba.cloud.ai.graph.store;

import java.util.List;
import java.util.Optional;

/**
 * 多智能体系统中的长期内存存储接口。
 * <p>
 * Store 提供持久化的跨会话内存管理能力，支持层级命名空间和结构化数据存储。
 * 这与专注于短期图状态持久化的 CheckpointSaver 不同。
 * </p>
 *
 * <h2>核心特性</h2>
 * <ul>
 * <li><strong>层级命名空间：</strong>使用嵌套命名空间组织数据</li>
 * <li><strong>结构化数据：</strong>存储基于 Map 的复杂数据结构</li>
 * <li><strong>搜索和过滤：</strong>通过命名空间、键模式和内容查询数据</li>
 * <li><strong>分页支持：</strong>支持使用 offset/limit 处理大型结果集</li>
 * <li><strong>跨会话：</strong>数据在不同执行会话之间持久化</li>
 * </ul>
 *
 * <h2>使用示例</h2> <pre>{@code
 * // 存储用户偏好设置
 * StoreItem preferences = StoreItem.of(
 *     List.of("users", "user123", "preferences"),
 *     "ui_settings",
 *     Map.of("theme", "dark", "language", "en-US")
 * );
 * store.putItem(preferences);
 *
 * // 检索数据
 * Optional<StoreItem> item = store.getItem(
 *     List.of("users", "user123", "preferences"),
 *     "ui_settings"
 * );
 *
 * // 搜索条目
 * StoreSearchRequest searchRequest = StoreSearchRequest.builder()
 *     .namespace("users")
 *     .query("preferences")
 *     .limit(10)
 *     .build();
 * StoreSearchResult result = store.searchItems(searchRequest);
 * }</pre>
 *
 * @author Spring AI Alibaba
 * @since 1.0.0.3
 */
public interface Store {

	/**
	 * 在指定命名空间中使用给定的键存储一个条目。如果具有相同命名空间和键的条目已存在，则会被更新。
	 * @param item 要存储的条目
	 * @throws IllegalArgumentException 如果 item 为 null 或无效
	 */
	void putItem(StoreItem item);

	/**
	 * 从指定命名空间中使用给定的键检索一个条目。
	 * @param namespace 层级命名空间路径
	 * @param key 条目键
	 * @return 如果找到则返回包含条目的 Optional，否则返回空
	 * @throws IllegalArgumentException 如果 namespace 或 key 为 null/无效
	 */
	Optional<StoreItem> getItem(List<String> namespace, String key);

	/**
	 * 从指定命名空间中删除具有给定键的条目。
	 * @param namespace 层级命名空间路径
	 * @param key 条目键
	 * @return 如果条目被删除则返回 true，如果不存在则返回 false
	 * @throws IllegalArgumentException 如果 namespace 或 key 为 null/无效
	 */
	boolean deleteItem(List<String> namespace, String key);

	/**
	 * 根据提供的搜索条件搜索条目。
	 * @param searchRequest 搜索参数
	 * @return 包含匹配条目的搜索结果
	 * @throws IllegalArgumentException 如果 searchRequest 为 null
	 */
	StoreSearchResult searchItems(StoreSearchRequest searchRequest);

	/**
	 * 根据提供的条件列出可用的命名空间。
	 * @param namespaceRequest 命名空间列表参数
	 * @return 命名空间路径列表
	 * @throws IllegalArgumentException 如果 namespaceRequest 为 null
	 */
	List<String> listNamespaces(NamespaceListRequest namespaceRequest);

	/**
	 * 清除存储中的所有条目。
	 * <p>
	 * <strong>警告：</strong>此操作不可逆，将删除所有存储的数据。
	 * </p>
	 */
	void clear();

	/**
	 * 获取存储中条目的总数。
	 * @return 存储的条目数量
	 */
	long size();

	/**
	 * 检查存储是否为空。
	 * @return 如果存储不包含任何条目则返回 true，否则返回 false
	 */
	boolean isEmpty();

}
