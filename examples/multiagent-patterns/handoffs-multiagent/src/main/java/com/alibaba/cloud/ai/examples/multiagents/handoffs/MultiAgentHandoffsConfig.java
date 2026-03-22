/*
 * Copyright 2025-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cloud.ai.examples.multiagents.handoffs;

import com.alibaba.cloud.ai.examples.multiagents.handoffs.route.RouteAfterSalesAction;
import com.alibaba.cloud.ai.examples.multiagents.handoffs.route.RouteAfterSupportAction;
import com.alibaba.cloud.ai.examples.multiagents.handoffs.route.RouteInitialAction;
import com.alibaba.cloud.ai.examples.multiagents.handoffs.state.MultiAgentStateConstants;
import com.alibaba.cloud.ai.examples.multiagents.handoffs.tools.TransferToSalesTool;
import com.alibaba.cloud.ai.examples.multiagents.handoffs.tools.TransferToSupportTool;
import com.alibaba.cloud.ai.graph.CompiledGraph;
import com.alibaba.cloud.ai.graph.StateGraph;
import com.alibaba.cloud.ai.graph.agent.ReactAgent;
import com.alibaba.cloud.ai.graph.exception.GraphStateException;
import com.alibaba.cloud.ai.graph.state.strategy.AppendStrategy;
import com.alibaba.cloud.ai.graph.state.strategy.ReplaceStrategy;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.cloud.ai.graph.StateGraph.END;
import static com.alibaba.cloud.ai.graph.StateGraph.START;

/**
 * Configures the multiple agent subgraphs handoffs pattern: sales and support agents
 * as separate graph nodes, with handoff tools that navigate between them.
 */
@Configuration
public class MultiAgentHandoffsConfig {

	private static final String SALES_PROMPT = """
			你是一名销售代理，负责协助客户处理销售咨询、定价和产品库存相关的问题。
			如果客户询问技术问题、故障排查或账户相关的问题，
			请使用 transfer_to_support 工具将会话交接给客服支持代理人。
			""";

	private static final String SUPPORT_PROMPT = """
			你是一名客服支持代理，负责处理客户的技术问题、故障排查和账户相关的问题。
			如果客户询问销售咨询、定价和产品库存相关的问题，
			请使用 transfer_to_sales 工具将会话交接给销售代理人。
			""";

	@Bean
	public ReactAgent multiAgentSalesAgent(ChatModel chatModel) {
		return ReactAgent.builder()
				.name(MultiAgentStateConstants.SALES_AGENT)
				.model(chatModel)
				.systemPrompt(SALES_PROMPT)
				.instruction("这是用户的查询或当前问题状态：{input}。")
				.methodTools(TransferToSupportTool.INSTANCE)
				.inputType(String.class)
				.includeContents(true)
				.returnReasoningContents(true)
				.build();
	}

	@Bean
	public ReactAgent multiAgentSupportAgent(ChatModel chatModel) {
		return ReactAgent.builder()
				.name(MultiAgentStateConstants.SUPPORT_AGENT)
				.model(chatModel)
				.systemPrompt(SUPPORT_PROMPT)
				.instruction("这是用户的查询或当前问题状态：{input}。")
				.methodTools(TransferToSalesTool.INSTANCE)
				.inputType(String.class)
				.includeContents(true)
				.returnReasoningContents(true)
				.build();
	}

	@Bean
	public CompiledGraph multiAgentHandoffsGraph(ReactAgent multiAgentSalesAgent,
			ReactAgent multiAgentSupportAgent) throws GraphStateException {

		StateGraph graph = new StateGraph("multi_agent_handoffs", () -> {
			Map<String, com.alibaba.cloud.ai.graph.KeyStrategy> strategies = new HashMap<>();
			strategies.put("messages", new AppendStrategy(false));
			strategies.put(MultiAgentStateConstants.ACTIVE_AGENT, new ReplaceStrategy());
			return strategies;
		});

		graph.addNode(MultiAgentStateConstants.SALES_AGENT, multiAgentSalesAgent.asNode());
		graph.addNode(MultiAgentStateConstants.SUPPORT_AGENT, multiAgentSupportAgent.asNode());

		// route_initial: default to sales_agent
		graph.addConditionalEdges(START, new RouteInitialAction(), Map.of(
				MultiAgentStateConstants.SALES_AGENT, MultiAgentStateConstants.SALES_AGENT,
				MultiAgentStateConstants.SUPPORT_AGENT, MultiAgentStateConstants.SUPPORT_AGENT));

		// route_after_sales: handoff to support or END
		graph.addConditionalEdges(MultiAgentStateConstants.SALES_AGENT,
				new RouteAfterSalesAction(),
				Map.of(
						MultiAgentStateConstants.SUPPORT_AGENT, MultiAgentStateConstants.SUPPORT_AGENT,
						"__end__", END));

		// route_after_support: handoff to sales or END
		graph.addConditionalEdges(MultiAgentStateConstants.SUPPORT_AGENT,
				new RouteAfterSupportAction(),
				Map.of(
						MultiAgentStateConstants.SALES_AGENT, MultiAgentStateConstants.SALES_AGENT,
						"__end__", END));

		return graph.compile();
	}

	@Bean
	public MultiAgentHandoffsService multiAgentHandoffsService(CompiledGraph multiAgentHandoffsGraph) {
		return new MultiAgentHandoffsService(multiAgentHandoffsGraph);
	}
}
