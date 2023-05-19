"""Clickhouse Agent."""
from typing import Any, List, Optional

from langchain.agents.agent import AgentExecutor
from langchain.agents.agent_toolkits.clickhouse.prompt import CLICKHOUSE_PREFIX, CLICKHOUSE_SUFFIX
from langchain.agents.agent_toolkits.clickhouse.toolkit import ClickHouseDatabaseToolkit
from langchain.agents.mrkl.base import ZeroShotAgent
from langchain.agents.mrkl.prompt import FORMAT_INSTRUCTIONS
from langchain.callbacks.base import BaseCallbackManager
from langchain.memory import ConversationBufferMemory
from langchain.chains.llm import LLMChain
from langchain.llms.base import BaseLLM

def create_clickhouse_agent(
    llm: BaseLLM,
    toolkit: ClickHouseDatabaseToolkit,
    callback_manager: Optional[BaseCallbackManager] = None,
    prefix: str = CLICKHOUSE_PREFIX,
    suffix: str = CLICKHOUSE_SUFFIX,
    format_instructions: str = FORMAT_INSTRUCTIONS,
    input_variables: Optional[List[str]] = None,
    top_k: int = 10,
    max_iterations: Optional[int] = 15,
    max_execution_time: Optional[float] = None,
    early_stopping_method: str = "force",
    verbose: bool = False,
    **kwargs: Any,
) -> AgentExecutor:
    """Construct a ClickHouse agent from an LLM and tools."""
    tools = toolkit.get_tools()
    prefix = prefix.format(top_k=top_k)
    prompt = ZeroShotAgent.create_prompt(
        tools,
        prefix=prefix,
        suffix=suffix,
        format_instructions=format_instructions,
        input_variables=input_variables,
    )
    llm_chain = LLMChain(
        llm=llm,
        prompt=prompt,
        callback_manager=callback_manager,
    )
    tool_names = [tool.name for tool in tools]
    agent = ZeroShotAgent(llm_chain=llm_chain, allowed_tools=tool_names, memory=ConversationBufferMemory(memory_key="chat_history", input_key='input', output_key="output"),  **kwargs)
    return AgentExecutor.from_agent_and_tools(
        agent=agent,
        tools=tools,
        verbose=verbose,
        max_iterations=max_iterations,
        max_execution_time=max_execution_time,
        early_stopping_method=early_stopping_method,
        return_intermediate_steps=True,
    )
