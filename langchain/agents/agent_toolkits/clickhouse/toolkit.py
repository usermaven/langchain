"""Toolkit for interacting with a ClickHouse Agent."""
from typing import List

from pydantic import Field

from langchain.agents.agent_toolkits.base import BaseToolkit
from langchain.llms.base import BaseLLM
from langchain.llms.openai import OpenAI
from langchain.clickhouse import ClickHouseDataBase
from langchain.tools import BaseTool
from langchain.tools.clickhouse.tool import (
    InfoClickHouseDatabaseTool,
    ListClickHouseDatabaseTool,
    QueryCheckerTool,
    QueryClickHouseDataBaseTool,
)

class ClickHouseDatabaseToolkit(BaseToolkit):
    """Toolkit for interacting with ClickHouse databases."""

    db: ClickHouseDataBase = Field()
    llm: BaseLLM = Field(default_factory=lambda: OpenAI(temperature=0))

    class Config:
        """Configuration for this pydantic object."""

        arbitrary_types_allowed = True

    def get_tools(self) -> List[BaseTool]:
        """Get the tools in the toolkit."""
        return [
            QueryClickHouseDataBaseTool(db=self.db),
            InfoClickHouseDatabaseTool(db=self.db),
            ListClickHouseDatabaseTool(db=self.db),
            QueryCheckerTool(db=self.db, llm=self.llm),
        ]
