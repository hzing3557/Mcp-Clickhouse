#!/usr/bin/env pytho3
"""
ClickHouse MCP Server - Main Application

"""

import asyncio
import datetime
import json
import logging
import os
import uuid
from typing import Dict, Any, Optional
from dataclasses import dataclass

# FastAPI imports
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# MCP imports
from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport

# Environment setup
from dotenv import load_dotenv

# Local imports
from clickhouse_mcp_tools import ClickHouseToolHandler

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# Configuration from environment
# CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "172.16.3.6")
# CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
# CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "devadmin")
# CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "Dev12837sadqdk")
# CLICKHOUSE_SECURE = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"

API_BASE_PATH = os.getenv("API_BASE_PATH", "/clickhouse")
PORT = int(os.getenv("PORT", "8200"))
HOST = os.getenv("HOST", "0.0.0.0")


@dataclass
class MCPRequest:
    jsonrpc: str
    method: str
    id: Optional[str] = None
    params: Optional[Dict[str, Any]] = None


class SchemaManager:
    """Manages tool schemas dynamically"""
    
    def __init__(self, tool_handler: ClickHouseToolHandler):
        self.tool_handler = tool_handler
        self._default_schema = {
            "type": "object",
            "properties": {},
            "required": []
        }
    
    def get_tool_schema(self, tool_name: str, tool_info: Dict[str, Any]) -> Dict[str, Any]:
        """Get schema for a tool - either custom or default"""
        tool_instance = self._get_tool_instance(tool_name)
        if tool_instance and hasattr(tool_instance, 'get_input_schema'):
            custom_schema = tool_instance.get_input_schema()
            if custom_schema:
                return {
                    "name": tool_name,
                    "description": tool_info["description"],
                    "inputSchema": custom_schema
                }
        
        return {
            "name": tool_name,
            "description": tool_info["description"],
            "inputSchema": self._default_schema
        }
    
    def get_tool_required_params(self, tool_name: str) -> list:
        """Get required parameters for a tool"""
        tool_instance = self._get_tool_instance(tool_name)
        if tool_instance and hasattr(tool_instance, 'get_input_schema'):
            custom_schema = tool_instance.get_input_schema()
            if custom_schema:
                return custom_schema.get("required", [])
        return []
    
    def validate_tool_params(self, tool_name: str, arguments: Dict[str, Any]) -> Optional[str]:
        """Validate tool parameters and return error message if invalid"""
        required_params = self.get_tool_required_params(tool_name)
        
        for param in required_params:
            if param not in arguments:
                return f"{param} is required for {tool_name}"
        
        return None
    
    def _get_tool_instance(self, tool_name: str):
        """Get tool instance by name"""
        tool_mapping = {
            "run_select_query": self.tool_handler.run_query,
            "list_databases": self.tool_handler.list_databases,
            "list_tables": self.tool_handler.list_tables,
            "describe_table": self.tool_handler.describe_table,
        }
        return tool_mapping.get(tool_name)


class SSEHandler:
    """Handles SSE (Server-Sent Events) connections and MCP protocol"""
    
    def __init__(self):
        self.tool_handler = ClickHouseToolHandler(
            host=CLICKHOUSE_HOST,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            port=CLICKHOUSE_PORT,
            secure=CLICKHOUSE_SECURE
        )
        self.schema_manager = SchemaManager(self.tool_handler)
        self.mcp = FastMCP(name="ClickHouse_MCP_Server")
        self.sse_transport = SseServerTransport("/messages/")
        self._register_mcp_tools()
    
    def _register_mcp_tools(self):
        """Register tools with MCP server using dynamic schema support"""
        available_tools = self.tool_handler.get_available_tools()
        
        for tool_name, tool_info in available_tools.items():
            tool_schema = self.schema_manager.get_tool_schema(tool_name, tool_info)
            input_schema = tool_schema["inputSchema"]
            
            def create_tool_function(name, schema, info):
                if name == "run_select_query":
                    @self.mcp.tool(name=name, description=info["description"])
                    def run_query_tool(sql: str) -> str:
                        """Execute SELECT query"""
                        return self.tool_handler.call_tool(name, sql=sql)
                    return run_query_tool
                
                elif name == "list_databases":
                    @self.mcp.tool(name=name, description=info["description"])
                    def list_db_tool() -> str:
                        """List all databases"""
                        return self.tool_handler.call_tool(name)
                    return list_db_tool
                
                elif name == "list_tables":
                    @self.mcp.tool(name=name, description=info["description"])
                    def list_tables_tool(database: str) -> str:
                        """List tables in database"""
                        return self.tool_handler.call_tool(name, database=database)
                    return list_tables_tool
                
                elif name == "describe_table":
                    @self.mcp.tool(name=name, description=info["description"])
                    def describe_table_tool(database: str, table: str) -> str:
                        """Describe table structure"""
                        return self.tool_handler.call_tool(name, database=database, table=table)
                    return describe_table_tool
                
                else:
                    @self.mcp.tool(name=name, description=info["description"])
                    def standard_tool() -> str:
                        """Standard tool function"""
                        return self.tool_handler.call_tool(name)
                    return standard_tool
            
            create_tool_function(tool_name, input_schema, tool_info)
    
    async def handle_sse_connection(self, request: Request):
        """Handle SSE connection with full MCP protocol"""
        logger.info("Handling SSE connection with full MCP protocol")
        try:
            async with self.sse_transport.connect_sse(request.scope, request.receive, request._send) as (in_stream, out_stream):
                await self.mcp._mcp_server.run(in_stream, out_stream, self.mcp._mcp_server.create_initialization_options())
        except Exception as e:
            logger.error(f"SSE connection error: {e}")
            raise
    
    def get_sse_transport(self):
        """Get SSE transport for mounting"""
        return self.sse_transport


class BridgeHandler:
    """Handles bridge mode connections (HTTP + SSE)"""
    
    def __init__(self):
        self.tool_handler = ClickHouseToolHandler(
            host=CLICKHOUSE_HOST,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            port=CLICKHOUSE_PORT,
            secure=CLICKHOUSE_SECURE
        )
        self.schema_manager = SchemaManager(self.tool_handler)
        self.connections: Dict[str, bool] = {}
    
    def process_bridge_request(self, request: MCPRequest) -> Dict[str, Any]:
        """Process MCP request for bridge mode"""
        try:
            if request.id is None:
                return {"jsonrpc": "2.0", "method": request.method, "params": request.params or {}}
            
            if request.method == "initialize":
                return {
                    "jsonrpc": "2.0",
                    "id": request.id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "clickhouse-mcp-server", "version": "1.0.0"}
                    }
                }
            
            elif request.method == "tools/list":
                available_tools = self.tool_handler.get_available_tools()
                tools_list = []
                
                for tool_name, tool_info in available_tools.items():
                    tool_schema = self.schema_manager.get_tool_schema(tool_name, tool_info)
                    tools_list.append(tool_schema)
                
                return {
                    "jsonrpc": "2.0",
                    "id": request.id,
                    "result": {"tools": tools_list}
                }
            
            elif request.method == "resources/list":
                return {"jsonrpc": "2.0", "id": request.id, "result": {"resources": []}}
            
            elif request.method == "prompts/list":
                return {"jsonrpc": "2.0", "id": request.id, "result": {"prompts": []}}
            
            elif request.method == "tools/call":
                tool_name = request.params.get("name")
                arguments = request.params.get("arguments", {})
                
                all_tools = self.tool_handler.get_available_tools()
                
                if tool_name not in all_tools:
                    return {
                        "jsonrpc": "2.0",
                        "id": request.id,
                        "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"}
                    }
                
                validation_error = self.schema_manager.validate_tool_params(tool_name, arguments)
                if validation_error:
                    return {
                        "jsonrpc": "2.0",
                        "id": request.id,
                        "error": {"code": -32602, "message": validation_error}
                    }
                
                result = self.tool_handler.call_tool(tool_name, **arguments)
                
                return {
                    "jsonrpc": "2.0",
                    "id": request.id,
                    "result": {
                        "content": [{"type": "text", "text": result}]
                    }
                }
            
            else:
                return {
                    "jsonrpc": "2.0",
                    "id": request.id,
                    "error": {"code": -32601, "message": f"Method not found: {request.method}"}
                }
        
        except Exception as e:
            logger.error(f"Error processing bridge request: {e}")
            return {
                "jsonrpc": "2.0",
                "id": getattr(request, 'id', 'unknown'),
                "error": {"code": -32603, "message": f"Internal error: {str(e)}"}
            }
    
    async def create_heartbeat_stream(self, request: Request):
        """Create heartbeat stream for bridge mode"""
        connection_id = str(uuid.uuid4())
        self.connections[connection_id] = True
        
        try:
            yield f"data: {json.dumps({'type': 'connection', 'id': connection_id})}\n\n"
            
            while self.connections.get(connection_id, False):
                yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': datetime.datetime.now().isoformat()})}\n\n"
                await asyncio.sleep(30)
                
                if await request.is_disconnected():
                    break
        except Exception as e:
            logger.error(f"Bridge connection error: {e}")
        finally:
            self.connections.pop(connection_id, None)
    
    def get_active_connections_count(self) -> int:
        """Get number of active bridge connections"""
        return len(self.connections)


class ClickHouseMCPServer:
    """Main server class"""
    
    def __init__(self):
        self.tool_handler = ClickHouseToolHandler(
            host=CLICKHOUSE_HOST,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            port=CLICKHOUSE_PORT,
            secure=CLICKHOUSE_SECURE
        )
        self.sse_handler = SSEHandler()
        self.bridge_handler = BridgeHandler()
        self.app = self._create_app()
    
    def _create_app(self) -> FastAPI:
        """Create and configure FastAPI application"""
        main_app = FastAPI(title="ClickHouse MCP Server Root", version="1.0.0")
        sub_app = FastAPI(title="ClickHouse MCP Server", version="1.0.0")
        
        sub_app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        self._register_routes(sub_app)
        sub_app.mount("/messages/", self.sse_handler.get_sse_transport().handle_post_message)
        main_app.mount(API_BASE_PATH, sub_app)
        
        return main_app
    
    def _register_routes(self, app: FastAPI):
        """Register all FastAPI routes"""
        
        @app.get("/")
        async def root():
            """Server information endpoint"""
            available_tools = list(self.tool_handler.get_available_tools().keys())
            
            return {
                "name": "ClickHouse MCP Server",
                "version": "1.0.0",
                "description": "MCP server for ClickHouse database operations",
                "modes": {"sse": "Full MCP SSE protocol", "bridge": "HTTP + SSE bridge mode"},
                "endpoints": {
                    "sse": f"{API_BASE_PATH}/sse",
                    "messages": f"{API_BASE_PATH}/messages/",
                    "message": f"{API_BASE_PATH}/message",
                    "health": f"{API_BASE_PATH}/health"
                },
                "tools": available_tools,
                "database": {
                    "host": CLICKHOUSE_HOST,
                    "port": CLICKHOUSE_PORT,
                    "secure": CLICKHOUSE_SECURE
                },
                "api_base_path": API_BASE_PATH
            }
        
        @app.get("/sse")
        async def sse_endpoint(request: Request):
            """Universal SSE endpoint with automatic mode detection"""
            accept_header = request.headers.get("accept", "").lower()
            
            if "text/event-stream" in accept_header:
                await self.sse_handler.handle_sse_connection(request)
            else:
                logger.info("Using bridge mode - heartbeat SSE")
                return StreamingResponse(
                    self.bridge_handler.create_heartbeat_stream(request),
                    media_type="text/event-stream",
                    headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "Access-Control-Allow-Origin": "*"}
                )
        
        @app.post("/message")
        async def bridge_message_handler(request: Request):
            """Handle MCP messages via HTTP POST for bridge mode"""
            try:
                body = await request.json()
                mcp_request = MCPRequest(**body)
                return self.bridge_handler.process_bridge_request(mcp_request)
            except Exception as e:
                logger.error(f"Bridge message error: {e}")
                return {
                    "jsonrpc": "2.0",
                    "id": body.get("id", "unknown") if 'body' in locals() else "unknown",
                    "error": {"code": -32603, "message": f"Internal error: {str(e)}"}
                }
        
        @app.get("/health")
        async def health_check():
            """Health check endpoint"""
            connection_ok = self.tool_handler.test_connection()
            available_tools = list(self.tool_handler.get_available_tools().keys())
            
            return {
                "status": "healthy" if connection_ok else "unhealthy",
                "timestamp": datetime.datetime.now().isoformat(),
                "database_connection": connection_ok,
                "activeBridgeConnections": self.bridge_handler.get_active_connections_count(),
                "tools": available_tools,
                "database": {
                    "host": CLICKHOUSE_HOST,
                    "port": CLICKHOUSE_PORT,
                    "secure": CLICKHOUSE_SECURE
                }
            }
    
    def run(self, host: str = None, port: int = None):
        """Run the server"""
        run_host = host or HOST
        run_port = port or PORT
        
        available_tools = list(self.tool_handler.get_available_tools().keys())
        
        print(" ClickHouse MCP Server")
        print("=" * 60)
        print(f"🗄️ Database: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
        print(f" Tools: {', '.join(available_tools)}")
        print(f" Total Tools: {len(available_tools)}")
        print(f" Base Path: {API_BASE_PATH}")
        print(f" SSE: http://{run_host}:{run_port}{API_BASE_PATH}/sse")
        print(f" Health: http://{run_host}:{run_port}{API_BASE_PATH}/health")
        print(f" Root: http://{run_host}:{run_port}{API_BASE_PATH}/")
        print("=" * 60)
        
        # Test connection before starting
        if self.tool_handler.test_connection():
            print(" Database connection successful")
        else:
            print(" Database connection failed - check credentials")
        
        uvicorn.run(self.app, host=run_host, port=run_port, log_level="info")


if __name__ == "__main__":
    server = ClickHouseMCPServer()
    server.run()
