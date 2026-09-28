# --- Superset MCP service (superset mcp run) ---------------------------------------
# AI agents (MCP clients) act in Superset as this user: create it with only the roles
# the agent needs (e.g. Gamma + database / dataset access). Required.
MCP_DEV_USERNAME = "mcp_agent"
# Links in the agent's answers
SUPERSET_WEBSERVER_ADDRESS = "https://superset.example.com"
WEBDRIVER_BASEURL_USER_FRIENDLY = "https://superset.example.com/"
# Local models: give them the plain tool list, not the search_tools / call_tool proxy
MCP_TOOL_SEARCH_CONFIG = {"enabled": False}
# Before exposing the service beyond localhost:
# MCP_AUTH_ENABLED = True
# MCP_JWT_ALGORITHM = "HS256"
# MCP_JWT_SECRET = "<secret>"
