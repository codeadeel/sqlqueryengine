#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import os
import uvicorn

# %%
# Server Configuration
SERVER_HOST = os.environ.get("SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(os.environ.get("SERVER_PORT", "8080"))
SERVER_WORKERS = int(os.environ.get("SERVER_WORKERS", "1"))

# %%
# Execution
if __name__ == "__main__":
    uvicorn.run(
        "sqlQueryEngine:app",
        host=SERVER_HOST,
        port=SERVER_PORT,
        workers=SERVER_WORKERS
    )
