"""
Mock module for notebookutils that provides local development replacements
for Fabric/Synapse-specific functionality.
"""
import logging
import os
from pathlib import Path
from typing import Dict, Optional

_logger = logging.getLogger(__name__)

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    # Load .env from project root
    project_root = Path(__file__).parent.parent
    env_file = project_root / ".env"
    if env_file.exists():
        load_dotenv(env_file)
except ImportError:
    pass  # python-dotenv is optional


class MockCredentials:
    """Mock for mssparkutils.credentials. Uses Azure Key Vault via SDK, with env-var fallback."""

    @staticmethod
    def getSecret(key_vault_url: str, secret_name: str) -> str:
        """
        Retrieve secret from Azure Key Vault or fall back to environment variables.

        Tries Key Vault first (uses DefaultAzureCredential / az login). Falls back
        to env vars on failure (offline dev or local overrides).
        """
        # Try Azure Key Vault first (uses az login credentials)
        if key_vault_url and key_vault_url.strip():
            try:
                from azure.identity import DefaultAzureCredential
                from azure.keyvault.secrets import SecretClient

                client = SecretClient(
                    vault_url=key_vault_url, credential=DefaultAzureCredential()
                )
                return client.get_secret(secret_name).value
            except Exception as e:
                _logger.warning(
                    "Key Vault get_secret failed for %s: %s. Falling back to env vars.",
                    secret_name,
                    e,
                )
                pass

        # Fallback to env vars (offline dev or overrides)
        env_var_name = secret_name.replace("-", "_").upper()
        value = os.getenv(env_var_name)
        if value is None:
            fallback_name = secret_name.split("-")[-1].upper()
            value = os.getenv(fallback_name)
        if value is None:
            raise ValueError(
                f"Secret '{secret_name}' not found in Key Vault ({key_vault_url}) or env vars. "
                f"Run 'az login' or set env var '{env_var_name}'."
            )
        return value


class MockRuntimeContext:
    """Mock for notebookutils.runtime.context."""

    @staticmethod
    def get(key: str) -> Optional[str]:
        """Get runtime context values."""
        # For local dev, return mock values
        context_values = {
            "currentWorkspaceId": os.getenv("FABRIC_WORKSPACE_ID", "local-dev-workspace-id")
        }
        return context_values.get(key)


class MockNotebook:
    """Mock for mssparkutils.notebook. Captures exit values for testing."""

    def __init__(self):
        self.exit_value = None

    def exit(self, value: str) -> None:
        """Mock notebook.exit() — stores the exit value instead of terminating."""
        _logger.info("mssparkutils.notebook.exit called with value: %s", value)
        self.exit_value = value


class MockMSSparkUtils:
    """Mock for mssparkutils module."""

    def __init__(self):
        self.credentials = MockCredentials()
        self.notebook = MockNotebook()


class MockNotebookUtils:
    """Mock for notebookutils module."""

    def __init__(self):
        self.mssparkutils = MockMSSparkUtils()

        class Runtime:
            def __init__(self):
                self.context = MockRuntimeContext()

        self.runtime = Runtime()


# Create singleton instance
_notebookutils = MockNotebookUtils()


def get_mock_notebookutils():
    """Get the mock notebookutils instance."""
    return _notebookutils
