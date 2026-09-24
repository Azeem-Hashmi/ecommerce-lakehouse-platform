from pathlib import Path
import yaml, os


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "configs" 


def load_yaml(file_path: Path) -> dict:
    """Load a YAML configuration file."""
    with file_path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def load_config(environment: str | None = None) -> dict:
    
    """Load project and environment-specific configuration."""

    environment = environment or os.getenv("APP_ENV", "dev")

    project_config = load_yaml(CONFIG_DIR / "project.yml")
    environment_config = load_yaml(CONFIG_DIR / f"{environment}.yml")

    return {
        "project": project_config.get("project", {}),
        "databricks": environment_config.get("databricks", {}),
        "schemas": environment_config.get("schemas", {}),
        "environment": environment_config.get("environment", {}),
        "logging": environment_config.get("logging", {}),
    }