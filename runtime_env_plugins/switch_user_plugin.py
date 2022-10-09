import ray
import logging
from typing import List, Optional
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray.runtime_env.runtime_env import RuntimeEnv


class SwitchUserPlugin(RuntimeEnvPlugin):
    name = "switch_user"

    @staticmethod
    def validate(runtime_env_dict: dict) -> None:
        """Validate user entry for this plugin.

        Args:
            runtime_env_dict: the user-supplied runtime environment dict.

        Raises:
            ValueError: if the validation fails.
        """
        pass

    def get_uris(self, runtime_env: "RuntimeEnv") -> List[str]:  # noqa: F821
        return []

    async def create(
        self,
        uri: Optional[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> float:
        """Create and install the runtime environment.

        Gets called in the runtime env agent at install time. The URI can be
        used as a caching mechanism.

        Args:
            uri: A URI uniquely describing this resource.
            runtime_env: The RuntimeEnv object.
            context: auxiliary information supplied by Ray.
            logger: A logger to log messages during the context modification.

        Returns:
            the disk space taken up by this plugin installation for this
            environment. e.g. for working_dir, this downloads the files to the
            local node.
        """
        return 0

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> None:
        """Modify context to change worker startup behavior.

        For example, you can use this to preprend "cd <dir>" command to worker
        startup, or add new environment variables.

        Args:
            uris: The URIs used by this resource.
            runtime_env: The RuntimeEnv object.
            context: Auxiliary information supplied by Ray.
            logger: A logger to log messages during the context modification.
        """
        switch_user_name = runtime_env.get(self.name)
        if switch_user_name:
            context.command_prefix = ["sudo su " + switch_user_name] + context.command_prefix
        return

    def delete_uri(self, uri: str, logger: logging.Logger) -> float:
        """Delete the the runtime environment given uri.

        Args:
            uri: a URI uniquely describing this resource.

        Returns:
            the amount of space reclaimed by the deletion.
        """
        return 0
