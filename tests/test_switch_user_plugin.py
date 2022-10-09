import os
import getpass
import pytest
import ray

@pytest.fixture
def set_runtime_env_plugins(request):
    runtime_env_plugins = getattr(request, "param", "0")
    try:
        os.environ["RAY_RUNTIME_ENV_PLUGINS"] = runtime_env_plugins
        yield runtime_env_plugins
    finally:
        del os.environ["RAY_RUNTIME_ENV_PLUGINS"]


@pytest.fixture
def ray_start_regular(request):  # pragma: no cover
    try:
        yield ray.init(num_cpus=16)
    finally:
        ray.shutdown()


@pytest.mark.parametrize(
    "set_runtime_env_plugins",
    [
        '[{"class":"runtime_env_plugins.switch_user_plugin.SwitchUserPlugin"}]',
    ],
    indirect=True,
)
def test_switch_user_plugin(set_runtime_env_plugins, ray_start_regular):

    @ray.remote
    def f():
        import getpass
        return getpass.getuser()

    current_user = getpass.getuser()
    user_name = ray.get(f.remote())
    assert user_name == current_user
    
    user_name = ray.get(f.options(runtime_env={"switch_user": "log"}).remote())
    assert user_name == "log"

if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
