import pytest

from rx_lakehouse.io import get_spark


@pytest.fixture(scope="session")
def spark():
    s = get_spark("rx-lakehouse-tests")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def lakehouse(tmp_path_factory, spark, monkeypatch_session):
    """Run the whole pipeline once on a small generated dataset."""
    from rx_lakehouse import pipeline
    from rx_lakehouse.generate import generate

    root = tmp_path_factory.mktemp("lake")
    generate(root / "raw", days=14, seed=3)
    monkeypatch_session.setenv("LAKEHOUSE_RAW_PATH", str(root / "raw"))
    monkeypatch_session.setenv("LAKEHOUSE_ROOT", str(root / "lakehouse"))
    monkeypatch_session.setenv("LAKEHOUSE_FORMAT", "parquet")
    report = pipeline.run("all")
    return root, report


@pytest.fixture(scope="session")
def monkeypatch_session():
    mp = pytest.MonkeyPatch()
    yield mp
    mp.undo()
