import pytest

pyspark = pytest.importorskip("pyspark")  # noqa: F841


@pytest.fixture(scope="session")
def spark():
    from sismos.procesamiento.sesion import crear_sesion

    sesion = crear_sesion("pruebas")
    yield sesion
    sesion.stop()
