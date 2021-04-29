from statsd import StatsClient

from simplekv.db.sql import SQLAlchemyStore
from sqlalchemy import create_engine, MetaData, exc
from prometheus_client.core import GaugeMetricFamily
import time

from airflow.settings import SQL_ALCHEMY_CONN


def int_to_bytes(x: int) -> bytes:
    return str(x).encode()


def int_from_bytes(xbytes: bytes) -> int:
    return int(xbytes.decode())


class PrometheusStatsClient(StatsClient):
    """If no StatsLogger is configured, DummyStatsLogger is used as a fallback"""

    engine = None
    metadata = None
    store = None

    @classmethod
    def describe(cls):
        return []

    @classmethod
    def collect(cls):
        """Collect metrics."""
        store = cls.get_store()
        print("HELLO!")
        lol = GaugeMetricFamily(
            "lol",
            "lol",
            labels=["key"],
        )
        for key in list(store.keys()):
            print(f"Iterate {key}")
            lol.add_metric([key], cls.get_key(store, key))
        yield lol

    @classmethod
    def get_store(cls):
        if cls.engine is not None:
            return cls.store
        cls.engine = create_engine(SQL_ALCHEMY_CONN, echo=False)#'sqlite:///:memory:', echo=True)
        cls.metadata = MetaData(bind=cls.engine)
        cls.store = SQLAlchemyStore(cls.engine, cls.metadata, 'kvstore')
        try:
            cls.metadata.create_all()
        except Exception:
            pass
        return cls.store

    @classmethod
    def get_key(cls, store, key, default_value=0):
        while True:
            try:
                return int_from_bytes(store.get(key))
            except KeyError:
                cls.set_key(store, key, default_value)
            except exc.OperationalError:
                time.sleep(1)
            time.sleep(1)
            print(f"Retry get {key}")

    @classmethod
    def set_key(cls, store, key, value):
        try:
            store.put(key, int_to_bytes(value))
        except exc.OperationalError:
            time.sleep(1)
            store.put(key, int_to_bytes(value))

    @classmethod
    def perform_add(cls, stat, delta):
        store = cls.get_store()
        while True:
            val = cls.get_key(store, stat)
            expected = val + delta
            cls.set_key(store, stat, expected)
            if cls.get_key(store, stat) != expected:
                cls.set_key(store, stat, val)
                time.sleep(1)
            break

    @classmethod
    def incr(cls, stat, count=1, rate=1):
        cls.perform_add(stat, count)

    @classmethod
    def decr(cls, stat, count=1, rate=1):
        cls.perform_add(stat, -count)

    @classmethod
    def gauge(cls, stat, value, rate=1, delta=False):
        store = cls.get_store()
        cls.set_key(store, stat, value)


