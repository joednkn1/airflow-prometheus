from pathlib import Path

from poetry_publish.publish import poetry_publish

import airflow_prometheus


def publish():
    poetry_publish(
        package_root=Path(airflow_prometheus.__file__).parent.parent,
        version=airflow_prometheus.__version__,
    )
