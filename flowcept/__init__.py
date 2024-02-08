import flowcept
from flowcept.configs import SETTINGS_PATH
from flowcept.version import __version__

from flowcept.commons.vocabulary import Vocabulary

from flowcept.flowcept_api.consumer_api import FlowceptConsumerAPI
from flowcept.flowcept_api.task_query_api import TaskQueryAPI
from flowcept.flowcept_api import db_api


try:
    from flowcept.flowceptor.decorators.responsible_ai import (
        model_explainer,
        model_profiler,
    )
except Exception as _exp:
    flowcept.commons.logger.exception(_exp)


if Vocabulary.Settings.ZAMBEZE_KIND in flowcept.configs.ADAPTERS:
    try:
        from flowcept.flowceptor.adapters.zambeze.zambeze_interceptor import (
            ZambezeInterceptor,
        )
    except Exception as _exp:
        flowcept.commons.logger.error(
            flowcept.commons._get_adapter_exception_msg(
                Vocabulary.Settings.ZAMBEZE_KIND
            )
        )
        flowcept.commons.logger.exception(_exp)

if Vocabulary.Settings.TENSORBOARD_KIND in flowcept.configs.ADAPTERS:
    try:
        from flowcept.flowceptor.adapters.tensorboard.tensorboard_interceptor import (
            TensorboardInterceptor,
        )
    except Exception as _exp:
        flowcept.commons.logger.error(
            flowcept.commons._get_adapter_exception_msg(
                Vocabulary.Settings.TENSORBOARD_KIND
            )
        )
        flowcept.commons.logger.exception(_exp)

if Vocabulary.Settings.MLFLOW_KIND in flowcept.configs.ADAPTERS:
    try:
        from flowcept.flowceptor.adapters.mlflow.mlflow_interceptor import (
            MLFlowInterceptor,
        )
    except Exception as _exp:
        flowcept.commons.loggerr.error(
            flowcept.commons._get_adapter_exception_msg(
                Vocabulary.Settings.MLFLOW_KIND
            )
        )
        flowcept.commons.logger.exception(_exp)

if Vocabulary.Settings.DASK_KIND in flowcept.configs.ADAPTERS:
    try:
        from flowcept.flowceptor.adapters.dask.dask_plugins import (
            FlowceptDaskSchedulerAdapter,
            FlowceptDaskWorkerAdapter,
        )
    except Exception as _exp:
        flowcept.commons._get_adapter_exception_msg(
            Vocabulary.Settings.DASK_KIND
        )
        flowcept.commons.logger.exception(_exp)
