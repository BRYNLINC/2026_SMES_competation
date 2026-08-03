import asyncio
import copy
import logging
import multiprocessing as mp
import uuid
from queue import Empty

from Algorithm.method.worker.PredictWorkerProcess import predict_worker_main


class PredictWorkerTimeoutError(TimeoutError):
    pass


class PredictWorkerManager:
    __DEFAULT_PREDICT_TIMEOUT_SECONDS = 1.0

    def __init__(
        self,
        predict_timeout_seconds: float | None = None,
        method_config: dict | None = None,
    ):
        self.__logger = logging.getLogger('algorithmLogger')
        self.__ctx = mp.get_context('spawn')
        if predict_timeout_seconds in (None, ''):
            predict_timeout_seconds = self.__DEFAULT_PREDICT_TIMEOUT_SECONDS
        self.__predict_timeout_seconds = float(predict_timeout_seconds)
        self.__session_sync_timeout_seconds = max(10.0, self.__predict_timeout_seconds * 5.0)
        self.__command_queue = None
        self.__result_queue = None
        self.__process = None
        self.__latest_session_payload: dict | None = None
        self.__session_loaded: bool = False
        self.__method_config = copy.deepcopy(method_config or {})

    def set_method_config(self, method_config: dict | None) -> None:
        self.__method_config = copy.deepcopy(method_config or {})
        if self.__process is not None:
            self.__process.terminate()
            self.__process.join(1.0)
            self.__process = None
            self.__command_queue = None
            self.__result_queue = None
            self.__session_loaded = False

    def set_timeout_seconds(self, predict_timeout_seconds: float) -> None:
        self.__predict_timeout_seconds = float(predict_timeout_seconds)
        self.__session_sync_timeout_seconds = max(10.0, self.__predict_timeout_seconds * 5.0)

    def get_timeout_seconds(self) -> float:
        return self.__predict_timeout_seconds

    async def sync_session(
        self,
        runtime_config: dict,
        stage_signature: tuple[str, str, str, str],
        sample_rate: int,
        channel_number: int,
        trial_point: int,
        model_state_dict: dict,
    ) -> None:
        self.__latest_session_payload = {
            'runtime_config': copy.deepcopy(runtime_config),
            'stage_signature': list(stage_signature),
            'sample_rate': int(sample_rate),
            'channel_number': int(channel_number),
            'trial_point': int(trial_point),
            'model_state_dict': copy.deepcopy(model_state_dict),
        }
        await self.__start_worker_if_needed()
        await self.__load_latest_session_payload()

    async def predict(self, trial_data) -> str:
        await self.__start_worker_if_needed()
        if not self.__session_loaded:
            await self.__load_latest_session_payload()
        request_id = str(uuid.uuid4())
        command_enqueue_wallclock = asyncio.get_running_loop().time()
        self.__logger.debug(
            'predict worker 提交请求: request_id=%s stage_signature=%s timeout_seconds=%s trial_shape=%s',
            request_id,
            self.__latest_session_payload.get('stage_signature') if isinstance(self.__latest_session_payload, dict) else None,
            self.__predict_timeout_seconds,
            getattr(trial_data, 'shape', None),
        )
        self.__command_queue.put(
            {
                'command': 'predict',
                'request_id': request_id,
                'trial_data': trial_data,
            }
        )
        try:
            response = await self.__wait_for_response(
                timeout_seconds=self.__predict_timeout_seconds,
                matcher=lambda payload: payload.get('type') == 'predict_result'
                and payload.get('request_id') == request_id,
            )
        except PredictWorkerTimeoutError:
            await self.__hard_restart_worker('predict timeout')
            raise
        except Exception:
            await self.__hard_restart_worker('predict failure')
            raise
        self.__logger.debug(
            'predict worker 请求完成: request_id=%s stage_signature=%s wait_ms=%.3f',
            request_id,
            self.__latest_session_payload.get('stage_signature') if isinstance(self.__latest_session_payload, dict) else None,
            (asyncio.get_running_loop().time() - command_enqueue_wallclock) * 1000.0,
        )
        return response.get('result')

    async def shutdown(self) -> None:
        if self.__process is None:
            return
        if self.__process.is_alive():
            try:
                self.__command_queue.put({'command': 'shutdown'})
                await asyncio.to_thread(self.__process.join, 0.3)
            except Exception:
                self.__logger.exception('predict worker graceful shutdown failed')
        if self.__process.is_alive():
            self.__process.terminate()
            await asyncio.to_thread(self.__process.join, 1.0)
        self.__process = None
        self.__command_queue = None
        self.__result_queue = None
        self.__session_loaded = False

    async def __start_worker_if_needed(self) -> None:
        if self.__process is not None and self.__process.is_alive():
            return
        await self.shutdown()
        self.__command_queue = self.__ctx.Queue()
        self.__result_queue = self.__ctx.Queue()
        self.__process = self.__ctx.Process(
            target=predict_worker_main,
            args=(self.__command_queue, self.__result_queue, copy.deepcopy(self.__method_config)),
            daemon=True,
        )
        self.__process.start()
        self.__session_loaded = False
        self.__logger.info('predict worker started: pid=%s', self.__process.pid)

    async def __load_latest_session_payload(self) -> None:
        if self.__latest_session_payload is None:
            raise RuntimeError('predict worker has no session payload to load')
        session_token = str(uuid.uuid4())
        self.__command_queue.put(
            {
                'command': 'load_session',
                'session_token': session_token,
                **copy.deepcopy(self.__latest_session_payload),
            }
        )
        response = await self.__wait_for_response(
            timeout_seconds=self.__session_sync_timeout_seconds,
            matcher=lambda payload: payload.get('type') == 'load_session_ready'
            and payload.get('session_token') == session_token,
        )
        self.__session_loaded = True
        self.__logger.info(
            'predict worker session synced: stage_signature=%s device=%s sync_timeout_seconds=%s predict_timeout_seconds=%s',
            response.get('stage_signature'),
            response.get('device'),
            self.__session_sync_timeout_seconds,
            self.__predict_timeout_seconds,
        )

    async def __hard_restart_worker(self, reason: str) -> None:
        self.__logger.warning('predict worker hard restart: reason=%s', reason)
        if self.__process is not None and self.__process.is_alive():
            self.__process.terminate()
            await asyncio.to_thread(self.__process.join, 1.0)
        self.__process = None
        self.__command_queue = None
        self.__result_queue = None
        self.__session_loaded = False
        if self.__latest_session_payload is not None:
            await self.__start_worker_if_needed()
            await self.__load_latest_session_payload()

    async def __wait_for_response(self, timeout_seconds: float, matcher) -> dict:
        deadline = asyncio.get_running_loop().time() + float(timeout_seconds)
        while True:
            if self.__process is not None and not self.__process.is_alive():
                exitcode = self.__process.exitcode
                raise RuntimeError(f'predict worker process exited unexpectedly: exitcode={exitcode}')
            remaining_seconds = deadline - asyncio.get_running_loop().time()
            if remaining_seconds <= 0:
                raise PredictWorkerTimeoutError(f'predict worker timed out after {timeout_seconds}s')
            try:
                payload = await asyncio.to_thread(self.__result_queue.get, True, remaining_seconds)
            except Empty as exc:
                if self.__process is not None and not self.__process.is_alive():
                    exitcode = self.__process.exitcode
                    raise RuntimeError(
                        f'predict worker process exited unexpectedly while waiting for response: exitcode={exitcode}'
                    ) from exc
                raise PredictWorkerTimeoutError(f'predict worker timed out after {timeout_seconds}s') from exc
            if payload.get('type') == 'worker_error':
                raise RuntimeError(
                    f"worker command failed: command={payload.get('command')} error={payload.get('error')}"
                )
            if matcher(payload):
                return payload
            self.__logger.warning('收到未匹配的 predict worker 响应，已忽略: %s', payload)
