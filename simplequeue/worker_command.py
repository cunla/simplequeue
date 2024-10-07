import json
import logging
import os
import signal
from typing import Any, Dict, Self

from redis import Redis

from rq.exceptions import InvalidJobOperation
from rq.job import Job

logger = logging.getLogger("rq.worker")


class WorkerCommand:
    _WORKER_COMMANDS: Dict[str, Self] = dict()
    _PUBSUB_CHANNEL_TEMPLATE = 'rq:worker-pubsub:{}'
    command_name: str

    @classmethod
    def _register_commands(cls):
        for cmd_cls in cls.__subclasses__():
            cls._WORKER_COMMANDS[cmd_cls.command_name] = cmd_cls()

    @classmethod
    def worker_pubsub_channel(cls, worker_name: str) -> str:
        return cls._PUBSUB_CHANNEL_TEMPLATE.format(worker_name)

    @classmethod
    def execute_command(cls, worker: 'Worker', message: Dict[Any, Any]) -> None:
        if worker is None:
            logger.warning(f'Received command without a worker: {message}')
            return
        payload = json.loads(message['data'].decode())
        command_name = payload.pop('command')
        if command_name is None:
            worker.log.warning(f'Received command without a command name: {payload}')
            return
        if command_name not in cls._WORKER_COMMANDS:
            cls._register_commands()
        if command_name not in cls._WORKER_COMMANDS:
            worker.log.warning(f'Unknown command: {command_name}')
            return
        worker_command = cls._WORKER_COMMANDS[command_name]
        worker_command.execute(worker, **payload)

    def send_command(self, connection: Redis, worker_name: str, **kwargs):
        payload = {'command': self.command_name}
        if kwargs:
            payload.update(kwargs)
        connection.publish(self.worker_pubsub_channel(worker_name), json.dumps(payload))

    def execute(self, worker: 'Worker', **kwargs: Any):
        raise NotImplementedError()


class ShutdownCommand(WorkerCommand):
    command_name = 'shutdown'

    def execute(self, worker: 'Worker', **kwargs: Any):
        worker.log.info('Received shutdown command, sending SIGINT signal.')
        pid = os.getpid()
        os.kill(pid, signal.SIGINT)


class KillHorseCommand(WorkerCommand):
    command_name = 'kill-horse'

    def execute(self, worker: 'Worker', **kwargs: Any):
        worker.log.info('Received kill horse command.')
        if worker.horse_pid:
            worker.log.info('Kiling horse...')
            worker.kill_horse()
        else:
            worker.log.info('Worker is not working, kill horse command ignored')


class StopJobCommand(WorkerCommand):
    command_name = 'stop-job'

    def send_command(self, connection: Redis, job_id: str, serializer=None, **kwargs):
        job = Job.fetch(job_id, connection=connection, serializer=serializer)
        if job is None or job.worker_name is None:
            raise InvalidJobOperation('Job is not currently executing')
        super().send_command(connection, job.worker_name, job_id=job_id)

    def execute(self, worker: 'Worker', job_id:str, **kwargs: Any):
        worker.log.debug('Received command to stop job %s', job_id)
        if job_id and worker.get_current_job_id() == job_id:
            # Sets the '_stopped_job_id' so that the job failure handler knows it
            # was intentional.
            worker._stopped_job_id = job_id
            worker.kill_horse()
        else:
            worker.log.info('Not working on job %s, command ignored.', job_id)
