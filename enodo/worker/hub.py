import logging
from random import randrange
from enodo.model.config.worker import WorkerConfigModel
from enodo.net import Package


class HubClient:

    def __init__(self, hub_id, writer, worker_config: dict):
        self.hub_id = hub_id
        self.writer = writer
        self.worker_config = WorkerConfigModel(**worker_config)

    def send(self, data, pt):
        if data is None:
            data = ""
        pkg = Package.make(
            pt,
            data=data
        )
        self.writer.write(pkg.to_bytes())


class ClientManager:
    clients = {}

    @classmethod
    def add_client(cls, client: HubClient, hub_id: str):
        cls.clients[hub_id] = client

    @classmethod
    def get_client(cls, hub_id: str):
        return cls.clients.get(hub_id)

    @classmethod
    def get_random_client(cls) -> HubClient:
        if len(cls.clients) == 0:
            return None
        if len(ClientManager.clients) == 1:
            return list(cls.clients.values())[0]
        return list(cls.clients.values())[randrange(0, len(cls.clients) - 1)]
