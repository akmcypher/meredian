"""Topic creation is not ready for an arbitrary discovery broker until metadata propagates."""
import unittest
from types import SimpleNamespace
from unittest.mock import patch
import test_kafka as certification


class MetadataReadiness(unittest.TestCase):
    def test_waits_for_lagging_broker_even_when_creator_already_sees_the_topic(self):
        class Client:
            def __init__(self, delayed=False):
                self.calls = 0
                self.delayed = delayed

            def list_topics(self, name, timeout):
                self.calls += 1
                ready = not self.delayed or self.calls > 1
                return SimpleNamespace(topics={name: SimpleNamespace(
                    partitions={0: SimpleNamespace(leader=1)} if ready else {})})

        brokers = 'broker-a:1,broker-b:2,broker-c:3'
        clients = {broker: Client(delayed=broker == 'broker-c:3') for broker in brokers.split(',')}
        creator = Client()
        creator.create_topics = lambda topics: {
            topics[0].topic: SimpleNamespace(result=lambda timeout: None)}
        case = certification.KafkaCluster()
        case.admin = creator
        with patch.object(certification, 'BROKERS', brokers), \
             patch.object(certification, 'AdminClient', side_effect=lambda config: clients[config['bootstrap.servers']]), \
             patch.object(certification.time, 'sleep'):
            case.topic(1)
        self.assertTrue(all(client.calls for client in clients.values()),
                        'discovery can bootstrap through any configured broker')
        self.assertGreater(clients['broker-c:3'].calls, 1,
                           'fixture returned before the lagging broker had topic metadata')


if __name__ == '__main__':
    unittest.main()
