from datetime import datetime
from kafka import KafkaProducer
import time



class ProducerMonitor:
    def __init__(self, producer: KafkaProducer):
        self.producer = producer
        self.metrics = {
            'messages_sent': 0,
            'start_time': time.time(),
            'topic_metrics': {
                'page_views': {'count': 0, 'last_sent': None},
                'click_events': {'count': 0, 'last_sent': None},
                'user_interactions': {'count': 0, 'last_sent': None}
            }
        }
    
    def send_with_monitoring(self, topic: str, value: dict):
        self.metrics['messages_sent'] += 1
        self.metrics['topic_metrics'][topic]['count'] += 1
        self.metrics['topic_metrics'][topic]['last_sent'] = datetime.now()

        self.producer.send(topic, value)

        if self.metrics['messages_sent'] % 100 == 0:
            self._print_metrics()
    
    def _print_metrics(self):
        elapsed_time = time.time() - self.metrics['start_time']
        messages_per_second = self.metrics['messages_sent']

        print(f"\nProducer Performance Metrics:")
        print(f"Total messages sent: {self.metrics['messages_sent']}")
        print(f"Average rate: {messages_per_second:.2f}, messages/second")
        print(f"\nPer-topic metrics:")
        for topic, metrics in self.metrics['topic_metrics'].items():
            print(f"{topic}: {metrics['count']} messages")
