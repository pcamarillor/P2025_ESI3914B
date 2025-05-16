from datetime import datetime
import time
import json
from kafka import KafkaProducer

class ProducerMonitor:
    def __init__(self, producer: KafkaProducer):
        self.producer = producer
        self.metrics = {
            'messages_sent': 0,
            'total_bytes_sent': 0,
            'start_time': time.time(),
            'topic_metrics': {
                'page_views': {'count': 0, 'total_bytes': 0},
                'click_events': {'count': 0, 'total_bytes': 0},
                'user_interactions': {'count': 0, 'total_bytes': 0}
            }
        }
    
    def _calculate_message_size(self, message: dict) -> int:
        """Calculate the size of a message in bytes after JSON serialization"""
        return len(json.dumps(message).encode('utf-8'))
    
    def send_with_monitoring(self, topic: str, value: dict):
        # Calculate and track message size
        message_size = self._calculate_message_size(value)
        self.metrics['messages_sent'] += 1
        self.metrics['total_bytes_sent'] += message_size
        self.metrics['topic_metrics'][topic]['count'] += 1
        self.metrics['topic_metrics'][topic]['total_bytes'] += message_size
        
        # Send the message
        self.producer.send(topic, value)
    
    def print_final_summary(self):
        """Print final summary of the producer's performance"""
        elapsed_time = time.time() - self.metrics['start_time']
        
        print("\n=== Final Producer Summary ===")
        print(f"Total Runtime: {elapsed_time:.2f} seconds")
        print(f"Total Messages: {self.metrics['messages_sent']:,}")
        print(f"Total Data: {self.metrics['total_bytes_sent']:,} bytes")
        
        print("\nPer-Topic Analysis:")
        for topic, metrics in self.metrics['topic_metrics'].items():
            if metrics['count'] > 0:
                avg_size = metrics['total_bytes'] / metrics['count']
                print(f"\n{topic}:")
                print(f"  Messages: {metrics['count']:,}")
                print(f"  Total Data: {metrics['total_bytes']:,} bytes")
                print(f"  Average Message Size: {avg_size:.2f} bytes")