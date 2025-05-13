from pyspark.sql.streaming import StreamingQueryListener
from datetime import datetime

class StreamingPerformanceMonitor(StreamingQueryListener):

    def __init__(self):
        self.metrics_history = {
            'page_views': [],
            'click_events': [],
            'user_interactions': []
        }

    def onQueryStarted(self,event):
        print(f"Query started: {event.id} at {datetime.now()}")
    
    def onQueryProgress(self, event):
        processed_rows_per_second = event.progress.processedRowsPerSecond
        num_input_rows = event.progress.numInputRows
        input_rows_per_second = event.progress.inputRowsPerSecond

        duration_ms = event.progress.durationMs
        processing_time = duration_ms.get('triggerExecution', 0)
        add_batch_time = duration_ms.get('addBatch', 0)
        get_batch_time = duration_ms.get('getBatch', 0)

        #Record the metrics
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'processed_rows_per_second': processed_rows_per_second,
            'num_input_rows': num_input_rows,
            'input_rows_per_second': input_rows_per_second,
            'processing_time_ms': processing_time,
            'add_batch_time_ms': add_batch_time,
            'get_batch_time_ms': get_batch_time
        }

        # Get topic name from the query name or use a default
        try:
            # Try to get the topic from the query name
            if event.progress.name:
                topic = event.progress.name.split('_')[0]
            else:
                # If name is None, try to get it from the sink
                if hasattr(event.progress, 'sink') and event.progress.sink:
                    topic = event.progress.sink.description.split('/')[-1]
                else:
                    # If we can't determine the topic, use a default
                    topic = 'unknown'
        except Exception as e:
            print(f"Warning: Could not determine topic name: {e}")
            topic = 'unknown'
        
        # Store metrics based on topic
        if topic in self.metrics_history:
            self.metrics_history[topic].append(metrics)
        else:
            self.metrics_history['unknown'] = self.metrics_history.get('unknown', []) + [metrics]

        print(f"\nPerformance summary for {topic}:")
        print(f"Processed {num_input_rows} rows at {processed_rows_per_second:.2f} rows/second")
        print(f"Input rate: {input_rows_per_second:.2f} rows/second")
        print(f"Processing time: {processing_time}ms")


    def onQueryTerminated(self,event):
        print(f"Query terminated: {event.id} at {datetime.now()}")

def analyze_performance_data(metrics_history):
    for topic, metrics in metrics_history.items():
        if not metrics:
            continue
    
        print(f"\nPerformance Analysis fro {topic}:")

        avg_processed_rate = sum(m['processed_rows_per_second'] for m in metrics) / len(metrics)
        avg_processing_time = sum(m['processing_time_ms'] for m in metrics) / len(metrics)

        max_processed_rate = max(m['processed_rows_per_second'] for m in metrics)
        max_processing_time = max(m['processing_time_ms'] for m in metrics)

        print(f"\nAverage processing rate: {avg_processed_rate:.2f} rows/second")
        print(f"Peak processing rate: {max_processed_rate:.2f} rows/second")
        print(f"Average processing time: {avg_processing_time:.2f}ms")
        print(f"Peak processing time: {max_processing_time}ms")

