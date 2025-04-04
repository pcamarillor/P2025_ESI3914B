from pyspark.sql.streaming import StreamingQueryListener

class TrafficListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        num_input_rows = event.progress.numInputRows
        print(f"Query made progress: {event.progress}")

        if num_input_rows >= 50:
            print("High volume of data! Input rows exceed 50.")

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")