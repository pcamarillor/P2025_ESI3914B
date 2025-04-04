from pyspark.sql.streaming import StreamingQueryListener

class TrafficListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        num_rows = event.progress.numInputRows
        print(f"Query made progress: {event.progress}")

        if num_rows >= 50:
            print("ALERT: The volume of data if high (numInputRows >= 50)")

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")