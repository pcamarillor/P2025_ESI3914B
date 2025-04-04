from pyspark.sql.streaming import StreamingQueryListener

class QueryListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        if event.progress.numInputRows > 50:
            print("Data volume is high")

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")