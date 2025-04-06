from pyspark.sql.streaming import StreamingQueryListener

class TrafficListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        num_rows = event.progress.numInputRows
        print(f"Query made progress: {event.progress}")
        print(f"Rows processed in this batch: {num_rows}")

        if num_rows >= 50:
            print("[ALERTA] ¡Se detectó un alto volumen de datos en este microbatch!")

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")
