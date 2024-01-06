import grpc
from station_pb2 import RecordTempsRequest, RecordTempsReply, StationMaxRequest, StationMaxReply
from station_pb2_grpc import add_StationServicer_to_server, StationServicer
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.query import SimpleStatement
from concurrent import futures
import cassandra

class StationService(StationServicer):
    def __init__(self):
        self.cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
        self.cass = self.cluster.connect('weather')
        self.prepare_statements()

    def prepare_statements(self):
        self.insert_statement = self.cass.prepare("INSERT INTO weather.stations (id, date, record) VALUES (?, ?, ?)")
        self.insert_statement.consistency_level = ConsistencyLevel.ONE
        self.max_statement = self.cass.prepare("SELECT MAX(record.tmax) FROM weather.stations WHERE id = ?")
        self.max_statement.consistency_level = ConsistencyLevel.THREE

    def RecordTemps(self, request, context):
        try:
            self.cass.execute(
                self.insert_statement,
                (request.station, request.date, (request.tmin, request.tmax))
            )
            return RecordTempsReply(error="")
        except Exception as e:
            error_message = self.error_handling(e)
            return RecordTempsReply(error=error_message)

    def StationMax(self, request, context):
        try:
            result = self.cass.execute(self.max_statement, (request.station,))
            max_temperature = result.one()[0] if result else None
            return StationMaxReply(tmax=max_temperature, error="")
        except Exception as e:
            error_message = self.error_handling(e)
            return StationMaxReply(tmax=None, error=error_message)

    def error_handling(self, e):
        if isinstance(e, cassandra.Unavailable):
            return f"need {e.required_replicas} replicas, but only have {e.alive_replicas}"
        elif isinstance(e, cassandra.cluster.NoHostAvailable):
            for error in e.errors.values():
                if isinstance(error, cassandra.Unavailable):
                    return f"need {error.required_replicas} replicas, but only have {error.alive_replicas}"
        else:
            return str(exception)


def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=3), options=(('\grpc.so_reuseport', 0),))
    add_StationServicer_to_server(StationService(), server)
    server.add_insecure_port("[::]:5440", )
    server.start()
    print("Started")
    server.wait_for_termination()


if __name__ == "__main__":
    main()

