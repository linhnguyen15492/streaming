from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
import os
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("PG_HOST")
PORT = os.getenv("PG_PORT")
DATABASE = os.getenv("PG_DATABASE")
USERNAME = os.getenv("PG_USER")
PASSWORD = os.getenv("PG_PASSWORD")
TABLE = os.getenv("PG_TABLE")


def create_session_source_kafka(t_env):
    table_name = "tip_events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            PULocationID INTEGER,
            tip_amount DOUBLE,
            -- Chuyển đổi sang TIMESTAMP và xử lý chính xác
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def create_session_sink_postgres(t_env):
    table_name = 'tip_aggregate'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PULocationID INT,
            total_tip DOUBLE,
            PRIMARY KEY (window_start, window_end, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://{HOST}:{PORT}/{DATABASE}',
            'table-name' = '{table_name}',
            'username' = '{USERNAME}',
            'password' = '{PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def run_aggregate_tip_job():
    # Khởi tạo môi trường (tương tự như script trước của bạn)
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    try:
        source = create_session_source_kafka(t_env)
        sink = create_session_sink_postgres(t_env)

        # Thực thi logic Session Window
        # Cửa sổ đóng khi không có sự kiện mới trong 5 phút (GAP)
        t_env.execute_sql(f"""
            INSERT INTO {sink}
            SELECT 
                window_start,
                window_end,
                PULocationID,
                SUM(tip_amount) AS total_tip
            FROM TABLE(
                TUMBLE(TABLE {source}, DESCRIPTOR(event_timestamp), INTERVAL '1' HOURS)
            )
            GROUP BY window_start, window_end, PULocationID
        """)
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    run_aggregate_tip_job()

# docker exec -it streaming-jobmanager-1 flink run -py /opt/src/job/aggregate_tip_job.py
