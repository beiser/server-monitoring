import os
import time
import logging
import subprocess
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
url = os.getenv("INFLUXDB_URL")
token = os.getenv("INFLUXDB_TOKEN")
org = os.getenv("INFLUXDB_ORG")
bucket = os.getenv("INFLUXDB_BUCKET")

if not all([url, token, org, bucket]):
    logger.error("Missing required environment variables")
    exit(1)

logger.info(f"Connecting to InfluxDB at {url} ...")
try:
    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    logger.info("Connected to InfluxDB successfully")
except Exception as e:
    logger.error(f"Failed to connect to InfluxDB: {e}")
    exit(1)

def get_tcp_connections():
    try:
        # Run the full pipeline: ss -nt state established '( sport = :443 )' | awk '{print $4}' | cut -d: -f1 | sort | uniq | wc -l
        result = subprocess.run(
            "ss -nt state established '( sport = :443 )' | awk '{print $4}' | cut -d: -f1 | sort | uniq | wc -l",
            shell=True, capture_output=True, text=True
        )
        if result.returncode == 0:
            count = int(result.stdout.strip())
            return count
        else:
            logger.error(f"Command failed: {result.stderr}")
            return 0
    except Exception as e:
        logger.error(f"Failed to get TCP connections: {e}")
        return 0

while True:
    try:
        tcp_count = get_tcp_connections()
        point = (
            Point("tcp_connections")
            .tag("host", "localhost")
            .field("count", tcp_count)
            .time(time.time_ns(), WritePrecision.NS)
        )

        write_api.write(bucket=bucket, org=org, record=point)
        logger.info(f"Wrote TCP connections: {tcp_count}")
    except Exception as e:
        logger.error(f"Failed to write TCP data: {e}")

    time.sleep(60)