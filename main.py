import argparse
from datetime import datetime
from src.log_parser.parser import LogParser
from pyspark.sql.functions import col
from pyspark.sql import DataFrame

def main() -> None:
    """
    Main function to analyze network logs. It parses command-line arguments and performs operations
    based on the user input such as fetching connections to or from a hostname, and identifying the most active host.
    
    Raises:
        ValueError: If required arguments for the 'custom_period' operation are missing.
    """
    parser = argparse.ArgumentParser(description="Network Log Analyzer")
    parser.add_argument("file", type=str, help="Path to the log file")
    parser.add_argument("operation", type=str, choices=['connections_to', 'connections_from', 'most_active', 'custom_period'],
                        help="Operation to perform")
    parser.add_argument("--hostname", type=str, help="Hostname to query connections")
    parser.add_argument("--current_time", type=str, default=None,
                        help="Current time in ISO format YYYY-MM-DDTHH:MM:SS for last hour operations")
    parser.add_argument("--time_init", type=str, default=None, help="Start time in ISO format YYYY-MM-DDTHH:MM:SS for custom period")
    parser.add_argument("--time_end", type=str, default=None, help="End time in ISO format YYYY-MM-DDTHH:MM:SS for custom period")

    args = parser.parse_args()

    # Initialize LogParser
    log_parser = LogParser()

    # Read logs once to avoid multiple file reading
    df_sorted: DataFrame = log_parser.read_logs(args.file)

    # Determine current time if not provided, used for last hour calculations
    # current_time: int = int(datetime.fromisoformat(args.current_time).timestamp() * 1000) if args.current_time else int(datetime.utcnow().timestamp() * 1000)
    current_time: int = 1565733598341  # Placeholder static time for example
    
    # Handle custom period operation
    if args.operation == 'custom_period':
        if not all([args.time_init, args.time_end, args.hostname]):
            raise ValueError("Hostname, time_init, and time_end are required for custom period operation")
        time_init: int = int(datetime.fromisoformat(args.time_init).timestamp() * 1000)
        time_end: int = int(datetime.fromisoformat(args.time_end).timestamp() * 1000)
        connections_custom: DataFrame = log_parser.get_connections(df_sorted.filter((col('timestamp') >= time_init) & (col('timestamp') <= time_end)), args.hostname)
        print(f"Connections to {args.hostname} between {args.time_init} and {args.time_end}:")
        connections_custom.show()

    # Handle last hour operations
    elif args.operation == 'connections_to':
        connections_to: DataFrame = log_parser.get_connections_to(df_sorted, args.hostname, current_time)
        print(f"Connections TO {args.hostname} in the last hour:")
        connections_to.show()

    elif args.operation == 'connections_from':
        connections_from: DataFrame = log_parser.get_connections_from(df_sorted, args.hostname, current_time)
        print(f"Connections FROM {args.hostname} in the last hour:")
        connections_from.show()

    elif args.operation == 'most_active':
        most_active: DataFrame = log_parser.most_active_host(df_sorted, current_time)
        print("Hostname that generated most connections in the last hour:")
        most_active.show()

if __name__ == "__main__":
    main()
