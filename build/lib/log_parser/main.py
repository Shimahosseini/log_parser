import argparse
from datetime import datetime
from .parser import LogParser

def main():
    parser = argparse.ArgumentParser(description="Parse log files for network connections.")
    parser.add_argument("file", type=str, help="Path to the log file")
    parser.add_argument("time_init", type=str, help="Start time in ISO format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("time_end", type=str, help="End time in ISO format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("hostname", type=str, help="Hostname to query connections")

    args = parser.parse_args()

    # Convert ISO format time to UNIX timestamp
    time_init = int(datetime.fromisoformat(args.time_init).timestamp())
    time_end = int(datetime.fromisoformat(args.time_end).timestamp())

    # Initialize LogParser and execute the query
    log_parser = LogParser()
    connections = log_parser.run(args.file, time_init, time_end, args.hostname)

    # Output the results
    print(f"Connections to and from {args.hostname} between {args.time_init} and {args.time_end}:")
    print(f"To: {connections['to']}")
    print(f"From: {connections['from']}")

if __name__ == "__main__":
    main()

