# Network Log Parser

## Project Description
This project involves parsing newline-terminated, space-separated log files that record network connections between hosts, formatted as follows:

<unix_timestamp> <hostname> <hostname>

Examples include:

1366815793 quark garak
1366815795 brunt quark
1366815811 lilac garak

Each line represents a connection from a host (left) to another host (right) at a given time. The lines are roughly sorted by timestamp but may be out of order by a maximum of 5 minutes.

## Goals
### 1. Data Parsing
Develop a tool that, given a file in the above format, a starting datetime (`init_datetime`), an ending datetime (`end_datetime`), and a hostname, can parse the file and return:
- A list of hostnames that connected to the specified host during the given period.

### 2. Unlimited Input Parser (Optional)
Enhance the tool to continuously parse both previously written log files and new log files as they are being written, with the capability to run indefinitely. The tool should output once every hour:
- A list of hostnames that connected to a specified (configurable) host during the last hour.
- A list of hostnames that received connections from a specified (configurable) host during the last hour.
- The hostname that generated the most connections in the last hour.

## Technology
The tool leverages **PySpark** for efficient data processing using its parallel and distributed computing capabilities. This allows for handling large datasets with optimal CPU usage and scalability.

## Project Structure
log_parser/
│
├── logs/
│ └── data.txt
│
├── src/
│ ├── init.py
│ ├── parser.py
│
├── tests/
│ ├── init.py
│ ├── test_log_parser.py
│
├── .devcontainer/
│ ├── Dockerfile
│ └── devcontainer.json
│
├── main.py
├── .flake8
├── Makefile
├── README.md
├── requirements.txt
├── pyproject.toml

## Development Environment Setup
1. **Install Visual Studio Code (VSCode)**: Download and install VSCode from the [official site](https://code.visualstudio.com/).

2. **Install Docker**: Ensure Docker is installed and running on your system. You can download it from the [Docker official website](https://www.docker.com/products/docker-desktop).

3. **Install the Remote - Containers Extension in VSCode**: Open VSCode, go to the Extensions view by clicking on the square icon on the sidebar, and search for 'Remote - Containers' and 'Dev Containers'. Install this extension.

4. **Open Project in a Dev Container**:
    - Open the Remote Explorer by clicking on the left side icon or pressing `Ctrl+Shift+P` and typing 'Remote Explorer'.
    - From the Remote Explorer sidebar, select 'Dev Containers'.
    - Click the '+' sign and select 'Open Folder in Container'.
    - Navigate to the directory where your project is located, ensure it includes the `.devcontainer/devcontainer.json` file, and select the folder.
    - VSCode will build the Docker environment as defined in `devcontainer.json` and open the project within a containerized environment.

## Building the Package
To build the `log_parser` package, execute the following command:
```bash
pip install .
```
## Running the Project
The project uses a Makefile to simplify running specific tasks. Open a terminal in VSCode (ensure it is within the Dev Container) and navigate to the project's root directory. You can first define your settings for operations in the top lines of the Makefile and accordingly run the following commands:

```bash
make run-connections-to       # Run the app to find connections to a specific host in the last hour.
make run-connections-from     # Run the app to find connections from a specific host in the last hour.
make run-most-active          # Run the app to find the most active host in the last hour.
make run-custom-period        # Run the main application with custom period.
```


