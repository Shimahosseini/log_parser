.PHONY: test run help run-connections-to run-connections-from run-most-active run-custom-period

# Set default Python
PYTHON=python3

# Default settings for operations
HOST=Talya
START_DATE="2019-01-01T00:00:00"
END_DATE="2019-08-13T21:00:00"
FILE_PATH=logs/input-file-10000.txt

# Help command to display callable targets
help:
	@echo "Available commands:"
	@echo "  make test                     - Run unit tests."
	@echo "  make run-connections-to       - Run the app to find connections to a specific host in the last hour."
	@echo "  make run-connections-from     - Run the app to find connections from a specific host in the last hour."
	@echo "  make run-most-active          - Run the app to find the most active host in the last hour."
	@echo "  make run-custom-period        - Run the main application with custom period."
	@echo "  make help                     - Display this help message."

# Run unit tests using PyTest
test:
	@echo "Running unit tests..."
	@$(PYTHON) -m pytest tests/

# Run main application to find connections to a specific host in the last hour
run-connections-to:
	@echo "Analyzing connections to $(HOST) in the last hour..."
	@$(PYTHON) main.py $(FILE_PATH) connections_to --hostname $(HOST)

# Run main application to find connections from a specific host in the last hour
run-connections-from:
	@echo "Analyzing connections from $(HOST) in the last hour..."
	@$(PYTHON) main.py $(FILE_PATH) connections_from --hostname $(HOST)

# Run main application to find the most active host in the last hour
run-most-active:
	@echo "Finding the most active host in the last hour..."
	@$(PYTHON) main.py $(FILE_PATH) most_active

# Run main application with a custom period
run-custom-period:
	@echo "Running main application for custom period..."
	@$(PYTHON) main.py $(FILE_PATH) custom_period --hostname $(HOST) --time_init $(START_DATE) --time_end $(END_DATE)
