import os
import re
import sys

def extract_selected_tests_count(output):
    # Regex to find the final summary line with test counts
    match = re.search(r'(\d+)/(\d+) tests collected \((\d+) deselected\)', output)
    if match == None:
        return 0
    if match:
        # Extract the number of selected tests
        selected_tests = int(match.group(1))
        return selected_tests
    else:
        print("Error: Unable to parse pytest output")
        return None

def verify_unmarked_tests(unmarked_tests):

    sys.exit(unmarked_tests != 0)

# Retrieve pytest output from the environment variable
pytest_output = os.getenv('pytest_output')

if pytest_output:
    unmarked_tests = extract_selected_tests_count(pytest_output)
    result = verify_unmarked_tests(unmarked_tests)
    print(f"Selected tests: {unmarked_tests}")
else:
    print("Error: pytest_output environment variable not set")
