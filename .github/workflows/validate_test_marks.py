import os
import re

def extract_selected_tests_count(output):
    # Regex to find the final summary line with test counts
    match = re.search(r'(\d+)/(\d+) tests collected \((\d+) deselected\)', output)
    if match:
        # Extract the number of selected tests
        selected_tests = int(match.group(1))
        return selected_tests
    else:
        print("Error: Unable to parse pytest output")
        return None

def verify_unmarked_tests(selected_tests):
    if selected_tests > 0:
        return False
    return True

# Retrieve pytest output from the environment variable
pytest_output = os.getenv('PYTEST_OUTPUT')

if pytest_output:
    selected_tests = extract_selected_tests_count(pytest_output)
    result = verify_unmarked_tests(selected_tests)
    print(f"Selected tests count: {selected_tests}")
    print(f"Unmarked tests: {result}")
else:
    print("Error: PYTEST_OUTPUT environment variable not set")
