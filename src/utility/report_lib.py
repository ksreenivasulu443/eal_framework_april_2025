import datetime
import os

# Ensure the 'report' directory exists
eal_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
report_dir = os.path.join(eal_path, "reports")
os.makedirs(report_dir, exist_ok=True)

# Create a single report filename for the session
timestamp = datetime.datetime.now().strftime("%d%m%Y%H%M%S")  #07072025072345
report_filename = os.path.join(report_dir, f"report_{timestamp}.txt")
print(report_filename)


def write_output(validation_type, status, details, number_of_failures=0):
    # Write the output to the report file
    with open(report_filename, "a") as report:
        report.write(f"{validation_type}: {status}\nDetails: {details}\n\n")
