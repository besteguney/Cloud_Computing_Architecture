input_csv = """
;1;2;4;8
blackscholes;2m7.008s;1m13.910s;0m48.683s;0m38.071s
canneal;5m44.740s;3m11.324s;2m42.236s;2m29.547s
dedup;0m20.977s;0m12.377s;0m10.911s;0m9.900s
ferret;5m25.166s;2m46.313s;1m37.466s;1m22.493s
freqmine;8m23.625s;4m14.923s;2m8.615s;1m44.457s
radix;0m58.782s;0m30.912s;0m15.404s;0m10.613s
vips;1m44.726s;0m53.833s;0m28.130s;0m23.953s
"""

# Function to convert time string to seconds
def time_to_seconds(time_str):
    if 'm' in time_str:
        minutes, seconds = time_str.split('m')
        seconds = seconds[:-1]  # Remove the 's' at the end
        return int(minutes) * 60 + float(seconds)
    else:
        return float(time_str[:-1])  # Remove the 's' at the end and return

output_csv = []
for line in input_csv.strip().split('\n'):
    parts = line.split(';')
    if 'm' in parts[1]:  # Skip the header row
        converted_parts = [parts[0]] + [str(time_to_seconds(part)) for part in parts[1:]]
    else:
        converted_parts = parts
    output_csv.append(';'.join(converted_parts))

# Join the converted lines back into a single string to get the final CSV format
output_csv_str = '\n'.join(output_csv)
print(output_csv_str)