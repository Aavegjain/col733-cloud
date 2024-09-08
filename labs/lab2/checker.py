import json

# Read and load JSON data from the files
r1 = open("checkpoints/Reducer_0_0.txt").read()
r2 = open("checkpoints/Reducer_1_0.txt").read()
r1 = json.loads(r1)
r2 = json.loads(r2)

# Combine the two JSON objects
r = r1 | r2

# Load the correct JSON data for comparison
correct = json.loads(open("seq.txt").read())

# Check if the combined JSON object matches the correct one
if r == correct:
    print("CORRECT")
else:
    print("INCORRECT")
    
    # Identify and print the differences
    for key in correct:
        if key not in r:
            print(f"Missing key in combined result: {key}")
        elif r[key] != correct[key]:
            print(f"Value mismatch for key '{key}': Expected {correct[key]}, but got {r[key]}")
    
    for key in r:
        if key not in correct:
            print(f"Extra key in combined result: {key}")

