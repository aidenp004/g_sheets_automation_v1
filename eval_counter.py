import json
import os
from pathlib import Path
from collections import Counter

def count_decisions():
    """Count all fields from eval files"""
    evals_dir = Path("evals")
    
    # Track counts for each field
    my_decisions = []
    gate_correct = []
    gate_missed = []
    expected_llm_override = []
    
    # Get all eval JSON files
    eval_files = sorted(evals_dir.glob("eval_*.json"))
    
    if not eval_files:
        print("No eval files found in evals/ directory")
        return
    
    print(f"Found {len(eval_files)} eval files\n")
    
    # Process each eval file
    for eval_file in eval_files:
        try:
            with open(eval_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Extract all fields from manual_label
                if "manual_label" in data:
                    label = data["manual_label"]
                    
                    if "my_decision" in label and label["my_decision"]:
                        my_decisions.append(label["my_decision"])
                    
                    if "gate_correct" in label:
                        gate_correct.append(label["gate_correct"])
                    
                    if "gate_missed" in label and label["gate_missed"] is not None:
                        gate_missed.append(label["gate_missed"])
                    
                    if "expected_llm_override" in label and label["expected_llm_override"]:
                        expected_llm_override.append(label["expected_llm_override"])
        except Exception as e:
            print(f"Error processing {eval_file.name}: {e}")
    
    # Count and display results
    print("=" * 50)
    print("MY_DECISION COUNTS")
    print("=" * 50)
    if my_decisions:
        for decision, count in Counter(my_decisions).most_common():
            print(f"{decision}: {count} times")
        print(f"Total: {len(my_decisions)}")
    else:
        print("No decisions found")
    
    print("\n" + "=" * 50)
    print("GATE_CORRECT COUNTS")
    print("=" * 50)
    if gate_correct:
        for value, count in Counter(gate_correct).most_common():
            print(f"{value}: {count} times")
        print(f"Total: {len(gate_correct)}")
    else:
        print("No gate_correct values found")
    
    print("\n" + "=" * 50)
    print("GATE_MISSED COUNTS")
    print("=" * 50)
    if gate_missed:
        for value, count in Counter(gate_missed).most_common():
            print(f"{value}: {count} times")
        print(f"Total: {len(gate_missed)}")
    else:
        print("No gate_missed values found")
    
    print("\n" + "=" * 50)
    print("EXPECTED_LLM_OVERRIDE COUNTS")
    print("=" * 50)
    if expected_llm_override:
        for value, count in Counter(expected_llm_override).most_common():
            print(f"{value}: {count} times")
        print(f"Total: {len(expected_llm_override)}")
    else:
        print("No expected_llm_override values found")

if __name__ == "__main__":
    count_decisions()
