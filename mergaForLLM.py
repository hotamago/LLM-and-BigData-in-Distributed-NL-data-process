import os
import re
import argparse

def read_files_and_save(folder_path, regex_patterns, output_file):
    # Compile regex patterns for efficiency
    compiled_patterns = [re.compile(pattern) for pattern in regex_patterns]

    with open(output_file, 'w', encoding='utf-8') as outfile:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                # Check if the file path matches any of the regex patterns
                if any(pattern.search(file_path) for pattern in compiled_patterns):
                    outfile.write(f"Path: {file_path}\n")
                    outfile.write("```\n")
                    try:
                        with open(file_path, 'r', encoding='utf-8') as infile:
                            content = infile.read()
                        outfile.write(content)
                    except Exception as e:
                        outfile.write(f"Error reading file: {e}")
                    outfile.write("\n```\n\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Read files from a folder that match provided regex patterns and save their content to an output file."
    )
    parser.add_argument("folder", help="The folder path to search files in")
    parser.add_argument("--regex", nargs='+', required=True, help="List of regex patterns to match file paths")
    parser.add_argument("--output", default="output.txt", help="The output file to write the content to (default: output.txt)")

    args = parser.parse_args()
    read_files_and_save(args.folder, args.regex, args.output)

# python mergaForLLM.py ./ --regex "\.[\\\/]client.*\\?[^_]?\\?.*\.py$" "\.[\\\/]docker.*\\?[^_]?\\?.*docker-compose.yml$" --output all_content.txt