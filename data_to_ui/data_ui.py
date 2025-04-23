from flask import Flask, jsonify, render_template
import os
import json

app = Flask(_name_)

# Path to your output file
OUTPUT_FILE_PATH = 'output.json'  # Change this to your actual file path


@app.route('/api/data', methods=['GET'])
def get_data():
    """
    Endpoint that reads data from the output file and returns it as JSON
    """
    try:
        # Check if file exists
        if not os.path.exists(OUTPUT_FILE_PATH):
            return jsonify({"error": "Output file not found"}), 404

        # Read the output file
        with open(OUTPUT_FILE_PATH, 'r') as file:
            # This assumes your output file is in JSON format
            # If it's in another format, you'll need to modify this part
            data = json.load(file)

        # Return the data as JSON
        return jsonify(data)

    except json.JSONDecodeError:
        return jsonify({"error": "Invalid JSON in output file"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/')
def index():
    """
    Serves the main HTML page that will fetch and display the data
    """
    return render_template('index.html')


# For handling different file formats, you might want additional endpoints
@app.route('/api/data/text', methods=['GET'])
def get_text_data():
    """
    Alternative endpoint for reading plain text output files
    """
    try:
        if not os.path.exists(OUTPUT_FILE_PATH):
            return jsonify({"error": "Output file not found"}), 404

        with open(OUTPUT_FILE_PATH, 'r') as file:
            data = file.read()

        return jsonify({"content": data})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if _name_ == '_main_':
    app.run(debug=True)