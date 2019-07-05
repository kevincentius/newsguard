
from flask import Flask, request, g
from flask_cors import CORS
import json
from bson import json_util

from predictor import predictor

class Server:
	def __init__(self):
		app = Flask(__name__)
		CORS(app)

		@app.route('/', methods=['GET'])
		def get():
			view_id = request.args.get('url')
			return response(predictor.request(view_id))

		app.run(port=34985, threaded=True)

def start_server():
	# disable logging
	import logging
	log = logging.getLogger('werkzeug')
	log.disabled = True

	Server()

def response(obj, status=200):
    return json.dumps(obj, default=json_util.default), status

start_server()
