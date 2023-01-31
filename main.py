from flask import Flask, jsonify
import os
from apscheduler.schedulers.background import BackgroundScheduler
from jobs_api import sync
from sudoswap import SudoSwapService
from flask_cors import CORS
from models import migrate

# Schedule the sync job to every minute
sched = BackgroundScheduler(daemon=True)
sched.add_job(sync, 'cron', hour=1)
sched.start()


app = Flask(__name__)
sudoswap = SudoSwapService()
CORS(app)


@app.route('/')
def health():
    return jsonify({"Status": "OK."})


@app.route('/collections/<collection_id>')
def all_collections(collection_id):
    res = {}

    collection = sudoswap.get_collection(collection_id)
    res["collection"] = dict(collection)

    pools_rows = sudoswap.get_pools_for_collection(collection_id)
    pools = [dict(row) for row in pools_rows]
    res["pools"] = pools

    timeseries_rows = sudoswap.get_timeseries(collection_id)
    timeseries = [dict(row) for row in timeseries_rows]
    res["timeseries"] = timeseries

    return jsonify(res)


@app.route('/collections')
def collections():
    result_set = sudoswap.get_all_collections()
    res = [dict(row) for row in result_set]
    return jsonify(res)


@app.route('/refresh')
def refresh():
    sync()
    return jsonify({"Status": "OK."})


@app.route('/pools')
def pools():
    result_set = sudoswap.get_pools()
    res = [dict(row) for row in result_set]
    return jsonify(res)


if __name__ == '__main__':
    migrate()
    sync()
    # Trigger manually the sync job
    app.run(debug=True, port=os.getenv("PORT", default=5000))
