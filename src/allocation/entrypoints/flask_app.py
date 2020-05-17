from datetime import datetime
from flask import Flask, jsonify, request
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys

sys.path.append(r'C:\Users\vlonc_000\Documents\08 code\learnproj\learnproj\code')
from src.allocation import config
from src.allocation.domain import model
from src.allocation.adapters import orm, repository
from src.allocation.service_layer import services, unit_of_work

orm.start_mappers()
#get_session = sessionmaker(bind=create_engine(config.get_postgres_uri()))
app = Flask(__name__)


@app.route("/add_batch", methods=['POST'])
def add_batch():
    uow = unit_of_work.SqlAlchemyUnitOfWork()
    eta = request.json['eta']
    if eta is not None:
        eta = datetime.fromisoformat(eta).date()
    services.add_batch(
        request.json['ref'], request.json['sku'], request.json['qty'], eta, uow)
    return 'OK', 201


@app.route("/allocate", methods=['POST'])
def allocate_endpoint():
   # return jsonify({'message': 'dentro.. al enpoint'})
    uow = unit_of_work.SqlAlchemyUnitOfWork()
    try:
       batchref = services.allocate(request.json['orderid'], request.json['sku'],
                                    request.json['qty'], uow)
       # batchref=request.json['orderid']+"lalalla"
    except (model.OutOfStock, services.InvalidSku) as e:
        return jsonify({'message': str(e)}), 400
    return jsonify({'batchref': batchref}), 201
