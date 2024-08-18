from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
CORS(app)

# Configure the SQLite database
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///graphdb.db'
db = SQLAlchemy(app)

class Namespace(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), unique=True, nullable=False)

# Ensure the database is created within the app context
with app.app_context():
    db.create_all()

BLAZEGRAPH_URL = 'http://localhost:9999/blazegraph/sparql'

# Route for the root URL
@app.route('/')
def home():
    return "Welcome to the GraphDB Backend!"

# Route to handle the favicon.ico request
@app.route('/favicon.ico')
def favicon():
    return "", 204

@app.route('/create_namespace', methods=['POST'])
def create_namespace():
    namespace = request.json.get('namespace')
    if Namespace.query.filter_by(name=namespace).first():
        return jsonify({'status': 'error', 'text': 'Namespace already exists.'})
    
    new_namespace = Namespace(name=namespace)
    db.session.add(new_namespace)
    db.session.commit()

    headers = {'Content-Type': 'text/plain'}
    payload = f'''
    com.bigdata.namespace.{namespace}.spo.com.bigdata.btree.BTree.branchingFactor=1024
    com.bigdata.namespace.{namespace}.lex.com.bigdata.btree.BTree.branchingFactor=400
    com.bigdata.rdf.sail.isolatableIndices=false
    com.bigdata.rdf.store.AbstractTripleStore.textIndex=false
    com.bigdata.rdf.store.AbstractTripleStore.justify=false
    com.bigdata.rdf.store.AbstractTripleStore.axiomsClass=com.bigdata.rdf.axioms.NoAxioms
    com.bigdata.namespace.{namespace}.graphMode=false
    com.bigdata.rdf.store.AbstractTripleStore.quads=false
    com.bigdata.rdf.store.AbstractTripleStore.statementIdentifiers=false
    com.bigdata.rdf.sail.truthMaintenance=false
    com.bigdata.rdf.sail.namespace={namespace}
    com.bigdata.rdf.sail.axiomsClass=com.bigdata.rdf.axioms.NoAxioms
    com.bigdata.rdf.sail.isolatableIndices=false
    com.bigdata.rdf.sail.truthMaintenance=false
    com.bigdata.rdf.sail.quads=false
    '''
    response = requests.post(f'{BLAZEGRAPH_URL}?verb=POST', headers=headers, data=payload)
    return jsonify({'status': response.status_code, 'text': response.text})

@app.route('/upload_ttl', methods=['POST'])
def upload_ttl():
    file = request.files['file']
    namespace = request.form.get('namespace')
    headers = {'Content-Type': 'application/x-turtle'}
    response = requests.post(f'{BLAZEGRAPH_URL}?context-uri={namespace}', headers=headers, data=file.read())
    return jsonify({'status': response.status_code, 'text': response.text})

if __name__ == '__main__':
    app.run(debug=True)
