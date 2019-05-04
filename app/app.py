from flask import Flask, render_template, request, session, url_for, redirect

import json

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == "__main__":
    app.run('127.0.0.1', 8888, debug = True)