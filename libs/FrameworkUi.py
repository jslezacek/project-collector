from flask import Flask, render_template
import configparser

# app = Flask(__name__, template_folder='templates')
app = Flask(__name__, template_folder='../templates', static_folder='../static')


@app.route('/')
def default():
    return render_template('index.html')


def start():
    app.run()
