from flask import Blueprint, render_template, jsonify

cts_bp = Blueprint('cts_bp', __name__)

@cts_bp.route('/home')
def home():
    return render_template('/cts/home.html')

