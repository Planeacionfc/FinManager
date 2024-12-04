from flask import Flask, render_template
from controller.cmiController import cmiController
from controller.ctsController import ctsController


app = Flask(
    __name__, 
    template_folder='view/templates',
    static_folder='view/static'
    )

app.register_blueprint(cmiController.cmi_bp, url_prefix='/cmi')
app.register_blueprint(ctsController.cts_bp, url_prefix='/cts')

@app.route('/')
def login():
    return render_template('login.html')

@app.route('/home')
def home():
    return render_template('home.html')

@app.route('/documentation')
def documentation():
    return render_template('documentacion.html')

@app.route('/404')
def page_404():
    return render_template('404.html')

if __name__ == '__main__':
    app.run(host='172.15.1.161', port=8510)
