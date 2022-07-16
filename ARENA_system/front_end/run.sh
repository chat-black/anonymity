# If the gunicorn is installed in a virtualenv, you can change the dir to automatically load the environment
# source Env/flask/bin/activate
gunicorn -c ./gunicorn.conf.py app:app
