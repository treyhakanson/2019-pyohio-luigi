# 2019 PyOhio Luigi Talk

Code example to go along with my talk [Dynamic Data Pipelining with Luigi](https://www.youtube.com/watch?v=5kSnkNDeQjQ) at PyOhio 2019.

To get started:

```sh
# To run a postgres db, if you don't want to run one yourself
docker-compose up -d

# Setup python environment
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt

# Run luigi!
PYTHONPATH='.' luigi --module tasks UploadTask --fname "./data/sample.csv" --local-scheduler
```

## Additional Reading

- [Official Luigi Example](https://luigi.readthedocs.io/en/stable/example_top_artists.html)
