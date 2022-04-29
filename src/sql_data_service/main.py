from fastapi import FastAPI

from . import __version__

app = FastAPI()


@app.get("/")
def get_status():
    return {"status": "OK", "version": __version__}
