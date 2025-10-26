from fastapi import FastAPI

app = FastAPI(title="Fintracker API")

@app.get("/health")
def health():
    return {"ok": True}
