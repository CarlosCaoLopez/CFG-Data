from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict, Any

app = FastAPI(title="Receptor de Resultados R")

# --- Función de Base de Datos (Placeholder) ---
def mock_db_save(data: Dict[str, Any]):
    """
    Simula la inserción en PostgreSQL.
    """
    print(f"DEBUG: Insertando en DB -> {data}")
    return True

# --- Modelo de datos ---
class RResult(BaseModel):
    task_id: str
    summary_metrics: Dict[str, float]
    processed_at: str

@app.post("/store-results")
async def store_results(data: RResult):
    # Convertimos a diccionario para la función de DB
    save_status = mock_db_save(data.model_dump())

    if save_status:
        return {"status": "success", "message": "Resultados guardados"}
    return {"status": "error", "message": "Error al guardar"}