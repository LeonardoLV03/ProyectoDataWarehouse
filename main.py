import uvicorn
import uuid
import os
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from typing import  Dict

from etl_pipeline import run_full_pipeline

app = FastAPI(
    title="Proyecto ETL de Calidad del Aire",
    description="API para procesar archivos de calidad del aire y ejecutar un pipeline ETL."
)

# Tracking de procesos en memoria (en producción usaría Redis o similar)
db_procesos: Dict[str, Dict] = {}

# Carpetas para archivos temporales
UPLOAD_DIR = "temp_uploads"
OUTPUT_DIR = "clean_data"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


def ejecutar_etl_en_background(process_id: str, path_hist: str, path_meas: str, path_json: str):
    # Worker que corre en background
    print(f"[{process_id}] Empezando ETL...")
    db_procesos[process_id]["status"] = "procesando"
    
    try:
        output_path = os.path.join(OUTPUT_DIR, f"{process_id}_clean.csv")
        
        # Ejecutar el pipeline
        success, stats_or_error = run_full_pipeline(path_hist, path_meas, path_json, output_path)
        
        if success:
            db_procesos[process_id]["status"] = "completado"
            db_procesos[process_id]["estadisticas"] = stats_or_error
            db_procesos[process_id]["resultado_path"] = output_path
            print(f"[{process_id}] Terminado OK")
        else:
            db_procesos[process_id]["status"] = "error"
            db_procesos[process_id]["error"] = str(stats_or_error)
            print(f"[{process_id}] Falló: {stats_or_error}")
            
    except Exception as e:
        db_procesos[process_id]["status"] = "error_critico"
        db_procesos[process_id]["error"] = str(e)
        print(f"[{process_id}] Error crítico: {e}")
    
@app.post("/iniciar-etl/", status_code=202)
async def iniciar_etl(
    background_tasks: BackgroundTasks,
    file_history: UploadFile = File(..., description="Archivo CSV"),
    file_measures: UploadFile = File(..., description="Archivo XLSX"),
    file_json: UploadFile = File(..., description="Archivo JSON")
):
    # Recibe los 3 archivos y lanza el ETL en background
    process_id = str(uuid.uuid4())
    print(f"Nueva solicitud ETL - ID: {process_id}")
    
    try:
        # Guardar los archivos subidos
        path_hist = os.path.join(UPLOAD_DIR, f"{process_id}_{file_history.filename}")
        path_meas = os.path.join(UPLOAD_DIR, f"{process_id}_{file_measures.filename}")
        path_json = os.path.join(UPLOAD_DIR, f"{process_id}_{file_json.filename}")
        
        with open(path_hist, "wb") as f:
            f.write(await file_history.read())
        with open(path_meas, "wb") as f:
            f.write(await file_measures.read())
        with open(path_json, "wb") as f:
            f.write(await file_json.read())
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error guardando archivos: {e}")

    # Registrar proceso
    db_procesos[process_id] = {
        "status": "iniciado",
        "mensaje": "Archivos recibidos. Proceso ETL en cola.",
        "archivos": [file_history.filename, file_measures.filename, file_json.filename]
    }
    
    # Agregar tarea al background
    background_tasks.add_task(
        ejecutar_etl_en_background,
        process_id,
        path_hist,
        path_meas,
        path_json
    )
    
    # Retornar el ID inmediatamente
    return {"process_id": process_id, "status": "iniciado"}


@app.get("/estado-etl/{process_id}")
async def consultar_estado_etl(process_id: str):
    # Consulta para ver como va el proceso
    proceso = db_procesos.get(process_id)
    
    if not proceso:
        raise HTTPException(status_code=404, detail="Proceso no encontrado")
    
    return proceso

if __name__ == "__main__":
    # Para correr: uvicorn main:app --reload
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)