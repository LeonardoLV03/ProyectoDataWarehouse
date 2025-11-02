import pandas as pd
import json

def extract_data(csv_history_path, csv_measures_path, json_air_path):
    # Cargar los archivos y devolver dataframes
    try:
        df_history = pd.read_csv(csv_history_path, encoding='latin-1')
        print(f"Leídos {len(df_history)} registros del historial")

        df_measures = pd.read_excel(csv_measures_path)
        print(f"Cargadas {len(df_measures)} mediciones")

        with open(json_air_path, 'r') as f:
            data_json = json.load(f)

        column_names = [col['name'] for col in data_json['meta']['view']['columns']]

        json_air_path_meta = pd.DataFrame(data_json['data'], columns=column_names)
        df_air_json = json_air_path_meta.iloc[:, 8:]
        
        return df_history, df_measures, df_air_json
    
    except FileNotFoundError as e:
        print(f"No encuentro el archivo: {e}")
        return None, None, None
    except Exception as e:
        print(f"Algo salió mal cargando los datos: {e}")
        return None, None, None

def transform_data(df_history, df_measures, df_json):
    # Limpieza y transformaciones básicas
    print("Empezando transformaciones...")
    
    try:
        df_history['DATETIME_LOCAL'] = pd.to_datetime(df_history['DATETIME_LOCAL'], errors='coerce')
        df_history['AQI'] = pd.to_numeric(df_history['AQI'], errors='coerce')
        
        # Eliminamos las filas que tengan AQI nulo
        df_history.dropna(subset=['AQI'], inplace=True)
        # Eliminamos county_name
        df_history.drop(columns=['COUNTY_NAME'], inplace=True, errors='ignore')
        
        # cambiamos todos los nombres a minúsculas
        df_history['CITY_NAME'] = df_history['CITY_NAME'].str.lower()
        df_history['STATE_NAME'] = df_history['STATE_NAME'].str.lower()

        print("Proceso de limpieza y transformacion en CSV listo")

    except Exception as e:
        print(f"Error con la limpieza y transformacion en el archivo CSV: {e}")

    # Procesar measures
    try:
        # Transformaciones a realizar:
            #Mantener solo los registros que tratan con los dias
            #Eliminar columnas: County fips, monitor only, state fips, unit, unit name
        filtros = ['Percent of days with PM2.5 levels over the National Ambient Air Quality Standard (monitor and modeled data)', 
        'Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard (monitor and modeled data)']
        df_measures = df_measures[df_measures['MeasureName'].isin(filtros)]
        #df_measures.groupby('MeasureName').count()
        mask = df_measures['MeasureName'] == 'Percent of days with PM2.5 levels over the National Ambient Air Quality Standard (monitor and modeled data)'
        df_measures.loc[mask, 'Value'] = ((df_measures.loc[mask, 'Value'] / 100) * 365).round()
        df_measures.drop(columns=['CountyFips', 'StateFips', 'MonitorOnly', 'Unit', 'UnitName'], inplace=True)
        df_measures
        #df_measures['Value'] = pd.to_numeric(df_measures['Value'], errors='coerce')
        #df_measures['StateName'] = df_measures['StateName'].str.lower()
        #df_measures['CountyName'] = df_measures['CountyName'].str.lower()
        print("Proceso de limpieza y transformacion en XLSX listo")
    except Exception as e:
        print(f"Error con la limpieza y transformacion en el archivo XLSX: {e}")
        
    # Procesar JSON - renombrar columnas
    try:
        df_json.drop(columns=['Message','Geo Join ID','Indicator ID'],inplace=True)
        df_json['Start_Date'] = pd.to_datetime(df_json['Start_Date'])
        df_json['Data Value'] = pd.to_datetime(df_json['Data Value'])

        print("Proceso de limpieza y transformacion en JSON listo")
    except Exception as e:
        print(f"Problema con las columnas del JSON: {e}")

    print("Transformaciones y limpiezas de archivos lista")
    return df_history, df_measures, df_json


def load_clean_data(df_hist, df_meas, df_json, base_output_path):
    # Guardamos cada dataframe en su propio CSV
    try:
        base_name = base_output_path.replace('_clean.csv', '')
        
        path_hist = f"{base_name}_history_clean.csv"
        df_hist.to_csv(path_hist, index=False, encoding='utf-8')
        print(f"Guardado: {path_hist}")

        path_meas = f"{base_name}_measures_clean.csv"
        df_meas.to_csv(path_meas, index=False, encoding='utf-8')
        print(f"Guardado: {path_meas}")
        
        path_json = f"{base_name}_json_clean.csv"
        df_json.to_csv(path_json, index=False, encoding='utf-8')
        print(f"Guardado: {path_json}")
        
        return True, [path_hist, path_meas, path_json]
        
    except Exception as e:
        print(f"No se pudieron guardar los archivos: {e}")
        return False, str(e)


def run_full_pipeline(csv_history_path, csv_measures_path, json_air_path, output_csv_path):
    # Ejecutar todo el proceso ETL
    print("Iniciando ETL")
    
    # Extraccion
    df_hist, df_meas, df_json = extract_data(csv_history_path, csv_measures_path, json_air_path)
    
    if df_hist is None or df_meas is None or df_json is None:
        print("Falló la extracción, abortando...")
        return False, "Error en extracción"
    
    # Transformacion
    try:
        df_hist_clean, df_meas_clean, df_json_clean = transform_data(df_hist, df_meas, df_json)
        
        stats = {
            "registros_history_limpios": len(df_hist_clean),
            "registros_measures_limpios": len(df_meas_clean),
            "registros_json_limpios": len(df_json_clean),
        }
    except Exception as e:
        print(f"Error en transformación: {e}")
        return False, f"Error en transformación: {e}"
        
    # Carga
    success, resultado = load_clean_data(df_hist_clean, df_meas_clean, df_json_clean, output_csv_path)
    
    if success:
        print("Se completo el proceso ETL")
        stats["archivos_guardados"] = resultado
        return True, stats
    else:
        print("Fallo al cargar")
        return False, f"Error cargando: {resultado}"