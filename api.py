import os
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from databricks import sql
from dotenv import load_dotenv

# 1. Load Config
load_dotenv()

app = FastAPI()

# 2. Setup CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 3. Connection Config
SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
ACCESS_TOKEN = os.getenv("DATABRICKS_TOKEN")

def get_db_connection():
    if not ACCESS_TOKEN:
        print("Warning: DATABRICKS_TOKEN missing")
        return None
    return sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=ACCESS_TOKEN
    )

# 4. API Endpoint
@app.get("/api/customers")
def get_customers():
    try:
        # แก้ไข Query: เพิ่ม QUALIFY เพื่อตัดข้อมูลซ้ำ (เอาเฉพาะงวดล่าสุดของแต่ละคน)
        query = """
            SELECT 
                user_id as id, 
                concat(first_name, ' ', last_name) as customer,
                province, 
                gender, 
                age,
                round_date,
                total_items_in_round as tickets_bought,
                total_win_rounds as total_wins,
                prize_type as last_prize,
                CASE 
                    WHEN total_win_rounds > 20 THEN 'High Luck'
                    WHEN total_win_rounds > 15 THEN 'Lucky Star'
                    ELSE 'General'
                END as segment
            FROM main.silver.dm_lucky_transaction_detail_api_webapp
            WHERE 1=1
            -- เพิ่มบรรทัดนี้: จัดกลุ่มตาม user_id แล้วเลือกแถวที่ round_date ล่าสุดมาแค่ 1 แถว
            QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY round_date DESC) = 1
            ORDER BY total_win_rounds DESC 
            LIMIT 500
        """
        
        conn = get_db_connection()
        if not conn:
            return [
                {"id": 101, "customer": "สมชาย (Mock)", "province": "BKK", "total_wins": 99, "segment": "Test"}
            ]

        with conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    result = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    return result
                else:
                    return []

    except Exception as e:
        print(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
