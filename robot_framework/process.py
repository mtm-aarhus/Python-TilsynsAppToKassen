"""This module contains the main process of the robot."""
from OpenOrchestrator.database.queues import QueueElement


from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from azure.cosmos import CosmosClient
import pyodbc

def process(orchestrator_connection: OrchestratorConnection, queue_element=None):
    orchestrator_connection.log_trace("Running Fakturering export process.")

    # --- Cosmos DB connection ---
    cosmos_credentials = orchestrator_connection.get_credential("AAKTilsynDB")
    COSMOS_URL = cosmos_credentials.username
    COSMOS_KEY = cosmos_credentials.password
    DB_NAME = "aak-tilsyn"
    CONTAINER = "henstillinger"

    client = CosmosClient(COSMOS_URL, COSMOS_KEY)
    container = client.get_database_client(DB_NAME).get_container_client(CONTAINER)

    # --- SQL Server connection ---
    sql_server = orchestrator_connection.get_constant("SqlServer")
    conn_string = "DRIVER={SQL Server};" + f"SERVER={sql_server.value};DATABASE=VejmanKassen;Trusted_Connection=yes;"
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()

    # Preload unit prices (maintained by the other robot)
    unit_price_lookup = load_unit_prices(conn)


    # --- Fetch pricebook ---
    # token = orchestrator_connection.get_credential("VejmanToken").password
    # pricebook_map = FetchPricebookData(token)

    # --- Query all 'Til fakturering' rows ---
    rows = list(container.query_items(
        query="SELECT * FROM c WHERE c.FakturaStatus = 'Til fakturering'",
        enable_cross_partition_query=True
    ))

    orchestrator_connection.log_info(f"Found {len(rows)} rows needing Fakturering export.")

    for item in rows:
        cosmos_id = item["id"]
        henstilling_id = item["HenstillingId"]
        firma = item.get("FirmaNavn")
        adresse = item.get("Adresse")
        cvr = item.get("CVR")
        tilladelsestype = item.get("Tilladelsestype")
        meter = item.get("Kvadratmeter")
        start = item.get("Startdato")
        slut = item.get("Slutdato")
        cosmos_id_converted = convert_cosmos_id(item["id"])
        orchestrator_connection.log_info(f"Sending {cosmos_id} into vejmankassen")

        # Determine price year from Startdato (YYYY-MM-DD)
        if not start:
            raise Exception(f"{cosmos_id}: Missing Startdato, cannot determine price year")

        price_year = int(start[:4])
        lookup_key = (price_year, tilladelsestype.strip().lower())
        enhedspris = unit_price_lookup.get(lookup_key)

        if enhedspris is None:
            raise Exception(
                f"{cosmos_id}: No unit price found in dbo.VejmanEnhedsPriser for "
                f"PriceYear={price_year}, PricebookText='{tilladelsestype}'"
    )


        # --- Check if already exists in SQL ---
        cursor.execute("SELECT COUNT(*) FROM dbo.VejmanFakturering WHERE VejmanFakturaID = ?", cosmos_id_converted)
        exists = cursor.fetchone()[0] > 0

        if exists:
            raise Exception(f"{cosmos_id} already exists in VejmanKassen, should not be possible")

        # --- Perform INSERT ---
        insert_sql = """
            INSERT INTO dbo.VejmanFakturering (
                FørsteSted, Tilladelsesnr, Ansøger, CvrNr,
                TilladelsesType, Enhedspris, Meter, Startdato, Slutdato,
                VejmanFakturaID, FakturaStatus
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        cursor.execute(
            insert_sql,
            adresse,
            henstilling_id,
            firma,
            cvr,
            tilladelsestype,
            enhedspris,
            meter,
            start,
            slut,
            cosmos_id_converted,
            "Ny"
        )
        conn.commit()

        old_status = item["FakturaStatus"]
        new_status = "Faktureret"
        item_id = item["id"]

        # 1. Read item from old partition
        old_doc = container.read_item(item=item_id, partition_key=old_status)

        # 2. Update fields
        old_doc["FakturaStatus"] = new_status

        # 3. Create new document in new partition
        container.create_item(body=old_doc)

        # 4. Delete old document
        container.delete_item(item=item_id, partition_key=old_status)
        orchestrator_connection.log_info("Marked as faktureret")



def convert_cosmos_id(cosmos_id: str) -> str:
    if "_" not in cosmos_id:
        return cosmos_id  # already clean

    left, right = cosmos_id.split("_", 1)

    # Pad with 1 zero if < 10
    if right.isdigit() and int(right) < 10:
        right = f"0{int(right)}"
    else:
        right = str(int(right))

    return f"{left}{right}"

def load_unit_prices(conn) -> dict[tuple[int, str], float]:
    """
    Load all unit prices from dbo.VejmanEnhedsPriser into memory.

    Key: (PriceYear, pricebook_text_lower)
    Value: UnitPrice
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT PriceYear, PricebookText, UnitPrice
            FROM dbo.VejmanEnhedsPriser
        """)
        price_map: dict[tuple[int, str], float] = {}
        for year, text, price in cur.fetchall():
            if year is None or text is None:
                continue
            price_map[(int(year), str(text).strip().lower())] = float(price)
        return price_map
