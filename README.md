# Amplitude Data Extraction Project

## Overview

This project extracts event data from **Amplitude**, our website analytics platform, and stores it in **Azure Blob Storage** for cheap and scalable storage. From here, we can transform the data and then analyse it in reporting tools like Tableau or Power BI, or even load it into CRMs. 

---



## 🧩 Business Value

| Business Question | Insight Enabled |
|--------------------|----------------|
| Who is viewing our website? | Identify potential new leads |
| Which blogs deliver the highest engagement? | Increase understanding of market trends |
| How and when are people viewing our website? | Tailor content to fit these trends and drive engagement |


---



## 🗂️ Extraction Architecture

![Architecture Diagram](https://github.com/toby49/amplitude/blob/main/images/Screenshot%202025-11-07%20095622.png)


---


## ⚙️ Approach Comparison

### Option 1: Airbyte Cloud (Managed, No-Code)
Airbyte is a **cloud-based ETL platform** that connects Amplitude to Azure Blob with minimal setup.

**Pros**
- Easy setup (no engineering required)
- Automated scheduling and retries
- Centralised monitoring and UI
- Connectors for many other data sources

**Cons**
- Ongoing **SaaS cost** (billed by data volume)
- Vendor dependency

---



### Option 2: Python Script (Custom, Open Source)

Our custom Python pipeline uses the **Amplitude Export API** to retrieve event data daily, handle logging, and save compressed JSON output.

**Pros**
- Minimal ongoing cost beyond compute/storage
- Fully flexible — can modify logic, parameters, or destinations
- Transparent process, simple handover to engineers
- Portable — can switch between Azure, AWS, or GCP easily

**Cons**
- More upfront development to simplify maintenance (via error handling, logging)
- Initial setup slightly longer than Airbyte

---



## 🔍 Technical Overview (Python Path)

Example: Extract events using the **Amplitude Export API**.

```python
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

api_key = os.getenv("AMP_API_KEY")
secret_key = os.getenv("AMP_SECRET_KEY")

yesterday = datetime.now() - timedelta(days=1)
start = yesterday.strftime('%Y%m%dT00')
end = yesterday.strftime('%Y%m%dT23')

url = "https://analytics.eu.amplitude.com/api/2/export"
params = {"start": start, "end": end}

response = requests.get(url, params=params, auth=(api_key, secret_key))

if response.status_code == 200:
    with open("data.zip", "wb") as f:
        f.write(response.content)
else:
    print(f"Error {response.status_code}: {response.text}")
```



## Logging


Logging is a means of tracking events that happen when code runs.


| Benefit             | Description                                                                                 |
| ------------------- | ------------------------------------------------------------------------------------------- |
| **Transparency**    | Every step of the extraction is logged — we know when the job started, succeeded, or failed |
| **Audit Trail**     | Historical logs act as a record for compliance and debugging                                |
| **Error Detection** | Try/Except blocks capture and report failed API calls instead of crashing the script        |
| **Resilience**      | The pipeline can retry or resume when issues occur, avoiding full reruns                    |
| **Maintainability** | Future engineers can quickly understand what happened and why, from clear log files         |


Sample script:

```python
import logging
from datetime import datetime

log_filename = f"logs/amplitude_extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=log_filename
)

logger = logging.getLogger()
logger.info("Amplitude extraction started")
```

## 📦 Zipped Response

When we request Amplitude’s **Export API endpoint**, it returns a **ZIP file** for each day of activity.

### Inside the ZIP

- There’s one **folder per hour** (e.g. `00:00–01:00`, `01:00–02:00`, etc.).  
- Inside each hourly folder are several **smaller `.gz` files**.  
- Each `.gz` file is like a **page of results**, because Amplitude splits large datasets into smaller chunks.  
- Inside each `.gz` file is a **JSON file** containing the real event data —  
  for example:  
  - “User viewed a blog post”  
  - “User clicked on a consultant profile”  

---

### Extraction Logic (Simplified)

Our Python script automates the following process:

1. **Download** the ZIP file from Amplitude’s API.  
2. **Unzip** it into a temporary working folder.  
3. **Open** each `.gz` file inside the unzipped structure.  
4. **Extract** the event data into readable `.json` files.  
5. **Save** all files neatly into a `data/` folder — ready for analysis or upload to Azure Blob Storage.

This ensures all daily user interactions are safely retrieved and stored in a structured format.

---

### Checking for Missing Files

Because there’s **one file per hour and per page**, we can calculate exactly how many files should exist for a full day of data.  

**Example:**
> 24 hours × 3 pages per hour = **72 files expected**

The script then:
- **Counts** how many `.json` files were actually extracted.  
- If the total is **less than expected** (e.g. 70 instead of 72), it logs which files are missing.  
- Common reasons for missing files include:
  - Temporary API timeouts  
  - Interrupted downloads  
  - Incomplete responses from Amplitude  

The log file might show messages such as:
> “Missing data for hour 02:00”  
> “File for 18:00, page 3 not found.”

---

### 🔁 Recalling the API for Missing Files

If missing files are detected, the script automatically:

1. **Logs** which hours or pages are incomplete.  
2. **Recalls** the Amplitude API — but *only* for those missing hours (not the entire day).  
3. **Downloads** and saves the missing `.gz` files.  

