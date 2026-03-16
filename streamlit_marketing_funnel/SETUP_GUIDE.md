# Complete setup guide — where to run everything

This guide covers every command in order. No prior cloud experience needed.

---

## 1. Where each piece runs

| Component           | Runs on              | Cost        |
|---------------------|----------------------|-------------|
| dbt models (.sql)   | Your laptop (terminal) | Free      |
| SQL execution       | BigQuery (Google Cloud) | Free tier |
| Airflow DAG         | Your laptop (Docker)  | Free       |
| Streamlit app (dev) | Your laptop          | Free        |
| Streamlit app (live)| Streamlit Cloud      | Free        |
| Data storage        | BigQuery             | Free tier   |

**Short version**: you run dbt and Streamlit on your laptop.
They send work to Google Cloud (BigQuery). Streamlit Cloud
hosts your live app for free.

---

## 2. One-time setup (do this first)

### Step 1 — Create a free Google Cloud account

1. Go to https://cloud.google.com
2. Click "Get started for free" — you get $300 credit + free tier
3. Create a new project, e.g. `marketing-funnel-portfolio`
4. Note your **Project ID** (shown in the top bar) — you'll need it everywhere

### Step 2 — Enable BigQuery

1. In Google Cloud Console → search "BigQuery API"
2. Click Enable
3. That's it — BigQuery is ready

### Step 3 — Install Google Cloud CLI on your laptop

Mac:
```bash
brew install --cask google-cloud-sdk
```

Windows: download installer from https://cloud.google.com/sdk/docs/install

Then authenticate:
```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud auth application-default login    # This is what dbt and Python use
```

### Step 4 — Install Python dependencies

```bash
# Create a virtual environment (keeps things clean)
python -m venv venv
source venv/bin/activate          # Mac/Linux
.\venv\Scripts\activate           # Windows

# Install everything needed
pip install dbt-bigquery streamlit plotly pandas \
            google-cloud-bigquery db-dtypes
```

---

## 3. Run the dbt pipeline

```bash
cd dbt_marketing_funnel

# Copy profiles.yml to the dbt home directory
cp profiles.yml ~/.dbt/profiles.yml

# Edit it — replace YOUR_GCP_PROJECT_ID with your actual project ID
nano ~/.dbt/profiles.yml            # or open in VS Code

# Install dbt packages (dbt-utils)
dbt deps

# Test the connection
dbt debug                           # Should say "All checks passed"

# Run all models (creates tables in BigQuery)
dbt run

# Run all tests
dbt test

# View lineage in your browser — screenshot this for your README!
dbt docs generate
dbt docs serve                      # Opens at http://localhost:8080
```

After `dbt run` completes, you'll see these tables in BigQuery:
- `marketing_funnel_dev.staging.stg_ga4_events`
- `marketing_funnel_dev.staging.stg_ga4_sessions`
- `marketing_funnel_dev.marts.fct_funnel_steps`
- `marketing_funnel_dev.marts.fct_funnel_summary`

---

## 4. Run the Streamlit app locally

```bash
cd streamlit_app

# Run with demo data first (no BigQuery needed)
streamlit run app.py
# Opens at http://localhost:8501

# To connect to your real BigQuery data:
# Toggle off "Use demo data" in the sidebar and enter your Project ID
```

---

## 5. Deploy to Streamlit Cloud (get a live URL)

This gives recruiters a URL they can click.

### Step 1 — Push to GitHub

```bash
# In your project root
git init
git add .
git commit -m "Initial commit: marketing funnel analytics pipeline"
git remote add origin https://github.com/YOUR_USERNAME/marketing-funnel-analytics
git push -u origin main
```

### Step 2 — Create a service account key (for Streamlit Cloud → BigQuery auth)

1. Google Cloud Console → IAM & Admin → Service Accounts
2. Create Service Account → name it `streamlit-dashboard`
3. Grant role: **BigQuery Data Viewer** + **BigQuery Job User**
4. Keys tab → Add Key → JSON → download the file
5. Open the JSON file — you'll copy values from it in the next step

### Step 3 — Deploy on Streamlit Cloud

1. Go to https://share.streamlit.io
2. Sign in with GitHub
3. New app → select your repo → set main file to `streamlit_app/app.py`
4. Click **Advanced settings** → **Secrets**
5. Paste the contents of `.streamlit/secrets.toml`, filling in values
   from the service account JSON you downloaded
6. Click Deploy

Your live URL will be: `https://YOUR-APP-NAME.streamlit.app`

---

## 6. Folder structure for GitHub

```
marketing-funnel-analytics/
│
├── dbt_marketing_funnel/              # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_ga4_events.sql
│   │   │   └── stg_ga4_sessions.sql
│   │   └── marts/
│   │       ├── fct_funnel_steps.sql
│   │       └── fct_funnel_summary.sql
│   ├── tests/
│   │   └── test_funnel_stage_ordering.sql
│   ├── models/schema.yml
│   ├── dbt_project.yml
│   └── packages.yml
│
├── streamlit_app/                     # Dashboard
│   ├── app.py
│   └── requirements.txt
│
├── .gitignore
└── README.md
```

### .gitignore — make sure this is in your repo root

```
# dbt
target/
dbt_packages/
logs/

# Streamlit secrets — NEVER commit this
.streamlit/secrets.toml

# Python
venv/
__pycache__/
*.pyc

# GCP keys — NEVER commit these
*.json
```

---

## 7. Troubleshooting

**`dbt debug` fails with "not found"**
→ Make sure you ran `gcloud auth application-default login`

**BigQuery says "Access Denied"**
→ Your GCP account needs BigQuery Data Viewer on `bigquery-public-data`
→ Run: `gcloud projects add-iam-policy-binding bigquery-public-data \
   --member="user:YOUR_EMAIL" --role="roles/bigquery.dataViewer"`

**Streamlit Cloud can't reach BigQuery**
→ Double-check that the service account has **BigQuery Job User** role
→ Make sure secrets.toml has no extra spaces around the private key

**`dbt run` times out**
→ The public GA4 dataset is large. Add a tighter date filter:
   `dbt run --vars '{"start_date": "20201101", "end_date": "20201130"}'`
