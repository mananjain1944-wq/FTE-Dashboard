# FTE QA Intelligence Dashboard

## Repository Structure
```
fte-qa-dashboard/
├── backend/          ← FastAPI Python backend
│   ├── main.py       ← API endpoints
│   ├── database.py   ← DB schema + connection
│   ├── ingestion.py  ← Excel ingestion pipeline
│   ├── intelligence.py ← Risk scoring + AI predictions
│   ├── requirements.txt
│   └── start.sh
└── frontend/
    └── index.html    ← Complete dashboard UI
```

## Environment Variables Required
- `DATABASE_URL` — Your Render PostgreSQL URL
- `GEMINI_API_KEY` — Your Gemini API key

## Deployment on Render
1. Push this repo to GitHub
2. Backend: New Web Service → connect repo → Root Dir: `backend` → Start: `sh start.sh`
3. Frontend: New Static Site → connect repo → Root Dir: `frontend`
4. Set environment variables in Render dashboard
