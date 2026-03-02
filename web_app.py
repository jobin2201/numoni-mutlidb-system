"""
Numoni Web Interface - Modular Architecture
Runs on localhost:8000
"""
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional, List
import sys
import os

# Add part1 to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'part1'))

from part1.db import DATABASES
from part1.router import detect_intent
from part1.query_generator import generate_query

app = FastAPI(title="Numoni Chatbot")


class QueryRequest(BaseModel):
    question: str


class QueryResponse(BaseModel):
    answer: str
    results: Optional[List[dict]] = None
    database: Optional[str] = None
    query: Optional[dict] = None
    error: Optional[str] = None


def execute_query(db, query):
    """Execute MongoDB query"""
    try:
        collection = query.get("collection")
        filter_q = query.get("filter", {})
        
        if not collection:
            return None
        
        return list(db[collection].find(filter_q).limit(50))
    except Exception as e:
        print(f"Query error: {e}")
        return None


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve web interface"""
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Numoni Chatbot</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: 'Segoe UI', Tahoma, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
        }
        
        .container {
            width: 90%;
            max-width: 900px;
            height: 90vh;
            background: white;
            border-radius: 12px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            display: flex;
            flex-direction: column;
        }
        
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 12px 12px 0 0;
            text-align: center;
        }
        
        .chat-area {
            flex: 1;
            overflow-y: auto;
            padding: 20px;
            background: #f8f9fa;
        }
        
        .message {
            margin-bottom: 15px;
            animation: slideIn 0.3s ease;
        }
        
        @keyframes slideIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        .user-message { text-align: right; }
        
        .user-message .bubble {
            background: #667eea;
            color: white;
            padding: 12px 16px;
            border-radius: 18px;
            display: inline-block;
            max-width: 70%;
            word-wrap: break-word;
        }
        
        .bot-message .bubble {
            background: #e9ecef;
            color: #333;
            padding: 12px 16px;
            border-radius: 18px;
            display: inline-block;
            max-width: 80%;
            word-wrap: break-word;
        }
        
        .results-table {
            width: 100%;
            border-collapse: collapse;
            margin: 10px 0;
            font-size: 13px;
            background: white;
            max-height: 300px;
            overflow-y: auto;
            display: block;
        }
        
        .results-table th {
            background: #667eea;
            color: white;
            padding: 8px;
            text-align: left;
            position: sticky;
            top: 0;
        }
        
        .results-table td {
            border: 1px solid #ddd;
            padding: 8px;
        }
        
        .input-area {
            border-top: 1px solid #ddd;
            padding: 15px;
            display: flex;
            gap: 10px;
            background: white;
        }
        
        #questionInput {
            flex: 1;
            border: 1px solid #ddd;
            border-radius: 24px;
            padding: 12px 20px;
            font-size: 16px;
            outline: none;
        }
        
        #questionInput:focus { border-color: #667eea; }
        
        #sendBtn {
            background: #667eea;
            color: white;
            border: none;
            border-radius: 24px;
            padding: 12px 30px;
            font-size: 16px;
            cursor: pointer;
        }
        
        #sendBtn:hover { background: #764ba2; }
        #sendBtn:disabled { background: #ccc; cursor: not-allowed; }
        
        .spinner {
            display: inline-block;
            width: 20px;
            height: 20px;
            border: 3px solid #f3f3f3;
            border-top: 3px solid #667eea;
            border-radius: 50%;
            animation: spin 1s linear infinite;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        .error {
            color: #d32f2f;
            background: #ffebee;
            padding: 10px;
            border-radius: 4px;
            margin: 10px 0;
        }
        
        .badge {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 12px;
            font-size: 11px;
            font-weight: bold;
            margin-bottom: 8px;
        }
        
        .badge-customer { background: #e3f2fd; color: #1976d2; }
        .badge-merchant { background: #f3e5f5; color: #7b1fa2; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🤖 Numoni Chatbot</h1>
            <p>Ask about customers & merchants | Powered by Groq</p>
        </div>
        
        <div class="chat-area" id="chatArea">
            <div class="message bot-message">
                <div class="bubble">👋 Hello! Ask me about customers or merchants!</div>
            </div>
        </div>
        
        <div class="input-area">
            <input 
                type="text" 
                id="questionInput" 
                placeholder="e.g., 'How many customers?'" 
                autocomplete="off"
            >
            <button id="sendBtn">Send</button>
        </div>
    </div>
    
    <script>
        let chatArea, questionInput, sendBtn;
        
        document.addEventListener('DOMContentLoaded', () => {
            chatArea = document.getElementById('chatArea');
            questionInput = document.getElementById('questionInput');
            sendBtn = document.getElementById('sendBtn');
            
            questionInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter' && !sendBtn.disabled) sendMessage();
            });
            
            sendBtn.addEventListener('click', sendMessage);
        });
        
        async function sendMessage() {
            const question = questionInput.value.trim();
            if (!question) return;
            
            questionInput.value = '';
            sendBtn.disabled = true;
            
            addMessage(question, 'user');
            const loadingId = showLoading();
            
            try {
                const response = await fetch('/api/query', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ question })
                });
                
                const data = await response.json();
                removeLoading(loadingId);
                
                if (data.error) {
                    showError(data.error);
                } else {
                    showResults(data);
                }
            } catch (error) {
                removeLoading(loadingId);
                showError(`Connection error: ${error.message}`);
            } finally {
                sendBtn.disabled = false;
                questionInput.focus();
            }
        }
        
        function addMessage(text, sender) {
            const div = document.createElement('div');
            div.className = `message ${sender}-message`;
            div.innerHTML = `<div class="bubble">${escapeHtml(text)}</div>`;
            chatArea.appendChild(div);
            chatArea.scrollTop = chatArea.scrollHeight;
        }
        
        function showLoading() {
            const id = 'loading-' + Date.now();
            const div = document.createElement('div');
            div.className = 'message bot-message';
            div.id = id;
            div.innerHTML = '<div class="bubble"><div class="spinner"></div> Thinking...</div>';
            chatArea.appendChild(div);
            chatArea.scrollTop = chatArea.scrollHeight;
            return id;
        }
        
        function removeLoading(id) {
            document.getElementById(id)?.remove();
        }
        
        function showError(msg) {
            const div = document.createElement('div');
            div.className = 'message bot-message';
            div.innerHTML = `<div class="bubble"><div class="error">❌ ${escapeHtml(msg)}</div></div>`;
            chatArea.appendChild(div);
            chatArea.scrollTop = chatArea.scrollHeight;
        }
        
        function showResults(data) {
            let html = '';
            
            // Database badge
            if (data.database) {
                html += `<span class="badge badge-${data.database}">${data.database.toUpperCase()}</span><br>`;
            }
            
            // Answer
            html += `<strong>${escapeHtml(data.answer)}</strong>`;
            
            // Results table
            if (data.results && data.results.length > 0) {
                html += '<details style="margin-top:10px;" open>';
                html += `<summary style="cursor:pointer;">📊 ${data.results.length} records</summary>`;
                html += '<table class="results-table"><thead><tr>';
                
                const keys = Object.keys(data.results[0]);
                keys.forEach(k => html += `<th>${escapeHtml(k)}</th>`);
                html += '</tr></thead><tbody>';
                
                data.results.forEach(row => {
                    html += '<tr>';
                    keys.forEach(k => {
                        const val = row[k] === null ? 'NULL' : String(row[k]).substring(0, 100);
                        html += `<td>${escapeHtml(val)}</td>`;
                    });
                    html += '</tr>';
                });
                
                html += '</tbody></table></details>';
            }
            
            const div = document.createElement('div');
            div.className = 'message bot-message';
            div.innerHTML = `<div class="bubble">${html}</div>`;
            chatArea.appendChild(div);
            chatArea.scrollTop = chatArea.scrollHeight;
        }
        
        function escapeHtml(text) {
            const map = {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'};
            return String(text).replace(/[&<>"']/g, m => map[m]);
        }
    </script>
</body>
</html>
    """


@app.post("/api/query", response_model=QueryResponse)
async def query(request: QueryRequest):
    """Process user query"""
    try:
        # Detect intent (customer or merchant)
        intent = detect_intent(request.question)
        
        # Generate MongoDB query
        query = generate_query(request.question, intent)
        
        if not query:
            return QueryResponse(
                answer="I couldn't generate a query for that question.",
                error="Query generation failed"
            )
        
        # Execute query
        db = DATABASES[intent]
        results = execute_query(db, query)
        
        if not results:
            return QueryResponse(
                answer=f"No results found in {intent} database.",
                database=intent,
                query=query
            )
        
        # Format answer
        answer = f"Found {len(results)} record(s) in {intent} database."
        
        return QueryResponse(
            answer=answer,
            results=results,
            database=intent,
            query=query
        )
        
    except Exception as e:
        return QueryResponse(
            answer=f"Error: {str(e)}",
            error=str(e)
        )


if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting Numoni Chatbot on http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
