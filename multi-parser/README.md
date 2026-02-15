
### 1. Configure proxies

Edit `proxies.json` and set your SOCKS5/HTTP proxy URLs:

```json
 [
    "http://login:pass@11.11.11.11:1111/",
 ]
```

### 2. Create docker-compose.yml

Just run generate_workers.py

### 3. Start workers

```bash
cd multi-parser
docker compose up --build -d
```
### 4. Start Livescrapper
Profit