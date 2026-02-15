import json
import os
import sys

# Define default configuration
DEFAULT_HOST_URL = "ws://host.docker.internal:8765/"
DEFAULT_RATE_LIMIT_MS = "500"

def generate_docker_compose(proxies_file="proxies.json", output_file="docker-compose.yml"):
    """
    Generates a docker-compose.yml file with one worker service per proxy URL.
    """
    
    # Check if proxies file exists
    if not os.path.exists(proxies_file):
        print(f"Error: {proxies_file} not found.")
        print("Please create a 'proxies.json' file with a list of proxy URLs.")
        print('Example: ["http://user:pass@host:port", "socks5://user:pass@host:port"]')
        return

    try:
        with open(proxies_file, "r") as f:
            proxies = json.load(f)
            
        if not proxies or not isinstance(proxies, list):
            print(f"Error: {proxies_file} must contain a JSON list of proxy strings.")
            return

        print(f"Found {len(proxies)} proxies. Generating {output_file}...")

        # Header of docker-compose
        content = "services:\n"

        for i, proxy_url in enumerate(proxies):
            worker_id = f"worker-{i+1}"
            
            # Service definition for each worker
            service_block = f"""  {worker_id}:
    build: ./worker
    environment:
      HOST_URL: "{DEFAULT_HOST_URL}"
      PROXY_URL: "{proxy_url}"
      WORKER_ID: "{worker_id}"
      RATE_LIMIT_MS: "{DEFAULT_RATE_LIMIT_MS}"
    restart: unless-stopped
"""
            content += service_block + "\n"

        # Write to file
        with open(output_file, "w") as f:
            f.write(content)
            
        print(f"Successfully generated {output_file} with {len(proxies)} workers.")
        print("Run 'docker compose up --build -d' to start them.")

    except json.JSONDecodeError:
        print(f"Error: Failed to parse {proxies_file}. Please ensure it is valid JSON.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    api_proxies_env = os.environ.get("WFM_PROXIES")
    
    # If env var exists, try to parse it (assuming JSON string)
    if api_proxies_env:
        try:
             # If passed as env var, write it to a temp file or handle directly
             # simpler for now: just prioritize file usage as per user request flow
             pass
        except:
             pass

    generate_docker_compose()
