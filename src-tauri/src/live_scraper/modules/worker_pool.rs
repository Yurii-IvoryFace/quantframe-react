use std::{
    collections::{HashMap, VecDeque},
    env,
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use utils::{Error, LoggerOptions, get_location, info, warning};
use uuid::Uuid;
use wf_market::types::{OrderList, OrderWithUser};

static COMPONENT: &str = "LiveScraper:Item:WorkerPool";
static WORKER_POOL: OnceLock<Arc<MultiWorkerPool>> = OnceLock::new();

#[derive(Clone)]
pub struct MultiWorkerPool {
    tx: mpsc::UnboundedSender<PoolCommand>,
}

#[derive(Debug)]
struct WorkerState {
    connection_id: String,
    sender: mpsc::UnboundedSender<String>,
}

struct QueuedTask {
    request_id: String,
    slug: String,
    response: oneshot::Sender<Result<OrderList<OrderWithUser>, String>>,
}

struct InflightTask {
    worker_id: String,
    task: QueuedTask,
}

enum PoolCommand {
    Enqueue(QueuedTask),
    WorkerUp {
        worker_id: String,
        connection_id: String,
        sender: mpsc::UnboundedSender<String>,
    },
    WorkerReady {
        worker_id: String,
        connection_id: String,
    },
    WorkerResult {
        request_id: String,
        orders: OrderList<OrderWithUser>,
    },
    WorkerError {
        request_id: String,
        error: String,
    },
    WorkerDown {
        worker_id: String,
        connection_id: String,
    },
    GetWorkerCount {
        response: oneshot::Sender<usize>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WorkerInboundMessage {
    Ready {
        worker_id: String,
    },
    Result {
        request_id: String,
        slug: String,
        data: serde_json::Value,
        worker_id: Option<String>,
        elapsed_ms: Option<u64>,
    },
    Error {
        request_id: String,
        slug: String,
        error: String,
        worker_id: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
struct OrdersPayloadCompat {
    #[serde(default)]
    sell_orders: Vec<OrderWithUser>,
    #[serde(default)]
    buy_orders: Vec<OrderWithUser>,
    #[serde(default)]
    sell: Vec<OrderWithUser>,
    #[serde(default)]
    buy: Vec<OrderWithUser>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WorkerOutboundMessage {
    Task {
        request_id: String,
        slug: String,
        fast_check: bool,
    },
}

impl MultiWorkerPool {
    pub fn global() -> Arc<Self> {
        WORKER_POOL.get_or_init(Self::create).clone()
    }

    fn create() -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel::<PoolCommand>();
        let pool = Arc::new(Self { tx: tx.clone() });

        tauri::async_runtime::spawn(run_dispatcher(rx));
        tauri::async_runtime::spawn(run_server(tx));

        pool
    }

    pub async fn worker_count(&self) -> usize {
        let (tx, rx) = oneshot::channel();
        if self
            .tx
            .send(PoolCommand::GetWorkerCount { response: tx })
            .is_err()
        {
            return 0;
        }
        rx.await.unwrap_or(0)
    }

    pub async fn fetch_orders(
        &self,
        slug: impl Into<String>,
        timeout_duration: Duration,
    ) -> Result<OrderList<OrderWithUser>, Error> {
        let slug = slug.into();
        let request_id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        let task = QueuedTask {
            request_id: request_id.clone(),
            slug: slug.clone(),
            response: tx,
        };

        if self.tx.send(PoolCommand::Enqueue(task)).is_err() {
            return Err(Error::new(
                COMPONENT,
                "Worker pool is unavailable",
                get_location!(),
            ));
        }

        match tokio::time::timeout(timeout_duration, rx).await {
            Ok(Ok(Ok(orders))) => Ok(orders),
            Ok(Ok(Err(message))) => Err(Error::new(
                COMPONENT,
                &format!("Worker request failed for {}: {}", slug, message),
                get_location!(),
            )),
            Ok(Err(_)) => Err(Error::new(
                COMPONENT,
                &format!("Worker response channel closed for {}", slug),
                get_location!(),
            )),
            Err(_) => Err(Error::new(
                COMPONENT,
                &format!(
                    "Worker request timed out for {} after {:?}",
                    slug, timeout_duration
                ),
                get_location!(),
            )),
        }
    }
}

async fn run_dispatcher(mut rx: mpsc::UnboundedReceiver<PoolCommand>) {
    let mut workers: HashMap<String, WorkerState> = HashMap::new();
    let mut ready_workers: VecDeque<String> = VecDeque::new();
    let mut pending: VecDeque<QueuedTask> = VecDeque::new();
    let mut inflight: HashMap<String, InflightTask> = HashMap::new();

    while let Some(command) = rx.recv().await {
        match command {
            PoolCommand::Enqueue(task) => pending.push_back(task),
            PoolCommand::WorkerUp {
                worker_id,
                connection_id,
                sender,
            } => {
                workers.insert(
                    worker_id.clone(),
                    WorkerState {
                        connection_id,
                        sender,
                    },
                );
                ready_workers.retain(|id| id != &worker_id);
                info(
                    COMPONENT,
                    &format!(
                        "Worker connected: {} (total workers: {})",
                        worker_id,
                        workers.len()
                    ),
                    &LoggerOptions::default(),
                );
            }
            PoolCommand::WorkerReady {
                worker_id,
                connection_id,
            } => {
                let is_current_connection = workers
                    .get(&worker_id)
                    .map(|worker| worker.connection_id == connection_id)
                    .unwrap_or(false);

                if is_current_connection
                    && !ready_workers.iter().any(|id| id == &worker_id)
                {
                    ready_workers.push_back(worker_id);
                }
            }
            PoolCommand::WorkerResult { request_id, orders } => {
                if let Some(inflight_task) = inflight.remove(&request_id) {
                    let _ = inflight_task.task.response.send(Ok(orders));
                } else {
                    warning(
                        COMPONENT,
                        &format!("Unknown result request_id: {}", request_id),
                        &LoggerOptions::default(),
                    );
                }
            }
            PoolCommand::WorkerError { request_id, error } => {
                if let Some(inflight_task) = inflight.remove(&request_id) {
                    let _ = inflight_task.task.response.send(Err(error));
                } else {
                    warning(
                        COMPONENT,
                        &format!("Unknown error request_id: {}", request_id),
                        &LoggerOptions::default(),
                    );
                }
            }
            PoolCommand::WorkerDown {
                worker_id,
                connection_id,
            } => {
                let should_remove = workers
                    .get(&worker_id)
                    .map(|worker| worker.connection_id == connection_id)
                    .unwrap_or(false);

                if !should_remove {
                    continue;
                }

                workers.remove(&worker_id);
                ready_workers.retain(|id| id != &worker_id);

                let orphaned: Vec<String> = inflight
                    .iter()
                    .filter(|(_, item)| item.worker_id == worker_id)
                    .map(|(request_id, _)| request_id.clone())
                    .collect();

                for request_id in orphaned {
                    if let Some(inflight_task) = inflight.remove(&request_id) {
                        pending.push_front(inflight_task.task);
                    }
                }

                warning(
                    COMPONENT,
                    &format!(
                        "Worker disconnected: {} (remaining workers: {})",
                        worker_id,
                        workers.len()
                    ),
                    &LoggerOptions::default(),
                );
            }
            PoolCommand::GetWorkerCount { response } => {
                let _ = response.send(workers.len());
            }
        }

        dispatch_tasks(&mut workers, &mut ready_workers, &mut pending, &mut inflight);
    }
}

fn dispatch_tasks(
    workers: &mut HashMap<String, WorkerState>,
    ready_workers: &mut VecDeque<String>,
    pending: &mut VecDeque<QueuedTask>,
    inflight: &mut HashMap<String, InflightTask>,
) {
    loop {
        let Some(worker_id) = ready_workers.pop_front() else {
            break;
        };

        // Drain stale tasks from previous rounds (idk if it's working)
        while pending.front().map_or(false, |t| t.response.is_closed()) {
            pending.pop_front();
        }

        let Some(task) = pending.pop_front() else {
            ready_workers.push_front(worker_id);
            break;
        };

        let payload = match serde_json::to_string(&WorkerOutboundMessage::Task {
            request_id: task.request_id.clone(),
            slug: task.slug.clone(),
            fast_check: false,
        }) {
            Ok(payload) => payload,
            Err(e) => {
                let _ = task
                    .response
                    .send(Err(format!("Failed to serialize task: {}", e)));
                continue;
            }
        };

        let send_result = workers
            .get(&worker_id)
            .map(|worker| worker.sender.send(payload))
            .unwrap_or_else(|| {
                Err(mpsc::error::SendError(String::from(
                    "worker sender missing",
                )))
            });

        if send_result.is_err() {
            workers.remove(&worker_id);
            pending.push_front(task);
            warning(
                COMPONENT,
                &format!("Failed to dispatch task to worker {}", worker_id),
                &LoggerOptions::default(),
            );
            continue;
        }

        inflight.insert(
            task.request_id.clone(),
            InflightTask {
                worker_id,
                task,
            },
        );
    }
}

async fn run_server(tx: mpsc::UnboundedSender<PoolCommand>) {
    let bind = env::var("QF_MULTI_WORKER_BIND").unwrap_or_else(|_| "0.0.0.0:8765".to_string());

    loop {
        match TcpListener::bind(&bind).await {
            Ok(listener) => {
                info(
                    COMPONENT,
                    &format!("Worker host listening on ws://{}", bind),
                    &LoggerOptions::default(),
                );
                loop {
                    match listener.accept().await {
                        Ok((stream, peer)) => {
                            let tx_clone = tx.clone();
                            tauri::async_runtime::spawn(async move {
                                if let Err(e) =
                                    handle_worker_connection(stream, peer, tx_clone).await
                                {
                                    warning(
                                        COMPONENT,
                                        &format!("Worker connection error from {}: {}", peer, e),
                                        &LoggerOptions::default(),
                                    );
                                }
                            });
                        }
                        Err(e) => {
                            warning(
                                COMPONENT,
                                &format!("Failed to accept worker socket: {}", e),
                                &LoggerOptions::default(),
                            );
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                warning(
                    COMPONENT,
                    &format!("Failed to bind worker host at {}: {}", bind, e),
                    &LoggerOptions::default(),
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn handle_worker_connection(
    stream: TcpStream,
    peer: SocketAddr,
    tx: mpsc::UnboundedSender<PoolCommand>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let connection_id = Uuid::new_v4().to_string();
    let ws_stream = accept_async(stream).await?;
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    let mut worker_id: Option<String> = None;

    let writer = tauri::async_runtime::spawn(async move {
        while let Some(payload) = out_rx.recv().await {
            if ws_sender.send(Message::Text(payload.into())).await.is_err() {
                break;
            }
        }
    });

    while let Some(message) = ws_receiver.next().await {
        let message = message?;
        let text = match message {
            Message::Text(text) => text.to_string(),
            Message::Binary(bytes) => String::from_utf8_lossy(&bytes).to_string(),
            Message::Close(_) => break,
            _ => continue,
        };

        let inbound: WorkerInboundMessage = match serde_json::from_str(&text) {
            Ok(msg) => msg,
            Err(e) => {
                warning(
                    COMPONENT,
                    &format!("Invalid worker payload from {}: {}", peer, e),
                    &LoggerOptions::default(),
                );
                continue;
            }
        };

        match inbound {
            WorkerInboundMessage::Ready { worker_id: ready_id } => {
                if worker_id.as_ref() != Some(&ready_id) {
                    worker_id = Some(ready_id.clone());
                    let _ = tx.send(PoolCommand::WorkerUp {
                        worker_id: ready_id.clone(),
                        connection_id: connection_id.clone(),
                        sender: out_tx.clone(),
                    });
                }

                let _ = tx.send(PoolCommand::WorkerReady {
                    worker_id: ready_id,
                    connection_id: connection_id.clone(),
                });
            }
            WorkerInboundMessage::Result {
                request_id,
                slug,
                data,
                worker_id: msg_worker_id,
                elapsed_ms,
            } => {
                match parse_orders_payload(data) {
                    Ok(orders) => {
                        if let Some(worker) = msg_worker_id.or_else(|| worker_id.clone()) {
                            info(
                                COMPONENT,
                                &format!(
                                    "Result from {} for {} ({} sell / {} buy, {}ms)",
                                    worker,
                                    slug,
                                    orders.sell_orders.len(),
                                    orders.buy_orders.len(),
                                    elapsed_ms.unwrap_or(0)
                                ),
                                &LoggerOptions::default().set_show_time(false),
                            );
                        }
                        let _ = tx.send(PoolCommand::WorkerResult { request_id, orders });
                    }
                    Err(parse_error) => {
                        let _ = tx.send(PoolCommand::WorkerError {
                            request_id,
                            error: format!("Failed to parse worker data for {}: {}", slug, parse_error),
                        });
                    }
                }
            }
            WorkerInboundMessage::Error {
                request_id,
                slug,
                error,
                worker_id: msg_worker_id,
            } => {
                if let Some(worker) = msg_worker_id.or_else(|| worker_id.clone()) {
                    warning(
                        COMPONENT,
                        &format!("Worker {} error for {}: {}", worker, slug, error),
                        &LoggerOptions::default(),
                    );
                }
                let _ = tx.send(PoolCommand::WorkerError { request_id, error });
            }
        }
    }

    if let Some(worker_id) = worker_id {
        let _ = tx.send(PoolCommand::WorkerDown {
            worker_id,
            connection_id,
        });
    }

    writer.abort();
    Ok(())
}

fn parse_orders_payload(value: serde_json::Value) -> Result<OrderList<OrderWithUser>, String> {
    if let Ok(list) = serde_json::from_value::<OrderList<OrderWithUser>>(value.clone()) {
        return Ok(list);
    }

    let compat: OrdersPayloadCompat = serde_json::from_value(value)
        .map_err(|e| format!("invalid orders payload shape: {}", e))?;

    let mut orders = Vec::new();
    orders.extend(compat.sell_orders);
    orders.extend(compat.buy_orders);
    orders.extend(compat.sell);
    orders.extend(compat.buy);

    Ok(OrderList::new(orders))
}
