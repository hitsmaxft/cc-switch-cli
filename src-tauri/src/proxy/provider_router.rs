use std::{collections::HashMap, path::PathBuf, sync::Arc};

use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::RwLock;

use crate::{app_config::AppType, database::Database, provider::Provider};

mod upstream_endpoint;

use super::{
    circuit_breaker::{AllowResult, CircuitBreaker, CircuitBreakerConfig, CircuitBreakerStats},
    error::ProxyError,
};

/// Provider 缓存项，按 app_type 存储当前 provider
#[derive(Clone, Debug)]
struct CachedProvider {
    provider: Provider,
    /// 缓存版本号，用于检测是否需要刷新
    version: u64,
}

pub struct ProviderRouter {
    db: Arc<Database>,
    circuit_breakers: Arc<RwLock<HashMap<String, Arc<CircuitBreaker>>>>,
    /// 当前 provider 缓存：app_type -> CachedProvider
    current_cache: Arc<RwLock<HashMap<String, CachedProvider>>>,
    /// 缓存版本计数器，每次文件变更时递增
    cache_version: Arc<RwLock<u64>>,
}

impl ProviderRouter {
    pub fn new(db: Arc<Database>) -> Self {
        let router = Self {
            db: db.clone(),
            circuit_breakers: Arc::new(RwLock::new(HashMap::new())),
            current_cache: Arc::new(RwLock::new(HashMap::new())),
            cache_version: Arc::new(RwLock::new(0)),
        };
        router.spawn_file_watcher();
        router.log_initial_providers();
        router
    }

    /// 启动时打印当前激活的 provider
    fn log_initial_providers(&self) {
        let db = self.db.clone();
        tokio::spawn(async move {
            let app_types = vec![
                AppType::Claude,
                AppType::Codex,
                AppType::Gemini,
                AppType::OpenCode,
                AppType::OpenClaw,
            ];
            for app_type in app_types {
                match Self::load_current_provider(&db, app_type.as_str()).await {
                    Ok(Some(provider)) => {
                        let stream_usage = provider.stream_include_usage();
                        log::info!(
                            "[ProviderRouter] [{}] initial provider: {} (stream_include_usage={})",
                            app_type.as_str(),
                            provider.name,
                            stream_usage
                        );
                    }
                    Ok(None) => {
                        log::info!(
                            "[ProviderRouter] [{}] no provider configured",
                            app_type.as_str()
                        );
                    }
                    Err(e) => {
                        log::warn!(
                            "[ProviderRouter] [{}] failed to load initial provider: {}",
                            app_type.as_str(),
                            e
                        );
                    }
                }
            }
        });
    }

    /// 启动文件 watcher，监听 provider 变更信号
    fn spawn_file_watcher(&self) {
        let watch_path = Self::provider_change_signal_path();
        let cache_version = self.cache_version.clone();
        let current_cache = self.current_cache.clone();
        let db = self.db.clone();

        tokio::spawn(async move {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<notify::Result<Event>>(32);

            let mut watcher = match RecommendedWatcher::new(
                move |res| {
                    let _ = tx.blocking_send(res);
                },
                Config::default(),
            ) {
                Ok(w) => w,
                Err(e) => {
                    log::error!("[ProviderRouter] Failed to create file watcher: {}", e);
                    return;
                }
            };

            // 确保父目录存在
            if let Some(parent) = watch_path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    log::error!("[ProviderRouter] Failed to create watch dir: {}", e);
                    return;
                }
            }

            // 如果信号文件不存在，创建一个空文件
            if !watch_path.exists() {
                if let Err(e) = std::fs::write(&watch_path, "") {
                    log::error!("[ProviderRouter] Failed to create watch file: {}", e);
                    return;
                }
            }

            if let Err(e) = watcher.watch(&watch_path, RecursiveMode::NonRecursive) {
                log::error!("[ProviderRouter] Failed to watch file: {}", e);
                return;
            }

            log::info!(
                "[ProviderRouter] Watching provider change signal at {:?}",
                watch_path
            );

            while let Some(res) = rx.recv().await {
                match res {
                    Ok(event) => {
                        // 只关心写入和创建事件
                        if event.kind.is_modify() || event.kind.is_create() {
                            log::info!("[ProviderRouter] Provider change signal detected");

                            // 递增版本号
                            let mut version = cache_version.write().await;
                            *version += 1;
                            let new_version = *version;
                            drop(version);

                            // 预加载所有 app_type 的 provider 到缓存
                            let app_types = vec![
                                AppType::Claude,
                                AppType::Codex,
                                AppType::Gemini,
                                AppType::OpenCode,
                                AppType::OpenClaw,
                            ];

                            let mut cache = current_cache.write().await;
                            for app_type in app_types {
                                match Self::load_current_provider(&db, app_type.as_str()).await {
                                    Ok(Some(provider)) => {
                                        let stream_usage = provider.stream_include_usage();
                                        log::info!(
                                            "[ProviderRouter] [{}] switched to provider: {} (stream_include_usage={})",
                                            app_type.as_str(),
                                            provider.name,
                                            stream_usage
                                        );
                                        cache.insert(
                                            app_type.as_str().to_string(),
                                            CachedProvider {
                                                provider,
                                                version: new_version,
                                            },
                                        );
                                    }
                                    Ok(None) => {
                                        log::info!(
                                            "[ProviderRouter] No provider configured for {}",
                                            app_type.as_str()
                                        );
                                        cache.remove(app_type.as_str());
                                    }
                                    Err(e) => {
                                        log::error!(
                                            "[ProviderRouter] Failed to preload provider for {}: {}",
                                            app_type.as_str(),
                                            e
                                        );
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("[ProviderRouter] Watch error: {}", e);
                    }
                }
            }

            log::warn!("[ProviderRouter] File watcher channel closed");
        });
    }

    /// 获取 provider 变更信号文件路径
    fn provider_change_signal_path() -> PathBuf {
        crate::config::get_app_config_dir().join("provider.changed")
    }

    /// 从 DB 加载指定 app_type 的当前 provider
    async fn load_current_provider(
        db: &Database,
        app_type: &str,
    ) -> Result<Option<Provider>, ProxyError> {
        let current_id = db
            .get_current_provider(app_type)
            .map_err(|error| ProxyError::DatabaseError(error.to_string()))?;

        match current_id {
            Some(current_id) => db
                .get_provider_by_id(&current_id, app_type)
                .map_err(|error| ProxyError::DatabaseError(error.to_string())),
            None => Ok(None),
        }
    }

    /// 获取当前 provider（带缓存）
    async fn get_cached_current_provider(
        &self,
        app_type: &str,
    ) -> Result<Option<Provider>, ProxyError> {
        let cache_version = *self.cache_version.read().await;

        // 先检查缓存
        {
            let cache = self.current_cache.read().await;
            if let Some(cached) = cache.get(app_type) {
                if cached.version == cache_version {
                    // 缓存命中且版本一致
                    return Ok(Some(cached.provider.clone()));
                }
                // 版本不匹配，需要刷新
            }
        }

        // 缓存未命中或版本过期，从 DB 加载
        match Self::load_current_provider(&self.db, app_type).await? {
            Some(provider) => {
                let mut cache = self.current_cache.write().await;
                cache.insert(
                    app_type.to_string(),
                    CachedProvider {
                        provider: provider.clone(),
                        version: cache_version,
                    },
                );
                Ok(Some(provider))
            }
            None => {
                let mut cache = self.current_cache.write().await;
                cache.remove(app_type);
                Ok(None)
            }
        }
    }

    pub async fn select_providers(&self, app_type: &str) -> Result<Vec<Provider>, ProxyError> {
        let mut result = Vec::new();
        let mut total_providers = 0usize;
        let mut circuit_open_count = 0usize;

        let auto_failover_enabled = self
            .db
            .get_proxy_config_for_app(app_type)
            .await
            .map(|config| config.auto_failover_enabled)
            .unwrap_or(false);

        if auto_failover_enabled {
            let all_providers = self
                .db
                .get_all_providers(app_type)
                .map_err(|error| ProxyError::DatabaseError(error.to_string()))?;
            let ordered_ids = self
                .db
                .get_failover_queue(app_type)
                .map_err(|error| ProxyError::DatabaseError(error.to_string()))?
                .into_iter()
                .map(|item| item.provider_id)
                .collect::<Vec<_>>();

            total_providers = ordered_ids.len();

            for provider_id in ordered_ids {
                let Some(provider) = all_providers.get(&provider_id).cloned() else {
                    continue;
                };

                let breaker = self
                    .get_or_create_circuit_breaker(&format!("{app_type}:{}", provider.id))
                    .await;

                if breaker.is_available().await {
                    result.push(provider);
                } else {
                    circuit_open_count += 1;
                }
            }
        } else {
            if let Some(current) = self.get_cached_current_provider(app_type).await? {
                total_providers = 1;
                result.push(current);
            }
        }

        if result.is_empty() {
            return if total_providers > 0 && circuit_open_count == total_providers {
                Err(ProxyError::AllProvidersCircuitOpen)
            } else {
                Err(ProxyError::NoProvidersConfigured)
            };
        }

        Ok(result)
    }

    pub async fn allow_provider_request(&self, provider_id: &str, app_type: &str) -> AllowResult {
        let breaker = self
            .get_or_create_circuit_breaker(&format!("{app_type}:{provider_id}"))
            .await;
        breaker.allow_request().await
    }

    pub async fn record_result(
        &self,
        provider_id: &str,
        app_type: &str,
        used_half_open_permit: bool,
        success: bool,
        error_msg: Option<String>,
    ) -> Result<(), ProxyError> {
        let failure_threshold = self
            .db
            .get_proxy_config_for_app(app_type)
            .await
            .map(|config| config.circuit_failure_threshold)
            .unwrap_or(5);

        let breaker = self
            .get_or_create_circuit_breaker(&format!("{app_type}:{provider_id}"))
            .await;

        if success {
            breaker.record_success(used_half_open_permit).await;
        } else {
            breaker.record_failure(used_half_open_permit).await;
        }

        self.db
            .update_provider_health_with_threshold(
                provider_id,
                app_type,
                success,
                error_msg,
                failure_threshold,
            )
            .await
            .map_err(|error| ProxyError::DatabaseError(error.to_string()))
    }

    pub async fn reset_circuit_breaker(&self, circuit_key: &str) {
        let breakers = self.circuit_breakers.read().await;
        if let Some(breaker) = breakers.get(circuit_key) {
            breaker.reset().await;
        }
    }

    pub async fn reset_provider_breaker(&self, provider_id: &str, app_type: &str) {
        self.reset_circuit_breaker(&format!("{app_type}:{provider_id}"))
            .await;
    }

    pub async fn release_permit_neutral(
        &self,
        provider_id: &str,
        app_type: &str,
        used_half_open_permit: bool,
    ) {
        if !used_half_open_permit {
            return;
        }

        let breaker = self
            .get_or_create_circuit_breaker(&format!("{app_type}:{provider_id}"))
            .await;
        breaker.release_half_open_permit();
    }

    pub async fn update_all_configs(&self, config: CircuitBreakerConfig) {
        let breakers = self.circuit_breakers.read().await;
        for breaker in breakers.values() {
            breaker.update_config(config.clone()).await;
        }
    }

    #[allow(dead_code)]
    pub async fn get_circuit_breaker_stats(
        &self,
        provider_id: &str,
        app_type: &str,
    ) -> Option<CircuitBreakerStats> {
        let circuit_key = format!("{app_type}:{provider_id}");
        let breakers = self.circuit_breakers.read().await;
        if let Some(breaker) = breakers.get(&circuit_key) {
            Some(breaker.get_stats().await)
        } else {
            None
        }
    }

    pub(super) fn upstream_endpoint(
        &self,
        app_type: &AppType,
        provider: &Provider,
        endpoint: &str,
    ) -> String {
        upstream_endpoint::rewrite_upstream_endpoint(app_type, provider, endpoint)
    }

    async fn get_or_create_circuit_breaker(&self, key: &str) -> Arc<CircuitBreaker> {
        {
            let breakers = self.circuit_breakers.read().await;
            if let Some(breaker) = breakers.get(key) {
                return breaker.clone();
            }
        }

        let mut breakers = self.circuit_breakers.write().await;
        if let Some(breaker) = breakers.get(key) {
            return breaker.clone();
        }

        let app_type = key.split(':').next().unwrap_or("claude");
        let config = self
            .db
            .get_proxy_config_for_app(app_type)
            .await
            .map(|app_config| CircuitBreakerConfig {
                failure_threshold: app_config.circuit_failure_threshold,
                success_threshold: app_config.circuit_success_threshold,
                timeout_seconds: app_config.circuit_timeout_seconds as u64,
                error_rate_threshold: app_config.circuit_error_rate_threshold,
                min_requests: app_config.circuit_min_requests,
            })
            .unwrap_or_default();

        let breaker = Arc::new(CircuitBreaker::new(config));
        breakers.insert(key.to_string(), breaker.clone());
        breaker
    }
}

#[cfg(test)]
mod tests;
